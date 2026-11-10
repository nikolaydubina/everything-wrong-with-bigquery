// Package bqread measures the two ways of getting a large float vector out of BigQuery.
//
// Both readers answer the same question — "give me every row of a 3072-wide vector, cut to the first
// 768" — and the point is that one of them cannot be asked for the cut:
//
//   - a QUERY can cut the array (`ARRAY(SELECT … WHERE o < 768)`) but its results come back as JSON text,
//     one value at a time, and the window function it needs to keep one row per id means no row is emitted
//     until the shuffle finishes;
//   - the STORAGE READ API returns Arrow binary and streams immediately, but it reads a table, not a
//     query: `SelectedFields` selects whole columns, so the 3072-wide column is what arrives and the cut
//     has to happen in the client.
//
// The benchmarks in readers_test.go run both against a table they create and drop.
package bqread

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"google.golang.org/api/iterator"
)

// Config is what the table and the benchmarks are built from.
type Config struct {
	Project string
	Dataset string
	Rows    int // rows written to the table
	Dim     int // stored vector width
	Prefix  int // how much of it the reader needs
	Limit   int // rows a reader stops at; 0 reads everything
}

func (c Config) qualified() string { return c.Project + "." + c.Dataset }

func (c Config) table() string { return c.qualified() + ".embeddings" }

func (c Config) resource() string {
	return "projects/" + c.Project + "/datasets/" + c.Dataset + "/tables/embeddings"
}

// Measurement is one read: how long until the first row, how long for all of them, and how many bytes
// crossed the wire where that is knowable.
type Measurement struct {
	Reader   string
	Streams  int
	Rows     int
	FirstRow time.Duration
	Took     time.Duration
	Wire     int64 // bytes received by the client; 0 when the transport does not report it
	WireRows int64 // rows those bytes carried, which is more than Rows when the reader stops mid-batch
	Values   int64 // vector elements materialised
}

// Setup creates the dataset and fills it with random vectors of the same magnitude a real embedding has,
// so the JSON text lengths are realistic.
func Setup(ctx context.Context, bq *bigquery.Client, cfg Config) error {
	if _, err := run(ctx, bq, "CREATE SCHEMA IF NOT EXISTS `"+cfg.qualified()+"` OPTIONS(location = \"us\")", nil); err != nil {
		return err
	}

	create := "CREATE OR REPLACE TABLE `" + cfg.table() + "` CLUSTER BY id AS SELECT id" +
		", 1 AS version" +
		", ARRAY(SELECT RAND() * 0.06 - 0.03 FROM UNNEST(GENERATE_ARRAY(1, @dim))) AS embedding" +
		" FROM UNNEST(GENERATE_ARRAY(1, @rows)) AS id"

	_, err := run(ctx, bq, create, []bigquery.QueryParameter{
		{Name: "rows", Value: int64(cfg.Rows)},
		{Name: "dim", Value: int64(cfg.Dim)},
	})
	return err
}

// Drop removes the dataset and everything in it.
func Drop(ctx context.Context, bq *bigquery.Client, cfg Config) error {
	_, err := run(ctx, bq, "DROP SCHEMA IF EXISTS `"+cfg.qualified()+"` CASCADE", nil)
	return err
}

func run(ctx context.Context, bq *bigquery.Client, sql string, params []bigquery.QueryParameter) (int64, error) {
	q := bq.Query(sql)
	q.Parameters = params

	job, err := q.Run(ctx)
	if err != nil {
		return 0, err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return 0, err
	}
	if status.Err() != nil {
		return 0, status.Err()
	}

	return status.Statistics.Details.(*bigquery.QueryStatistics).TotalBytesProcessed, nil
}

// ReadQuery is the reader that can be asked for the cut: a query that slices the array and keeps one row
// per id, whose results arrive as JSON and are decoded one value at a time.
func ReadQuery(ctx context.Context, bq *bigquery.Client, cfg Config) (Measurement, error) {
	m := Measurement{Reader: "Query + JSON, array cut in SQL"}

	q := bq.Query(`
		SELECT
			id
			, version
			, ARRAY(
				SELECT e FROM UNNEST(embedding) AS e
				WITH OFFSET o WHERE o < @dim ORDER BY o
			) AS embedding
		FROM ` + "`" + cfg.table() + "`" + `
		QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) = 1
	`)
	q.Parameters = []bigquery.QueryParameter{{Name: "dim", Value: int64(cfg.Prefix)}}

	start := time.Now()

	it, err := q.Read(ctx)
	if err != nil {
		return m, err
	}

	var sum float64
	for {
		var row []bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return m, err
		}

		if m.Rows == 0 {
			m.FirstRow = time.Since(start)
		}
		m.Rows++

		if cfg.Limit > 0 && m.Rows >= cfg.Limit {
			break
		}

		// the decode a real reader does: every element is an interface holding a float64
		if len(row) > 2 {
			values, ok := row[2].([]bigquery.Value)
			if !ok {
				return m, fmt.Errorf("row %d: embedding is %T", m.Rows, row[2])
			}
			for _, v := range values {
				f, ok := v.(float64)
				if !ok {
					return m, fmt.Errorf("row %d: value is %T", m.Rows, v)
				}
				sum += f
			}
			m.Values += int64(len(values))
		}
	}

	m.Took = time.Since(start)
	_ = sum

	return m, nil
}

// ReadSession is the reader that cannot be asked for the cut: it names the columns to read, the whole
// 3072-wide column arrives as Arrow, and the prefix is copied here.
func ReadSession(ctx context.Context, read *storage.BigQueryReadClient, cfg Config) (Measurement, error) {
	m := Measurement{Reader: "Storage Read API + ARROW"}

	start := time.Now()

	session, err := read.CreateReadSession(ctx, &storagepb.CreateReadSessionRequest{
		Parent: "projects/" + cfg.Project,
		ReadSession: &storagepb.ReadSession{
			Table:      cfg.resource(),
			DataFormat: storagepb.DataFormat_ARROW,
			ReadOptions: &storagepb.ReadSession_TableReadOptions{
				SelectedFields: []string{"id", "embedding"},
			},
		},
	})
	if err != nil {
		return m, err
	}
	m.Streams = len(session.GetStreams())

	serializedSchema := session.GetArrowSchema().GetSerializedSchema()

	schemaReader, err := ipc.NewReader(bytes.NewReader(serializedSchema))
	if err != nil {
		return m, err
	}
	schema := schemaReader.Schema()
	schemaReader.Release()

	var embeddingAt int
	for i, field := range schema.Fields() {
		if field.Name == "embedding" {
			embeddingAt = i
		}
	}

	var sum float64
streams:
	for _, stream := range session.GetStreams() {
		responses, err := read.ReadRows(ctx, &storagepb.ReadRowsRequest{ReadStream: stream.GetName()})
		if err != nil {
			return m, err
		}

		for {
			response, err := responses.Recv()
			if err != nil {
				break
			}

			batch := response.GetArrowRecordBatch()
			if batch == nil {
				continue
			}

			serialized := batch.GetSerializedRecordBatch()
			m.Wire += int64(len(serialized))

			// this arrow version reads a stream that begins with a schema message, and a record batch
			// arrives without one
			body := make([]byte, 0, len(serializedSchema)+len(serialized))
			body = append(body, serializedSchema...)
			body = append(body, serialized...)

			records, err := ipc.NewReader(bytes.NewReader(body))
			if err != nil {
				return m, err
			}

			for records.Next() {
				record := records.Record()
				m.WireRows += record.NumRows()

				vectors := record.Column(embeddingAt).(*array.List)
				values := vectors.ListValues().(*array.Float64)

				for i := 0; i < int(record.NumRows()); i++ {
					from, to := vectors.ValueOffsets(i)
					if int(to-from) < cfg.Prefix {
						records.Release()
						return m, fmt.Errorf("row %d: stored %d values, want at least %d", m.Rows, to-from, cfg.Prefix)
					}

					if m.Rows == 0 {
						m.FirstRow = time.Since(start)
					}
					m.Rows++

					// the cut a read session cannot do
					for _, f := range values.Float64Values()[from : from+int64(cfg.Prefix)] {
						sum += f
					}
					m.Values += int64(cfg.Prefix)

					if cfg.Limit > 0 && m.Rows >= cfg.Limit {
						records.Release()
						break streams
					}
				}
			}
			records.Release()
		}
	}

	m.Took = time.Since(start)
	_ = sum

	return m, nil
}
