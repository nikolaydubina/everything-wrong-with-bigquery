package bqread_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/bigquery/storage/apiv1"

	bqread "github.com/ndx-technologies/everything-wrong-with-bigquery"
)

var (
	config bqread.Config
	client *bigquery.Client
	reader *storage.BigQueryReadClient
)

func TestMain(m *testing.M) {
	project := os.Getenv("PROJECT_ID")
	if project == "" {
		fmt.Println("PROJECT_ID is not set, nothing to do")
		os.Exit(0)
	}

	config = bqread.Config{
		Project: project,
		Dataset: "everything_wrong_with_bigquery",
		Rows:    envInt("BENCH_ROWS", 2000),
		Dim:     envInt("BENCH_DIM", 3072),
		Prefix:  envInt("BENCH_PREFIX", 768),
		Limit:   envInt("BENCH_LIMIT", 200),
	}

	ctx := context.Background()

	var err error
	if client, err = bigquery.NewClient(ctx, project); err != nil {
		fmt.Println("cannot create the BigQuery client:", err)
		os.Exit(1)
	}
	defer client.Close()

	if reader, err = storage.NewBigQueryReadClient(ctx); err != nil {
		fmt.Println("cannot create the Storage Read client:", err)
		os.Exit(1)
	}
	defer reader.Close()

	fmt.Printf("%s: %d rows of %d float64 (%.1f MB of vector), reading %d of them\n\n",
		config.Dataset, config.Rows, config.Dim, float64(config.Rows*config.Dim*8)/1e6, config.Prefix)

	if err := bqread.Setup(ctx, client, config); err != nil {
		fmt.Println("cannot create the table:", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := bqread.Drop(ctx, client, config); err != nil {
		fmt.Println("cannot drop the dataset:", err)
	}

	os.Exit(code)
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

func BenchmarkQueryJSON(b *testing.B) {
	if client == nil {
		b.Skip("PROJECT_ID is not set")
	}

	ctx := context.Background()

	for b.Loop() {
		m, err := bqread.ReadQuery(ctx, client, config)
		if err != nil {
			b.Fatal(err)
		}
		report(b, m)
	}
}

func BenchmarkReadSessionArrow(b *testing.B) {
	if reader == nil {
		b.Skip("PROJECT_ID is not set")
	}

	ctx := context.Background()

	for b.Loop() {
		m, err := bqread.ReadSession(ctx, reader, config)
		if err != nil {
			b.Fatal(err)
		}
		report(b, m)
	}
}

func report(b *testing.B, m bqread.Measurement) {
	if m.Rows == 0 {
		b.Fatalf("%s: read no rows in %s", m.Reader, m.Took)
	}

	b.ReportMetric(float64(m.Rows)/m.Took.Seconds(), "rows/s")
	b.ReportMetric(float64(m.Values*8)/1e6/m.Took.Seconds(), "payload-MB/s")
	b.ReportMetric(m.FirstRow.Seconds(), "first-row-s")

	// only the storage read API reports what crossed the wire; a query's HTTP responses are not counted,
	// and the divisor is the rows those bytes carried, not the rows this reader stopped at
	if m.Wire > 0 && m.WireRows > 0 {
		b.ReportMetric(float64(m.Wire)/float64(m.WireRows), "wire-B/row")
	}

	if config.Limit > 0 && m.Rows < config.Limit {
		b.Logf("read %d of %d rows in %s, then the read limit hit", m.Rows, config.Limit, m.Took)
	}
}
