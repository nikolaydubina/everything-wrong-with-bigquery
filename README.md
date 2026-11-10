# Everything Wrong with BigQuery

> nuances, missing features, strange API, things to watchout, and wishlist

## Read Binary Vectors

Say you want to store 3072 dim float vector in BigQuery column, and then read first 768 floats of it pushed down to BigQuery in binary driver. — **This is impossible.**

- BigQuery does not support binary reader over prefix of array column.
- BigQuery cannot have generated column on array float columns.

Best you can do is pushing down query and reading text back, no binary driver allowed here, transport of your floats is in strings here.
Compare, 20 Bytes string + decoding CPU cost vs 8 Bytes binary and zero CPU cost.
This is 2.5x wasted bandwidth and extra burned CPU.
On 1TB table you are paying 2.5TB transfer for aboslutely no good reason.

```sql
SELECT
  id
  , ARRAY(
    SELECT e FROM UNNEST(embedding) AS e
    WITH OFFSET o WHERE o < @dim ORDER BY o
  ) AS embedding
FROM my-embedding-table
```

Benchmarks

```bash
$ go test -bench=.
BenchmarkQueryJSON
BenchmarkQueryJSON-16        21721577667 ns/op  21.72  first-row-s  0.05629 payload-MB/s  9.207 rows/s
BenchmarkReadSessionArrow
BenchmarkReadSessionArrow-16  3468335750 ns/op   3.468 first-row-s  0.3543  payload-MB/s  57.67 rows/s  24589 wire-B/row
```

2,000-row table of 3072 float64 (49 MB), each reader stops after 200 rows,
so the query row above is almost entirely the `QUALIFY` prologue and the binary row includes one session setup.
Same comparison on a 2.8M-row table: 26.99 s / 10.09 rows/s against 3.62 s (**7.5x**) / 348.8 rows/s (**34.6x**) .
