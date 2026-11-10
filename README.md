# Everything Wrong with BigQuery

> nuances, missing features, strange API, things to watchout, and wishlist

## Read Binary Vectors

Say you want to store 3072 dim float vector in BigQuery column, and then read first 768 floats of it pushed down to BigQuery in binary driver. — **This is impossible.**

- BigQuery does not support binary reader over prefix of array column.
- BigQuery cannot have generated column on array float columns.

Best you can do is pushing down query and reading text back, no binary driver allowed here, transport of your floats are in strings here.

```sql
SELECT
  id
  , ARRAY(
    SELECT e FROM UNNEST(embedding) AS e
    WITH OFFSET o WHERE o < @dim ORDER BY o
  ) AS embedding
FROM my-embedding-table
```
