# Everything Wrong with BigQuery

> nuances, missing features, strange API, things to watchout, and wishlist

## Read Binary Vectors

Say you want to store 3072 dim float vector in BigQuery column, and then read first 768 floats of it pushed down to BigQuery in binary driver. — **This is impossible.**

- BigQuery does not support binary reader over prefix of array column.
- BigQuery cannot have generated column on array float columns.
