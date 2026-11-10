# Everything Wrong with BigQuery

> nuances, missing features, strange API, things to watchout, and wishlist

## Read Binary Vectors

Say you want to store 3072 dim float vector in BigQuery column, and then read in binary prefix of it pushed down to the BigQuery to take that prefix.  
Now, that is impossible.

- BigQuery does not support binary reader over prefix of array column. this means that matryshka-style embeddings stored in BigQuery (e.g. `gemini-embedding-2) cannot take prefix effectively in binary reader form.
- BigQuery cannot have generated column on array float columns.
