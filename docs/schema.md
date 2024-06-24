# Schema considerations

A _schema_ is a set of named fields that describe what data types exist for each column in a tabular dataset.

JSON and Arrow/Parquet have opposite expectations around schemas. JSON is fully schemaless; every object in a JSON array may have a completely different schema. Meanwhile Arrow and Parquet require a strict schema upfront, and it is impossible for a row in an Arrow table or Parquet file to not comply with the upfront schema. Trying to write STAC data with mixed schemas to Parquet can easily produce exceptions.

Because of these opposite expectations, schema inference is the most difficult part of converting STAC from JSON to GeoParquet.

## Full scan over input data

The most foolproof way to convert STAC JSON to GeoParquet is to perform a full scan over input data. This is done automatically by [`parse_stac_ndjson_to_arrow`][stac_geoparquet.arrow.parse_stac_ndjson_to_arrow] when a schema is not provided.

This is time consuming as it requires two full passes over the input data: once to infer a common schema and again to actually write to Parquet (though items are never fully held in memory, allowing this process to scale).

## User-provided schema

Alternatively, the user can pass in an Arrow schema themselves using the `schema` parameter of [`parse_stac_ndjson_to_arrow`][stac_geoparquet.arrow.parse_stac_ndjson_to_arrow]. This `schema` must match the on-disk schema of the the STAC JSON data.

## Merging data with schema mismatch

If you've created STAC GeoParquet data where the schema has updated, you can use [`pyarrow.concat_tables`][pyarrow.concat_tables] with `promote_options="permissive"` to combine multiple STAC GeoParquet files.

```py
import pyarrow as pa
import pyarrow.parquet as pq

table_1 = pq.read_table("stac1.parquet")
table_2 = pq.read_table("stac2.parquet")
combined_table = pa.concat_tables([table1, table2], promote_options="permissive")
```

## Future work

Schema operations is an area where future work can improve reliability and ease of use of STAC GeoParquet.

It's possible that in the future we could automatically infer an Arrow schema from the STAC specification's published JSON Schema files. If you're interested in this, open an issue and discuss.
