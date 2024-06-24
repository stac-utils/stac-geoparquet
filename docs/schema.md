# Schema considerations

A STAC Item is a JSON object to describe an external geospatial dataset. The STAC specification defines a common core, plus a variety of extensions. Additionally, STAC Items may include custom extensions outside the common ones. Crucially, the majority of the specified fields in the core spec and extensions define optional keys. Those keys often differ across STAC collections and may even differ within a single collection across items.

STAC's flexibility is a blessing and a curse. The flexibility of schemaless JSON allows for very easy writing as each object can be dumped separately to JSON. Every item is allowed to have a different schema. And newer items are free to have a different schema than older items in the same collection. But this write-time flexibility makes it harder to read as there are no guarantees (outside STAC's few required fields) about what fields exist.

Parquet is the complete opposite of JSON. Parquet has a strict schema that must be known before writing can start. This puts the burden of work onto the writer instead of the reader. Reading Parquet is very efficient because the file's metadata defines the exact schema of every record. This also enables use cases like reading specific columns that would not be possible without a strict schema.

This conversion from schemaless to strict-schema is the difficult part of converting STAC from JSON to GeoParquet, especially for large input datasets like STAC that are often larger than memory.

## Full scan over input data

The most foolproof way to convert STAC JSON to GeoParquet is to perform a full scan over input data. This is done automatically by [`parse_stac_ndjson_to_arrow`][stac_geoparquet.arrow.parse_stac_ndjson_to_arrow] when a schema is not provided.

This is time consuming as it requires two full passes over the input data: once to infer a common schema and again to actually write to Parquet (though items are never fully held in memory, allowing this process to scale).

## User-provided schema

Alternatively, the user can pass in an Arrow schema themselves using the `schema` parameter of [`parse_stac_ndjson_to_arrow`][stac_geoparquet.arrow.parse_stac_ndjson_to_arrow]. This `schema` must match the on-disk schema of the the STAC JSON data.

## Multiple schemas per collection

It is also possible to write multiple Parquet files with STAC data where each Parquet file may have a different schema. This simplifies the conversion and writing process but makes reading and using the Parquet data harder.

### Merging data with schema mismatch

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
