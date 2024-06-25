# Usage

[Apache Arrow](https://arrow.apache.org/) is used as the in-memory interchange format between all formats. While some end-to-end helper functions are provided, the user can go through Arrow objects for maximal flexibility in the conversion process.

All functionality that goes through Arrow is currently exported via the `stac_geoparquet.arrow` namespace.

## `dict`/JSON - Arrow conversion

### Convert `dict`s to Arrow

Use [`parse_stac_items_to_arrow`][stac_geoparquet.arrow.parse_stac_items_to_arrow] to convert STAC items either in memory or on disk to a stream of Arrow record batches. This accepts either an iterable of Python `dict`s or an iterable of [`pystac.Item`][pystac.Item] objects.

For example:

```py
import pyarrow as pa
import pystac

import stac_geoparquet

item = pystac.read_file(
    "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a/items/S2A_MSIL2A_20230112T104411_R008_T29NPE_20230113T053333"
)
assert isinstance(item, pystac.Item)

record_batch_reader = stac_geoparquet.arrow.parse_stac_items_to_arrow([item])
table = record_batch_reader.read_all()
```

### Convert JSON to Arrow

[`parse_stac_ndjson_to_arrow`][stac_geoparquet.arrow.parse_stac_ndjson_to_arrow] is a helper function to take one or more JSON or newline-delimited JSON files on disk, infer the schema from all of them, and convert the data to a stream of Arrow record batches.

### Convert Arrow to `dict`s

Use [`stac_table_to_items`][stac_geoparquet.arrow.stac_table_to_items] to convert a table or stream of Arrow record batches of STAC data to a generator of Python `dict`s. This accepts either a `pyarrow.Table` or a `pyarrow.RecordBatchReader`, which allows conversions of larger-than-memory files in a streaming manner.

### Convert Arrow to JSON

Use [`stac_table_to_ndjson`][stac_geoparquet.arrow.stac_table_to_ndjson] to convert a table or stream of Arrow record batches of STAC data to a newline-delimited JSON file. This accepts either a `pyarrow.Table` or a `pyarrow.RecordBatchReader`, which allows conversions of larger-than-memory files in a streaming manner.

## Parquet

Use [`to_parquet`][stac_geoparquet.arrow.to_parquet] to write STAC Arrow data from memory to a path or file-like object. This is a special function to ensure that [GeoParquet](https://geoparquet.org/) 1.0 or 1.1 metadata is written to the Parquet file.

[`parse_stac_ndjson_to_parquet`][stac_geoparquet.arrow.parse_stac_ndjson_to_parquet] is a helper that connects reading (newline-delimited) JSON on disk to writing out to a Parquet file.

No special API is required for reading a STAC GeoParquet file back into Arrow. You can use [`pyarrow.parquet.read_table`][pyarrow.parquet.read_table] or [`pyarrow.parquet.ParquetFile`][pyarrow.parquet.ParquetFile] directly to read the STAC GeoParquet data back into Arrow.

## Delta Lake


Use [`parse_stac_ndjson_to_delta_lake`][stac_geoparquet.arrow.parse_stac_ndjson_to_delta_lake] to read (newline-delimited) JSON on disk and write out to a Delta Lake table.

No special API is required for reading a STAC Delta Lake table back into Arrow. You can use the [`DeltaTable`][deltalake.DeltaTable] class directly to read the data back into Arrow.

!!! important
    Arrow has a null data type, where every value in the column is always null, but Delta Lake does not. This means that for any column inferred to have a `null` data type, writing to Delta Lake will error with
    ```
    _internal.SchemaMismatchError: Invalid data type for Delta Lake: Null
    ```

    This is a problem because if all items in a STAC Collection have a `null` JSON key, it gets inferred as an Arrow `null` type. For example, in the `3dep-lidar-copc` collection in the tests, it has `start_datetime` and `end_datetime` fields, and so according to the spec, `datetime` is always `null`. This column would need to be casted to a timestamp type before being written to Delta Lake.

    This means we cannot write this collection to Delta Lake **solely with automatic schema inference**.

    In such cases, users may need to manually update the inferred schema to cast any `null` type to another Delta Lake-compatible type.
