# Release Notes

This is a list of changes to `stac-geoparquet`.

## Unreleased

- Don't configure logging level at module level (<https://github.com/stac-utils/stac-geoparquet/pull/152>)

## 0.8.1

- Drop Python 3.9 (<https://github.com/stac-utils/stac-geoparquet/pull/133>)
- Fixed resolution of scheme-prefixed `output_path`s (e.g., `s3://bucket/item.parquet`) (<https://github.com/stac-utils/stac-geoparquet/pull/143>)
- Fixed duplication of STAC Item properties with top-level keys (e.g., "collection") (<https://github.com/stac-utils/stac-geoparquet/pull/144>)
- Allow users to provide their own PgSTAC connection factory (<https://github.com/stac-utils/stac-geoparquet/pull/145>)

## 0.8.0

- Make `deltalake` an optional dependency (<https://github.com/stac-utils/stac-geoparquet/pull/106>)
- Updated `pgstac_reader` and removed Planetary Computer specific code (`cli.py` and `pc_runner.py`); added an example notebook for dumping pgstac partitions (<https://github.com/stac-utils/stac-geoparquet/pull/101>)
- Spec: use `collections` (plural) for storing collection metadata (<https://github.com/stac-utils/stac-geoparquet/pull/108>)
- Spill chunks to disk when writing parquet to merge schemas, and add chunking options to the pgstac readers (<https://github.com/stac-utils/stac-geoparquet/pull/109>)
- Removed the in-repo specification documents in favor of the upstream spec (<https://github.com/stac-utils/stac-geoparquet/pull/120>)
- Fixed `stac_table_to_ndjson` to always insert a `type` field if one isn't already present (<https://github.com/stac-utils/stac-geoparquet/pull/105>)
- Fixed a case where conversion to JSON created assets values as `None` (<https://github.com/stac-utils/stac-geoparquet/pull/111>)
- Fixed `pgstac_to_arrow` to pass through `tmpdir` (<https://github.com/stac-utils/stac-geoparquet/pull/118>)
- Fixed `pyarrow.dataset` import in `arrow._api` (<https://github.com/stac-utils/stac-geoparquet/pull/119>)
- Hydrate links in `PgstacRowFactory` (<https://github.com/stac-utils/stac-geoparquet/pull/121>)
- Convert `Path` to `str` for remote filesystem compatibility (<https://github.com/stac-utils/stac-geoparquet/pull/123>)

## 0.7.0

- Updated stac-geoparquet Parquet File metadata fields (<https://github.com/stac-utils/stac-geoparquet/pull/98>)
  - All fields are placed under the `stac-geoparquet` key in the parquet file metadata.
  - Added a `version` field.
  - Added json-schema for the metadata parquet file metadata.
- Fixed `ValueError` when converting some strings to dates (<https://github.com/stac-utils/stac-geoparquet/issues/79>)
- Removed upper bound on pyarrow dependency (<https://github.com/stac-utils/stac-geoparquet/pull/102>)
