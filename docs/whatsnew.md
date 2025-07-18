# Release Notes

This is a list of changes to `stac-geoparquet`.

## 0.8.0 (Unreleased)

- Make `deltalake` an optional dependency (<https://github.com/stac-utils/stac-geoparquet/pull/106>)
- Fixed `stac_table_to_ndjson` to always insert a `type` field if one isn't already present (<https://github.com/stac-utils/stac-geoparquet/pull/105>)
- Fixed a case where conversion to JSON created assets values as `None` (<https://github.com/stac-utils/stac-geoparquet/pull/111>)

## 0.7.0

- Updated stac-geoparquet Parquet File metadata fields (<https://github.com/stac-utils/stac-geoparquet/pull/98>)
  - All fields are placed under the `stac-geoparquet` key in the parquet file metadata.
  - Added a `version` field.
  - Added json-schema for the metadata parquet file metadata.
- Fixed `ValueError` when converting some strings to dates (<https://github.com/stac-utils/stac-geoparquet/issues/79>)
- Removed upper bound on pyarrow dependency (<https://github.com/stac-utils/stac-geoparquet/pull/102>)
