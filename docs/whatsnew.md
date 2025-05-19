# Release Notes

This is a list of changes to `stac-geoparquet`.

## 0.8.0

- Make `deltalake` an optional dependency (https://github.com/stac-utils/stac-geoparquet/pull/106)

## 0.7.0

- Updated stac-geoparquet Parquet File metadata fields (https://github.com/stac-utils/stac-geoparquet/pull/98)
    - All fields are placed under the `stac-geoparquet` key in the parquet file metadata.
    - Added a `version` field.
    - Added json-schema for the metadata parquet file metadata.
- Fixed `ValueError` when converting some strings to dates (https://github.com/stac-utils/stac-geoparquet/issues/79)
- Removed upper bound on pyarrow dependency (https://github.com/stac-utils/stac-geoparquet/pull/102)
