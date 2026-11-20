# Release Notes

This is a list of changes to `stac-geoparquet`.

## [0.9.0](https://github.com/stac-utils/stac-geoparquet/compare/0.8.3...0.9.0) (2026-11-20)


### ⚠ BREAKING CHANGES

* a row_func in pgstac_to_arrow, pgstac_to_parquet, sync_pgstac_to_parquet and pgstac_schema gets WKB geometries by default; pass geometry="geojson" for GeoJSON.

### Features

* add PreScan and pgstac_schema ([#194](https://github.com/stac-utils/stac-geoparquet/issues/194)) ([798bbf8](https://github.com/stac-utils/stac-geoparquet/commit/798bbf8111e67f71eb083ed0b5fe142fc025eefd))
* end chunks early when memory is short ([#191](https://github.com/stac-utils/stac-geoparquet/issues/191)) ([7ee2b4a](https://github.com/stac-utils/stac-geoparquet/commit/7ee2b4a32dc3bc86b81735a7f282b984f2757a97))
* make InferredSchema public ([#193](https://github.com/stac-utils/stac-geoparquet/issues/193)) ([cd131b1](https://github.com/stac-utils/stac-geoparquet/commit/cd131b16c69fe3bdcee8897d4af8bbb6fcd20a87))
* work through connection poolers ([#195](https://github.com/stac-utils/stac-geoparquet/issues/195)) ([853e5bd](https://github.com/stac-utils/stac-geoparquet/commit/853e5bd76f84680aa563629aa4eeba5bd55900fd))


### Bug Fixes

* declare readers with the converted schema ([#186](https://github.com/stac-utils/stac-geoparquet/issues/186)) ([990a818](https://github.com/stac-utils/stac-geoparquet/commit/990a818814b931670cc1de660f8daed7476a9f84))
* make search= work, hydrate with pgstac:base_item ([#188](https://github.com/stac-utils/stac-geoparquet/issues/188)) ([46a8261](https://github.com/stac-utils/stac-geoparquet/commit/46a82614a81a0087a48edcb936514c428e7faaa6))
* pass WKB through to Arrow ([#189](https://github.com/stac-utils/stac-geoparquet/issues/189)) ([318a4d8](https://github.com/stac-utils/stac-geoparquet/commit/318a4d88a896f97c8886996bd37f23243f61741c))
* reject path-like collection ids and non-integer timeouts ([#185](https://github.com/stac-utils/stac-geoparquet/issues/185)) ([a71341f](https://github.com/stac-utils/stac-geoparquet/commit/a71341f211190e9cf7f28ba27bae400896c26366))
* stream rows instead of holding whole fetches ([#190](https://github.com/stac-utils/stac-geoparquet/issues/190)) ([b6dc0ce](https://github.com/stac-utils/stac-geoparquet/commit/b6dc0ce987d527d207e36cebe78a021dfea9b5a0))
* write ChunksToDisk chunks as Arrow IPC ([#192](https://github.com/stac-utils/stac-geoparquet/issues/192)) ([e2ae0b9](https://github.com/stac-utils/stac-geoparquet/commit/e2ae0b9cc9bee8d6596b34eef95b3de6e7bae286))

## [0.8.3](https://github.com/stac-utils/stac-geoparquet/compare/0.8.2...0.8.3) (2026-10-21)


### Bug Fixes

* fails when every item has empty assets. ([#177](https://github.com/stac-utils/stac-geoparquet/issues/177)) ([0f09e0d](https://github.com/stac-utils/stac-geoparquet/commit/0f09e0d72190b3b911a68bc13ac6cb2dc40de1e3))
* keep each item's stac_version when reading from pgstac ([#172](https://github.com/stac-utils/stac-geoparquet/issues/172)) ([8bff18c](https://github.com/stac-utils/stac-geoparquet/commit/8bff18cd2c0562da792280cff62da6adbc8a2a24))


### Documentation

* remove duplicate changelog line ([#162](https://github.com/stac-utils/stac-geoparquet/issues/162)) ([a3825a3](https://github.com/stac-utils/stac-geoparquet/commit/a3825a3426b43eb420b9c435ba3b679bb3340eec))

## [0.8.2](https://github.com/stac-utils/stac-geoparquet/compare/0.8.1...0.8.2) (2026-08-12)


### Bug Fixes

* don't configure global logging at module level ([#152](https://github.com/stac-utils/stac-geoparquet/issues/152)) ([72cd3a6](https://github.com/stac-utils/stac-geoparquet/commit/72cd3a6f1b8b8fc0d33963c5df6a3716bff2cb32))
* null datetime columns written TZ-aware ([#157](https://github.com/stac-utils/stac-geoparquet/issues/157)) ([7cd5800](https://github.com/stac-utils/stac-geoparquet/commit/7cd58003dfc42a93dd548f0c52754639160fce69))

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
