# STAC-geoparquet

Convert [STAC](https://stacspec.org/en) items between JSON, [GeoParquet](https://geoparquet.org/), [pgstac](https://github.com/stac-utils/pgstac), and [Delta Lake](https://delta.io/).

## Purpose

The STAC spec defines a JSON-based schema. But it can be hard to manage and search through many millions of STAC items in JSON format. For one, JSON is very large on disk. And you need to parse the entire JSON data into memory to extract just a small piece of information, say the `datetime` and one `asset` of an Item.

GeoParquet can be a good complement to JSON for many bulk-access and analytic use cases. While STAC Items are commonly distributed as individual JSON files on object storage or through a [STAC API](https://github.com/radiantearth/stac-api-spec), STAC GeoParquet allows users to access a large number of STAC items in bulk without making repeated HTTP requests.

For analytic questions like "find the items in the Sentinel-2 collection in June 2024 over New York City with cloud cover of less than 20%" it can be much, much faster to find the relevant data from a GeoParquet source than from JSON, because GeoParquet needs to load only the relevant columns for that query, not the full data.

See the [STAC-GeoParquet specification](./spec/stac-geoparquet-spec.md) for details on the exact schema of the written Parquet files.


<!--
## pgstac integration

`stac_geoparquet.pgstac_reader` has some helpers for working with items coming from a `pgstac.items` table. It takes care of

- Rehydrating the dehydrated items
- Partitioning by time
- Injecting dynamic links and assets from a STAC API -->
