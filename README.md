# STAC-geoparquet

Convert STAC items to GeoParquet.

## Purpose

This library helps convert [STAC Items](https://github.com/radiantearth/stac-spec/blob/master/overview.md#item-overview) to [GeoParquet](https://github.com/opengeospatial/geoparquet). While STAC Items are commonly distributed as individual JSON files on object storage or through a [STAC API](https://github.com/radiantearth/stac-api-spec), STAC GeoParquet allows users to access a large number of STAC items in bulk without making repeated HTTP requests.

## Usage

Use `stac_geoparquet.to_arrow.stac_items_to_arrow` and
`stac_geoparquet.from_arrow.stac_table_to_items` to convert between STAC items
and Arrow tables. Arrow Tables of STAC items can be written to parquet with
`stac_geoparquet.to_parquet.to_parquet`.

Note that `stac_geoparquet` lifts the keys in the item `properties` up to the top level of the DataFrame, similar to `geopandas.GeoDataFrame.from_features`.

```python
>>> import requests
>>> import stac_geoparquet.arrow
>>> import pyarrow.parquet
>>> import pyarrow as pa

>>> items = requests.get(
...     "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a/items"
... ).json()["features"]
>>> table = pa.Table.from_batches(stac_geoparquet.arrow.parse_stac_items_to_arrow(items))
>>> stac_geoparquet.arrow.to_parquet(table, "items.parquet")
>>> table2 = pyarrow.parquet.read_table("items.parquet")
>>> items2 = list(stac_geoparquet.arrow.stac_table_to_items(table2))
```

See the [specification](./spec/stac-geoparquet-spec.md) for details on the output stac-geoparquet dataset.

## pgstac integration

`stac_geoparquet.pgstac_reader` has some helpers for working with items coming from a `pgstac.items` table. It takes care of

- Rehydrating the dehydrated items
- Partitioning by time
- Injecting dynamic links and assets from a STAC API
