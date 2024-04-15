# STAC-geoparquet

Convert STAC items to GeoParquet.

## Purpose

This library helps convert [STAC Items](https://github.com/radiantearth/stac-spec/blob/master/overview.md#item-overview) to [GeoParquet](https://github.com/opengeospatial/geoparquet). While STAC Items are commonly distributed as individual JSON files on object storage or through a [STAC API](https://github.com/radiantearth/stac-api-spec), STAC GeoParquet allows users to access a large number of STAC items in bulk without making repeated HTTP requests.

## Usage

`stac_geoparquet.to_dataframe` does it all. You give it a list of (STAC Item) dictionaries. It just converts them to a `geopandas.GeoDataFrame`, which can be written to parquet with `.to_parquet`.

```python
>>> import requests
>>> import stac_geoparquet
>>> item = requests.get("https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip/items/ia_m_4209150_sw_15_060_20190828_20191105").json()
>>> df = stac_geoparquet.to_geodataframe([item])
>>> df.to_parquet("naip.parquet")
```

Note that `stac_geoparquet` lifts the keys in the item `properties` up to the top level of the DataFrame, similar to `geopandas.GeoDataFrame.from_features`.

```python
>>> list(df.columns)
['type',
 'stac_version',
 'stac_extensions',
 'id',
 'geometry',
 'bbox',
 'links',
 'assets',
 'collection',
 'gsd',
 'datetime',
 'naip:year',
 'proj:bbox',
 'proj:epsg',
 'naip:state',
 'proj:shape',
 'proj:transform']
```

We also provide `stac_geoparquet.to_dict` and `stac_geoparquet.to_item_collection` helpers that can be used to convert from DataFrames back to the original STAC items.

## pgstac integration

`stac_geoparquet.pgstac_reader` has some helpers for working with items coming from a `pgstac.items` table. It takes care of

- Rehydrating the dehydrated items
- Partitioning by time
- Injecting dynamic links and assets from a STAC API
