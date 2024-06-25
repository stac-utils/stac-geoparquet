# Direct GeoPandas conversion (Legacy)

The API listed here was the initial non-Arrow-based STAC-GeoParquet implementation, converting between JSON and GeoPandas directly. For large collections of STAC items, using the new Arrow-based functionality (under the `stac_geoparquet.arrow` namespace) will be more performant.

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

::: stac_geoparquet.to_geodataframe
::: stac_geoparquet.to_item_collection
::: stac_geoparquet.to_dict
