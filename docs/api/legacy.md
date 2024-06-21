# Direct GeoPandas conversion (Legacy)

The API listed here was the initial non-Arrow-based STAC-GeoParquet implementation, converting between JSON and GeoPandas directly. For large collections of STAC items, using the new Arrow-based functionality (under the `stac_geoparquet.arrow` namespace) will be more performant.

::: stac_geoparquet.to_geodataframe
::: stac_geoparquet.to_item_collection
::: stac_geoparquet.to_dict
