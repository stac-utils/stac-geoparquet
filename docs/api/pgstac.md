# pgstac integration

`stac_geoparquet.pgstac_reader` has some helpers for working with items coming from a `pgstac.items` table. It takes care of

- Rehydrating the dehydrated items
- Partitioning by time
- Injecting dynamic links and assets from a STAC API

## Changing items with `row_func`

Pass a function as `row_func` to change items before they are written, for example to add the links and assets a STAC API would add:

```py
def add_self_link(item):
    item.setdefault("links", []).append({
        "rel": "self",
        "href": f"https://example.com/collections/{item['collection']}/items/{item['id']}",
    })
    return item

pgstac_to_parquet(conninfo, "items.parquet", collection="naip", row_func=add_self_link)
```

A `row_func` gets each item as `pgstac_to_iter` would yield it with the same `geometry`. `pgstac_to_iter` gives GeoJSON geometries by default, as STAC items have them. `pgstac_to_arrow`, `pgstac_to_parquet` and `sync_pgstac_to_parquet` give ISO WKB bytes by default, as GeoParquet stores them, so a `row_func` that adds links or assets doesn't convert every geometry to GeoJSON and back. If a `row_func` reads or changes geometries as GeoJSON, pass `geometry="geojson"`. For 100,000 NAIP-like items with large footprints, an export with `geometry="geojson"` took more than four times as long as with `"wkb"`, and two to three times the memory. Without a `row_func`, geometries are never converted to GeoJSON.

::: stac_geoparquet.pgstac_reader
    options:
      members:
        - pgstac_to_parquet
        - pgstac_to_arrow
        - pgstac_to_iter
        - sync_pgstac_to_parquet
