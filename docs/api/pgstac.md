# pgstac integration

`stac_geoparquet.pgstac_reader` has some helpers for working with items coming from a `pgstac.items` table. It takes care of

- Rehydrating the dehydrated items
- Partitioning by time
- Injecting dynamic links and assets from a STAC API

`pgstac_to_parquet` and `pgstac_to_arrow` convert the items `chunk_size` at a time, unless `schema` is `"FullFile"`. Each chunk becomes a record batch, and a row group in Parquet.

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

A `row_func` gets each item as `pgstac_to_iter` would yield it with the same `geometry`. `pgstac_to_iter` gives GeoJSON geometries by default, as STAC items have them. `pgstac_to_arrow`, `pgstac_to_parquet` and `sync_pgstac_to_parquet` give ISO WKB bytes by default, as GeoParquet stores them, so a `row_func` that adds links or assets doesn't convert every geometry to GeoJSON and back. If a `row_func` reads or changes geometries as GeoJSON, pass `geometry="geojson"`. For 100,000 NAIP-like items with large footprints, an export with `geometry="geojson"` took more than four times as long as with `"wkb"`, with about the same peak memory. Without a `row_func`, geometries are never converted to GeoJSON.

## Memory

Memory use grows with `chunk_size`, as a chunk's items are all held as Python dicts while it is converted.

With the default `chunk_size` of 65536, exporting 100,000 NAIP-like items peaked at 2.0 GB, and 100,000 Sentinel-2-like items, with more assets, at 2.8 GB. With `chunk_size=8192` they peaked at 0.31 GB and 0.39 GB, and took about as long. The files were 414 MB and 3.0 MB at 65536, against 415 MB and 4.0 MB at 8192.

To use less memory, pass a smaller `chunk_size`, or set [`STAC_GEOPARQUET_CHUNK_SIZE`](../usage.md#settings) to change the default for every call:

```py
pgstac_to_parquet(conninfo, "items.parquet", collection="naip", chunk_size=8192)
```

A chunk ends early, with an INFO log message, if it wouldn't fit in the memory the process had left when the export started: the system's available memory, or a container (cgroup) limit if that is lower. This is an estimate, so items with very many distinct assets can still need more. Set [`STAC_GEOPARQUET_MEMORY_CAP`](../usage.md#settings) to `false` to keep every row group at `chunk_size`.

## Schema options

| `schema` | Reads the items | Memory | Schema covers |
| --- | --- | --- | --- |
| `"FirstBatch"` (default) | once | one chunk | the first chunk only, which is shorter than `chunk_size` when memory is short: a later chunk whose schema differs in any way fails |
| `"ChunksToDisk"` | once, then again from a copy on disk | one chunk | every item |
| `"FullFile"` | once | every item | every item |
| a `pa.Schema` | once | one chunk | what the schema has: fields it lacks are dropped |

For collections whose items don't all have the same fields, use `"ChunksToDisk"`. It writes each chunk to an uncompressed Arrow file and reads them back with the schema of all of them. The files take about as much space as the items do in Arrow. `pgstac_to_arrow` needs a `tmpdir`, writes them in a new directory inside it, and leaves them there; `pgstac_to_parquet` uses a temporary directory and deletes it unless you pass a `tmpdir`. If the directory is in memory (tmpfs, as `/tmp` is on some systems), the files take memory too.

::: stac_geoparquet.pgstac_reader
    options:
      members:
        - pgstac_to_parquet
        - pgstac_to_arrow
        - pgstac_to_iter
        - sync_pgstac_to_parquet
