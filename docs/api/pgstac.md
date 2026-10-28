# pgstac integration

`stac_geoparquet.pgstac_reader` has some helpers for working with items coming from a `pgstac.items` table. It takes care of

- Rehydrating the dehydrated items
- Partitioning by time
- Injecting dynamic links and assets from a STAC API

::: stac_geoparquet.pgstac_reader
    options:
      members:
        - pgstac_to_parquet
        - pgstac_to_arrow
        - pgstac_to_iter
        - sync_pgstac_to_parquet
