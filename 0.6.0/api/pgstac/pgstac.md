# pgstac integration

`stac_geoparquet.pgstac_reader` has some helpers for working with items coming from a `pgstac.items` table. It takes care of

- Rehydrating the dehydrated items
- Partitioning by time
- Injecting dynamic links and assets from a STAC API

::: stac_geoparquet.pgstac_reader.CollectionConfig
    options:
        show_if_no_docstring: true
