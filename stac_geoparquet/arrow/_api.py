import os
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional, Union

import pyarrow as pa

from stac_geoparquet.arrow._batch import CleanBatch, RawBatch
from stac_geoparquet.arrow._schema.models import InferredSchema
from stac_geoparquet.arrow._util import batched_iter
from stac_geoparquet.json_reader import read_json_chunked


def parse_stac_items_to_arrow(
    items: Iterable[Dict[str, Any]],
    *,
    chunk_size: int = 8192,
    schema: Optional[Union[pa.Schema, InferredSchema]] = None,
) -> Iterable[pa.RecordBatch]:
    """Parse a collection of STAC Items to an iterable of :class:`pyarrow.RecordBatch`.

    The objects under `properties` are moved up to the top-level of the
    Table, similar to :meth:`geopandas.GeoDataFrame.from_features`.

    Args:
        items: the STAC Items to convert
        chunk_size: The chunk size to use for Arrow record batches. This only takes
            effect if `schema` is not None. When `schema` is None, the input will be
            parsed into a single contiguous record batch. Defaults to 8192.
        schema: The schema of the input data. If provided, can improve memory use;
            otherwise all items need to be parsed into a single array for schema
            inference. Defaults to None.

    Returns:
        an iterable of pyarrow RecordBatches with the STAC-GeoParquet representation of items.
    """
    if schema is not None:
        if isinstance(schema, InferredSchema):
            schema = schema.inner

        # If schema is provided, then for better memory usage we parse input STAC items
        # to Arrow batches in chunks.
        for chunk in batched_iter(items, chunk_size):
            yield stac_items_to_arrow(chunk, schema=schema)

    else:
        # If schema is _not_ provided, then we must convert to Arrow all at once, or
        # else it would be possible for a STAC item late in the collection (after the
        # first chunk) to have a different schema and not match the schema inferred for
        # the first chunk.
        yield stac_items_to_arrow(items)


def parse_stac_ndjson_to_arrow(
    path: Union[str, Path, Iterable[Union[str, Path]]],
    *,
    chunk_size: int = 65536,
    schema: Optional[pa.Schema] = None,
    limit: Optional[int] = None,
) -> Iterator[pa.RecordBatch]:
    """
    Convert one or more newline-delimited JSON STAC files to a generator of Arrow
    RecordBatches.

    Each RecordBatch in the returned iterator is guaranteed to have an identical schema,
    and can be used to write to one or more Parquet files.

    Args:
        path: One or more paths to files with STAC items.
        chunk_size: The chunk size. Defaults to 65536.
        schema: The schema to represent the input STAC data. Defaults to None, in which
            case the schema will first be inferred via a full pass over the input data.
            In this case, there will be two full passes over the input data: one to
            infer a common schema across all data and another to read the data.

    Other args:
        limit: The maximum number of JSON Items to use for schema inference

    Yields:
        Arrow RecordBatch with a single chunk of Item data.
    """
    # If the schema was not provided, then we need to load all data into memory at once
    # to perform schema resolution.
    if schema is None:
        inferred_schema = InferredSchema()
        inferred_schema.update_from_json(path, chunk_size=chunk_size, limit=limit)
        yield from parse_stac_ndjson_to_arrow(
            path, chunk_size=chunk_size, schema=inferred_schema
        )
        return

    if isinstance(schema, InferredSchema):
        schema = schema.inner

    for batch in read_json_chunked(path, chunk_size=chunk_size):
        yield stac_items_to_arrow(batch, schema=schema)


def stac_table_to_items(table: pa.Table) -> Iterable[dict]:
    """Convert a STAC Table to a generator of STAC Item `dict`s"""
    for batch in table.to_batches():
        clean_batch = CleanBatch(batch)
        yield from clean_batch.to_raw_batch().iter_dicts()


def stac_table_to_ndjson(
    table: pa.Table, dest: Union[str, Path, os.PathLike[bytes]]
) -> None:
    """Write a STAC Table to a newline-delimited JSON file."""
    for batch in table.to_batches():
        clean_batch = CleanBatch(batch)
        clean_batch.to_raw_batch().to_ndjson(dest)


def stac_items_to_arrow(
    items: Iterable[Dict[str, Any]], *, schema: Optional[pa.Schema] = None
) -> pa.RecordBatch:
    """Convert dicts representing STAC Items to Arrow

    This converts GeoJSON geometries to WKB before Arrow conversion to allow multiple
    geometry types.

    All items will be parsed into a single RecordBatch, meaning that each internal array
    is fully contiguous in memory for the length of `items`.

    Args:
        items: STAC Items to convert to Arrow

    Kwargs:
        schema: An optional schema that describes the format of the data. Note that this
            must represent the geometry column as binary type.

    Returns:
        Arrow RecordBatch with items in Arrow
    """
    raw_batch = RawBatch.from_dicts(items, schema=schema)
    return raw_batch.to_clean_batch().inner
