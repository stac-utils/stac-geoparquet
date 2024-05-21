"""Convert STAC data into Arrow tables"""

from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Union,
)

import pyarrow as pa

from stac_geoparquet.json_reader import read_json
from stac_geoparquet.arrow._util import (
    stac_items_to_arrow,
    batched_iter,
    update_batch_schema,
)


def parse_stac_items_to_batches(
    items: Iterable[Dict[str, Any]],
    *,
    chunk_size: int = 8192,
    schema: Optional[pa.Schema] = None,
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
    for item_batch in batched_iter(items, chunk_size):
        yield stac_items_to_arrow(item_batch)


def parse_stac_items_to_arrow(
    items: Iterable[Dict[str, Any]],
    *,
    chunk_size: int = 8192,
    schema: Optional[pa.Schema] = None,
) -> pa.Table:
    batches = parse_stac_items_to_batches(items, chunk_size=chunk_size, schema=schema)
    if schema is not None:
        return pa.Table.from_batches(batches, schema=schema)

    for batch in batches:
        if schema is None:
            schema = batch.schema
        else:
            schema = pa.unify_schemas(
                [schema, batch.schema], promote_options="permissive"
            )
    return pa.Table.from_batches(
        (update_batch_schema(batch, schema) for batch in batches), schema=schema
    )


def parse_stac_ndjson_to_batches(
    path: Union[str, Path],
    *,
    chunk_size: int = 8192,
    schema: Optional[pa.Schema] = None,
) -> Iterable[pa.RecordBatch]:
    return parse_stac_items_to_batches(
        read_json(path), chunk_size=chunk_size, schema=schema
    )


def parse_stac_ndjson_to_arrow(
    path: Union[Union[str, Path], Iterable[Union[str, Path]]],
    *,
    chunk_size: int = 65536,
    schema: Optional[pa.Schema] = None,
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

    Yields:
        Arrow RecordBatch with a single chunk of Item data.
    """
    return parse_stac_items_to_arrow(
        read_json(path), chunk_size=chunk_size, schema=schema
    )
