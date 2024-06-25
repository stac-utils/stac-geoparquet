from __future__ import annotations

import itertools
import os
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pystac

from stac_geoparquet.arrow._batch import StacArrowBatch, StacJsonBatch
from stac_geoparquet.arrow._constants import DEFAULT_JSON_CHUNK_SIZE
from stac_geoparquet.arrow._schema.models import InferredSchema
from stac_geoparquet.arrow._util import batched_iter
from stac_geoparquet.arrow.types import ArrowStreamExportable
from stac_geoparquet.json_reader import read_json_chunked


def parse_stac_items_to_arrow(
    items: Iterable[pystac.Item | dict[str, Any]],
    *,
    chunk_size: int = 8192,
    schema: pa.Schema | InferredSchema | None = None,
) -> pa.RecordBatchReader:
    """
    Parse a collection of STAC Items to an iterable of
    [`pyarrow.RecordBatch`][pyarrow.RecordBatch].

    The objects under `properties` are moved up to the top-level of the
    Table, similar to
    [`geopandas.GeoDataFrame.from_features`][geopandas.GeoDataFrame.from_features].

    Args:
        items: the STAC Items to convert
        chunk_size: The chunk size to use for Arrow record batches. This only takes
            effect if `schema` is not None. When `schema` is None, the input will be
            parsed into a single contiguous record batch. Defaults to 8192.
        schema: The schema of the input data. If provided, can improve memory use;
            otherwise all items need to be parsed into a single array for schema
            inference. Defaults to None.

    Returns:
        pyarrow RecordBatchReader with a stream of STAC Arrow RecordBatches.
    """
    if schema is not None:
        if isinstance(schema, InferredSchema):
            schema = schema.inner

        # If schema is provided, then for better memory usage we parse input STAC items
        # to Arrow batches in chunks.
        batches = (
            stac_items_to_arrow(batch, schema=schema)
            for batch in batched_iter(items, chunk_size)
        )
        return pa.RecordBatchReader.from_batches(schema, batches)

    else:
        # If schema is _not_ provided, then we must convert to Arrow all at once, or
        # else it would be possible for a STAC item late in the collection (after the
        # first chunk) to have a different schema and not match the schema inferred for
        # the first chunk.
        batch = stac_items_to_arrow(items)
        return pa.RecordBatchReader.from_batches(batch.schema, [batch])


def parse_stac_ndjson_to_arrow(
    path: str | Path | Iterable[str | Path],
    *,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: pa.Schema | None = None,
    limit: int | None = None,
) -> pa.RecordBatchReader:
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

    Keyword Args:
        limit: The maximum number of JSON Items to use for schema inference

    Returns:
        pyarrow RecordBatchReader with a stream of STAC Arrow RecordBatches.
    """
    # If the schema was not provided, then we need to load all data into memory at once
    # to perform schema resolution.
    if schema is None:
        inferred_schema = InferredSchema()
        inferred_schema.update_from_json(path, chunk_size=chunk_size, limit=limit)
        inferred_schema.manual_updates()
        return parse_stac_ndjson_to_arrow(
            path, chunk_size=chunk_size, schema=inferred_schema
        )

    if isinstance(schema, InferredSchema):
        schema = schema.inner

    batches_iter = (
        stac_items_to_arrow(batch, schema=schema)
        for batch in read_json_chunked(path, chunk_size=chunk_size)
    )
    first_batch = next(batches_iter)
    # Need to take this schema from the iterator; the existing `schema` is the schema of
    # JSON batch
    resolved_schema = first_batch.schema
    return pa.RecordBatchReader.from_batches(
        resolved_schema, itertools.chain([first_batch], batches_iter)
    )


def stac_table_to_items(
    table: pa.Table | pa.RecordBatchReader | ArrowStreamExportable,
) -> Iterable[dict]:
    """Convert STAC Arrow to a generator of STAC Item `dict`s.

    Args:
        table: STAC in Arrow form. This can be a pyarrow Table, a pyarrow
            RecordBatchReader, or any other Arrow stream object exposed through the
            [Arrow PyCapsule
            Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
            A RecordBatchReader or stream object will not be materialized in memory.

    Yields:
        A STAC `dict` for each input row.
    """
    # Coerce to record batch reader to avoid materializing entire stream
    reader = pa.RecordBatchReader.from_stream(table)

    for batch in reader:
        clean_batch = StacArrowBatch(batch)
        yield from clean_batch.to_json_batch().iter_dicts()


def stac_table_to_ndjson(
    table: pa.Table | pa.RecordBatchReader | ArrowStreamExportable,
    dest: str | Path | os.PathLike[bytes],
) -> None:
    """Write STAC Arrow to a newline-delimited JSON file.

    !!! note
        This function _appends_ to the JSON file at `dest`; it does not overwrite any
        existing data.

    Args:
        table: STAC in Arrow form. This can be a pyarrow Table, a pyarrow
            RecordBatchReader, or any other Arrow stream object exposed through the
            [Arrow PyCapsule
            Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
            A RecordBatchReader or stream object will not be materialized in memory.
        dest: The destination where newline-delimited JSON should be written.
    """

    # Coerce to record batch reader to avoid materializing entire stream
    reader = pa.RecordBatchReader.from_stream(table)

    for batch in reader:
        clean_batch = StacArrowBatch(batch)
        clean_batch.to_json_batch().to_ndjson(dest)


def stac_items_to_arrow(
    items: Iterable[pystac.Item | dict[str, Any]], *, schema: pa.Schema | None = None
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
    raw_batch = StacJsonBatch.from_dicts(items, schema=schema)
    return raw_batch.to_arrow_batch().inner
