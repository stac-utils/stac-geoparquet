from __future__ import annotations

import itertools
import logging
import os
import sys
import tempfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import psutil
import pyarrow as pa
import pystac

from stac_geoparquet.arrow._batch import StacArrowBatch, StacJsonBatch
from stac_geoparquet.arrow._constants import (
    ACCEPTED_SCHEMA_OPTIONS,
    DEFAULT_JSON_CHUNK_SIZE,
    DEFAULT_PARQUET_SCHEMA_VERSION,
    SUPPORTED_PARQUET_SCHEMA_VERSIONS,
)
from stac_geoparquet.arrow._schema.models import InferredSchema
from stac_geoparquet.arrow._to_parquet import to_parquet
from stac_geoparquet.arrow._util import batched_iter
from stac_geoparquet.arrow.types import ArrowStreamExportable
from stac_geoparquet.json_reader import read_json_chunked

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

PID = psutil.Process(os.getpid())


def memlog(msg: str) -> None:
    """Log Memory and CPU usage of the current process."""
    with PID.oneshot():
        logger.info(
            f"{msg} | CPU%: {PID.cpu_percent()} | CPU_USER_TIME: {PID.cpu_times().user:.3f} | RSS(MB):{PID.memory_full_info().rss / 1024 / 1024:.2f} | USS(MB):{PID.memory_full_info().uss / 1024 / 1024:.2f}"
        )


def from_batches(batches: Iterable[pa.RecordBatch]) -> pa.RecordBatchReader:
    batches = iter(batches)
    init = next(batches)

    def check_batches(
        s: pa.Schema, batches: Iterable[pa.RecordBatch]
    ) -> Iterable[pa.RecordBatch]:
        for c, b in enumerate(batches):
            memlog(f"Batch {c}")
            if not s.equals(b.schema):
                raise ValueError("Batch Schemas Not Equal")
            if b.num_rows < 1:
                logger.warning("Batch had no rows.")
            else:
                yield b

    checked = check_batches(init.schema, itertools.chain([init], batches))
    return pa.RecordBatchReader.from_batches(init.schema, checked)


def parse_stac_items_to_arrow(
    items: Iterable[pystac.Item | dict[str, Any]],
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: ACCEPTED_SCHEMA_OPTIONS = "FullFile",
    tmpdir: str | Path | None = None,
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
            inference. This can also be set to a string value of "FullFile" which
            will scan the entire input in memory to get the schema, "FirstBatch" which
            will use the first batch of items to infer the schema, or "ChunksToDisk"
            which will write each chunk of items to disk as a Parquet file and then read
            them back in to unify the schema. Defaults to "FullFile".

    Returns:
        pyarrow RecordBatchReader with a stream of STAC Arrow RecordBatches.
    """
    memlog("parse_stac_items_to_arrow start")

    if isinstance(schema, InferredSchema):
        schema = schema.inner

    if isinstance(schema, pa.Schema):
        # If schema is provided, then for better memory usage we parse input STAC items
        # to Arrow batches in chunks.
        batches = (
            stac_items_to_arrow(batch, schema=schema)
            for batch in batched_iter(items, chunk_size)
        )
        return pa.RecordBatchReader.from_batches(schema, batches)

    elif schema == "FullFile":
        batch = stac_items_to_arrow(items)
        logger.debug(batch.schema, batch)
        return pa.RecordBatchReader.from_batches(batch.schema, [batch])

    elif schema == "FirstBatch":
        batches = (
            stac_items_to_arrow(batch) for batch in batched_iter(items, chunk_size)
        )
        return from_batches(batches)

    else:
        assert schema == "ChunksToDisk"
        if tmpdir is None or Path(tmpdir).exists() is False:
            raise FileNotFoundError("tmpdir must be provided for ChunksToDisk schema")
        else:
            logger.info(f"Using temporary directory {tmpdir} for chunked schema")
            for cnt, chunk in enumerate(batched_iter(items, chunk_size)):
                batch = stac_items_to_arrow(chunk)
                if not isinstance(schema, pa.Schema):
                    schema = batch.schema
                elif not schema.equals(batch.schema):
                    logger.info("Unifying schema...")
                    schema = pa.unify_schemas(
                        [schema, batch.schema], promote_options="permissive"
                    )
                fname = f"{tmpdir}/{cnt}.parquet"
                to_parquet(
                    pa.RecordBatchReader.from_batches(batch.schema, [batch]),
                    output_path=fname,
                )
                memlog(f"Batch {cnt}")
            ds = pa.dataset.dataset(
                tmpdir, schema=schema, format="parquet", batch_size=chunk_size
            )
            memlog("Created Dataset")
            batches = ds.to_batches()
            memlog("Created Batches")
            return pa.RecordBatchReader.from_batches(schema, batches)


def parse_stac_items_to_parquet(
    items: Iterable[pystac.Item | dict[str, Any]],
    *,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: ACCEPTED_SCHEMA_OPTIONS = "FirstBatch",
    output_path: str | Path,
    tmpdir: str | Path | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    filesystem: pa.fs.FileSystem | None = None,
    **kwargs: Any,
) -> str:
    """
    Parse an iterable of Stac Items into Parquet.
    """
    logger.info("Saving STAC Items to Parquet")

    if filesystem is None:
        filesystem, filepath = pa.fs.FileSystem.from_uri(output_path)
    else:
        filepath = output_path

    filedir = Path(filepath).parent
    filesystem.create_dir(str(filedir), recursive=True)

    logger.info(f"Exporting PgSTAC to {filesystem} {filepath}")

    if schema == "ChunksToDisk" and tmpdir is None:
        with tempfile.TemporaryDirectory() as td:
            reader = parse_stac_items_to_arrow(
                items=items,
                chunk_size=chunk_size,
                schema=schema,
                tmpdir=td,
            )
            memlog("Parsed to arrow")
            to_parquet(
                reader,
                output_path=filepath,
                filesystem=filesystem,
                schema_version=schema_version,
                **kwargs,
            )
    else:
        reader = parse_stac_items_to_arrow(
            items=items,
            chunk_size=chunk_size,
            schema=schema,
            tmpdir=tmpdir,
        )
        memlog("Parsed to arrow")
        to_parquet(
            reader,
            output_path=filepath,
            filesystem=filesystem,
            schema_version=schema_version,
            **kwargs,
        )
    memlog("Written to parquet")
    return str(filepath)


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


def parse_stac_ndjson_to_parquet(
    input_path: str | Path | Iterable[str | Path],
    output_path: str | Path,
    *,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
    schema: pa.Schema | InferredSchema | None = None,
    limit: int | None = None,
    schema_version: SUPPORTED_PARQUET_SCHEMA_VERSIONS = DEFAULT_PARQUET_SCHEMA_VERSION,
    collections: Mapping[str, Mapping[str, Any]] | None = None,
    collection_metadata: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> None:
    """Convert one or more newline-delimited JSON STAC files to GeoParquet

    Args:
        input_path: One or more paths to files with STAC items.
        output_path: A path to the output Parquet file.

    Keyword Args:
        chunk_size: The chunk size. Defaults to 65536.
        schema: The schema to represent the input STAC data. Defaults to None, in which
            case the schema will first be inferred via a full pass over the input data.
            In this case, there will be two full passes over the input data: one to
            infer a common schema across all data and another to read the data and
            iteratively convert to GeoParquet.
        limit: The maximum number of JSON records to convert.
        schema_version: GeoParquet specification version; if not provided will default
            to latest supported version.
        collections: A dictionary mapping collection IDs to
            dictionaries representing a Collection in a SpatioTemporal
            Asset Catalog. This will be stored under the key `stac-geoparquet` in the
            parquet file metadata, under the key `collections`.

        collection_metadata: A dictionary representing a Collection in a SpatioTemporal
            Asset Catalog. This will be stored under the key `stac-geoparquet` in the
            parquet file metadata, under the key `collection`.

            Deprecated in favor of `collections`.

    All other keyword args are passed on to
    [`pyarrow.parquet.ParquetWriter`][pyarrow.parquet.ParquetWriter].
    """
    record_batch_reader = parse_stac_ndjson_to_arrow(
        input_path, chunk_size=chunk_size, schema=schema, limit=limit
    )
    to_parquet(
        record_batch_reader,
        output_path=output_path,
        schema_version=schema_version,
        **kwargs,
        collections=collections,
        collection_metadata=collection_metadata,
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

            The 'type' field is not required in stac-geoparquet. If not present,
            a 'type' field will be added where each record is 'Feature'.
        dest: The destination where newline-delimited JSON should be written.
    """
    # Coerce to record batch reader to avoid materializing entire stream
    reader = pa.RecordBatchReader.from_stream(table)

    for batch in reader:
        if "type" not in batch.schema.names:
            type_arr = pa.DictionaryArray.from_arrays(
                indices=pa.array([0] * len(batch), type=pa.int8()),
                dictionary=pa.array(["Feature"], type=pa.string()),
            )
            batch = batch.add_column(0, "type", type_arr)

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
