from __future__ import annotations

import itertools
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional, Union

import pyarrow as pa
from deltalake import write_deltalake

from stac_geoparquet.arrow._api import parse_stac_ndjson_to_arrow
from stac_geoparquet.arrow._to_parquet import create_geoparquet_metadata

if TYPE_CHECKING:
    from deltalake import DeltaTable


def parse_stac_ndjson_to_delta_lake(
    input_path: Union[str, Path, Iterable[Union[str, Path]]],
    table_or_uri: Union[str, Path, DeltaTable],
    *,
    chunk_size: int = 65536,
    schema: Optional[pa.Schema] = None,
    limit: Optional[int] = None,
    **kwargs,
):
    batches_iter = parse_stac_ndjson_to_arrow(
        input_path, chunk_size=chunk_size, schema=schema, limit=limit
    )
    first_batch = next(batches_iter)
    schema = first_batch.schema.with_metadata(create_geoparquet_metadata())
    combined_iter = itertools.chain([first_batch], batches_iter)
    write_deltalake(table_or_uri, combined_iter, schema=schema, engine="rust", **kwargs)
