import os
from typing import Any, Dict, Iterable, Optional, Union

import pyarrow as pa

from stac_geoparquet.arrow._batch import CleanBatch, RawBatch


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


def stac_table_to_items(table: pa.Table) -> Iterable[dict]:
    """Convert a STAC Table to a generator of STAC Item `dict`s"""
    for batch in table.to_batches():
        clean_batch = CleanBatch(batch)
        yield from clean_batch.to_raw_batch().iter_dicts()


def stac_table_to_ndjson(table: pa.Table, dest: Union[str, os.PathLike[bytes]]) -> None:
    """Write a STAC Table to a newline-delimited JSON file."""
    for batch in table.to_batches():
        clean_batch = CleanBatch(batch)
        clean_batch.to_raw_batch().to_ndjson(dest)
