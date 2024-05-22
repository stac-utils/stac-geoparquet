"""Return an iterator of items from an ndjson, a json array of items, or a featurecollection of items."""

from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Union

import orjson

from stac_geoparquet.arrow._util import batched_iter


def read_json(
    path: Union[str, Path, Iterable[Union[str, Path]]],
) -> Iterable[Dict[str, Any]]:
    """Read a json or ndjson file."""
    if isinstance(path, (str, Path)):
        path = [path]

    for p in path:
        with open(p) as f:
            try:
                # Support ndjson or json list/FeatureCollection without any whitespace
                # (all on first line)
                for line in f:
                    item = orjson.loads(line.strip())
                    if isinstance(item, list):
                        yield from item
                    elif "features" in item:
                        yield from item["features"]
                    else:
                        yield item
            except orjson.JSONDecodeError:
                f.seek(0)
                # read full json file as either a list or FeatureCollection
                json = orjson.loads(f.read())
                if isinstance(json, list):
                    yield from json
                else:
                    yield from json["features"]


def read_json_chunked(
    path: Union[str, Path, Iterable[Union[str, Path]]],
    chunk_size: int,
    *,
    limit: Optional[int] = None,
) -> Iterable[Sequence[Dict[str, Any]]]:
    """Read from a JSON or NDJSON file in chunks of `chunk_size`."""
    return batched_iter(read_json(path), chunk_size, limit=limit)
