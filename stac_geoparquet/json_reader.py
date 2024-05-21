"""Return an iterator of items from an ndjson, a json array of items, or a featurecollection of items."""

import orjson
from typing import Iterator, Dict, Any, Union, Iterable
from pathlib import Path


def read_json(
    path: Union[str, Path, Iterable[Union[str, Path]]],
) -> Iterator[Dict[str, Any]]:
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
