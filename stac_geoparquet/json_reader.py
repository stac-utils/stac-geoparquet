"""Return an iterator of items from an ndjson, a json array of items, or a featurecollection of items."""

import orjson
from typing import Iterator, Dict, Any, Union, Iterable
from pathlib import Path


def read_json(
    path: Union[str, Path, Iterable[Union[str, Path]]]
) -> Iterator[Dict[str, Any]]:
    """Read a json or ndjson file."""
    if isinstance(path, (str, Path)):
        path = [path]

    for p in path:
        with open(p) as f:
            try:
                # read ndjson
                for line in f:
                    yield orjson.loads(line.strip())
            except orjson.JSONDecodeError:
                f.seek(0)
                # read full json file as either a list or FeatureCollection
                json = orjson.loads(f.read())
                if isinstance(json, list):
                    yield from json
                else:
                    yield from json["features"]
