import operator
from functools import reduce
from itertools import islice
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import pyarrow as pa

T = TypeVar("T")


def update_batch_schema(
    batch: pa.RecordBatch,
    schema: pa.Schema,
) -> pa.RecordBatch:
    """Update a batch with new schema."""
    return pa.record_batch(batch.to_pydict(), schema=schema)


def batched_iter(
    lst: Iterable[T], n: int, *, limit: Optional[int] = None
) -> Iterable[Sequence[T]]:
    """Yield successive n-sized chunks from iterable."""
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(lst)
    count = 0
    while batch := tuple(islice(it, n)):
        yield batch
        count += len(batch)
        if limit and count >= limit:
            return


def convert_tuples_to_lists(t: Union[list, tuple]) -> List[Any]:
    """Convert tuples to lists, recursively

    For example, converts:
    ```
    (
        (
            (-112.4820566, 38.1261015),
            (-112.4816283, 38.1331311),
            (-112.4833551, 38.1338897),
            (-112.4832919, 38.1307687),
            (-112.4855415, 38.1291793),
            (-112.4820566, 38.1261015),
        ),
    )
    ```

    to

    ```py
    [
        [
            [-112.4820566, 38.1261015],
            [-112.4816283, 38.1331311],
            [-112.4833551, 38.1338897],
            [-112.4832919, 38.1307687],
            [-112.4855415, 38.1291793],
            [-112.4820566, 38.1261015],
        ]
    ]
    ```

    From https://stackoverflow.com/a/1014669.
    """
    return list(map(convert_tuples_to_lists, t)) if isinstance(t, (list, tuple)) else t


def get_by_path(root: Dict[str, Any], keys: Sequence[str]) -> Any:
    """Access a nested object in root by item sequence.

    From https://stackoverflow.com/a/14692747
    """
    return reduce(operator.getitem, keys, root)


def set_by_path(root: Dict[str, Any], keys: Sequence[str], value: Any) -> None:
    """Set a value in a nested object in root by item sequence.

    From https://stackoverflow.com/a/14692747
    """
    get_by_path(root, keys[:-1])[keys[-1]] = value  # type: ignore
