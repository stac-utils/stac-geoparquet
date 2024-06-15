import math
from typing import Any, Dict, Sequence, Union

from ciso8601 import parse_rfc3339

JsonValue = Union[list, tuple, int, float, dict, str, bool, None]


def assert_json_value_equal(
    result: JsonValue,
    expected: JsonValue,
    *,
    key_name: str = "root",
    precision: float = 0.0001,
) -> None:
    """Assert that the JSON value in `result` and `expected` are equal for our purposes.

    We allow these variations between result and expected:

    - We allow numbers to vary up to `precision`.
    - We consider `key: None` and a missing key to be equivalent.
    - We allow RFC3339 date strings with varying precision levels, as long as they
      represent the same parsed datetime.

    Args:
        result: The result to assert against.
        expected: The expected item to compare against.
        key_name: The key name of the current path in the JSON. Used for error messages.
        precision: The precision to use for comparing integers and floats.

    Raises:
        AssertionError: If the two values are not equal
    """
    if isinstance(result, list) and isinstance(expected, list):
        assert_sequence_equal(result, expected, key_name=key_name, precision=precision)

    elif isinstance(result, tuple) and isinstance(expected, tuple):
        assert_sequence_equal(result, expected, key_name=key_name, precision=precision)

    elif isinstance(result, (int, float)) and isinstance(expected, (int, float)):
        assert_number_equal(result, expected, key_name=key_name, precision=precision)

    elif isinstance(result, dict) and isinstance(expected, dict):
        assert_dict_equal(result, expected, key_name=key_name, precision=precision)

    elif isinstance(result, str) and isinstance(expected, str):
        assert_string_equal(result, expected, key_name=key_name)

    elif isinstance(result, bool) and isinstance(expected, bool):
        assert_bool_equal(result, expected, key_name=key_name)

    elif result is None and expected is None:
        pass

    else:
        raise AssertionError(
            f"Mismatched types at {key_name}. {type(result)=}, {type(expected)=}"
        )


def assert_sequence_equal(
    result: Sequence, expected: Sequence, *, key_name: str, precision: float
) -> None:
    """Compare two JSON arrays, recursively"""
    assert len(result) == len(expected), (
        f"List at {key_name} has different lengths." f"{len(result)=}, {len(expected)=}"
    )

    for i in range(len(result)):
        assert_json_value_equal(
            result[i], expected[i], key_name=f"{key_name}.[{i}]", precision=precision
        )


def assert_number_equal(
    result: Union[int, float],
    expected: Union[int, float],
    *,
    precision: float,
    key_name: str,
) -> None:
    """Compare two JSON numbers"""
    # Allow NaN equality
    if math.isnan(result) and math.isnan(expected):
        return

    assert abs(result - expected) <= precision, (
        f"Number at {key_name} not within precision. "
        f"{result=}, {expected=}, {precision=}."
    )


def assert_string_equal(
    result: str,
    expected: str,
    *,
    key_name: str,
) -> None:
    """Compare two JSON strings.

    We attempt to parse each string to a datetime. If this succeeds, then we compare the
    datetime.datetime representations instead of the bare strings.
    """

    # Check if both strings are dates, then assert the parsed datetimes are equal
    try:
        result_datetime = parse_rfc3339(result)
        expected_datetime = parse_rfc3339(expected)

        assert result_datetime == expected_datetime, (
            f"Date string at {key_name} not equal. "
            f"{result=}, {expected=}."
            f"{result_datetime=}, {expected_datetime=}."
        )

    except ValueError:
        assert (
            result == expected
        ), f"String at {key_name} not equal. {result=}, {expected=}."


def assert_bool_equal(
    result: bool,
    expected: bool,
    *,
    key_name: str,
) -> None:
    """Compare two JSON booleans."""
    assert result == expected, f"Bool at {key_name} not equal. {result=}, {expected=}."


def assert_dict_equal(
    result: Dict[str, Any],
    expected: Dict[str, Any],
    *,
    key_name: str,
    precision: float,
) -> None:
    """
    Assert that two JSON dicts are equal, recursively, allowing missing keys to equal
    None.
    """
    result_keys = set(result.keys())
    expected_keys = set(expected.keys())

    # For any keys that exist in result but not expected, assert that the result value
    # is None
    for key in result_keys - expected_keys:
        assert (
            result[key] is None
        ), f"Expected key at {key_name} to be None in result. Got {result['key']}"

    # And vice versa
    for key in expected_keys - result_keys:
        assert (
            expected[key] is None
        ), f"Expected key at {key_name} to be None in expected. Got {expected['key']}"

    # For any overlapping keys, assert that their values are equal
    for key in result_keys & expected_keys:
        assert_json_value_equal(
            result[key],
            expected[key],
            key_name=f"{key_name}.{key}",
            precision=precision,
        )
