from __future__ import annotations

import functools
from typing import Any

import pystac
import shapely.geometry
import shapely.geometry.base


@functools.singledispatch
def assert_equal(result: Any, expected: Any, ignore_none: bool = False) -> bool:
    raise TypeError(f"Invalid type {type(result)}")


@assert_equal.register(pystac.ItemCollection)
def assert_equal_ic(
    result: pystac.ItemCollection,
    expected: pystac.ItemCollection,
    ignore_none: bool = False,
) -> None:
    assert type(result) == type(expected)
    assert len(result) == len(expected)
    assert result.extra_fields == expected.extra_fields
    for a, b in zip(result.items, expected.items):
        assert_equal(a, b, ignore_none=ignore_none)


@assert_equal.register(pystac.Item)
def assert_equal_item(
    result: pystac.Item, expected: pystac.Item, ignore_none: bool = False
) -> None:
    assert type(result) == type(expected)
    assert result.id == expected.id
    assert shapely.geometry.shape(result.geometry) == shapely.geometry.shape(
        expected.geometry
    )
    assert result.bbox == expected.bbox
    assert result.datetime == expected.datetime
    assert isinstance(result.stac_extensions, type(expected.stac_extensions))
    assert sorted(result.stac_extensions) == sorted(expected.stac_extensions)
    assert result.collection_id == expected.collection_id
    assert result.extra_fields == expected.extra_fields

    result_links = sorted(result.links, key=lambda x: x.href)
    expected_links = sorted(expected.links, key=lambda x: x.href)
    assert len(result_links) == len(expected_links)
    for a, b in zip(result_links, expected_links):
        assert_equal(a, b, ignore_none=ignore_none)

    assert set(result.assets) == set(expected.assets)
    for k in result.assets:
        assert_equal(result.assets[k], expected.assets[k], ignore_none=ignore_none)


@assert_equal.register(pystac.Link)
@assert_equal.register(pystac.Asset)
def assert_link_equal(
    result: pystac.Link | pystac.Asset,
    expected: pystac.Link | pystac.Asset,
    ignore_none: bool = False,
) -> None:
    assert type(result) == type(expected)
    resultd = result.to_dict()
    expectedd = expected.to_dict()

    left = {}

    if ignore_none:
        for k, v in resultd.items():
            if v is None and k not in expectedd:
                pass
            elif isinstance(v, list) and k in expectedd:
                out = []
                for val in v:
                    if isinstance(val, dict):
                        out.append({k: v2 for k, v2 in val.items() if v2 is not None})
                    else:
                        out.append(val)
                left[k] = out
            else:
                left[k] = v
    else:
        left = resultd

    assert left == expectedd


def fix_empty_multipolygon(
    item_geometry: dict[str, Any],
) -> shapely.geometry.base.BaseGeometry:
    # Filter out missing geoms in MultiPolygons
    # https://github.com/shapely/shapely/issues/1407
    # geometry = [shapely.geometry.shape(x["geometry"]) for x in items2]
    if item_geometry["type"] == "MultiPolygon":
        item_geometry = dict(item_geometry)
        item_geometry["coordinates"] = [
            x for x in item_geometry["coordinates"] if any(x)
        ]

    return shapely.geometry.shape(item_geometry)
