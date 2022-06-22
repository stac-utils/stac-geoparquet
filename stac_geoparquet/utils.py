import functools
from typing import Any, Union

import shapely.geometry
import pystac


@functools.singledispatch
def assert_equal(result: Any, expected: Any) -> bool:
    raise TypeError(f"Invalid type {type(result)}")


@assert_equal.register(pystac.ItemCollection)
def assert_equal_ic(
    result: pystac.ItemCollection, expected: pystac.ItemCollection
) -> bool:
    assert type(result) == type(expected)
    assert len(result) == len(expected)
    assert result.extra_fields == expected.extra_fields
    for a, b in zip(result.items, expected.items):
        assert_equal(a, b)


@assert_equal.register(pystac.Item)
def assert_equal_item(result: pystac.Item, expected: pystac.Item) -> bool:
    assert type(result) == type(expected)
    assert result.id == expected.id
    assert shapely.geometry.shape(result.geometry) == shapely.geometry.shape(
        expected.geometry
    )
    assert result.bbox == expected.bbox
    assert result.datetime == expected.datetime
    assert type(result.stac_extensions) == type(expected.stac_extensions)
    assert sorted(result.stac_extensions) == sorted(expected.stac_extensions)
    assert result.collection_id == expected.collection_id
    assert result.extra_fields == expected.extra_fields

    result_links = sorted(result.links, key=lambda x: x.href)
    expected_links = sorted(expected.links, key=lambda x: x.href)
    assert len(result_links) == len(expected_links)
    for a, b in zip(result_links, expected_links):
        assert_equal(a, b)

    assert set(result.assets) == set(expected.assets)
    for k in result.assets:
        assert_equal(result.assets[k], expected.assets[k])


@assert_equal.register(pystac.Link)
@assert_equal.register(pystac.Asset)
def assert_link_equal(
    result: Union[pystac.Link, pystac.Asset], expected: Union[pystac.Link, pystac.Asset]
) -> bool:
    assert type(result) == type(expected)
    assert result.to_dict() == expected.to_dict()


def fix_empty_multipolygon(item_geometry):
    # Filter out missing geoms in MultiPolygons
    # https://github.com/shapely/shapely/issues/1407
    # geometry = [shapely.geometry.shape(x["geometry"]) for x in items2]
    if item_geometry["type"] == "MultiPolygon":
        item_geometry = dict(item_geometry)
        item_geometry["coordinates"] = [
            x for x in item_geometry["coordinates"] if any(x)
        ]

    return shapely.geometry.shape(item_geometry)
