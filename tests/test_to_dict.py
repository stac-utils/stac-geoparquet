import datetime
import pathlib

import geopandas
import pystac
import pytest
from pystac import Asset, Item

import stac_geoparquet

HERE = pathlib.Path(__file__).parent


@pytest.fixture
def naip():
    return geopandas.read_parquet(HERE / "data" / "naip.parquet")


def test_to_dict(naip):
    result = stac_geoparquet.to_item_collection(naip)
    expected = {
        "assets": {
            "image": {
                "eo:bands": [
                    {"common_name": "red", "description": None, "name": "Red"},
                    {"common_name": "green", "description": None, "name": "Green"},
                    {"common_name": "blue", "description": None, "name": "Blue"},
                    {
                        "common_name": "nir",
                        "description": "near-infrared",
                        "name": "NIR",
                    },
                ],
                "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ok/2010/ok_100cm_2010/34099/m_3409901_nw_14_1_20100425.tif",  # noqa: E501
                "roles": ["data"],
                "title": "RGBIR COG tile",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            },
            "rendered_preview": {
                "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/preview.png?collection=naip&item=ok_m_3409901_nw_14_1_20100425&assets=image&asset_bidx=image%7C1%2C2%2C3",  # noqa: E501
                "rel": "preview",
                "roles": ["overview"],
                "title": "Rendered preview",
                "type": "image/png",
            },
            "thumbnail": {
                "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ok/2010/ok_100cm_2010/34099/m_3409901_nw_14_1_20100425.200.jpg",  # noqa: E501
                "roles": ["thumbnail"],
                "title": "Thumbnail",
                "type": "image/jpeg",
            },
            "tilejson": {
                "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/tilejson.json?collection=naip&item=ok_m_3409901_nw_14_1_20100425&assets=image&asset_bidx=image%7C1%2C2%2C3",  # noqa: E501
                "roles": ["tiles"],
                "title": "TileJSON with default rendering",
                "type": "application/json",
            },
        },
        "bbox": [-100.004084, 34.934259, -99.933454, 35.00323],
        "collection": "naip",
        "geometry": {
            "coordinates": (
                (
                    (-99.933454, 34.934815),
                    (-99.93423, 35.00323),
                    (-100.004084, 35.002673),
                    (-100.00325, 34.934259),
                    (-99.933454, 34.934815),
                ),
            ),
            "type": "Polygon",
        },
        "id": "ok_m_3409901_nw_14_1_20100425",
        "links": [
            {
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip",
                "rel": "collection",
                "type": "application/json",
            },
            {
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip",
                "rel": "parent",
                "type": "application/json",
            },
            {
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/",
                "rel": "root",
                "type": "application/json",
            },
            {
                "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip/items/ok_m_3409901_nw_14_1_20100425",  # noqa: E501
                "rel": "self",
                "type": "application/geo+json",
            },
            {
                "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/map?collection=naip&item=ok_m_3409901_nw_14_1_20100425",  # noqa: E501
                "rel": "preview",
                "title": "Map of item",
                "type": "text/html",
            },
        ],
        "properties": {
            "datetime": "2010-04-25T00:00:00Z",
            "gsd": 1.0,
            "naip:state": "ok",
            "naip:year": "2010",
            "proj:bbox": [408377.0, 3866212.0, 414752.0, 3873800.0],
            "proj:epsg": 26914,
            "proj:shape": [7588, 6375],
            "proj:transform": [1.0, 0.0, 408377.0, 0.0, -1.0, 3873800.0, 0.0, 0.0, 1.0],
        },
        "stac_extensions": [
            "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
        ],
        "stac_version": pystac.get_stac_version(),
        "type": "Feature",
    }
    assert result[0].to_dict(transform_hrefs=False) == expected


def test_to_dict_optional_asset():
    items = [
        {
            "id": "a",
            "geometry": None,
            "bbox": None,
            "links": [],
            "type": "Feature",
            "stac_version": "1.0.0",
            "properties": {"datetime": "2021-01-01T00:00:00Z"},
            "assets": {"a": {"href": "a.txt"}, "b": {"href": "b.txt"}},
        },
        {
            "id": "b",
            "geometry": None,
            "bbox": None,
            "links": [],
            "type": "Feature",
            "stac_version": "1.0.0",
            "properties": {"datetime": "2021-01-01T00:00:00Z"},
            "assets": {"a": {"href": "a.txt"}},
        },
    ]
    df = stac_geoparquet.to_geodataframe(items, dtype_backend="pyarrow")
    result = stac_geoparquet.to_item_collection(df)
    assert result[0].assets["a"].to_dict() == {"href": "a.txt"}
    assert result[0].assets["b"].to_dict() == {"href": "b.txt"}
    assert result[1].assets["a"].to_dict() == {"href": "a.txt"}
    assert "b" not in result[1].assets


def test_heterogenous_assets() -> None:
    # https://github.com/stac-utils/stac-geoparquet/issues/82
    longmont = {"type": "Point", "coordinates": [-105.1019, 40.1672]}
    bbox = [-105.1019, 40.1672, -105.1019, 40.1672]
    item_a = Item(
        id="a",
        geometry=longmont,
        bbox=bbox,
        datetime=datetime.datetime.now(),
        properties={},
    )
    item_a.add_asset("asset", Asset("http://stac-geoparquet.test/asset.tif"))
    item_b = Item(
        id="b",
        geometry=longmont,
        bbox=bbox,
        datetime=datetime.datetime.now(),
        properties={},
    )
    record_batch_reader = stac_geoparquet.arrow.parse_stac_items_to_arrow(
        [item_a, item_b]
    )
    data_frame = geopandas.GeoDataFrame.from_arrow(record_batch_reader)
    batch = stac_geoparquet.arrow.stac_table_to_items(data_frame.to_arrow())
    for item in batch:
        # Before the fix, item b has `None` for the asset value, which explodes this conversion
        Item.from_dict(item)
