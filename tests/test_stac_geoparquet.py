import json
import pathlib

import geopandas
import pandas as pd
import pandas.testing
import pyarrow as pa
import pystac
import pytest
import requests
import shapely.geometry

import stac_geoparquet
from stac_geoparquet.stac_geoparquet import to_item_collection
from stac_geoparquet.utils import assert_equal, fix_empty_multipolygon

HERE = pathlib.Path(__file__).parent


def test_assert_equal():
    a = pystac.read_file(
        "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a/items/S2B_MSIL2A_20220612T182919_R027_T24XWR_20220613T123251"  # noqa: E501
    )
    b = pystac.read_file(
        "https://planetarycomputer.microsoft.com/api/stac/v1/collections/landsat-c2-l2/items/LC08_L2SP_202033_20220327_02_T1"  # noqa: E501
    )
    with pytest.raises(AssertionError):
        assert_equal(a, b)


ITEM_SELF_HREF = "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip/items/ia_m_4209150_sw_15_060_20190828_20191105"  # noqa: E501
ITEM = {
    "id": "ia_m_4209150_sw_15_060_20190828_20191105",
    "bbox": [-91.879788, 42.121621, -91.807132, 42.191372],
    "type": "Feature",
    "links": [
        {
            "rel": "collection",
            "type": "application/json",
            "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip",
        },
        {
            "rel": "parent",
            "type": "application/json",
            "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip",
        },
        {
            "rel": "root",
            "type": "application/json",
            "href": "https://planetarycomputer.microsoft.com/api/stac/v1/",
        },
        {"rel": "self", "type": "application/geo+json", "href": ITEM_SELF_HREF},
        {
            "rel": "preview",
            "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/map?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105",  # noqa: E501
            "title": "Map of item",
            "type": "text/html",
        },
    ],
    "assets": {
        "image": {
            "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_60cm_2019/42091/m_4209150_sw_15_060_20190828.tif",  # noqa: E501
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "roles": ["data"],
            "title": "RGBIR COG tile",
            "eo:bands": [
                {"name": "Red", "common_name": "red", "description": "Red"},
                {"name": "Green", "common_name": "green", "description": "Green"},
                {"name": "Blue", "common_name": "blue", "description": "Blue"},
                {"name": "NIR", "common_name": "nir", "description": "near-infrared"},
            ],
        },
        "metadata": {
            "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_fgdc_2019/42091/m_4209150_sw_15_060_20190828.txt",  # noqa: E501
            "type": "text/plain",
            "roles": ["metadata"],
            "title": "FGDC Metdata",
        },
        "thumbnail": {
            "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_60cm_2019/42091/m_4209150_sw_15_060_20190828.200.jpg",  # noqa: E501
            "type": "image/jpeg",
            "roles": ["thumbnail"],
            "title": "Thumbnail",
        },
        "tilejson": {
            "title": "TileJSON with default rendering",
            "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/tilejson.json?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105&assets=image&asset_bidx=image%7C1%2C2%2C3",  # noqa: E501
            "type": "application/json",
            "roles": ["tiles"],
        },
        "rendered_preview": {
            "title": "Rendered preview",
            "rel": "preview",
            "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/preview.png?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105&assets=image&asset_bidx=image%7C1%2C2%2C3",  # noqa: E501
            "roles": ["overview"],
            "type": "image/png",
        },
    },
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-91.808427, 42.121621],
                [-91.807132, 42.190651],
                [-91.87857, 42.191372],
                [-91.879788, 42.12234],
                [-91.808427, 42.121621],
            ]
        ],
    },
    "collection": "naip",
    "properties": {
        "gsd": 0.6,
        "datetime": "2019-08-28T00:00:00Z",
        "naip:year": "2019",
        "proj:bbox": [592596.0, 4663966.8, 598495.8, 4671633.0],
        "proj:epsg": 26915,
        "naip:state": "ia",
        "proj:shape": [12777, 9833],
        "proj:transform": [0.6, 0.0, 592596.0, 0.0, -0.6, 4671633.0, 0.0, 0.0, 1.0],
    },
    "stac_extensions": [
        "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
        "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
    ],
    "stac_version": "1.0.0",
}


EXPECTED_GDF = geopandas.GeoDataFrame(
    {
        "type": ["Feature"],
        "stac_version": ["1.0.0"],
        "stac_extensions": [
            [
                "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
                "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
            ]
        ],
        "id": ["ia_m_4209150_sw_15_060_20190828_20191105"],
        "geometry": geopandas.array.from_shapely(
            [shapely.geometry.shape(ITEM["geometry"])]
        ),
        "bbox": [[-91.879788, 42.121621, -91.807132, 42.191372]],
        "links": [
            [
                {
                    "rel": "collection",
                    "type": "application/json",
                    "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip",
                },
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip",
                },
                {
                    "rel": "root",
                    "type": "application/json",
                    "href": "https://planetarycomputer.microsoft.com/api/stac/v1/",
                },
                {
                    "rel": "self",
                    "type": "application/geo+json",
                    "href": ITEM_SELF_HREF,
                },
                {
                    "rel": "preview",
                    "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/map?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105",  # noqa: E501
                    "title": "Map of item",
                    "type": "text/html",
                },
            ]
        ],
        "assets": [
            {
                "image": {
                    "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_60cm_2019/42091/m_4209150_sw_15_060_20190828.tif",  # noqa: E501
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "roles": ["data"],
                    "title": "RGBIR COG tile",
                    "eo:bands": [
                        {"name": "Red", "common_name": "red", "description": "Red"},
                        {
                            "name": "Green",
                            "common_name": "green",
                            "description": "Green",
                        },
                        {
                            "name": "Blue",
                            "common_name": "blue",
                            "description": "Blue",
                        },
                        {
                            "name": "NIR",
                            "common_name": "nir",
                            "description": "near-infrared",
                        },
                    ],
                },
                "metadata": {
                    "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_fgdc_2019/42091/m_4209150_sw_15_060_20190828.txt",  # noqa: E501
                    "type": "text/plain",
                    "roles": ["metadata"],
                    "title": "FGDC Metdata",
                },
                "thumbnail": {
                    "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_60cm_2019/42091/m_4209150_sw_15_060_20190828.200.jpg",  # noqa: E501
                    "type": "image/jpeg",
                    "roles": ["thumbnail"],
                    "title": "Thumbnail",
                },
                "tilejson": {
                    "title": "TileJSON with default rendering",
                    "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/tilejson.json?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105&assets=image&asset_bidx=image%7C1%2C2%2C3",  # noqa: E501
                    "type": "application/json",
                    "roles": ["tiles"],
                },
                "rendered_preview": {
                    "title": "Rendered preview",
                    "rel": "preview",
                    "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/preview.png?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105&assets=image&asset_bidx=image%7C1%2C2%2C3",  # noqa: E501
                    "roles": ["overview"],
                    "type": "image/png",
                },
            }
        ],
        "collection": ["naip"],
        "gsd": [0.6],
        "datetime": pd.to_datetime(["2019-08-28 00:00:00+0000"]).as_unit("ns"),
        "naip:year": ["2019"],
        "proj:bbox": [[592596.0, 4663966.8, 598495.8, 4671633.0]],
        "proj:epsg": [26915],
        "naip:state": ["ia"],
        "proj:shape": [[12777, 9833]],
        "proj:transform": [[0.6, 0.0, 592596.0, 0.0, -0.6, 4671633.0, 0.0, 0.0, 1.0]],
    }
)


@pytest.mark.parametrize("dtype_backend", ["numpy_nullable", "pyarrow"])
def test_to_geodataframe(dtype_backend):
    result = stac_geoparquet.to_geodataframe([ITEM], dtype_backend=dtype_backend)
    expected = EXPECTED_GDF.copy()

    if dtype_backend == "numpy_nullable":
        for k in ["type", "stac_version", "id", "collection"]:
            expected[k] = expected[k].astype(pd.StringDtype())

    else:
        for k, v in EXPECTED_GDF.items():
            if k != "geometry":
                expected[k] = pd.arrays.ArrowExtensionArray(pa.array(v))

    pandas.testing.assert_frame_equal(result, expected)

    ic1 = to_item_collection(result)
    ic2 = pystac.ItemCollection([ITEM])
    assert_equal(ic1, ic2)


def test_dtype_backend_warns():
    with pytest.warns(FutureWarning, match="dtype_backend"):
        stac_geoparquet.to_geodataframe([ITEM])


def test_to_geodataframe_with_self_link():
    result = stac_geoparquet.to_geodataframe(
        [ITEM], add_self_link=True, dtype_backend="pyarrow"
    )
    expected = EXPECTED_GDF.copy()
    expected["self_link"] = pd.arrays.ArrowExtensionArray(pa.array([ITEM_SELF_HREF]))

    for k, v in EXPECTED_GDF.items():
        if k != "geometry":
            expected[k] = pd.arrays.ArrowExtensionArray(pa.array(v))

    pandas.testing.assert_frame_equal(result, expected)

    ic1 = to_item_collection(result)
    ic2 = pystac.ItemCollection([ITEM])
    assert_equal(ic1, ic2)


def test_s1_grd():
    # item = requests.get("https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-1-grd/items/S1A_EW_GRDM_1SSH_20150129T081916_20150129T081938_004383_005598").json()  # noqa: E501
    item = requests.get(
        "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-1-grd/items/S1A_EW_GRDM_1SSH_20150129T081916_20150129T081938_004383_005598"  # noqa: E501
    ).json()

    EO_V10 = "https://stac-extensions.github.io/eo/v1.0.0/schema.json"
    EO_V11 = "https://stac-extensions.github.io/eo/v1.1.0/schema.json"

    # pystac migrates EO extension to latest version, but PC is still on 1.0.0
    for i, ext in enumerate(item["stac_extensions"]):
        if ext == EO_V10:
            item["stac_extensions"][i] = EO_V11

    item["geometry"] = fix_empty_multipolygon(item["geometry"]).__geo_interface__
    df = stac_geoparquet.to_geodataframe([item], dtype_backend="pyarrow")

    result = to_item_collection(df)[0]
    assert_equal(result, pystac.read_dict(item))


@pytest.mark.parametrize(
    "collection_id",
    [
        "3dep-lidar-classification",
        "3dep-lidar-copc",
        "3dep-lidar-dsm",
        "3dep-lidar-dtm",
        "3dep-lidar-dtm-native",
        "3dep-lidar-hag",
        "3dep-lidar-intensity",
        "3dep-lidar-pointsourceid",
        "3dep-lidar-returns",
        "3dep-seamless",
        "alos-dem",
        "alos-fnf-mosaic",
        "alos-palsar-mosaic",
        "aster-l1t",
        "chloris-biomass",
        "cil-gdpcir-cc-by",
        "cil-gdpcir-cc-by-sa",
        "cil-gdpcir-cc0",
        "cop-dem-glo-30",
        "cop-dem-glo-90",
        "eclipse",
        "ecmwf-forecast",
        "era5-pds",
        "esa-worldcover",
        "fia",
        "gap",
        "gbif",
        "gnatsgo-rasters",
        "gnatsgo-tables",
        "goes-cmi",
        "hrea",
        "io-lulc",
        "io-lulc-9-class",
        "jrc-gsw",
        "landsat-c2-l1",
        "landsat-c2-l2",
        "mobi",
        "modis-09A1-061",
        "modis-09Q1-061",
        "modis-10A1-061",
        "modis-10A2-061",
        "modis-11A1-061",
        "modis-11A2-061",
        "modis-13A1-061",
        "modis-13Q1-061",
        "modis-14A1-061",
        "modis-14A2-061",
        "modis-15A2H-061",
        "modis-15A3H-061",
        "modis-16A3GF-061",
        "modis-17A2H-061",
        "modis-17A2HGF-061",
        "modis-17A3HGF-061",
        "modis-21A2-061",
        "modis-43A4-061",
        "modis-64A1-061",
        "mtbs",
        "naip",
        "nasa-nex-gddp-cmip6",
        "nasadem",
        "noaa-c-cap",
        "nrcan-landcover",
        "planet-nicfi-analytic",
        "planet-nicfi-visual",
        "sentinel-1-grd",
        "sentinel-1-rtc",
        "sentinel-2-l2a",
        "us-census",
    ],
)
def test_smoke(collection_id):
    r = requests.get(
        f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{collection_id}/items?limit=1"
    )
    r.raise_for_status()
    items = r.json()["features"]
    df = stac_geoparquet.to_geodataframe(items, dtype_backend="pyarrow")

    result = to_item_collection(df)
    expected = pystac.ItemCollection(items)
    assert_equal(result, expected, ignore_none=True)


def test_mixed_date_format():
    a = json.loads((HERE / "sentinel-2-item.json").read_text())
    b = json.loads((HERE / "sentinel-2-item.json").read_text())
    a["properties"]["datetime"] = "2000-12-10T22:04:58Z"
    b["properties"]["datetime"] = "2000-12-10T22:04:57.998000Z"
    a["geometry"] = {"type": "Point", "coordinates": [0, 0]}
    b["geometry"] = {"type": "Point", "coordinates": [0, 0]}

    result = stac_geoparquet.to_geodataframe([a, b], dtype_backend="pyarrow")
    expected = [
        pd.Timestamp("2000-12-10 22:04:58+0000", tz="UTC"),
        pd.Timestamp("2000-12-10 22:04:57.998000+0000", tz="UTC"),
    ]

    assert result["datetime"].tolist() == expected


@pytest.mark.parametrize("datetime_precision", ["us", "ns"])
def test_datetime_precision(datetime_precision):
    item = json.loads((HERE / "sentinel-2-item.json").read_text())
    item["properties"]["datetime"] = "2000-12-10T22:00:00.123456Z"
    df = stac_geoparquet.to_geodataframe(
        [item], dtype_backend="pyarrow", datetime_precision=datetime_precision
    )
    result = df["datetime"].iloc[0]
    expected = pd.Timestamp("2000-12-10 22:00:00.123456+0000", tz="UTC").as_unit(
        datetime_precision
    )
    assert result == expected
    assert result.unit == datetime_precision
