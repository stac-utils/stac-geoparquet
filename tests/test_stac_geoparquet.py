import stac_geoparquet
import shapely.geometry
import pandas as pd
import pandas.testing
import geopandas
import requests
import pytest


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
        {
            "rel": "self",
            "type": "application/geo+json",
            "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip/items/ia_m_4209150_sw_15_060_20190828_20191105",
        },
        {
            "rel": "preview",
            "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/map?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105",
            "title": "Map of item",
            "type": "text/html",
        },
    ],
    "assets": {
        "image": {
            "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_60cm_2019/42091/m_4209150_sw_15_060_20190828.tif",
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "roles": ["data"],
            "title": "RGBIR COG tile",
            "eo:bands": [
                {"name": "Red", "common_name": "red"},
                {"name": "Green", "common_name": "green"},
                {"name": "Blue", "common_name": "blue"},
                {"name": "NIR", "common_name": "nir", "description": "near-infrared"},
            ],
        },
        "metadata": {
            "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_fgdc_2019/42091/m_4209150_sw_15_060_20190828.txt",
            "type": "text/plain",
            "roles": ["metadata"],
            "title": "FGDC Metdata",
        },
        "thumbnail": {
            "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_60cm_2019/42091/m_4209150_sw_15_060_20190828.200.jpg",
            "type": "image/jpeg",
            "roles": ["thumbnail"],
            "title": "Thumbnail",
        },
        "tilejson": {
            "title": "TileJSON with default rendering",
            "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/tilejson.json?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105&assets=image&asset_bidx=image%7C1%2C2%2C3",
            "type": "application/json",
            "roles": ["tiles"],
        },
        "rendered_preview": {
            "title": "Rendered preview",
            "rel": "preview",
            "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/preview.png?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105&assets=image&asset_bidx=image%7C1%2C2%2C3",
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


def test_to_geodataframe():
    result = stac_geoparquet.to_geodataframe([ITEM])
    expected = geopandas.GeoDataFrame(
        {
            "type": {0: "Feature"},
            "stac_version": {0: "1.0.0"},
            "stac_extensions": {
                0: [
                    "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
                    "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
                ]
            },
            "id": {0: "ia_m_4209150_sw_15_060_20190828_20191105"},
            "geometry": {0: shapely.geometry.shape(ITEM["geometry"])},
            "bbox": {0: [-91.879788, 42.121621, -91.807132, 42.191372]},
            "links": {
                0: [
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
                        "href": "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip/items/ia_m_4209150_sw_15_060_20190828_20191105",
                    },
                    {
                        "rel": "preview",
                        "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/map?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105",
                        "title": "Map of item",
                        "type": "text/html",
                    },
                ]
            },
            "assets": {
                0: {
                    "image": {
                        "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_60cm_2019/42091/m_4209150_sw_15_060_20190828.tif",
                        "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                        "roles": ["data"],
                        "title": "RGBIR COG tile",
                        "eo:bands": [
                            {"name": "Red", "common_name": "red"},
                            {"name": "Green", "common_name": "green"},
                            {"name": "Blue", "common_name": "blue"},
                            {
                                "name": "NIR",
                                "common_name": "nir",
                                "description": "near-infrared",
                            },
                        ],
                    },
                    "metadata": {
                        "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_fgdc_2019/42091/m_4209150_sw_15_060_20190828.txt",
                        "type": "text/plain",
                        "roles": ["metadata"],
                        "title": "FGDC Metdata",
                    },
                    "thumbnail": {
                        "href": "https://naipeuwest.blob.core.windows.net/naip/v002/ia/2019/ia_60cm_2019/42091/m_4209150_sw_15_060_20190828.200.jpg",
                        "type": "image/jpeg",
                        "roles": ["thumbnail"],
                        "title": "Thumbnail",
                    },
                    "tilejson": {
                        "title": "TileJSON with default rendering",
                        "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/tilejson.json?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105&assets=image&asset_bidx=image%7C1%2C2%2C3",
                        "type": "application/json",
                        "roles": ["tiles"],
                    },
                    "rendered_preview": {
                        "title": "Rendered preview",
                        "rel": "preview",
                        "href": "https://planetarycomputer.microsoft.com/api/data/v1/item/preview.png?collection=naip&item=ia_m_4209150_sw_15_060_20190828_20191105&assets=image&asset_bidx=image%7C1%2C2%2C3",
                        "roles": ["overview"],
                        "type": "image/png",
                    },
                }
            },
            "collection": {0: "naip"},
            "gsd": {0: 0.6},
            "datetime": {0: pd.Timestamp("2019-08-28 00:00:00+0000", tz="UTC")},
            "naip:year": {0: "2019"},
            "proj:bbox": {0: [592596.0, 4663966.8, 598495.8, 4671633.0]},
            "proj:epsg": {0: 26915},
            "naip:state": {0: "ia"},
            "proj:shape": {0: [12777, 9833]},
            "proj:transform": {
                0: [0.6, 0.0, 592596.0, 0.0, -0.6, 4671633.0, 0.0, 0.0, 1.0]
            },
        }
    )
    for k in ["type", "stac_version", "id", "collection"]:
        expected[k] = expected[k].astype("string")


    pandas.testing.assert_frame_equal(result, expected)



def test_s1_grd():
    item = requests.get("https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-1-grd/items/S1A_EW_GRDM_1SSH_20150129T081916_20150129T081938_004383_005598").json()  # noqa: E501
    stac_geoparquet.to_geodataframe([item])