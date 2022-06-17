import datetime
import json

import requests
import pandas as pd

import stac_geoparquet.pgstac_reader


def test_naip_item():
    base_item = {
        "type": "Feature",
        "assets": {
            "image": {
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
                "type": "text/plain",
                "roles": ["metadata"],
                "title": "FGDC Metdata",
            },
            "thumbnail": {
                "type": "image/jpeg",
                "roles": ["thumbnail"],
                "title": "Thumbnail",
            },
        },
        "collection": "naip",
        "stac_version": "1.0.0",
    }

    records = [
        (
            "pa_m_4108053_se_17_1_20150725_20151201",
            "0103000020E61000000100000005000000D0D03FC1C51754C0D4635B069C8F44407D259012BB1754C0382D78D15798444089601C5C3A1C54C0D94125AE63984440A8E49CD8431C54C0A4FCA4DAA78F4440D0D03FC1C51754C0D4635B069C8F4440",  # noqa: E501
            "naip",
            datetime.datetime(2015, 7, 25, 0, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2015, 7, 25, 0, 0, tzinfo=datetime.timezone.utc),
            {
                "assets": {
                    "image": {
                        "href": "https://naipeuwest.blob.core.windows.net/naip/v002/pa/2015/pa_100cm_2015/41080/m_4108053_se_17_1_20150725.tif"
                    },
                    "metadata": {
                        "href": "https://naipeuwest.blob.core.windows.net/naip/v002/pa/2015/pa_fgdc_2015/41080/m_4108053_se_17_1_20150725.txt"
                    },
                    "thumbnail": {
                        "href": "https://naipeuwest.blob.core.windows.net/naip/v002/pa/2015/pa_100cm_2015/41080/m_4108053_se_17_1_20150725.200.jpg"
                    },
                },
                "properties": {
                    "gsd": 1.0,
                    "datetime": "2015-07-25T00:00:00Z",
                    "naip:year": "2015",
                    "proj:bbox": [546872.0, 4552485.0, 552765.0, 4560060.0],
                    "proj:epsg": 26917,
                    "naip:state": "pa",
                    "proj:shape": [7575, 5893],
                    "proj:transform": [
                        1.0,
                        0.0,
                        546872.0,
                        0.0,
                        -1.0,
                        4560060.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                },
                "stac_extensions": [
                    "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
                    "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
                ],
            },
        )
    ]

    cfg = stac_geoparquet.pgstac_reader.CollectionConfig(collection_id="naip")
    result = cfg.make_pgstac_items(records, base_item)[0]
    # shapely uses tuples instead of lists
    result = json.loads(json.dumps(result))

    expected = requests.get(
        "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip/items/pa_m_4108053_se_17_1_20150725_20151201"
    ).json()

    assert result == expected
    assert result["properties"]["datetime"]



def test_generate_endpoints():
    cfg = stac_geoparquet.pgstac_reader.CollectionConfig(collection_id="naip", partition_frequency="AS")
    endpoints = cfg.generate_endpoints()
    assert endpoints[0][0] == pd.Timestamp('2010-01-01 00:00:00+0000', tz='utc')
    assert endpoints[-1][1] == pd.Timestamp('2020-01-01 00:00:00+0000', tz='utc')

    endpoints = cfg.generate_endpoints(since=pd.Timestamp("2018-01-01", tz='utc'))
    assert endpoints[0][0] == pd.Timestamp('2018-01-01 00:00:00+0000', tz='utc')
    assert endpoints[-1][1] == pd.Timestamp('2020-01-01 00:00:00+0000', tz='utc')

