import datetime
import json
import pathlib

import dateutil
import pandas as pd
import pystac
import pytest

import stac_geoparquet.pgstac_reader
from stac_geoparquet._compat import PYSTAC_1_7_0
from stac_geoparquet.utils import assert_equal

HERE = pathlib.Path(__file__).parent


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
                        "href": "https://naipeuwest.blob.core.windows.net/naip/v002/pa/2015/pa_100cm_2015/41080/m_4108053_se_17_1_20150725.tif"  # noqa: E501
                    },
                    "metadata": {
                        "href": "https://naipeuwest.blob.core.windows.net/naip/v002/pa/2015/pa_fgdc_2015/41080/m_4108053_se_17_1_20150725.txt"  # noqa: E501
                    },
                    "thumbnail": {
                        "href": "https://naipeuwest.blob.core.windows.net/naip/v002/pa/2015/pa_100cm_2015/41080/m_4108053_se_17_1_20150725.200.jpg"  # noqa: E501
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

    cfg = stac_geoparquet.pgstac_reader.CollectionConfig(
        collection_id="naip",
        render_config="assets=image&asset_bidx=image%7C1%2C2%2C3&format=png",
    )
    result = cfg.make_pgstac_items(records, base_item)[0]
    # shapely uses tuples instead of lists
    result = pystac.read_dict(result)

    expected = pystac.read_file(
        "https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip/items/pa_m_4108053_se_17_1_20150725_20151201"  # noqa: E501
    )

    if PYSTAC_1_7_0:
        # https://github.com/stac-utils/pystac/issues/1102
        expected.remove_links(rel=pystac.RelType.SELF)
        result.remove_links(rel=pystac.RelType.SELF)

    assert_equal(result, expected, ignore_none=True)


def test_sentinel2_l2a():
    record = json.loads(HERE.joinpath("record_sentinel2_l2a.json").read_text())
    base_item = json.loads(HERE.joinpath("base_sentinel2_l2a.json").read_text())
    record[3] = dateutil.parser.parse(record[3])
    record[4] = dateutil.parser.parse(record[4])

    config = stac_geoparquet.pgstac_reader.CollectionConfig(
        collection_id="sentinel-2-l2a",
        partition_frequency=None,
        stac_api="https://planetarycomputer.microsoft.com/api/stac/v1",
        should_inject_dynamic_properties=True,
        render_config="assets=visual&asset_bidx=visual%7C1%2C2%2C3&nodata=0&format=png",
    )
    result = pystac.read_dict(config.make_pgstac_items([record], base_item)[0])
    expected = pystac.read_file(
        "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a/items/S2A_MSIL2A_20150704T101006_R022_T35XQA_20210411T133707"  # noqa: E501
    )
    if PYSTAC_1_7_0:
        # https://github.com/stac-utils/pystac/issues/1102
        expected.remove_links(rel=pystac.RelType.SELF)
        result.remove_links(rel=pystac.RelType.SELF)

    expected.remove_links(rel=pystac.RelType.LICENSE)
    assert_equal(result, expected, ignore_none=True)


def test_generate_endpoints():
    cfg = stac_geoparquet.pgstac_reader.CollectionConfig(
        collection_id="naip", partition_frequency="AS"
    )
    endpoints = cfg.generate_endpoints()
    assert endpoints[0][0] == pd.Timestamp("2010-01-01 00:00:00+0000", tz="utc")
    assert endpoints[-1][1] >= pd.Timestamp("2021-01-01 00:00:00+0000", tz="utc")

    endpoints = cfg.generate_endpoints(since=pd.Timestamp("2018-01-01", tz="utc"))
    assert endpoints[0][0] == pd.Timestamp("2018-01-01 00:00:00+0000", tz="utc")
    assert endpoints[-1][1] >= pd.Timestamp("2021-01-01 00:00:00+0000", tz="utc")


@pytest.mark.parametrize(
    "part_number, total, start_datetime, end_datetime, expected",
    [
        (
            None,
            None,
            pd.Timestamp("2017-05-15 00:16:52+0000", tz="UTC"),
            pd.Timestamp("2017-05-22 00:16:52+0000", tz="UTC"),
            "items/part-6c6824fb552381663cc7c7a113560cc7_2017-05-15T00:16:52+00:00_2017-05-22T00:16:52+00:00.parquet",
        ),
        (
            1,
            2,
            pd.Timestamp("2017-05-15 00:16:52+0000", tz="UTC"),
            pd.Timestamp("2017-05-22 00:16:52+0000", tz="UTC"),
            "items/part-01_2017-05-15T00:16:52+00:00_2017-05-22T00:16:52+00:00.parquet",
        ),
        (
            1,
            10,
            pd.Timestamp("2017-05-15 00:16:52+0000", tz="UTC"),
            pd.Timestamp("2017-05-22 00:16:52+0000", tz="UTC"),
            "items/part-001_2017-05-15T00:16:52+00:00_2017-05-22T00:16:52+00:00.parquet",
        ),
    ],
)
def test_build_output_path(part_number, total, start_datetime, end_datetime, expected):
    base_output_path = "items/"
    result = stac_geoparquet.pgstac_reader._build_output_path(
        base_output_path, part_number, total, start_datetime, end_datetime
    )
    assert result == expected
