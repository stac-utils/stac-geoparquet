import struct

import pytest
import shapely

from stac_geoparquet._wkb import ewkb_to_iso

needs_m = pytest.mark.skipif(not hasattr(shapely, "has_m"), reason="needs shapely>=2.1")

WKT = [
    "POINT (1 2)",
    "POINT EMPTY",
    "LINESTRING (0 0, 1 1)",
    "POLYGON ((0 0, 1 0, 1 1, 0 0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))",
    "POLYGON EMPTY",
    "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
    "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))",
    "POINT Z (1 2 3)",
    "POLYGON Z ((0 0 1, 1 0 1, 1 1 1, 0 0 1), (0 0 2, 1 0 2, 1 1 2, 0 0 2))",
    "MULTIPOINT Z ((0 0 1), (1 1 2))",
    "MULTIPOLYGON Z (((0 0 1, 1 0 1, 1 1 1, 0 0 1)))",
    "GEOMETRYCOLLECTION Z (POINT Z (1 2 3), MULTILINESTRING Z ((0 0 0, 1 1 1)))",
]


def _ewkb(wkt, byte_order, include_srid):
    geom = shapely.from_wkt(wkt)
    if include_srid:
        geom = shapely.set_srid(geom, 4326)
    return shapely.to_wkb(geom, byte_order=byte_order, include_srid=include_srid)


@pytest.mark.parametrize("wkt", WKT)
@pytest.mark.parametrize("byte_order", [0, 1], ids=["big-endian", "little-endian"])
@pytest.mark.parametrize("include_srid", [True, False], ids=["srid", "no-srid"])
def test_ewkb_to_iso(wkt, byte_order, include_srid):
    ewkb = _ewkb(wkt, byte_order, include_srid)

    iso = ewkb_to_iso(ewkb)

    expected = shapely.to_wkb(
        shapely.from_wkt(wkt), byte_order=byte_order, flavor="iso"
    )
    assert iso == expected


# M is written as Z, as when converting through GeoJSON, which has no M
@needs_m
@pytest.mark.parametrize(
    ("wkt", "expected"),
    [
        ("POINT M (1 2 4)", "POINT Z (1 2 4)"),
        (
            "MULTILINESTRING M ((0 0 1, 1 1 2), (2 2 3, 3 3 4))",
            "MULTILINESTRING Z ((0 0 1, 1 1 2), (2 2 3, 3 3 4))",
        ),
        (
            "POLYGON ZM ((0 0 1 2, 1 0 1 2, 1 1 1 2, 0 0 1 2))",
            "POLYGON ZM ((0 0 1 2, 1 0 1 2, 1 1 1 2, 0 0 1 2))",
        ),
    ],
)
def test_ewkb_to_iso_with_m(wkt, expected):
    iso = ewkb_to_iso(_ewkb(wkt, 1, True))

    assert iso == shapely.to_wkb(shapely.from_wkt(expected), flavor="iso")


def test_ewkb_to_iso_unsupported_type():
    # A CircularString Z, which isn't one of the simple feature types
    ewkb = b"\x01" + struct.pack("<I", 0x80000008) + bytes(28)

    with pytest.raises(ValueError, match="geometry type 8"):
        ewkb_to_iso(ewkb)


def test_iso_wkb_is_unchanged():
    iso = shapely.to_wkb(shapely.from_wkt("POINT Z (1 2 3)"), flavor="iso")

    assert ewkb_to_iso(iso) == iso
