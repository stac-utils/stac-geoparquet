import struct
from typing import Any

import shapely
import shapely.geometry

# EWKB flags in a geometry's type code
_EWKB_Z = 0x80000000
_EWKB_M = 0x40000000
_EWKB_SRID = 0x20000000


def to_iso_wkb(geometry: dict[str, Any] | bytes) -> bytes:
    """Convert a GeoJSON geometry, or WKB or EWKB bytes, to ISO WKB."""
    if isinstance(geometry, bytes):
        return ewkb_to_iso(geometry)
    return shapely.to_wkb(shapely.geometry.shape(geometry), flavor="iso")


def ewkb_to_iso(wkb: bytes) -> bytes:
    """
    Convert PostGIS EWKB to ISO WKB by rewriting its headers. XYM is written as
    XYZ, as it is through GeoJSON, which has no M.
    """
    order = "<" if wkb[0] == 1 else ">"
    (code,) = struct.unpack_from(order + "I", wkb, 1)
    if not code & (_EWKB_Z | _EWKB_M):
        # Nested 2D headers are the same in both, so only the SRID needs dropping
        if code & _EWKB_SRID:
            header = wkb[:1] + struct.pack(order + "I", code & 0xFFFF)
            return b"".join((header, memoryview(wkb)[9:]))
        return wkb
    parts: list[bytes | memoryview] = []
    _rewrite_ewkb(memoryview(wkb), 0, parts)
    return b"".join(parts)


def _rewrite_ewkb(src: memoryview, pos: int, parts: list[bytes | memoryview]) -> int:
    """
    Append the ISO WKB of the EWKB geometry at `pos` to `parts`, and return the
    position after it.
    """
    order = "<" if src[pos] == 1 else ">"
    u32 = order + "I"
    (code,) = struct.unpack_from(u32, src, pos + 1)
    geometry_type = code & 0xFFFF
    has_z = bool(code & _EWKB_Z)
    has_m = bool(code & _EWKB_M)
    iso_type = geometry_type + (3000 if has_z and has_m else 1000)
    dims = 2 + has_z + has_m

    parts.append(src[pos : pos + 1])
    parts.append(struct.pack(u32, iso_type))
    pos += 9 if code & _EWKB_SRID else 5

    if geometry_type == 1:  # Point
        end = pos + 8 * dims
    elif geometry_type == 2:  # LineString
        end = pos + 4 + 8 * dims * struct.unpack_from(u32, src, pos)[0]
    elif geometry_type == 3:  # Polygon
        (rings,) = struct.unpack_from(u32, src, pos)
        end = pos + 4
        for _ in range(rings):
            end += 4 + 8 * dims * struct.unpack_from(u32, src, end)[0]
    elif 4 <= geometry_type <= 7:  # Multi* and GeometryCollection
        (count,) = struct.unpack_from(u32, src, pos)
        parts.append(src[pos : pos + 4])
        pos += 4
        for _ in range(count):
            pos = _rewrite_ewkb(src, pos, parts)
        return pos
    else:
        raise ValueError(f"Unsupported WKB geometry type {geometry_type}")
    parts.append(src[pos:end])
    return end
