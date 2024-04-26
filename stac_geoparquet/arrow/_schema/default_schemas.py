from __future__ import annotations

import pyarrow as pa
from typing import List, Literal, Optional, Sequence, Tuple

TimestampResolution = Literal["s", "ms", "us", "ns"]

# Unsure
# - "license relation": https://github.com/radiantearth/stac-spec/blob/v1.0.0/item-spec/common-metadata.md#relation-types


def item_core(
    *,
    timestamp_resolution: TimestampResolution = "us",
    asset_keys: Optional[List[str]] = None,
) -> pa.Schema:
    provider_object = pa.struct(
        [
            ("name", pa.utf8()),
            ("description", pa.utf8()),
            ("roles", pa.list_(pa.utf8())),
            ("url", pa.utf8()),
        ]
    )

    core_properties = [
        ("title", pa.utf8()),
        ("description", pa.utf8()),
        ("datetime", pa.timestamp(timestamp_resolution, "UTC")),
        ("created", pa.timestamp(timestamp_resolution, "UTC")),
        ("updated", pa.timestamp(timestamp_resolution, "UTC")),
        ("start_datetime", pa.timestamp(timestamp_resolution, "UTC")),
        ("end_datetime", pa.timestamp(timestamp_resolution, "UTC")),
        ("license", pa.utf8()),
        ("providers", pa.list_(provider_object)),
        ("platform", pa.utf8()),
        ("instruments", pa.list_(pa.utf8())),
        ("constellation", pa.utf8()),
        ("mission", pa.utf8()),
        ("gsd", pa.float64()),
    ]

    link_object = pa.struct(
        [
            ("href", pa.utf8()),
            ("rel", pa.utf8()),
            ("type", pa.utf8()),
            ("title", pa.utf8()),
        ]
    )

    asset_object = pa.struct(
        [
            ("href", pa.utf8()),
            ("title", pa.utf8()),
            ("description", pa.utf8()),
            ("type", pa.utf8()),
            ("roles", pa.list_(pa.utf8())),
        ]
    )

    # If asset_keys was not provided, we use a Map type
    if asset_keys is not None:
        assets_type = pa.struct([(key, asset_object) for key in asset_keys])
    else:
        assets_type = pa.map_(pa.utf8(), asset_object)

    return pa.schema(
        [
            ("type", pa.dictionary(index_type=pa.int8(), value_type=pa.utf8())),
            (
                "stac_version",
                pa.dictionary(index_type=pa.int8(), value_type=pa.utf8()),
            ),
            (
                "stac_extensions",
                pa.list_(pa.dictionary(index_type=pa.int16(), value_type=pa.utf8())),
            ),
            ("id", pa.utf8()),
            ("geometry", pa.binary()),
            ("bbox", bbox_type(dim=3)),
            *core_properties,
            ("links", pa.list_(link_object)),
            ("assets", assets_type),
            ("collection", pa.utf8()),
        ]
    )


def bbox_type(dim: int) -> pa.StructType:
    fields = [
        ("xmin", pa.float64()),
        ("ymin", pa.float64()),
    ]
    if dim == 3:
        fields.append(("zmin", pa.float64()))

    fields.extend(
        [
            ("xmax", pa.float64()),
            ("ymax", pa.float64()),
        ]
    )

    if dim == 3:
        fields.append(("zmax", pa.float64()))

    return pa.struct(fields)


def item_eo(
    *, properties: bool, asset_keys: Optional[Sequence[str]] = None
) -> pa.Schema:
    """Construct the partial schema for the STAC EO extension

    The EO extension allows information to be assigned either at the top-level properties or within assets.

    Args:
        properties: Set to `True` if EO information is set on properties.
        asset_keys: Pass a sequence of string asset keys that contain EO information.

    Returns:
        Partial EO extension Arrow schema
    """
    band_object = pa.struct(
        [
            ("name", pa.utf8()),
            ("common_name", pa.utf8()),
            ("description", pa.utf8()),
            ("center_wavelength", pa.float64()),
            ("full_width_half_max", pa.float64()),
            ("solar_illumination", pa.float64()),
        ]
    )
    eo_fields = [
        ("eo:bands", pa.list_(band_object)),
        ("eo:cloud_cover", pa.float64()),
        ("eo:snow_cover", pa.float64()),
    ]

    eo_object = pa.struct(eo_fields)
    if asset_keys is not None:
        assets_type = pa.struct([(key, eo_object) for key in asset_keys])
    else:
        assets_type = pa.map_(pa.utf8(), eo_object)
    fields: List[Tuple[str, pa.Field]] = [
        ("assets", assets_type),
    ]

    if properties:
        fields.extend(eo_fields)

    return pa.schema(fields)


def item_proj(
    *, properties: bool, asset_keys: Optional[Sequence[str]] = None
) -> pa.Schema:
    centroid_object = pa.struct(
        [
            ("lat", pa.float64()),
            ("lon", pa.float64()),
        ]
    )

    proj_fields = [
        ("proj:epsg", pa.uint16()),
        ("proj:wkt2", pa.utf8()),
        # TODO: this arbitrary JSON will need special handling
        ("proj:projjson", pa.utf8()),
        # TODO: this arbitrary JSON will need special handling
        ("proj:geometry", pa.binary()),
        # TODO: this bbox will need special handling
        # TODO: should this use list or struct encoding?
        # ("proj:bbox", bbox_type(dim=3)),
        ("proj:bbox", pa.list_(pa.float64())),
        ("proj:centroid", centroid_object),
        ("proj:shape", pa.list_(pa.uint32(), 2)),
        # TODO: switch this to a fixed size list of 6 or 9 elements
        ("proj:transform", pa.list_(pa.float64())),
    ]

    proj_object = pa.struct(proj_fields)
    if asset_keys is not None:
        assets_type = pa.struct([(key, proj_object) for key in asset_keys])
    else:
        assets_type = pa.map_(pa.utf8(), proj_object)
    fields: List[Tuple[str, pa.Field]] = [
        ("assets", assets_type),
    ]

    if properties:
        fields.extend(proj_fields)

    return pa.schema(fields)


def item_sci() -> pa.Schema:
    publication_object = pa.struct(
        [
            ("doi", pa.utf8()),
            ("citation", pa.utf8()),
        ]
    )

    sci_fields = [
        ("sci:doi", pa.utf8()),
        ("sci:citation", pa.utf8()),
        ("sci:publications", pa.list_(publication_object)),
    ]

    return pa.schema(sci_fields)


def item_view(*, properties: bool, asset_keys: Optional[Sequence[str]] = None):
    view_fields = [
        ("view:off_nadir", pa.float64()),
        ("view:incidence_angle", pa.float64()),
        ("view:azimuth", pa.float64()),
        ("view:sun_azimuth", pa.float64()),
        ("view:sun_elevation", pa.float64()),
    ]

    view_object = pa.struct(view_fields)
    if asset_keys is not None:
        assets_type = pa.struct([(key, view_object) for key in asset_keys])
    else:
        assets_type = pa.map_(pa.utf8(), view_object)
    fields: List[Tuple[str, pa.Field]] = [
        ("assets", assets_type),
    ]

    if properties:
        fields.extend(view_fields)

    return pa.schema(fields)
