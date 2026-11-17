"""Convert STAC Items in Arrow Table format to JSON Lines or Python dicts."""

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


def convert_timestamp_columns_to_string(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Convert any datetime columns in the table to a string representation"""
    allowed_column_names = {
        "datetime",  # common metadata
        "start_datetime",
        "end_datetime",
        "created",
        "updated",
        "expires",  # timestamps extension
        "published",
        "unpublished",
    }
    for column_name in allowed_column_names:
        try:
            column = batch[column_name]
        except KeyError:
            continue

        batch = batch.drop_columns((column_name,)).append_column(
            column_name,
            pc.strftime(column, format="%Y-%m-%dT%H:%M:%SZ"),  # type: ignore
        )

    return batch


def lower_properties_from_top_level(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Take properties columns from the top level and wrap them in a struct column"""
    stac_top_level_keys = {
        "stac_version",
        "stac_extensions",
        "type",
        "id",
        "bbox",
        "geometry",
        "collection",
        "links",
        "assets",
    }

    properties_column_names: list[str] = []
    properties_column_fields: list[pa.Field] = []
    for column_idx in range(batch.num_columns):
        column_name = batch.column_names[column_idx]
        if column_name in stac_top_level_keys:
            continue

        properties_column_names.append(column_name)
        properties_column_fields.append(batch.schema.field(column_idx))

    struct_arr = pa.StructArray.from_arrays(
        batch.select(properties_column_names).columns, fields=properties_column_fields
    )

    return batch.drop_columns(properties_column_names).append_column(
        "properties", struct_arr
    )


def _null_mask(bbox_col: pa.StructArray) -> pa.Array | None:
    """Null mask for a bbox struct, or None when no row is null."""
    if bbox_col.null_count == 0:
        return None
    return bbox_col.is_null()


def _bbox_values(bbox_col: pa.StructArray, name: str) -> np.ndarray:
    """One bbox field as numpy, with 0.0 under null bboxes (masked again later)."""
    return bbox_col.field(name).fill_null(0.0).to_numpy()


def convert_bbox_to_array(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Convert the struct bbox column back to a list column for writing to JSON"""

    bbox_col_idx = batch.schema.get_field_index("bbox")
    if bbox_col_idx == -1:
        # Items with null geometry have no bbox, so a batch of only those has none
        return batch
    bbox_col = batch.column(bbox_col_idx)

    # new_chunks = []
    # for chunk in bbox_col.chunks:
    assert pa.types.is_struct(bbox_col.type)

    if bbox_col.type.num_fields == 4:
        xmin = _bbox_values(bbox_col, "xmin")
        ymin = _bbox_values(bbox_col, "ymin")
        xmax = _bbox_values(bbox_col, "xmax")
        ymax = _bbox_values(bbox_col, "ymax")
        coords = np.column_stack(
            [
                xmin,
                ymin,
                xmax,
                ymax,
            ]
        )

        list_arr = pa.FixedSizeListArray.from_arrays(
            coords.flatten("C"), 4, mask=_null_mask(bbox_col)
        )

    elif bbox_col.type.num_fields == 6:
        xmin = _bbox_values(bbox_col, "xmin")
        ymin = _bbox_values(bbox_col, "ymin")
        zmin = _bbox_values(bbox_col, "zmin")
        xmax = _bbox_values(bbox_col, "xmax")
        ymax = _bbox_values(bbox_col, "ymax")
        zmax = _bbox_values(bbox_col, "zmax")
        coords = np.column_stack(
            [
                xmin,
                ymin,
                zmin,
                xmax,
                ymax,
                zmax,
            ]
        )

        list_arr = pa.FixedSizeListArray.from_arrays(
            coords.flatten("C"), 6, mask=_null_mask(bbox_col)
        )

    else:
        raise ValueError("Expected 4 or 6 fields in bbox struct.")

    return batch.set_column(bbox_col_idx, "bbox", list_arr)
