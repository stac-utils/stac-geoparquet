"""
Generate geoparquet from a sequence of STAC items.
"""

from __future__ import annotations

import collections
import warnings
from typing import Any, Literal, Sequence
from urllib.parse import urlparse

import geopandas
import numpy as np
import pandas as pd
import pyarrow as pa
import pystac
import shapely.geometry

from stac_geoparquet.utils import fix_empty_multipolygon

STAC_ITEM_TYPES = ["application/json", "application/geo+json"]
DTYPE_BACKEND = Literal["numpy_nullable", "pyarrow"]
SELF_LINK_COLUMN = "self_link"


def _fix_array(v: Any) -> Any:
    if isinstance(v, np.ndarray):
        v = v.tolist()

    elif isinstance(v, dict):
        v = {k: _fix_array(v2) for k, v2 in v.items()}

    return v


def to_geodataframe(
    items: Sequence[dict[str, Any]],
    add_self_link: bool = False,
    dtype_backend: DTYPE_BACKEND | None = None,
    datetime_precision: str = "ns",
) -> geopandas.GeoDataFrame:
    """
    Convert a sequence of STAC items to a [`geopandas.GeoDataFrame`][geopandas.GeoDataFrame].

    The objects under `properties` are moved up to the top-level of the
    DataFrame, similar to
    [`geopandas.GeoDataFrame.from_features`][geopandas.GeoDataFrame.from_features].

    Args:
        items: A sequence of STAC items.
        add_self_link: bool, default False
            Add the absolute link (if available) to the source STAC Item
            as a separate column named "self_link"
        dtype_backend: `{'pyarrow', 'numpy_nullable'}`, optional
            The dtype backend to use for storing arrays.

            By default, this will use 'numpy_nullable' and emit a
            FutureWarning that the default will change to 'pyarrow' in
            the next release.

            Set to 'numpy_nullable' to silence the warning and accept the
            old behavior.

            Set to 'pyarrow' to silence the warning and accept the new behavior.

            There are some difference in the output as well: with
            ``dtype_backend="pyarrow"``, struct-like fields will explicitly
            contain null values for fields that appear in only some of the
            records. For example, given an ``assets`` like::

            ```json
            {
                "a": {
                    "href": "a.tif",
                },
                "b": {
                    "href": "b.tif",
                    "title": "B",
                }
            }
            ```

            The ``assets`` field of the output for the first row with
            ``dtype_backend="numpy_nullable"`` will be a Python dictionary with
            just ``{"href": "a.tiff"}``.

            With ``dtype_backend="pyarrow"``, this will be a pyarrow struct
            with fields ``{"href": "a.tif", "title", None}``. pyarrow will
            infer that the struct field ``asset.title`` is nullable.

        datetime_precision: str, default "ns"
            The precision to use for the datetime columns. For example,
            "us" is microsecond and "ns" is nanosecond.

    Returns:
        The converted GeoDataFrame.
    """
    items2 = collections.defaultdict(list)

    for item in items:
        keys = set(item) - {"properties", "geometry"}

        for k in keys:
            items2[k].append(item[k])

        item_geometry = item["geometry"]
        if item_geometry:
            item_geometry = fix_empty_multipolygon(item_geometry)

        items2["geometry"].append(item_geometry)

        for k, v in item["properties"].items():
            if k in item:
                msg = f"Key '{k}' appears in both 'properties' and the top level."
                raise ValueError(msg)
            items2[k].append(v)

        if add_self_link:
            self_href = None
            for link in item["links"]:
                if (
                    link["rel"] == "self"
                    and (not link["type"] or link["type"] in STAC_ITEM_TYPES)
                    and urlparse(link["href"]).netloc
                ):
                    self_href = link["href"]
                    break
            items2[SELF_LINK_COLUMN].append(self_href)

    # TODO: Ideally we wouldn't have to hard-code this list.
    # Could we get it from the JSON schema.
    DATETIME_COLUMNS = {
        "datetime",  # common metadata
        "start_datetime",
        "end_datetime",
        "created",
        "updated",
        "expires",  # timestamps extension
        "published",
        "unpublished",
    }

    items2["geometry"] = geopandas.array.from_shapely(items2["geometry"])

    if dtype_backend is None:
        msg = (
            "The default argument for 'dtype_backend' will change from "
            "'numpy_nullable' to 'pyarrow'. To keep the previous default "
            "specify ``dtype_backend='numpy_nullable'``. To accept the future "
            "behavior specify ``dtype_backend='pyarrow'."
        )
        warnings.warn(FutureWarning(msg))
        dtype_backend = "numpy_nullable"

    if dtype_backend == "pyarrow":
        for k, v in items2.items():
            if k in DATETIME_COLUMNS:
                dt = pd.to_datetime(v, format="ISO8601").as_unit(datetime_precision)
                items2[k] = pd.arrays.ArrowExtensionArray(pa.array(dt))

            elif k != "geometry":
                items2[k] = pd.arrays.ArrowExtensionArray(pa.array(v))

    elif dtype_backend == "numpy_nullable":
        for k, v in items2.items():
            if k in DATETIME_COLUMNS:
                items2[k] = pd.to_datetime(v, format="ISO8601").as_unit(
                    datetime_precision
                )

            if k in {"type", "stac_version", "id", "collection", SELF_LINK_COLUMN}:
                items2[k] = pd.array(v, dtype="string")
    else:
        msg = f"Invalid 'dtype_backend={dtype_backend}'."
        raise TypeError(msg)

    gdf = geopandas.GeoDataFrame(items2, geometry="geometry", crs="WGS84")

    columns = [
        "type",
        "stac_version",
        "stac_extensions",
        "id",
        "geometry",
        "bbox",
        "links",
        "assets",
        "collection",
    ]
    opt_columns = ["stac_extensions", "collection"]
    for col in opt_columns:
        if col not in gdf.columns:
            columns.remove(col)

    gdf = pd.concat([gdf[columns], gdf.drop(columns=columns)], axis="columns")
    return gdf


def to_dict(record: dict) -> dict:
    """
    Create a dictionary representing a STAC item from a row of the GeoDataFrame.

    Parameters:
        record: dict
    """
    properties = {}
    top_level_keys = {
        "type",
        "stac_version",
        "id",
        "geometry",
        "bbox",
        "links",
        "assets",
        "collection",
        "stac_extensions",
    }
    item = {}
    for k, v in record.items():
        v = _fix_array(v)

        if k == SELF_LINK_COLUMN:
            continue
        elif k == "assets":
            item[k] = {k2: v2 for k2, v2 in v.items() if v2 is not None}
        elif k in top_level_keys:
            item[k] = v
        else:
            properties[k] = v

    if item["geometry"]:
        item["geometry"] = shapely.geometry.mapping(item["geometry"])

    item["properties"] = properties

    return item


def to_item_collection(df: geopandas.GeoDataFrame) -> pystac.ItemCollection:
    """
    Convert a GeoDataFrame of STAC items to a [`pystac.ItemCollection`][pystac.ItemCollection].

    Parameters:
        df: A GeoDataFrame with a schema similar to that exported by stac-geoparquet.

    Returns:
        The converted `ItemCollection`. There will be one record / feature per
            row in the in the GeoDataFrame.
    """
    df2 = df.copy()
    datelike = df2.select_dtypes(
        include=["datetime64[ns, UTC]", "datetime64[ns]"]
    ).columns
    for k in datelike:
        # %f isn't implemented in pyarrow
        # https://github.com/apache/arrow/issues/20146
        if isinstance(df2[k].dtype, pd.ArrowDtype):
            df2[k] = df2[k].astype("datetime64[ns, utc]")

        df2[k] = (
            df2[k].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ").fillna("").replace({"": None})
        )

    records = [to_dict(record) for record in df2.to_dict(orient="records")]
    return pystac.ItemCollection(records)
