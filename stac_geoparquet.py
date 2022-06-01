"""
General questions:

1. How do we get the STAC items?
    - From the API?
    - Direct connection to the DB?
        - How do we handle dynamic things like rehydration, links?
2. Per dataset questions:
    - How do we partition the data (spatially, temporally?)
    - Does it require dynamic updates?
"""
from __future__ import annotations
from collections import namedtuple

from typing import Sequence, Any, TypedDict

import pystac
import geopandas
import pandas as pd
import shapely.geometry


class ItemLike(TypedDict):
    type: str
    stac_version: str
    stac_extensions: list[str]
    geometry: dict[str, Any] | None
    bbox: list[float] | None
    properties: dict[str, Any]
    links: list[dict[str, Any]]
    assets: dict[str, Any]
    collection: str | None


def to_geodataframe(items: Sequence[ItemLike]) -> geopandas.GeoDataFrame:
    """
    Convert a sequence of STAC items to a :class:`geopandas.GeoDataFrame`.

    The objects under `properties` are moved up to the top-level of the
    DataFrame, similar to :meth:`geopandas.GeoDataFrame.from_features`.

    Parameters
    ----------
    items: A sequence of STAC items.

    Returns
    -------
    The converted GeoDataFrame.
    """
    # lift properties up to the top level.
    items2 = []
    for item in items:
        item2 = {k: v for k, v in item.items() if k != "properties"}
        for k, v in item["properties"].items():
            if k in item2:
                raise ValueError("k", k)
            item2[k] = v
        items2.append(item2)
        
    geometry = [
        shapely.geometry.shape(x["geometry"]) for x in items2
    ]
    gdf = geopandas.GeoDataFrame(items2, geometry=geometry, crs="WGS84")

    for column in ["datetime", "start_datetime", "end_datetime"]:
        if column in gdf.columns:
            gdf[column] = pd.to_datetime(gdf[column])

    return gdf


def to_item_dict(row: namedtuple):
    keys = {
        "type", "stac_version", "id", "geometry", "bbox", "links", "assets", "collection",
    }
    item = row._asdict()
    item["datetime"] = item["datetime"].isoformat()
    out = {
        "properties": {}
    }
    for k, v in item.items():
        if k in keys:
            out[k] = v
        else:
            out["properties"][k] = v
    return out


def to_item_collection(df):
    return pystac.ItemCollection([to_item_dict(row) for row in df.itertuples(index=False)])