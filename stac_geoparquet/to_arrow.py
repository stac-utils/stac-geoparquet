# This doesn't work inline on these imports for some reason
# flake8: noqa: F401

import warnings

from stac_geoparquet.arrow._api import (
    parse_stac_items_to_arrow,
    parse_stac_ndjson_to_arrow,
)

warnings.warn(
    "stac_geoparquet.to_arrow is deprecated. Please use stac_geoparquet.arrow instead.",
    FutureWarning,
)
