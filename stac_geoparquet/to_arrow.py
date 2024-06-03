import warnings

warnings.warn(
    "stac_geoparquet.to_arrow is deprecated. Please use stac_geoparquet.arrow instead.",
    FutureWarning,
)

from stac_geoparquet.arrow import parse_stac_items_to_arrow, parse_stac_ndjson_to_arrow  # noqa
