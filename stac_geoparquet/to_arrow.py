import warnings

warnings.warn(
    "stac_geoparquet.to_arrow is deprecated. Please use stac_geoparquet.arrow instead.",
    FutureWarning,
)

from stac_geoparquet.arrow._to_arrow import *  # noqa
