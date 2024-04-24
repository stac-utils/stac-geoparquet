import warnings

warnings.warn(
    "stac_geoparquet.from_arrow is deprecated. Please use stac_geoparquet.arrow instead.",
    FutureWarning,
)

from stac_geoparquet.arrow._from_arrow import *  # noqa
