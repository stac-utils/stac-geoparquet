import warnings

warnings.warn(
    "stac_geoparquet.to_parquet is deprecated. Please use stac_geoparquet.arrow instead.",
    FutureWarning,
)

from stac_geoparquet.arrow._to_parquet import *  # noqa
