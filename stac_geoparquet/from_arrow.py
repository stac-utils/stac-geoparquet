import warnings

warnings.warn(
    "stac_geoparquet.from_arrow is deprecated. Please use stac_geoparquet.arrow instead.",
    FutureWarning,
)


from stac_geoparquet.arrow._api import stac_items_to_arrow  # noqa
from stac_geoparquet.arrow._api import stac_table_to_items  # noqa
from stac_geoparquet.arrow._api import stac_table_to_ndjson  # noqa
