from typing import Literal, Union

import pyarrow as pa

from stac_geoparquet.arrow._schema.models import InferredSchema

DEFAULT_JSON_CHUNK_SIZE = 65536
"""The default chunk size to use for reading JSON into memory."""

SUPPORTED_PARQUET_SCHEMA_VERSIONS = Literal["1.0.0", "1.1.0"]
"""A Literal type with the supported GeoParquet schema versions."""

DEFAULT_PARQUET_SCHEMA_VERSION: SUPPORTED_PARQUET_SCHEMA_VERSIONS = "1.1.0"
"""The default GeoParquet schema version written to file."""

ACCEPTED_SCHEMA_OPTIONS = Union[
    pa.Schema, InferredSchema, Literal["FirstBatch", "FullFile", "ChunksToDisk"]
]
