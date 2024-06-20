from typing import Literal

DEFAULT_JSON_CHUNK_SIZE = 65536
"""The default chunk size to use for reading JSON into memory."""

SUPPORTED_PARQUET_SCHEMA_VERSIONS = Literal["1.0.0", "1.1.0"]
"""A Literal type with the supported GeoParquet schema versions."""

DEFAULT_PARQUET_SCHEMA_VERSION: SUPPORTED_PARQUET_SCHEMA_VERSIONS = "1.1.0"
"""The default GeoParquet schema version written to file."""
