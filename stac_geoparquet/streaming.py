from contextlib import ExitStack
from dataclasses import dataclass
from io import SEEK_END, BytesIO
from pathlib import Path
from typing import IO, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from stac_geoparquet.to_arrow import parse_stac_ndjson_to_arrow

DEFAULT_JSON_CHUNK_SIZE = 300 * 1024 * 1024
DELIMITER_SEEK_SIZE = 64 * 1024
JSON_DELIMITER = b"\n"


@dataclass
class JsonChunkRange:
    offset: int
    """The byte offset of the file where this chunk starts."""

    length: int
    """The number of bytes in this chunk"""


# path = "/Users/kyle/data/sentinel-stac/out.jsonl"
# path = "/Users/kyle/data/sentinel-stac/out_1.0.0-beta.2.jsonl"
# path = "/Users/kyle/data/sentinel-stac/out_1.0.0.jsonl"
# input_file = open(path, "rb")
# output_path = 'tmp_out_streaming.parquet'


def jsonl_to_geoparquet(
    input_file: IO[bytes],
    output_path: Path,
    *,
    chunk_size: int = DEFAULT_JSON_CHUNK_SIZE,
):
    json_chunks = find_json_chunks(input_file)
    len(json_chunks)

    schemas = []
    with ExitStack() as ctx:
        writer: Optional[pq.ParquetWriter] = None
        for json_chunk in json_chunks:
            input_file.seek(json_chunk.offset)
            buf = input_file.read(json_chunk.length)
            buf[:100]
            table = parse_stac_ndjson_to_arrow(BytesIO(buf))
            schemas.append(table.schema)

            # if writer is None:
            #     writer = ctx.enter_context(pq.ParquetWriter(output_path, schema=table.schema))

            # writer.write_table(table)

    pa.unify_schemas(schemas)
    len(schemas)
    len(json_chunks)
    schemas


def find_json_chunks(input_file: IO[bytes]) -> List[JsonChunkRange]:
    total_byte_length = input_file.seek(0, SEEK_END)
    input_file.seek(0)

    chunk_ranges = []
    previous_chunk_offset = 0
    while True:
        if previous_chunk_offset + DEFAULT_JSON_CHUNK_SIZE >= total_byte_length:
            chunk_range = JsonChunkRange(
                offset=previous_chunk_offset,
                length=total_byte_length - previous_chunk_offset,
            )
            chunk_ranges.append(chunk_range)
            break

        # Advance by chunk size bytes
        # TODO: don't advance past end of file
        byte_offset = input_file.seek(previous_chunk_offset + DEFAULT_JSON_CHUNK_SIZE)
        delim_index = -1
        while delim_index == -1:
            delim_search_buf = input_file.read(DELIMITER_SEEK_SIZE)
            delim_index = delim_search_buf.find(JSON_DELIMITER)
            byte_offset += delim_index

        chunk_range = JsonChunkRange(
            offset=previous_chunk_offset, length=byte_offset - previous_chunk_offset
        )
        chunk_ranges.append(chunk_range)
        # + 1 to skip the newline character
        previous_chunk_offset = byte_offset + 1

    assert (
        chunk_ranges[-1].offset + chunk_ranges[-1].length == total_byte_length
    ), "The last chunk range should end at the file length"
    return chunk_ranges
