import argparse
import sys
import json
import os
from stac_geoparquet import pc_runner


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("collection", help="STAC collection ID")
    parser.add_argument(
        "--output-protocol",
        help="fsspec protocol for writing (e.g. 'abfs').",
        default=None,
    )
    parser.add_argument(
        "--output-path", help="fsspec protocol for writing", default="output.parquet"
    )
    parser.add_argument(
        "--storage-options",
        type=json.loads,
        default="{}",
        help="fsspec storage options for writing.",
    )
    parser.add_argument(
        "-c",
        "--connection-info",
        default=os.environ.get("STAC_GEOPARQUET_CONNECTION_INFO"),
    )

    return parser.parse_args()


def setup_logging():
    import logging
    import warnings

    warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

    logger = logging.getLogger("stac_geoparquet.pgstac_reader")
    logger.setLevel(logging.INFO)


def main(args=None):
    from tqdm.contrib.logging import logging_redirect_tqdm

    args = parse_args(args)
    setup_logging()

    config = pc_runner.CONFIGS[args.collection]
    with logging_redirect_tqdm():
        config.export_collection(
            args.connection_info,
            output_protocol="az",
            output_path=args.output_path,
            storage_options=args.storage_options,
        )


if __name__ == "__main__":
    sys.exit(main())
