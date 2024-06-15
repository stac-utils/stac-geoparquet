from __future__ import annotations

import argparse
import logging
import os
import sys

from stac_geoparquet import pc_runner

logger = logging.getLogger("stac_geoparquet.pgstac_reader")


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-protocol",
        help="fsspec protocol for writing (e.g. 'abfs').",
        default=None,
    )
    parser.add_argument(
        "-c",
        "--connection-info",
        default=os.environ.get("STAC_GEOPARQUET_CONNECTION_INFO"),
    )

    parser.add_argument(
        "--table-credential",
        default=os.environ.get("STAC_GEOPARQUET_TABLE_CREDENTIAL"),
        help="Azure data tables client SAS credential for reading.",
    )
    parser.add_argument(
        "--table-name",
        help="Azure data tables name with the collection config.",
        default=os.environ.get("STAC_GEOPARQUET_TABLE_NAME"),
    )
    parser.add_argument(
        "--table-account-url",
        help="Azure data tables account URL name with the collection config.",
        default=os.environ.get("STAC_GEOPARQUET_TABLE_ACCOUNT_URL"),
    )
    parser.add_argument(
        "--storage-options-account-name",
        default=os.environ.get("STAC_GEOPARQUET_STORAGE_OPTIONS_ACCOUNT_NAME"),
    )
    parser.add_argument(
        "--storage-options-credential",
        default=os.environ.get("STAC_GEOPARQUET_STORAGE_OPTIONS_CREDENTIAL"),
    )
    parser.add_argument("--extra-skip", help="Extra collections to skip")
    return parser.parse_args(args)


def setup_logging() -> None:
    import logging
    import warnings

    import rich.logging

    warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")
    logger.setLevel(logging.INFO)
    handler = rich.logging.RichHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)


SKIP = {
    "daymet-daily-na",
    "daymet-daily-pr",
    "daymet-daily-hi",
    "daymet-monthly-na",
    "daymet-monthly-pr",
    "daymet-monthly-hi",
    "daymet-annual-na",
    "daymet-annual-pr",
    "daymet-annual-hi",
    "terraclimate",
    "gridmet",
    "landsat-8-c2-l2",
    "gpm-imerg-hhr",
    "deltares-floods",
    "goes-mcmip",
    # errors
    "cil-gdpcir-cc0",
    "3dep-lidar-intensity",
    "cil-gdpcir-cc-by",
    "ecmwf-forecast",
    "3dep-lidar-copc",
    "era5-pds",
    "3dep-lidar-classification",
    "3dep-lidar-dtm-native",
    "cil-gdpcir-cc-by-sa",
}


def main(inp: list[str] | None = None) -> int:
    import azure.data.tables

    args = parse_args(inp)

    skip = set(SKIP)
    if args.extra_skip:
        skip |= set(args.extra_skip.split())

    setup_logging()

    table_client = azure.data.tables.TableClient(
        args.table_account_url,
        args.table_name,
        credential=azure.core.credentials.AzureSasCredential(args.table_credential),
    )
    configs = pc_runner.get_configs(table_client)

    configs = {k: v for k, v in configs.items() if k not in skip}
    storage_options = {
        "account_name": args.storage_options_account_name,
        "credential": args.storage_options_credential,
    }

    def f(config: pc_runner.CollectionConfig) -> None:
        config.export_collection(
            args.connection_info,
            args.output_protocol,
            f"items/{config.collection_id}.parquet",
            storage_options,
            skip_empty_partitions=True,
        )

    N = len(configs)
    success = []
    failure = []

    for i, config in enumerate(configs.values(), 1):
        logger.info(f"processing {config.collection_id} [{i}/{N}]")
        try:
            f(config)
        except Exception as e:
            failure.append((config.collection_id, e))
            logger.exception(f"Failed processing {config.collection_id}")
        else:
            success.append(config.collection_id)

    return len(failure)


if __name__ == "__main__":
    sys.exit(main())
