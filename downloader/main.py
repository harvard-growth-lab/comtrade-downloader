from data.static.constants import CONVERSION_LINKS
from src.download.configure_downloader import build_config_for_classification
from src.download.api_downloader import ComtradeDownloader
from src.download.converter import ClassificationConverter
from datetime import datetime
import logging

import importlib
import argparse


def run(config_file):
    """
    Downloads and converts comtrade data for the requested classifications starting from the
    released year of requested classification up to the previous year or a specified end year

    If RUN_WEIGHTED_CONVERSION is True, the data is converted to the requested
    classification and saved in the converted folder

    If RUN_WEIGHTED_CONVERSION is False, the data is downloaded from Comtrade
    already converted by Comtrade to the requested classification
    """
    try:
        if config_file == "user_config":
            config_module = importlib.import_module(config_file)
        else:
            config_module = importlib.import_module(f"config.{config_file}")
    except ImportError:
        raise ImportError(f"Config module '{config_file}' not found")

    # Get config variables from the imported module
    ENABLED_CLASSIFICATIONS = config_module.ENABLED_CLASSIFICATIONS
    PROCESSING_STEPS = config_module.PROCESSING_STEPS
    RUN_WEIGHTED_CONVERSION = config_module.RUN_WEIGHTED_CONVERSION
    CLASSIFICATION_START_YEARS = config_module.CLASSIFICATION_START_YEARS
    config_dict = config_module.config_dict
    END_YEAR = config_dict["end_year"]

    for requested_classification, enabled in ENABLED_CLASSIFICATIONS.items():
        target_classification_config = build_config_for_classification(
            requested_classification,
            CLASSIFICATION_START_YEARS[requested_classification],
            **config_dict,
        )

        if not enabled:
            continue

        if PROCESSING_STEPS["run_converter"]:
            if RUN_WEIGHTED_CONVERSION:
                logging.info(
                    "Running weighted conversion... beginning to download all classifications as reported by country to then be converted to the target classification"
                )
                logging.info(
                    f"Downloading classifications as reported by country from {CLASSIFICATION_START_YEARS[requested_classification]} to {END_YEAR if END_YEAR is not None else datetime.now().year - 1}"
                )
                # need as reported data for all classifications
                for classification in CONVERSION_LINKS:
                    logging.info(
                        f"Downloading any country reported files for {classification} starting in {CLASSIFICATION_START_YEARS[classification]}"
                    )
                    if (
                        END_YEAR is not None
                        and CLASSIFICATION_START_YEARS[requested_classification]
                        > END_YEAR
                    ):
                        continue
                    config = build_config_for_classification(
                        classification,
                        CLASSIFICATION_START_YEARS[requested_classification],
                        **config_dict,
                    )
                    if PROCESSING_STEPS["run_downloader"]:
                        downloader = ComtradeDownloader(config)
                        downloader.download_comtrade_yearly_bilateral_flows()

                logging.info(
                    f"Beginning conversion for classification as reported by country to {requested_classification}"
                )
                convert = ClassificationConverter(
                    target_classification_config, requested_classification
                )
                convert.run()
                logging.info(
                    f"Weighted conversion complete for {requested_classification}"
                )

            else:
                if PROCESSING_STEPS["run_downloader"]:
                    downloader = ComtradeDownloader(target_classification_config)
                    downloader.download_comtrade_yearly_bilateral_flows()

        if PROCESSING_STEPS["run_compactor"]:
            downloader = ComtradeDownloader(target_classification_config)
            logging.info(
                "Initating compactor, aggregating data by year in the requested classification"
            )
            downloader.run_compactor()
            logging.info(
                f"program complete {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
            )


def main():
    parser = argparse.ArgumentParser(
        description="Run Comtrade data processing with specified config"
    )
    parser.add_argument(
        "--config",
        choices=["user_config", "atlas_dev_config"],
        default="user_config",
        help="Config file to use (default: user_config)",
    )

    args = parser.parse_args()

    logging.info(f"Using config: {args.config}")
    run(args.config)


if __name__ == "__main__":
    main()
