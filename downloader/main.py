from downloader.data.static.constants import CONVERSION_LINKS, SERVICES
from downloader.src.download.configure_downloader import build_config_for_classification
from downloader.src.download.api_downloader import ComtradeDownloader
from downloader.src.download.converter import ClassificationConverter
from downloader.src.download.config_generator import ConfigGenerator
from datetime import datetime
import logging

import importlib
import argparse
from pathlib import Path




def run():
    """
    Downloads and converts comtrade data for the requested classifications starting from the
    released year of requested classification up to the previous year or a specified end year

    If RUN_WEIGHTED_CONVERSION is True, the data is converted to the requested
    classification and saved in the converted folder

    If RUN_WEIGHTED_CONVERSION is False, the data is downloaded from Comtrade
    already converted by Comtrade to the requested classification
    """
    try:
        config_module = importlib.import_module("config.generated_config")
    except ImportError:
        raise ImportError(f"Config module not found. Please run the config generator.")

    # Get config variables from the imported module
    ENABLED_CLASSIFICATIONS = config_module.ENABLED_CLASSIFICATIONS
    PROCESSING_STEPS = config_module.PROCESSING_STEPS
    DOWNLOAD_FOR_CONVERSION = config_module.DOWNLOAD_FOR_CONVERSION
    CLASSIFICATION_START_YEARS = config_module.CLASSIFICATION_START_YEARS
    config_dict = config_module.config_dict
    END_YEAR = config_dict["end_year"]

    min_start_year = min(CLASSIFICATION_START_YEARS.values())
    import pdb; pdb.set_trace()


    if DOWNLOAD_FOR_CONVERSION:
        for classification in CONVERSION_LINKS:
            logging.info(
                f"Downloading any country reported files for {classification} starting in {CLASSIFICATION_START_YEARS[classification]}"
            )
            if (
                min_start_year > CLASSIFICATION_START_YEARS[classification]
            ):
                continue
            
            download_for_conversion_config = build_config_for_classification(
                classification,
                CLASSIFICATION_START_YEARS[classification],
                **config_dict,
            )
            conversion_downloader = ComtradeDownloader(download_for_conversion_config)
            conversion_downloader.download_comtrade_yearly_bilateral_flows()


    for requested_classification, enabled in ENABLED_CLASSIFICATIONS.items():
        target_classification_config = build_config_for_classification(
            requested_classification,
            CLASSIFICATION_START_YEARS[requested_classification],
            **config_dict,
        )
        downloader = ComtradeDownloader(target_classification_config)

        if SERVICES in requested_classification and download_for_conversion or processing_steps["run_downloader"]:
            target_classification_config.download_type = "services"
            downloader.download_comtrade_yearly_bilateral_flows()

        if PROCESSING_STEPS["run_downloader"] and not download_for_conversion:
            # download if downloader not already called for the converter which will 
            # always be inclusive for any downloader classification requests 
            downloader.download_comtrade_yearly_bilateral_flows()

        if PROCESSING_STEPS["run_converter"]:
            logging.info(
                "Running weighted conversion... "
            )
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

        if PROCESSING_STEPS["run_compactor"]:
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
        choices=["user_config", "atlas_dev_config", "dev"],
        default="user_config",
        help="Config file to use (default: user_config)",
    )

    args = parser.parse_args()
    config_file = args.config

        # Generate Python config from YAML
    if config_file == "user_config":
        config_path = Path(f"{config_file}.yaml")
    else:
        config_path = Path("config") / f"{config_file}.yaml"


    # Create generator and generate config
    generator = ConfigGenerator(config_path)
    generator.generate_python_config('config/generated_config.py')

    logging.info(f"Using config: {config_file}")

    run()


if __name__ == "__main__":
    main()
