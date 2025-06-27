from config.user_config import build_config_for_classification
from config.user_config import ENABLED_CLASSIFICATIONS
from config.constants import CLASSIFICATION_RELEASE_YEARS
from config.user_config import RUN_WEIGHTED_CONVERSION
from config.user_config import END_YEAR
from src.download.api_downloader import ComtradeDownloader
from src.download.converter import ClassificationConverter
from datetime import datetime
import logging


def run():
    """
    Downloads and converts comtrade data for the requested classifications starting from the
    released year of requested classification up to the previous year or a specified end year

    If RUN_WEIGHTED_CONVERSION is True, the data is converted to the requested
    classification and saved in the converted folder

    If RUN_WEIGHTED_CONVERSION is False, the data is downloaded from Comtrade
    already converted by Comtrade to the requested classification
    """
    for requested_classification, enabled in ENABLED_CLASSIFICATIONS.items():
        if not enabled:
            continue
        if RUN_WEIGHTED_CONVERSION:
            logging.info(
                f"Downloading classifications as reported by country from {CLASSIFICATION_RELEASE_YEARS[requested_classification]} to {END_YEAR if END_YEAR is not None else datetime.now().year - 1}"
            )
            for classification in CLASSIFICATION_RELEASE_YEARS.keys():
                logging.info(
                    f"Downloading any country reported files for {classification} starting in {CLASSIFICATION_RELEASE_YEARS[classification]}"
                )
                if (
                    END_YEAR is not None
                    and CLASSIFICATION_RELEASE_YEARS[requested_classification]
                    > END_YEAR
                ):
                    continue
                config = build_config_for_classification(
                    classification,
                    CLASSIFICATION_RELEASE_YEARS[requested_classification],
                )
                downloader = ComtradeDownloader(config)
                downloader.download_comtrade_yearly_bilateral_flows()
            target_classification_config = build_config_for_classification(
                requested_classification,
                CLASSIFICATION_RELEASE_YEARS[requested_classification],
            )
            logging.info(
                f"Beginning conversion for classification as reported by country to {requested_classification}"
            )
            convert = ClassificationConverter(
                target_classification_config, requested_classification
            )
            convert.run()
            logging.info(f"Weighted conversion complete for {requested_classification}")

        else:
            target_classification_config = build_config_for_classification(
                requested_classification,
                CLASSIFICATION_RELEASE_YEARS[requested_classification],
            )
            downloader = ComtradeDownloader(target_classification_config)
            downloader.download_comtrade_yearly_bilateral_flows()

        downloader = ComtradeDownloader(target_classification_config)
        logging.info(
            "Initating compactor, aggregating data by year in the requested classification"
        )
        downloader.run_compactor()
        print(f"program complete {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")


if __name__ == "__main__":
    run()
