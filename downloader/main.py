from user_config import build_config_for_classification
from user_config import ENABLED_CLASSIFICATIONS
from data.static.constants import CLASSIFICATION_RELEASE_YEARS, CONVERSION_LINKS
from user_config import RUN_WEIGHTED_CONVERSION
from user_config import END_YEAR
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
    for target_classification, enabled in ENABLED_CLASSIFICATIONS.items():
        if not enabled:
            continue
        if RUN_WEIGHTED_CONVERSION:
            logging.info("Requested to run weighted conversion... will need to download all classifications as reported"
            logging.info(
                f"Downloading classifications as reported by country from {CLASSIFICATION_RELEASE_YEARS[target_classification]} to {END_YEAR if END_YEAR is not None else datetime.now().year - 1}"
            )
            # for classification in CLASSIFICATION_RELEASE_YEARS.keys():
            #     logging.info(
            #         f"Downloading any country reported files for {classification} starting in {CLASSIFICATION_RELEASE_YEARS[classification]}"
            #     )
            #     if (
            #         END_YEAR is not None
            #         and CLASSIFICATION_RELEASE_YEARS[target_classification]
            #         > END_YEAR
            #     ):
            #         continue
            #     config = build_config_for_classification(
            #         classification,
            #         CLASSIFICATION_RELEASE_YEARS[target_classification],
            #     )
            #     downloader = ComtradeDownloader(config)
            #     downloader.download_comtrade_yearly_bilateral_flows()
            # need as reported data for all subsequent classifications
            for classification in CONVERSION_LINKS:
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

>>>>>>> b5160227736c35e761372e99bbf141f7c953789e
            target_classification_config = build_config_for_classification(
                target_classification,
                CLASSIFICATION_RELEASE_YEARS[target_classification],
            )
            logging.info(
                f"Beginning conversion for classification as reported by country to {target_classification}"
            )
            convert = ClassificationConverter(
                target_classification_config, target_classification
            )
            convert.run()
            logging.info(f"Weighted conversion complete for {target_classification}")

        else:
            target_classification_config = build_config_for_classification(
                target_classification,
                CLASSIFICATION_RELEASE_YEARS[target_classification],
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
