from config.user_config import build_config_for_classification
from config.user_config import ENABLED_CLASSIFICATIONS
from config.user_config import CLASSIFICATION_CONFIGS
from src.download.api_downloader import ComtradeDownloader
from src.download.converter import ClassificationConverter
from datetime import datetime


def run():
    for classification, enabled in ENABLED_CLASSIFICATIONS.items():
        if enabled:
            config = build_config_for_classification(
                classification, CLASSIFICATION_CONFIGS[classification]
            )
            downloader = ComtradeDownloader(config)
            downloader.download_comtrade_yearly_bilateral_flows()
            convert = ClassificationConverter(config, classification)
            convert.run()
            downloader = ComtradeDownloader(config)
            downloader.run_compactor()
            print(f"program complete {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")


if __name__ == "__main__":
    run()
