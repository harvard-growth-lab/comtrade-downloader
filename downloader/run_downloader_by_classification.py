# intiates downloader objects for Comtrade Classification (HS12, HS92, SITC)

from src.api_downloader import ComtradeDownloader
from src.configure_downloader import ComtradeConfig
import requests
import pandas as pd
import requests
import comtradeapicall
import glob
import os
import shutil
import sys
import re
import time
from datetime import date, timedelta, datetime
import logging


def main():
    """
    Downloads Final Data from Comtrade

    Comtrade provides data as reported referred to as Classic and data that has been
    coverted into each classification, referred to as Final

    Use Comtrade Config class to request data from Comtrade

    Downloader output aggregates data across all reporters for one year
    """

    downloaders = {"H0": 1995, "H4": 2012}
    # downloaders = {"H0": 2022} #, "H4": 2012}

    for classification, classification_start_year in downloaders.items():
        config_HS = ComtradeConfig(
            api_key=os.environ.get("ELLIE_API_KEY"),
            output_dir="/n/hausmann_lab/lab/atlas/data/",
            download_type="final",  # options "classic", "final"
            product_classification=classification,
            log_level="INFO",
            start_year=classification_start_year,  # 1960,
            end_year=datetime.now().year,
            reporter_iso3_codes=[],  # list of iso3codes
            partner_iso3_codes=[],
            commodity_codes=[],
            flow_codes=[],  # exports (X), imports (M), Cost of Insurance-Freight (CA)
            mot_codes=[0],
            mos_codes=[0],
            customs_codes=[],
            drop_world_partner=False,
            drop_secondary_partners=True,
            delete_tmp_files=False,
            compress_output=True,
            suppress_print=False,
        )
        print(
            f"initiating {classification} download {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        )
        downloader = ComtradeDownloader(config_HS)
        # downloader.download_comtrade_yearly_bilateral_flows()
        print(f"requires memory allocation of at least 20GB")
        downloader.run_compactor()
        print(f"program complete {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")

    downloaders = {"S1": [1962, 1975], "S2": [1976, datetime.now().year - 2]}
    # SITC is run through concordance table conversion in atlas cleaning

    for classification, classification_years in downloaders.items():
        config_SITC = ComtradeConfig(
            api_key=os.environ.get("ELLIE_API_KEY"),
            output_dir="/n/hausmann_lab/lab/atlas/data/",
            download_type="final",  # options "classic", "final"
            product_classification=classification,
            log_level="INFO",
            start_year=min(classification_years),  # 1960,
            end_year=max(classification_years),  # datetime.now().year,
            reporter_iso3_codes=[],  # list of iso3codes
            partner_iso3_codes=[],
            commodity_codes=[],
            flow_codes=[],  # exports (X), imports (M), Cost of Insurance-Freight (CA)
            mot_codes=[0],
            mos_codes=[0],
            customs_codes=[],
            drop_world_partner=False,
            drop_secondary_partners=True,
            delete_tmp_files=False,
            compress_output=True,
            suppress_print=False,
        )

        print(
            f"initiating {classification} download {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        )
        downloader_SITC = ComtradeDownloader(config_SITC)
        # downloader_SITC.download_comtrade_yearly_bilateral_flows()
        print(f"requires memory allocation of at least 80GB")
        downloader_SITC.run_compactor()
        print(f"program complete {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")


if __name__ == "__main__":
    main()
