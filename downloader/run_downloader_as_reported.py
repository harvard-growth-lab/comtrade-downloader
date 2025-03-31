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
    Downloads Classic Data from Comtrade or data as reported by reporting countries

    Comtrade provides data as reported referred to as Classic and data that has been
    coverted into each classification, referred to as Final

    Use Comtrade Config class to request data from Comtrade

    Downloader output aggregates data across all reporters for one year
    """

    downloaders = {"S1":1962, "S2": 1976, "S3": 1988, "S4": 2007} #"H6": 2022} # "H2": 2020, "H3": 2007, "H4": 2012, "H5": 2017, "H6": 2022} 
    # get all as reported

    for classification, classification_start_year in downloaders.items():
        # download as reported data
        config_HS = ComtradeConfig(
            api_key=os.environ.get("ELLIE_API_KEY"),
            output_dir="/n/hausmann_lab/lab/atlas/data/",
            download_type="classic",  # options "classic", "final"
            product_classification=classification,
            log_level="INFO",
            start_year=classification_start_year,  # 1960,
            end_year=datetime.now().year - 1,
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
            converted_files=False,
        )
        print(f"initiating program {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")
        downloader_HS = ComtradeDownloader(config_HS)
        downloader_HS.download_comtrade_yearly_bilateral_flows()
        downloader_HS.run_compactor()
        print(f"program complete {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")


if __name__ == "__main__":
    main()
