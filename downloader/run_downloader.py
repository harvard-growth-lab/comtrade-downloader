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
    Comtrade provides data as reported referred to as Classic and data that has been
    coverted into each classification, referred to as Final
    
    Use Comtrade Config class to request data from Comtrade
    
    Downloader output aggregates data across all reporters for one year
    """
    
    downloaders = {"H0":1995, "H4": 2012}
    # SITC is run through concordance table conversion in atlas cleaning
    
    for classification, classification_start_year in downloaders.items():
        # download as reported data
        config_H0 = ComtradeConfig(
            api_key=os.environ.get('ELLIE_API_KEY'),
            output_dir="/n/hausmann_lab/lab/atlas/data/",
            download_type='final', #options "classic", "final"
            classification_code=classification,
            log_level='INFO',
            start_year=classification_start_year, #1960,
            end_year=2023, #datetime.now().year,
            reporter_iso3_codes=[], #list of iso3codes
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
            force_full_download=False,
        )
        print(f"initiating program {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")
        downloader_H0 = ComtradeDownloader(config_H0)
        # downloader_H0.download_comtrade_yearly_bilateral_flows()
        downloader_H0.run_compactor()
        print(f"program complete {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")
    
        # download as reported data
    config_HS = ComtradeConfig(
        api_key=os.environ.get('ELLIE_API_KEY'),
        output_dir="/n/hausmann_lab/lab/atlas/data/",
        download_type='classic', #options "classic", "final"
        classification_code='HS',
        log_level='INFO',
        start_year=2022, #1960,
        end_year=2022, #datetime.now().year,
        reporter_iso3_codes=[], #list of iso3codes
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
        force_full_download=False,
    )
    # print(f"initiating program {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")
    # downloader_HS = ComtradeDownloader(config_HS)
    # downloader_HS.download_comtrade_yearly_bilateral_flows()
    # print(f"program completed {datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}")



if __name__ == "__main__":
    main()
