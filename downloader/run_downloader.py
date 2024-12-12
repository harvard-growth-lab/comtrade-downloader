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
    
    # download as reported data
    config_HS = ComtradeConfig(
        api_key=os.environ.get('ELLIE_API_KEY'),
        output_dir="/n/hausmann_lab/lab/atlas/data/",
        download_type='final', #options "classic", "final"
        classification_code='H0',
        log_level='INFO',
        start_year=1995, #1960,
        end_year=1995, #datetime.now().year,
        reporter_iso3_codes=[],
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
    downloader_HS = ComtradeDownloader(config_HS)
    downloader_HS.download_comtrade_yearly_bilateral_flows()


if __name__ == "__main__":
    main()
