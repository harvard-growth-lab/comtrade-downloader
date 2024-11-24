# intiates downloader objects for Comtrade Classification (HS12, HS92, SITC)

from src.api_downloader import ComtradeDownloader
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
    
    # download as reported data
    downloader_HS = ComtradeDownloader(
        api_key=os.environ.get('ELLIE_API_KEY'),
        output_dir="/n/hausmann_lab/lab/atlas/data/",
        classification_code='HS',  # generates all as reported classifications
        file_format='parquet',
        log_level='INFO',
        start_year=2021, #1960,
        end_year=2022, #datetime.now().year,
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

    # Call the method
    downloader_HS.download_comtrade_yearly_bilateral_flows()
    downloader_HS.download_comtrade_bilateral_totals()


if __name__ == "__main__":
    main()
