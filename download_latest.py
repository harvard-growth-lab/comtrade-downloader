# intiates downloader objects for Comtrade Classification (HS12, HS92, SITC)

from downloader import ComtradeDownloader, API_KEYS
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
    # download hs92 updated data
    downloader_hs92 = ComtradeDownloader(
        api_key=API_KEYS["Ellie"],
        output_dir="/n/hausmann_lab/lab/atlas/data/",
        classification_code="H0",  # H0: HS92, H4: HS12 SITC Rev 2: S2
        start_year=1992,
        end_year=datetime.now().year - 1,
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
        suppress_print=True,
        force_full_download=False,
    )

    # Call the method
    # downloader_hs92.download_comtrade_yearly_bilateral_flows()
    # downloader_hs92.download_comtrade_bilateral_totals()

    # download hs12 updated data
    downloader_hs12 = ComtradeDownloader(
        api_key=API_KEYS["Ellie"],
        output_dir="/n/hausmann_lab/lab/atlas/data/",
        classification_code="H4",  # H0: HS92, H4: HS12 SITC Rev 2: S2
        start_year=2012,
        end_year=datetime.now().year - 1,
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
        suppress_print=True,
        force_full_download=False,
    )

    # Call the method
    downloader_hs12.download_comtrade_yearly_bilateral_flows()
    downloader_hs12.download_comtrade_bilateral_totals()

    # download SITC (v S2) updated data
    downloader_hs12 = ComtradeDownloader(
        api_key=API_KEYS["Ellie"],
        output_dir="/n/hausmann_lab/lab/atlas/data/",
        classification_code="S2",  # H0: HS92, H4: HS12 SITC Rev 2: S2
        start_year=2010,
        end_year=datetime.now().year,
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
        suppress_print=True,
        force_full_download=False,
    )

    # Call the method
    downloader_sitc.download_comtrade_yearly_bilateral_flows()
    downloader_sitc.download_comtrade_bilateral_totals()


if __name__ == "__main__":
    main()
