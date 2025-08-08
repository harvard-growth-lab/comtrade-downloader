"""
Comtrade Downloader Configuration File
=====================================

"""

import os
from datetime import datetime
from pathlib import Path
from downloader.src.utils.handle_config import (
    get_download_type,
    get_end_year,
    get_enabled_classifications,
)

# =============================================================================
# API KEYS
# =============================================================================

# UN Comtrade API Key - REQUIRED
# Option 1: Set as environment variable (recommended for security)
API_KEY = os.environ.get("COMTRADE_API_KEY")

# Option 2: Set directly (less secure - do not commit to version control)
# API_KEY = "your_api_key_here"

if not API_KEY:
    raise ValueError(
        "API key required! Set COMTRADE_API_KEY environment variable or "
        "modify API_KEY in this config file. "
        "Get your key at: https://comtradeplus.un.org/"
    )

# =============================================================================
# PATHS SETUP
# =============================================================================

# Base directory for all downloaded data
# Adjust this path to your preferred data storage location

OUTPUT_BASE_DIR = "/n/hausmann_lab/lab/atlas/data/"

# =============================================================================
# REQUESTED CLASSIFICATIONS AND YEAR RANGE
# =============================================================================

# Which trade classifications to download (leave True for the ones you want)
PROCESS_HS92 = False  # HS92 data from 1992-END_YEAR
PROCESS_HS12 = False  # HS12 data from 2012-END_YEAR
PROCESS_HS96 = False
PROCESS_HS02 = False
PROCESS_HS07 = False
PROCESS_HS17 = False
PROCESS_HS22 = True

PROCESS_SITC1 = False  # SITC data from 1962-END_YEAR
PROCESS_SITC2 = False  # SITC data from 1976-END_YEAR
PROCESS_SITC3 = False  # SITC data from 1988-END_YEAR

# Year range configuration
END_YEAR = 2023  # Will default to datetime.now().year - 1

CLASSIFICATION_START_YEARS = {
    # set start years for each classification
    "S1": 1962,  # SITC Revision 1 (1962-present)
    "S2": 1976,  # SITC Revision 2 (1976-present)
    "S3": 1988,  # SITC Revision 3 (1988-present)
    "H0": 1992,  # HS Combined (1992-present)
    "H1": 1996,  # HS 1992 vintage (1996-present)
    "H2": 2002,  # HS 2002 vintage (2002-present)
    "H3": 2007,  # HS 2007 vintage (2007-present)
    "H4": 2012,  # HS 2012 vintage (2012-present)
    "H5": 2017,  # HS 2017 vintage (2017-present)
    "H6": 2017,  # HS 2022 vintage (2022-present)
}


# =============================================================================
# PROCESSING STEPS
# =============================================================================

PROCESSING_STEPS = {
    "run_downloader": False,  # Download trade data and convert to requested classification
    "run_converter": True,  # Convert to requested classification
    "run_compactor": True,  # Aggregate reporter files by classificaiton by year
}


# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL = "INFO"
SUPPRESS_PRINT = False

# =============================================================================
# COMTRADE DATA REQUEST PARAMETERS (advanced users only)
# =============================================================================

"""
WARNING: Changing these parameters is not recommended
"""

# Reporter countries (empty list for all countries)
# Use ISO3 codes ["VEN", "CUB", "ARG", "CAN"]
REPORTER_COUNTRIES = []

# Partner countries (leave empty list for all country partners)
# Use ISO3 codes ["VEN", "CUB", "ARG", "CAN"]
PARTNER_COUNTRIES = []

# Specific commodity codes (empty list for all products)
# Format depends on classification:
# - HS: 6-digit codes, e.g., ["010121", "010129"]
# - SITC: 4-digit codes, e.g., ["0011", "0012"]
COMMODITY_CODES = []

# Trade flow types; Leave empty for all flows
FLOW_CODES = []  # Default: all flows
MOT_CODES = [0]  # Mode of transport (0 = all modes)
MOS_CODES = [0]  # Mode of supply (0 = all modes)
CUSTOMS_CODES = []  # Customs procedure codes
DROP_WORLD_PARTNER = False
DROP_SECONDARY_PARTNERS = True

# Download type - determines data download type as provided by Comtrade
RUN_WEIGHTED_CONVERSION = True


# =============================================================================
# PROCESSING OPTIONS  (advanced users only)
# =============================================================================

# File management
DELETE_TEMP_FILES = False  # Keep temporary download files
COMPRESS_OUTPUT = True  # Compress final output files
CONVERT_TO_PROCESSED_FILES = True  # Apply classification conversions

# =============================================================================
# CONFIGURATION DICTIONARY
# =============================================================================

classifications_dict = {
    "HS92": PROCESS_HS92,
    "HS12": PROCESS_HS12,
    "HS96": PROCESS_HS96,
    "HS02": PROCESS_HS02,
    "HS07": PROCESS_HS07,
    "HS17": PROCESS_HS17,
    "HS22": PROCESS_HS22,
    "SITC1": PROCESS_SITC1,
    "SITC2": PROCESS_SITC2,
    "SITC3": PROCESS_SITC3,
}


ENABLED_CLASSIFICATIONS = get_enabled_classifications(classifications_dict)

config_dict = {
    "api_key": API_KEY,
    "output_dir": OUTPUT_BASE_DIR,
    "download_type": get_download_type(RUN_WEIGHTED_CONVERSION),
    "log_level": LOG_LEVEL,
    "end_year": get_end_year(END_YEAR),
    "reporter_iso3_codes": REPORTER_COUNTRIES,
    "partner_iso3_codes": PARTNER_COUNTRIES,
    "commodity_codes": COMMODITY_CODES,
    "flow_codes": FLOW_CODES,
    "mot_codes": MOT_CODES,
    "mos_codes": MOS_CODES,
    "customs_codes": CUSTOMS_CODES,
    "drop_world_partner": DROP_WORLD_PARTNER,
    "drop_secondary_partners": DROP_SECONDARY_PARTNERS,
    "delete_tmp_files": DELETE_TEMP_FILES,
    "compress_output": COMPRESS_OUTPUT,
    "suppress_print": SUPPRESS_PRINT,
    "converted_files": CONVERT_TO_PROCESSED_FILES,
}
