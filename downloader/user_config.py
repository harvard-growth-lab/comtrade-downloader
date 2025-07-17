"""
Comtrade Downloader Configuration File
=====================================

This configuration file provides a user-friendly interface for downloading

Before running, ensure you have:
1. A valid UN Comtrade API key
2. Sufficient disk space (datasets can be several GB per year)
3. Stable internet connection for large downloads

For API key registration: https://comtradeplus.un.org/
"""

import os
from datetime import datetime
from pathlib import Path

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

# OUTPUT_BASE_DIR = "/home/parallels/Desktop/Parallels Shared Folders/AllFiles/Users/ELJ479/projects/data_downloads/comtrade/"
# OUTPUT_BASE_DIR = "/n/hausmann_lab/lab/atlas/data"
OUTPUT_BASE_DIR = "/home/parallels/Desktop/Parallels Shared Folders/AllFiles/Users/ELJ479/projects/data_downloads/comtrade_test/"

# Create output directory if it doesn't exist
Path(OUTPUT_BASE_DIR).mkdir(parents=True, exist_ok=True)

# =============================================================================
# REQUESTED CLASSIFICATIONS AND YEAR RANGE
# =============================================================================

# Set boolean to True for classifications you want to download
ENABLED_CLASSIFICATIONS = {
    "H0": True,
    "H1": False,
    "H2": False,
    "H3": False,
    "H4": False,
    "H5": False,
    "H6": False,
    "S1": False,
    "S2": False,
    "S3": False,
    "S4": False,
}


# Year range configuration
END_YEAR = 2023  # Will default to datetime.now().year - 1

# =============================================================================
# PROCESSING STEPS
# =============================================================================

PROCESSING_STEPS = {
    "run_downloader": False,  # Download bilateral trade reporter files
    "run_converter": False,  # Convert files to desired classification
    "run_compactor": True,  # Aggregate reporter files by classificaiton by year
}

# =============================================================================
# COMTRADE DATA REQUEST PARAMETERS (advanced users only)
# =============================================================================

"""
WARNING: Changing these parameters is not recommended and will produce unexpected 
product conversion and mirroring results
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
# PROCESSING OPTIONS
# =============================================================================

# File management
DELETE_TEMP_FILES = False  # Keep temporary download files
COMPRESS_OUTPUT = True  # Compress final output files
CONVERT_TO_PROCESSED_FILES = True  # Apply classification conversions

# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL = "INFO"
SUPPRESS_PRINT = False

# =============================================================================
# RUNTIME CONFIGURATION BUILDER
# =============================================================================


def get_enabled_classifications():
    """Return list of enabled classifications with their start years."""
    return {
        code: start_year
        for code, start_year in ENABLED_CLASSIFICATIONS.items()
        if ENABLED_CLASSIFICATIONS.get(code, False)
    }


def get_end_year():
    """Calculate end year based on user configuration."""
    if END_YEAR is not None:
        return END_YEAR
    return datetime.now().year - 1


def get_download_type(run_weights: bool) -> str:
    """
    Determine download type based on whether weights are being run

    Two options:
     "classic" = as-reported data (original country classifications)
     "final" = standardized data (converted to specific classification)

    """
    if run_weights:
        return "classic"
    return "final"


def build_config_for_classification(classification_code, start_year):
    """Build ComtradeConfig object for a one classification."""
    from src.download.configure_downloader import ComtradeConfig

    return ComtradeConfig(
        api_key=API_KEY,
        output_dir=OUTPUT_BASE_DIR,
        download_type=get_download_type(RUN_WEIGHTED_CONVERSION),
        product_classification=classification_code,
        log_level=LOG_LEVEL,
        start_year=start_year,
        end_year=get_end_year(),
        reporter_iso3_codes=REPORTER_COUNTRIES,
        partner_iso3_codes=PARTNER_COUNTRIES,
        commodity_codes=COMMODITY_CODES,
        flow_codes=FLOW_CODES,
        mot_codes=MOT_CODES,
        mos_codes=MOS_CODES,
        customs_codes=CUSTOMS_CODES,
        drop_world_partner=DROP_WORLD_PARTNER,
        drop_secondary_partners=DROP_SECONDARY_PARTNERS,
        delete_tmp_files=DELETE_TEMP_FILES,
        compress_output=COMPRESS_OUTPUT,
        suppress_print=SUPPRESS_PRINT,
        converted_files=CONVERT_TO_PROCESSED_FILES,
    )
