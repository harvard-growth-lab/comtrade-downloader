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
OUTPUT_BASE_DIR = "/path/to/your/data/directory"

# Create output directory if it doesn't exist
Path(OUTPUT_BASE_DIR).mkdir(parents=True, exist_ok=True)

# =============================================================================
# REQUESTED CLASSIFICATIONS AND YEAR RANGE
# =============================================================================

# Set boolean to True for classifications you want to download
ENABLED_CLASSIFICATIONS = {
    "H0": False,
    "H4": True,
    "H5": False,
    "H6": False,
    "S1": False,
    "S2": False,
    "S3": False,
    "S4": False,
}

# Requested classifications and year to start downloading from
# Format: {classification_code: start_year}
CLASSIFICATION_CONFIGS = {
    # Harmonized System (HS) Classifications
    "H0": 1988,  # HS Combined (1988-present)
    "H1": 1996,  # HS 1992 vintage (1996-present)
    "H2": 2002,  # HS 2002 vintage (2002-present)
    "H3": 2007,  # HS 2007 vintage (2007-present)
    "H4": 2012,  # HS 2012 vintage (2012-present)
    "H5": 2017,  # HS 2017 vintage (2017-present)
    "H6": 2022,  # HS 2022 vintage (2022-present)
    # Standard International Trade Classification (SITC)
    "S1": 1962,  # SITC Revision 1 (1962-present)
    "S2": 1976,  # SITC Revision 2 (1976-present)
    "S3": 1988,  # SITC Revision 3 (1988-present)
    "S4": 2007,  # SITC Revision 4 (2007-present)
}

# Year range configuration
END_YEAR = None  # Will default to datetime.now().year - 1

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
# "classic" = as-reported data (original country classifications)
# "final" = standardized data (converted to specific classification)
DOWNLOAD_TYPE = "classic"

# =============================================================================
# PROCESSING OPTIONS
# =============================================================================

# File management
DELETE_TEMP_FILES = False  # Keep temporary download files
COMPRESS_OUTPUT = True  # Compress final output files
CONVERT_TO_PROCESSED_FILES = False  # Apply classification conversions

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
        for code, start_year in CLASSIFICATION_CONFIGS.items()
        if ENABLED_CLASSIFICATIONS.get(code, False)
    }


def get_end_year():
    """Calculate end year based on user configuration."""
    if END_YEAR is not None:
        return END_YEAR
    return datetime.now().year - 1


def build_config_for_classification(classification_code, start_year):
    """Build ComtradeConfig object for a one classification."""
    from src.download.configure_downloader import ComtradeConfig

    return ComtradeConfig(
        api_key=API_KEY,
        output_dir=OUTPUT_BASE_DIR,
        download_type=DOWNLOAD_TYPE,
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
