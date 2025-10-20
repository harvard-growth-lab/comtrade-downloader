"""
Configuration Generator
Reads YAML configuration and generates equivalent Python config file for Comtrade Downloader
"""

import yaml
from pathlib import Path
from typing import Dict, Any


class ConfigGenerator:
    """Generate Python config files from YAML configuration for Comtrade Downloader"""
    
    def __init__(self, yaml_path: str):
        """Initialize with path to YAML config file"""
        self.yaml_path = Path(yaml_path)
        self.config_data = self._load_yaml()
    
    def _load_yaml(self) -> Dict[str, Any]:
        """Load and parse YAML configuration file"""
        try:
            with open(self.yaml_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML config file not found: {self.yaml_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")
    
    def _map_classification_flags(self) -> Dict[str, bool]:
        """Map YAML classification settings to Python boolean flags"""
        classifications = self.config_data.get('classifications', {})
        
        return {
            'PROCESS_SITC1': classifications.get('sitc1', False),
            'PROCESS_SITC2': classifications.get('sitc2', False),
            'PROCESS_SITC3': classifications.get('sitc3', False),
            'PROCESS_HS92': classifications.get('hs92', False),
            'PROCESS_HS96': classifications.get('hs96', False),
            'PROCESS_HS02': classifications.get('hs02', False),
            'PROCESS_HS07': classifications.get('hs07', False),
            'PROCESS_HS12': classifications.get('hs12', False),
            'PROCESS_HS17': classifications.get('hs17', False),
            'PROCESS_HS22': classifications.get('hs22', False),
            'PROCESS_EB10': classifications.get('eb10', False),
        }
        
    def _map_processing_steps(self) -> Dict[str, bool]:
        """Map YAML processing steps to Python dictionary"""
        download = self.config_data.get('download', {})
        steps = download.get('processing_steps', [])
        
        return {
            'run_downloader': steps.get('run_downloader', False),
            'run_converter': steps.get('run_converter', False),
            'run_compactor': steps.get('run_compactor', False),
        }
    
    def _get_paths(self) -> Dict[str, str]:
        """Extract path configuration from YAML"""
        download = self.config_data.get('download', {})
        shared = self.config_data.get('shared', {})
        paths_list = download.get('paths', [])
        
        # Convert list of dicts to single dict, fallback to shared settings
        paths = {}
        for path_dict in paths_list:
            paths.update(path_dict)
        
        return {
            'output_base_dir': paths.get('output_base_dir', shared.get('output_base_dir', './output')),
        }
    
    def _get_api_settings(self) -> Dict[str, Any]:
        """Extract API settings from YAML"""
        download = self.config_data.get('download', {})
        api_settings = download.get('api_settings', {})
        
        return {
            'reporter_countries': api_settings.get('reporter_countries', []),
            'partner_countries': api_settings.get('partner_countries', []),
            'commodity_codes': api_settings.get('commodity_codes', []),
            'flow_codes': api_settings.get('flow_codes', []),
            'mot_codes': api_settings.get('mot_codes', [0]),
            'mos_codes': api_settings.get('mos_codes', [0]),
            'customs_codes': api_settings.get('customs_codes', []),
            'drop_world_partner': api_settings.get('drop_world_partner', False),
            'drop_secondary_partners': api_settings.get('drop_secondary_partners', True),
        }
    
    def _get_trade_settings(self) -> Dict[str, Any]:
        """Extract trade settings from YAML"""
        download = self.config_data.get('download', {})
        trade_settings = download.get('trade_settings', {})
        
        return {
            'download_for_conversion': trade_settings.get('download_for_conversion', True),
            'delete_temp_files': trade_settings.get('delete_temp_files', False),
            'compress_output': trade_settings.get('compress_output', True),
            'convert_to_processed_files': trade_settings.get('convert_to_processed_files', True),
            'suppress_print': trade_settings.get('suppress_print', False),
        }
    
    def generate_python_config(self, output_path: str = 'config.py') -> None:
        """Generate Python configuration file from YAML data"""
        
        # Extract configuration values
        shared = self.config_data.get('shared', {})
        download = self.config_data.get('download', {})
        
        # Map values
        classification_flags = self._map_classification_flags()
        classification_start_years = self.config_data.get('classification_start_years')
        processing_steps = self._map_processing_steps()
        paths = self._get_paths()
        api_settings = self._get_api_settings()
        trade_settings = self._get_trade_settings()
        
        # Generate Python config content
        config_content = self._generate_config_template(
            end_year=shared.get('end_year', 2023),
            log_level=shared.get('log_level', 'INFO'),
            download_type=download.get('download_type', 'as_reported'),
            classification_flags=classification_flags,
            classification_start_years=classification_start_years,
            processing_steps=processing_steps,
            paths=paths,
            api_settings=api_settings,
            trade_settings=trade_settings,
        )
        
        # Write to file
        with open(output_path, 'w') as f:
            f.write(config_content)
        
        print(f"Generated Python config file: {output_path}")
    
    def _generate_config_template(self, **kwargs) -> str:
        """Generate the actual Python config file content"""
        
        classification_flags = kwargs['classification_flags']
        classification_start_years = kwargs['classification_start_years']
        processing_steps = kwargs['processing_steps']
        paths = kwargs['paths']
        api_settings = kwargs['api_settings']
        trade_settings = kwargs['trade_settings']
        
        # Format classification start years
        formatted_years = self._format_classification_start_years(classification_start_years)
        
        return f'''"""
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

OUTPUT_BASE_DIR = "{paths['output_base_dir']}"

# =============================================================================
# REQUESTED CLASSIFICATIONS AND YEAR RANGE
# =============================================================================

# Which trade classifications to download (leave True for the ones you want)
PROCESS_HS92 = {classification_flags['PROCESS_HS92']}  # HS92 data from 1992-END_YEAR
PROCESS_HS12 = {classification_flags['PROCESS_HS12']}  # HS12 data from 2012-END_YEAR
PROCESS_HS96 = {classification_flags['PROCESS_HS96']}
PROCESS_HS02 = {classification_flags['PROCESS_HS02']}
PROCESS_HS07 = {classification_flags['PROCESS_HS07']}
PROCESS_HS17 = {classification_flags['PROCESS_HS17']}
PROCESS_HS22 = {classification_flags['PROCESS_HS22']}

PROCESS_SITC1 = {classification_flags['PROCESS_SITC1']}  # SITC data from 1962-END_YEAR
PROCESS_SITC2 = {classification_flags['PROCESS_SITC2']}  # SITC data from 1976-END_YEAR
PROCESS_SITC3 = {classification_flags['PROCESS_SITC3']}  # SITC data from 1988-END_YEAR

PROCESS_EB10 = {classification_flags['PROCESS_EB10']}  # Services data from 2005-END_YEAR

# Year range configuration
END_YEAR = {kwargs['end_year']}  # Will default to datetime.now().year - 1

CLASSIFICATION_START_YEARS = {{
{formatted_years}
}}


# =============================================================================
# PROCESSING STEPS
# =============================================================================

PROCESSING_STEPS = {{
    "run_downloader": {processing_steps['run_downloader']},  # Download trade data and convert to requested classification
    "run_converter": {processing_steps['run_converter']},  # Convert to requested classification
    "run_compactor": {processing_steps['run_compactor']},  # Aggregate reporter files by classificaiton by year
}}

DOWNLOAD_FOR_CONVERSION = {trade_settings['download_for_conversion']}

# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL = "{kwargs['log_level']}"
SUPPRESS_PRINT = {trade_settings['suppress_print']}

# =============================================================================
# COMTRADE DATA REQUEST PARAMETERS (advanced users only)
# =============================================================================

"""
WARNING: Changing these parameters is not recommended
"""

# Reporter countries (empty list for all countries)
# Use ISO3 codes ["VEN", "CUB", "ARG", "CAN"]
REPORTER_COUNTRIES = {api_settings['reporter_countries']}

# Partner countries (leave empty list for all country partners)
# Use ISO3 codes ["VEN", "CUB", "ARG", "CAN"]
PARTNER_COUNTRIES = {api_settings['partner_countries']}

# Specific commodity codes (empty list for all products)
# Format depends on classification:
# - HS: 6-digit codes, e.g., ["010121", "010129"]
# - SITC: 4-digit codes, e.g., ["0011", "0012"]
COMMODITY_CODES = {api_settings['commodity_codes']}

# Trade flow types; Leave empty for all flows
FLOW_CODES = {api_settings['flow_codes']}  # Default: all flows
MOT_CODES = {api_settings['mot_codes']}  # Mode of transport (0 = all modes)
MOS_CODES = {api_settings['mos_codes']}  # Mode of supply (0 = all modes)
CUSTOMS_CODES = {api_settings['customs_codes']}  # Customs procedure codes
DROP_WORLD_PARTNER = {api_settings['drop_world_partner']}
DROP_SECONDARY_PARTNERS = {api_settings['drop_secondary_partners']}

# =============================================================================
# PROCESSING OPTIONS  (advanced users only)
# =============================================================================

# File management
DELETE_TEMP_FILES = {trade_settings['delete_temp_files']}  # Keep temporary download files
COMPRESS_OUTPUT = {trade_settings['compress_output']}  # Compress final output files
CONVERT_TO_PROCESSED_FILES = {trade_settings['convert_to_processed_files']}  # Apply classification conversions

# =============================================================================
# CONFIGURATION DICTIONARY
# =============================================================================

classifications_dict = {{
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
    "EB10" : PROCESS_EB10,
}}


ENABLED_CLASSIFICATIONS = get_enabled_classifications(classifications_dict)

config_dict = {{
    "api_key": API_KEY,
    "output_dir": OUTPUT_BASE_DIR,
    "download_type": get_download_type(PROCESSING_STEPS['run_converter']),
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
    "download_for_conversion": DOWNLOAD_FOR_CONVERSION,
}}
'''
    
    def _format_classification_start_years(self, start_years: Dict[str, int]) -> str:
        """Format classification start years dictionary for Python code"""
        lines = []
        lines.append("    # set start years for each classification")
        
        # SITC classifications
        for key in ['S1', 'S2', 'S3']:
            if key in start_years:
                year = start_years[key]
                comment = {
                    'S1': 'SITC Revision 1 (1962-present)',
                    'S2': 'SITC Revision 2 (1976-present)', 
                    'S3': 'SITC Revision 3 (1988-present)'
                }[key]
                lines.append(f'    "{key}": {year},  # {comment}')
        
        # HS classifications
        for key in ['H0', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6']:
            if key in start_years:
                year = start_years[key]
                comment = {
                    'H0': 'HS Combined (1992-present)',
                    'H1': 'HS 1992 vintage (1996-present)',
                    'H2': 'HS 2002 vintage (2002-present)',
                    'H3': 'HS 2007 vintage (2007-present)',
                    'H4': 'HS 2012 vintage (2012-present)',
                    'H5': 'HS 2017 vintage (2017-present)',
                    'H6': 'HS 2022 vintage (2022-present)'
                }[key]
                lines.append(f'    "{key}": {year},  # {comment}')
                
        for key in ['EB10']:
            if key in start_years:
                year = start_years[key]
                comment = {
                    'EB10': 'Services EBOPS (2005-present)',
                }[key]
                lines.append(f'    "{key}": {year},  # {comment}')
        return '\n'.join(lines)    