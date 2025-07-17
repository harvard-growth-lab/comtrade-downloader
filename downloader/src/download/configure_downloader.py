from pathlib import Path
import re
from datetime import datetime
import logging
import os
import sys


class ComtradeConfig:
    REQUESTED_DATA = {"classic": "as_reported", "final": "by_classification"}

    def __init__(
        self,
        api_key: str,
        output_dir: str,
        download_type: str,
        product_classification: str,
        start_year: int,
        end_year: int,
        log_level: str,
        reporter_iso3_codes: list,
        partner_iso3_codes: list,
        commodity_codes: list,
        flow_codes: list,
        mot_codes: list,
        mos_codes: list,
        customs_codes: list,
        drop_world_partner: bool,
        drop_secondary_partners: bool,
        delete_tmp_files: bool,
        compress_output: bool,
        suppress_print: bool,
        converted_files: bool,
    ):
        # Required fields
        self.api_key = api_key
        self.download_type = download_type
        self.output_dir = Path(output_dir) / self.REQUESTED_DATA[self.download_type]
        self.classification_code = product_classification
        self.start_year = start_year
        self.end_year = end_year
        self.years = range(start_year, end_year + 1)

        # Optional fields with defaults
        self.reporter_iso3_codes = reporter_iso3_codes or []
        self.partner_iso3_codes = partner_iso3_codes or []
        self.commodity_codes = commodity_codes or []
        self.flow_codes = flow_codes or []
        self.mot_codes = mot_codes or [0]
        self.mos_codes = mos_codes or [0]
        self.customs_codes = customs_codes or []

        # Boolean flags
        self.drop_world_partner = drop_world_partner or False
        self.drop_secondary_partners = drop_secondary_partners or True
        self.delete_tmp_files = delete_tmp_files or False
        self.compress_output = compress_output or True
        self.suppress_print = suppress_print or True
        self.converted_files = converted_files or True

        self._validate()
        self._setup_paths()
        self._setup_logger(log_level)

        if self.compress_output:
            self.file_extension = "gz"
        else:
            self.file_extension = "csv"

    def _validate(self):
        if not self.api_key:
            raise ValueError(f"Requires an API KEY for Comtrade")

        start_year, end_year = self.years[0], self.years[-1]
        if not 1962 <= start_year <= datetime.now().year:
            raise ValueError(f"Invalid start_year: {start_year}")
        if not 1962 <= end_year <= datetime.now().year:
            raise ValueError(f"Invalid end_year: {end_year}")
        if start_year > end_year:
            raise ValueError(
                f"start_year ({start_year}) must be <= end_year ({end_year})"
            )

    def _setup_logger(self, log_level) -> logging.Logger:
        logger = logging.getLogger("ComtradeDownloader")
        if logger.handlers:
            logger.handlers.clear()

        logger.setLevel(log_level)
        # Create handler
        log_path = self.base_path / "logs"
        log_path.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(
            filename=log_path / f"run_downloader_{log_level}_{datetime.now()}.log",
            delay=False,
        )
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.logger = logger

    def _setup_paths(self):
        """Initialize all paths at once"""
        output_base = self.output_dir
        classification = self.classification_code

        # Paths with classification code
        self.latest_path = output_base / "latest" / classification
        self.raw_files_path = output_base / "raw" / classification
        self.raw_files_parquet_path = output_base / "raw_parquet" / classification
        self.intermediate_class_path = (
            output_base / "intermediate_files" / classification
        )
        self.converted_final_path = output_base / "converted" / classification
        self.archived_path = output_base / "archived" / classification

        # Paths without classification code
        self.corrupted_path = output_base / "corrupted"
        self.aggregated_by_year_stata_path = (
            output_base / "aggregated_by_year" / "stata"
        )
        self.aggregated_by_year_parquet_path = (
            output_base / "aggregated_by_year" / "parquet"
        )
        self.download_report_path = output_base / "atlas_download_reports"
        self.handle_empty_files_path = output_base / "handle_empty_files"

        # base paths
        self.base_path = Path(__file__).parent.parent.parent
        self.data_path = self.base_path / "data"
        self.conversion_weights_path = self.data_path / "conversion_weights"
        self.intermediate_data_path = self.data_path / "intermediate"

        # Create all directories
        for attr_name in dir(self):
            if attr_name.endswith("_path") and not attr_name.startswith("_"):
                path = getattr(self, attr_name)
                if isinstance(path, Path):
                    path.mkdir(parents=True, exist_ok=True)
