#!/usr/bin/env python
# coding: utf-8

"""
ComtradeDownloader object uses apicomtradecall to output reporter data 
by Classification Code by Year by Reporter
"""
import pandas as pd
import glob
import os
import shutil
import sys
import re
import time
from datetime import date, timedelta, datetime
import logging
from src.comtrade_file import ComtradeFile
from src.configure_downloader import ComtradeConfig
from src.downloader import BaseDownloader
from pathlib import Path


class ComtradeDownloader(object):
    
    TAIWAN = {'S19': 'TWN'}

    def __init__(self, config: ComtradeConfig):
        self.config = config

    def download_comtrade_yearly_bilateral_flows(self):
        """ """
        reporter_codes = []
        self.downloader = BaseDownloader.create_downloader(self.config)
        for year in self.config.years:
            last_updated = self.get_last_download_date(year)
            if (
                last_updated > self.downloader.earliest_date
                and not self.config.force_full_download
            ):
                updated_reporters = self.downloader.get_reporters_by_data_availability(
                    year, last_updated
                )
                year_path = Path(self.config.latest_path / str(year))
                self.config.logger.info(
                    f"Downloading reporter {self.config.classification_code} - {year} "
                    f"files updated since {last_updated}."
                )
            else:
                last_updated = self.downloader.earliest_date
                year_path = Path(self.config.raw_files_path / str(year))
                self.config.logger.info(
                    f"Downloading all {self.config.classification_code} - {year}."
                )
                
            year_path.mkdir(parents=True, exist_ok=True)
            self.downloader.download_with_retries(year, year_path, last_updated)

            relocated_files = (
                []
                if last_updated == self.downloader.earliest_date or self.config.force_full_download
                else self.replace_raw_files_with_updated_reports(year, year_path)
            )
            self.generate_download_report(year_path, relocated_files)
            self.config.logger.info(f"Generated download report for {year}.")
            self.downloader.handle_corrupt_files(year)
            df = self.aggregate_data_by_year(year)
            # integrated compactor
            df = self.downloader.atlas_data_filter(df)
            df = self.downloader.clean_data(df)            
            self.save_combined_comtrade_year(df, year)

    def get_last_download_date(self, year):
        """
        Get information about last date raw files for the classification code
        and year were last downloaded
        """
        year_path = Path(self.config.raw_files_path) / str(year)
        if not year_path.exists():
            return self.downloader.earliest_date

        files = list(year_path.glob("*.gz"))
        if not files:
            return self.downloader.earliest_date

        self.config.logger.info(
            f"{self.config.classification_code} - {year}: {len(files)} reporter files found"
        )
        comtrade_files = [ComtradeFile(f) for f in files]
        latest_date = max(file.published_date for file in comtrade_files)
        return latest_date


    def replace_raw_files_with_updated_reports(self, year, year_path):
        """ """
        updated_files = list(Path(year_path).glob("*.gz"))
        if not updated_files:
            return []

        raw_year_path = Path(self.config.raw_files_path) / str(year)
        archive_path = Path(self.config.archived_path) / str(year)
        archive_path.mkdir(exist_ok=True)

        relocated = []
        raw_files = {
            ComtradeFile(f).reporter_code: f for f in raw_year_path.glob("*.gz")
        }

        for updated in updated_files:
            try:
                # Archive existing file if present
                if outdated := raw_files.get(ComtradeFile(updated).reporter_code):
                    relocated.append(outdated)
                # Move new file to raw
                shutil.move(str(updated), str(raw_year_path))
                relocated.append(updated)
            except shutil.Error as e:
                self.config.logger.error(f"Failed to move updated {updated} to raw files: {e}")
            try:
                shutil.move(str(outdated), str(archive_path))
            except shutil.Error as e:
                self.config.logger.error(f"Failed to move {outdated} to archived files: {e}")

        self.config.logger.info("Raw files updated with latest data")
        return relocated

    def aggregate_data_by_year(self, year):
        """"""
        year_path = Path(self.config.raw_files_path) / str(year)
        df = pd.concat(
            (
                pd.read_csv(
                    f,
                    compression="gzip",
                    sep="\t",
                    usecols=list(self.downloader.columns.keys()),
                    dtype=self.downloader.columns,
                )
                for f in glob.glob(os.path.join(year_path, "*.gz"))
            ),
            ignore_index=True,
        )

        # Merge reporter and partner reference tables for ISO3 codes
        df = df.merge(self.downloader.reporters, on="reporterCode", how="left")
        df = df.merge(self.downloader.partners, on="partnerCode", how="left")

        if self.config.partner_iso3_codes:
            df = df[df.partnerISO3.isin(self.config.partner_iso3_codes)]

        if not self.config.drop_secondary_partners:
            df = df.merge(
                self.downloader.partners.rename(
                    columns={
                        "partnerCode": "partner2Code",
                        "partnerISO3": "partner2ISO3",
                    }
                ),
                on="partner2Code",
                how="left",
            )
        return df
    

    def save_combined_comtrade_year(self, df, year):
        """
        Saves output to parquet and stata. Compactor uses parquet file format
        """
        self.config.logger.info(f"Saving aggregated data file for {year}.")
        df = df[['period','reporterISO3','partnerISO3','flowCode','classificationCode', 'digitLevel','cmdCode', 'CIFValue', 'FOBValue', 'primaryValue']]
               
        # used in pipeline
        df.to_parquet(
            Path(
                self.config.aggregated_by_year_stata_path / 
                f"comtrade_{self.config.classification_code}_{year}.parquet",
            ),
            compression="snappy",
            index=False,
        )
        # requirement to cast ints to floats for stata files
        df.loc[:, 'period'] = df.period.astype(float)

        # ready for stata users of the lab
        df.to_stata(
            Path(
                self.config.aggregated_by_year_stata_path / f"comtrade_{self.config.classification_code}_{year}.dta",
            ),
            write_index=False,
        )
        del df

    def generate_download_report(
        self, year_path: Path, replaced_files: list[Path]
    ) -> None:
        """Generate detailed download report with file and processing metadata."""
        report_data = {
            "report_time": datetime.now(),
            "classification": self.config.classification_code,
            "files": [],
        }

        for file in Path(year_path).glob("*.gz"):
            comtrade_file = ComtradeFile(file)
            file_data = {
                "reporter_code": comtrade_file.reporter_code,
                "published_date": comtrade_file.published_date,
                "year": comtrade_file.year,
                "file_size": file.stat().st_size,
                "replaced": file in replaced_files,
                "status": "replaced" if file in replaced_files else "new",
            }
            report_data["files"].append(file_data)

        df = pd.DataFrame(report_data["files"])
        df["download_time"] = report_data["report_time"]
        df["classification"] = report_data["classification"]

        file_name = f"download_report_{datetime.now().strftime('%Y-%m-%d')}.csv"
        try:
            existing = pd.read_csv(Path(self.config.download_report_path / file_name))
            df = pd.concat([existing, df], ignore_index=True)
        except FileNotFoundError:
            pass
        df.to_csv(Path(self.config.download_report_path / file_name), index=False)


    def remove_tmp_dir(self, tmp_path):
        """ """
        for f in glob.glob(os.path.join(tmp_path, "*.gz")):
            try:
                os.move(f)
            except OSError as e:
                self.config.logger.info(f"Error: {f} : {e.strerror}")

        try:
            os.rmdir(tmp_path)
        except OSError as e:
            self.config.logger.info(f"Error: {tmp_path} : {e.strerror}")
