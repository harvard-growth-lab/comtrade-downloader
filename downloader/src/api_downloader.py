#!/usr/bin/env python
# coding: utf-8

"""
ComtradeDownloader object uses apicomtradecall to output reporter data 
by Classification Code by Year by Reporter
"""
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import glob
import os
import shutil
import sys
import re
import time
from datetime import date, timedelta, datetime
import logging
from src.comtrade_file import ComtradeFile, ComtradeFiles
from src.configure_downloader import ComtradeConfig
from src.downloader import BaseDownloader
from pathlib import Path


class ComtradeDownloader(object):
    TAIWAN = {"S19": "TWN"}

    def __init__(self, config: ComtradeConfig):
        self.config = config

    def download_comtrade_yearly_bilateral_flows(self):
        """ """
        reporter_codes = []
        self.downloader = BaseDownloader.create_downloader(self.config)
        for year in self.config.years:
            year_path = Path(self.config.raw_files_path / str(year))

            last_updated = self.get_last_download_date(year)
            if (
                last_updated > self.downloader.earliest_date
                and not self.config.force_full_download
                and not self.config.reporter_iso3_codes
            ):
                updated_reporters = self.downloader.get_reporters_by_data_availability(
                    year, last_updated
                )
                self.config.logger.info(
                    f"Downloading reporter {self.config.classification_code} - {year} "
                    f"files updated since {last_updated}."
                )
            else:
                last_updated = self.downloader.earliest_date
                self.config.logger.info(
                    f"Downloading {self.config.classification_code} - {year} for {self.config.reporter_iso3_codes if self.config.reporter_iso3_codes else 'all reporters'}."
                )

            year_path.mkdir(parents=True, exist_ok=True)
            self.downloader.download_with_retries(year, year_path, last_updated)

            # folder to save parquet files
            parquet_path = Path(self.config.raw_files_parquet_path / str(year))
            parquet_path.mkdir(parents=True, exist_ok=True)

            # process files (validate and convert to parquet)
            self.downloader.process_downloaded_files(year, convert=True)

            relocated_files = self.keep_most_recent_published_data(year, parquet_path)

            downloaded_files = self.generate_download_report(
                parquet_path, relocated_files
            )
            if not downloaded_files:
                self.config.logger.info(f"No new files downloaded for {year}.")
                continue
            self.config.logger.info(f"Generated download report for {year}.")

    def run_compactor(self):
        self.downloader = BaseDownloader.create_downloader(self.config)
        # client = Client(n_workers=2, threads_per_worker=8)
        # self.config.logger.info(f"Dask client started: {client}")

        for year in self.config.years:
            self.config.logger.info(f"Running compactor for {year}")
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

    def keep_most_recent_published_data(self, year, path):
        """ """
        # generate dictionary with reporter code key and datetimes as column values
        files = glob.glob(os.path.join(path, "*.parquet"))
        reporter_dates = {
            code: [] for code in {ComtradeFile(f).reporter_code for f in files}
        }
        for f in files:
            reporter_dates[ComtradeFile(f).reporter_code].append(
                ComtradeFile(f).published_date
            )

        duplicated = {
            code: dates for code, dates in reporter_dates.items() if len(dates) >= 2
        }
        if duplicated:
            archive_path = Path(self.config.archived_path) / str(year)
            archive_path.mkdir(exist_ok=True)
        else:
            return

        relocated = []
        for reporter, dates in duplicated.items():
            dates.remove(max(dates))
            # get the file names
            outdated_files = ComtradeFiles(files).get_file_names(reporter, dates)

            for outdated_file in outdated_files:
                try:
                    shutil.move(outdated_file, str(archive_path))
                    relocated.append(outdated_file)
                except shutil.Error as e:
                    self.config.logger.error(
                        f"Failed to move {outdated_file} to archived files: {e}"
                    )
        return relocated

        return relocated

    def aggregate_data_by_year(self, year):
        """ """
        # TODO: setup so only reading in and concating new files to existing file
        year_path = Path(self.config.raw_files_parquet_path) / str(year)
        dfs = [
            dd.read_parquet(
                f,
                compression="snappy",
                sep="\t",
                usecols=list(self.downloader.columns.keys()),
                dtype=self.downloader.columns,
                blocksize=None,  # gzip files can't be broken down further
                # ignore_index=True
            )
            for f in glob.glob(os.path.join(year_path, "*.parquet"))
        ]

        ddf = dd.concat(dfs)
        ddf.groupby(["reporterCode", "partnerCode", "flowCode", "cmdCode"]).agg(
            "sum"
        ).reset_index()
        ddf = ddf.drop(columns="qty")
        df = ddf.compute()

        # Merge reporter and partner reference tables for ISO3 codes
        df = df.merge(self.downloader.reporters, on="reporterCode", how="left").merge(
            self.downloader.partners, on="partnerCode", how="left"
        )

        if self.config.partner_iso3_codes:
            df = ddf[ddf.partnerISO3.isin(self.config.partner_iso3_codes)]

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
        df.to_parquet(
            Path(
                self.config.aggregated_by_year_parquet_path
                / f"comtrade_{self.config.classification_code}_{year}.parquet",
            ),
            compression="snappy",
            index=False,
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
        if df.empty:
            return False
        df["download_time"] = report_data["report_time"]
        df["classification"] = report_data["classification"]

        file_name = f"download_report_{datetime.now().strftime('%Y-%m-%d')}.csv"
        try:
            existing = pd.read_csv(Path(self.config.download_report_path / file_name))
            df = pd.concat([existing, df], ignore_index=True)
        except FileNotFoundError:
            pass
        df.to_csv(Path(self.config.download_report_path / file_name), index=False)
        return True

    def remove_tmp_dir(self, tmp_path):
        """ """
        for f in glob.glob(os.path.join(tmp_path, "*.gz")):
            try:
                os.remove(f)
            except OSError as e:
                self.config.logger.info(f"Error: {f} : {e.strerror}")

        try:
            os.rmdir(tmp_path)
        except OSError as e:
            self.config.logger.info(f"Error: {tmp_path} : {e.strerror}")
