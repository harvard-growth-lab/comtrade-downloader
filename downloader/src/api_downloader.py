#!/usr/bin/env python
# coding: utf-8

"""
ComtradeDownloader object uses apicomtradecall to output reporter data 
by Classification Code by Year by Reporter
"""
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

# from dask.distributed import Client
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
        self.config.logger.info(f"Beginning downloader...")
        for year in self.config.years:
            year_path = Path(self.config.raw_files_path / str(year))

            last_updated = self.get_last_download_date(year)
            if (
                last_updated > self.downloader.earliest_date
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

            parquet_path = Path(self.config.raw_files_parquet_path / str(year))
            parquet_path.mkdir(parents=True, exist_ok=True)

            # TODO only process if newly downloaded files

            self.downloader.process_downloaded_files(
                year, convert=True, save_all_parquet_files=False
            )

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

        self.config.logger.info(f"Starting compactor...")
        for year in self.config.years:
            self.config.logger.info(f"Running compactor for {year}")
            df = self.aggregate_data_by_year(year)
            if df is None:
                self.config.logger.info(f"No data for {year} to compact")
                continue

            df = self.merge_iso_codes(df)
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
        path_dir = os.path.dirname(path)
        files = [f for f in os.listdir(path_dir) if f.endswith(".parquet")]
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
                    if os.path.isfile(archive_path / outdated_file):
                        os.remove(archive_path / outdated_file)
                    source_path = Path(path_dir) / outdated_file
                    shutil.move(str(source_path), str(archive_path))
                    relocated.append(outdated_file)
                except shutil.Error as e:
                    self.config.logger.error(
                        f"Failed to move {outdated_file} to archived files: {e}"
                    )
        return relocated

    def aggregate_data_by_year(self, year):
        """ """
        # TODO: improve performance
        if self.config.converted_files:
            year_path = Path(self.config.converted_final_path) / str(year)
        else:
            year_path = Path(self.config.raw_files_parquet_path) / str(year)

        # Get CPU count for parallel processing
        n_cores = max(
            int(os.environ.get("SLURM_CPUS_PER_TASK", 1)),
            int(os.environ.get("SLURM_JOB_CPUS_PER_NODE", 1)),
        )
        mem = int(os.environ.get("SLURM_MEM_PER_NODE")) / 1024

        dfs = []

        for f in glob.glob(os.path.join(year_path, "*.parquet")):
            df = pd.read_parquet(
                f,
                columns=list(self.downloader.columns.keys()),
            )
            
            # handle known errors
            df = self.handle_known_errors(df, f)
            # assert values are expected

            # Comtrade API returns aggregated data, need to filter to only include rolled up totals
            
                                
            try:
                df = df[
                    (df.motCode == "0")
                    & (df.mosCode == "0")
                    & (df.customsCode == "C00")
                    & (df.flowCode.isin(["M", "X", "RM", "RX"]))
                    & (df.partner2Code == 0)
                ]
                df = df.drop(
                    columns=[
                        "isAggregate",
                        "customsCode",
                        "motCode",
                        "mosCode",
                        "partner2Code",
                    ]
                )

            except:
                df = df[(df.flowCode.isin(["M", "X", "RM", "RX"]))]
                df = df.drop(columns=["isAggregate"])
                
            if df.empty:
                self.config.logger.info(f"Error check file: {f}, returning empty after filtering")
                                       
            df.groupby(
                ["reporterCode", "partnerCode", "flowCode", "cmdCode"], observed=False
            ).agg({
                "qty":"sum",
                "CIFValue":"sum",
                "FOBValue":"sum",
                "primaryValue":"sum"}).reset_index()
            

            dfs.append(df)
            
        return pd.concat(dfs)
    
    
    def handle_known_errors(self, df, f):
        """
        """
        string_columns = df.select_dtypes(
                include=['object', 'string', 'category']
            ).columns
            
        for col in string_columns:
            df[col] = df[col].str.strip()
        
        # known issue, comtrade mosCode value is -1, update to 0
        try:
            if '-1' in df.mosCode.unique():         
                df['mosCode'] = df['mosCode'].astype(str)
                df.loc[df.mosCode=='-1', 'mosCode'] = '0'
                file_obj = ComtradeFile(f.split('/')[-1])
                self.config.logger.info(f"handled -1 value in mosCode for {file_obj.reporter_code}, year {file_obj.year}")
        except:
            pass
        
        try:
            if df.cmdCode.nunique() == 1:
                self.config.logger.info(f"Country {file_obj.reporter_code}, year {file_obj.year} only reported TOTALS, expect empty df")
        except:
            pass
        return df


    def merge_iso_codes(self, df):
        """ """
        df["reporterCode"] = df.reporterCode.astype("int16")
        df["partnerCode"] = df.partnerCode.astype("int16")
        df = df.merge(self.downloader.reporters, on="reporterCode", how="left").merge(
            self.downloader.partners, on="partnerCode", how="left"
        )

        if self.config.partner_iso3_codes:
            df = ddf[ddf.partnerISO3.isin(self.config.partner_iso3_codes)]

        return df

    def save_combined_comtrade_year(self, df, year):
        """
        Saves output
        """
        self.config.logger.info(f"Saving aggregated data file for {year}.")
        save_dir = Path(
            self.config.aggregated_by_year_parquet_path
            / self.config.classification_code
        )
        save_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(
            Path(save_dir / f"{self.config.classification_code}_{year}.parquet"),
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
