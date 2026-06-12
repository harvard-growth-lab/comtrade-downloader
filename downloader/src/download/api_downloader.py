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
import subprocess
import sys
import re
import time
from datetime import date, timedelta, datetime
from downloader.src.download.comtrade_file import ComtradeFile, ComtradeFiles
from downloader.src.download.configure_downloader import ComtradeConfig
from downloader.src.download.downloader import BaseDownloader
from downloader.data.static.constants import FILTER_CONDITIONS, CLASSIFICATION_RELEASE_YEARS
from pathlib import Path


class ComtradeDownloader(object):
    TAIWAN = {"S19": "TWN"}
    NUMBER_OF_YEAR_PAST_SINCE_LAST_REPORT = 2

    def __init__(self, config: ComtradeConfig):
        self.config = config

    def download_comtrade_yearly_bilateral_flows(self) -> None:
        """
        Creates a ComtradeDownloader object and downloads comtrade data
        for the requested years if the most recently reported data by each
        reporter country has not already been downloaded

        Downloaded data is then saved in a raw file in the gunzipped format
        as provided by the Comtrade API

        Next, the data is converted to a parquet file and saved in the
        raw_parquet_path directory

        Finally, the most recent data by each reporter country is kept and
        the rest is moved to the archived_path directory if a previous version
        of the data had been downloaded for the same year

        A download report is then generated and saved in the download_report_path directory
        """
        reporter_codes = []
        self.downloader = BaseDownloader.create_downloader(self.config)
        self.config.logger.info(f"Beginning downloader...")
        start_year = max(CLASSIFICATION_RELEASE_YEARS[self.config.classification_code], self.config.start_year)

        year_dates = {}
        for year in range(start_year, self.config.end_year + 1):
            year_path = Path(self.config.raw_files_path / str(year))
            year_path.mkdir(parents=True, exist_ok=True)
            parquet_path = Path(self.config.raw_files_parquet_path / str(year))
            parquet_path.mkdir(parents=True, exist_ok=True)

            last_updated = self.get_last_download_date(year)
            if last_updated != self.downloader.earliest_date:
                year_dates[year] = last_updated
            updated_reporters = self.downloader.get_reporters_by_data_availability(year, last_updated)
            if len(updated_reporters) > 0:
                self.config.logger.info(
                    f"New data has been reported for {self.config.classification_code} - {year} since {last_updated}."
                )
                self.config.logger.info(f"Downloading new data for {self.config.classification_code} - {year}.")
            else:
                if year < CLASSIFICATION_RELEASE_YEARS[self.config.classification_code]:
                    self.config.logger.info(f"Classification {self.config.classification_code} was not released until {CLASSIFICATION_RELEASE_YEARS[self.config.classification_code]} nothing to download for {year}.")
                elif last_updated != self.downloader.earliest_date:
                    self.config.logger.info(f"No new data has been reported for {self.config.classification_code} - {year} since {last_updated}.")
                    if last_updated.year < datetime.now().year - self.NUMBER_OF_YEAR_PAST_SINCE_LAST_REPORT:
                        self.config.logger.info("No recent data has been reported, skipping remaining years for this classification")
                        self.write_data_version_csv(year_dates)
                        return
                relocated_files = self.keep_most_recent_published_data(year, parquet_path)
                continue

            self.downloader.download_with_retries(year, year_path, last_updated)

            self.downloader.process_downloaded_files(
                year, convert_to_parquet=True, save_all_parquet_files=False
            )
            relocated_files = self.keep_most_recent_published_data(year, parquet_path)
            downloaded_files = self.generate_download_report(
                parquet_path, relocated_files
            )

        self.write_data_version_csv(year_dates)


    def run_compactor(self) -> None:
        """
        This method processes trade data by aggregating it annually and applying
        data transformations before saving the results as parquet files

        Compactor steps:
        1. Data aggregation by year from source files
        2. Add iso codes to dataset
        3. Atlas data filtering (removes Not Elsewhere Specified locations)
        4. Digit level standardization for trade classification codes
        5. ISO country code recoding and modernization

        Final output saved to aggregated_by_year directory

        Raises:
            Exception: If data processing fails for any year

        Notes:
            While memory is actively managed by deleting DFs after processing,
            the size of data may exceed memory limits and require being run with additional memory
        """
        self.downloader = BaseDownloader.create_downloader(self.config)

        self.config.logger.info(f"Starting compactor for {self.config.classification_code}")
        for year in self.config.years:
            self.config.logger.info(f"... compacting year {year}")
            df = self.aggregate_data_by_year(year)
            if df.empty:
                self.config.logger.info(f"...no data in {year} to compact")
                continue
            df = self.add_iso_codes(df)
            df = self.downloader.atlas_data_filter(df)
            df = self.downloader.handle_digit_level(df)
            df = self.downloader.handle_iso_codes_recoding(df)
            self.save_combined_comtrade_year(df, year)
            del df

    def get_last_download_date(self, year: int) -> datetime:
        """
        Using the ComtradeFile object, get the last date raw files for the
        classification code and year were last downloaded

        Returns:
            datetime: Last date the data was downloaded
        """
        year_path = Path(self.config.raw_files_path) / str(year)
        if not year_path.exists():
            return self.downloader.earliest_date

        files = list(year_path.glob("*.gz"))
        if not files:
            return self.downloader.earliest_date
        comtrade_files = [ComtradeFile(f) for f in files]
        latest_date = max(file.published_date for file in comtrade_files)
        return latest_date

    def keep_most_recent_published_data(self, year: int, path: Path) -> list[Path]:
        """
        Generate a dictionary with reporter code key and datetimes as column values
        and keep the most recent published data for each reporter.

        If a reporter has multiple files for the same year, keep the most recent
        file and move the rest to the archived_path directory

        Returns:
            list[Path]: List of paths to the archived files
        """
        path_dir = str(path)
        files = [f for f in path.glob("*.parquet")]
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
                    source_path = path / outdated_file
                    shutil.move(str(source_path), str(archive_path))
                    relocated.append(outdated_file)
                    self.config.logger.info(f"Moved duplicated file {outdated_file} to archived files")
                except FileNotFoundError:
                    print(f"File not found: {source_path}")
                except shutil.Error as e:
                    self.config.logger.error(
                        f"Failed to move {outdated_file} to archived files: {e}"
                    )
        return relocated

    def aggregate_data_by_year(self, year: int) -> pd.DataFrame:
        """
        Reads in parquet files for a given year and aggregates the data

        Handles known errors in Comtrade API data or lower quality reporting data
        that does not meet the expected quality standards or required product level
        detail.

        Returns:
            pd.DataFrame: Aggregated data by year
        """
        if self.config.converted_files:
            year_path = Path(self.config.converted_final_path) / str(year)
        else:
            year_path = Path(self.config.raw_files_parquet_path) / str(year)

        if not year_path.exists():
            return pd.DataFrame()

        dfs = []
        country_reporter_files = glob.glob(os.path.join(year_path, "*.parquet"))
        if country_reporter_files:
            for f in country_reporter_files:
                df = pd.read_parquet(
                    f,
                    columns=list(self.downloader.columns.keys())
                )
                df = self.handle_known_errors(df, f)
                df = self.enforce_unique_partner_product_level(df, f)
                dfs.append(df)
            return pd.concat(dfs).reset_index(drop=True)
        else:
            return pd.DataFrame()

    def enforce_unique_partner_product_level(
        self, df: pd.DataFrame, f: Path
    ) -> pd.DataFrame:
        """
        Enforces that each partner-product combination is unique in the dataset
        by filtering out duplciated records provided by Comtrade especially if
        additional detail was requested via the API

        Starting in 2017 Comtrade began including both aggregated and not aggregated data
        prior to this, they only provided aggregated data

        Filters on the following conditions:
        - customsCode: C00
        - motCode: 0
        - mosCode: 0
        - partner2Code: 0
        - flowCode: M, X, RM, RX

        Performs error handling if the data is empty after filtering

        Finally groups by reporter, partner, flow, and cmd code

        Returns:
            pd.DataFrame: Filtered data
        """
        df_temp = df.copy()

        conditions = []
        for col, value in FILTER_CONDITIONS.items():
            if col in df.columns:
                if isinstance(value, list):
                    conditions.append(f"{col}.isin({value})")
                else:
                    conditions.append(f"{col} == {repr(value)}")

        if conditions:
            query_str = " and ".join(conditions)
            try:
                df = df.query(query_str)
            except Exception as e:
                self.config.logger.warning(f"Error applying filters: {e}")

        drop_cols = ["isAggregate", "customsCode", "motCode", "mosCode", "partner2Code"]
        for col in drop_cols:
            try:
                df = df.drop(columns=col)
            except KeyError:
                continue

        if df.empty:
            df_temp.to_parquet(
                Path(self.config.handle_empty_files_path / Path(f).name),
                compression="snappy",
                index=False,
            )
            self.config.logger.warning(
                f"Error check file: {Path(f).name}, returning empty after filtering"
            )

        del df_temp
        return (
            df.groupby(
                ["reporterCode", "partnerCode", "flowCode", "cmdCode"], observed=False
            )
            .agg(
                {
                    "qty": "sum",
                    "CIFValue": "sum",
                    "FOBValue": "sum",
                    "primaryValue": "sum",
                }
            )
            .reset_index()
        )

    def handle_known_errors(self, df: pd.DataFrame, f: Path) -> pd.DataFrame:
        """
        Checks and handles known errors in the data as provided by Comtrade

        Logs warnings for known errors or discovered data quality issues

        Returns:
            pd.DataFrame: Data with known errors handled
        """
        string_columns = df.select_dtypes(
            include=["object", "string", "category"]
        ).columns

        for col in string_columns:
            df[col] = df[col].str.strip()

        try:
            # known issue, comtrade mosCode value is -1, update to 0
            if "-1" in df.mosCode.unique():
                df["mosCode"] = df["mosCode"].astype(str)
                df.loc[df.mosCode == "-1", "mosCode"] = "0"
                file_obj = ComtradeFile(f.split("/")[-1])
                self.config.logger.warning(
                    f"handled -1 value in mosCode for {file_obj.reporter_code}, year {file_obj.year}"
                )
        except:
            pass

        try:
            if df.cmdCode.nunique() == 1:
                self.config.logger.warning(
                    f"Country {file_obj.reporter_code}, year {file_obj.year} only reported TOTALS, expect empty df"
                )
        except:
            pass
        return df

    def add_iso_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds ISO codes to the dataset using the reporters and partners reference
        tables from Comtrade's API

        Returns:
            pd.DataFrame: Data with ISO codes added
        """
        df["reporterCode"] = df.reporterCode.astype("int16")
        df["partnerCode"] = df.partnerCode.astype("int16")
        return df.merge(self.downloader.reporters, on="reporterCode", how="left").merge(
            self.downloader.partners, on="partnerCode", how="left"
        )

    def save_combined_comtrade_year(self, df: pd.DataFrame, year: int) -> None:
        """
        Saves compacted data as a parquet file within the requested classification directory
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

    def write_data_version_csv(self, year_dates: dict) -> None:
        """
        Appends a record to atlas_download_reports/comtrade_data_version.csv capturing
        the last published date per year and the overall data_as_of_date (max across all
        years). Uses year_dates already collected during the download loop — no additional
        file scans.
        """
        if not year_dates:
            return
        df = pd.DataFrame(
            [{"year": y, "last_published_date": d} for y, d in sorted(year_dates.items())]
        )
        df["data_as_of_date"] = df["last_published_date"].max()
        df["run_timestamp"] = datetime.now()
        try:
            df["git_sha"] = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
            ).decode().strip()
        except Exception:
            df["git_sha"] = "unknown"

        self.config.download_report_path.mkdir(parents=True, exist_ok=True)
        out_path = self.config.download_report_path / "comtrade_data_version.csv"
        try:
            existing = pd.read_csv(out_path)
            df = pd.concat([existing, df], ignore_index=True)
        except FileNotFoundError:
            pass
        df.to_csv(out_path, index=False)
        self.config.logger.info(
            f"Data version CSV updated at {out_path}. Data as of: {df['data_as_of_date'].iloc[-1]}"
        )

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
