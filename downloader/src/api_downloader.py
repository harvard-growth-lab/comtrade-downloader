#!/usr/bin/env python
# coding: utf-8


"""
ComtradeDownloader object uses apicomtradecall to output reporter data 
by Classification Code by Year by Reporter
"""
import pandas as pd
import comtradeapicall
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
from pathlib import Path

class ComtradeDownloader(object):

    def __init__(self, config: ComtradeConfig):
        self.config = config
        
        
        

        ###############################################################



        # # Remove temporary directory
        # if self.delete_tmp_files:
        #     self.remove_tmp_dir(self.tmp_path)    
    
#     def execute_download(self):
#         for year in self.years:
#             self.logger.info(f"Downloading Comtrade Files for year: {year}")
#             last_updated = self.get_last_download_date(year)
#             year_path = self.get_year_path(year, last_updated)

#             if self._download_files(year_path, year, last_updated):
#                self._process_files(year_path, year, last_updated)
#                corrupted_files = self._handle_corrupted_files(year)

#                if not corrupted_files:
#                    df = self._transform_data(year)
#                    self._save_data(df, year)
#                else:
#                    self._handle_remaining_corrupted(corrupted_files)
    
#     def get_year_path(self, year: int, last_updated: Optional[str]) -> Path:
#         return (self.latest_path if last_updated else self.raw_files_path) / str(year)


    
    def download_comtrade_yearly_bilateral_flows(self):
        """ """

        class HiddenPrints:
            def __enter__(self):
                self._original_stdout = sys.stdout
                sys.stdout = open(os.devnull, "w")

            def __exit__(self, exc_type, exc_val, exc_tb):
                sys.stdout.close()
                sys.stdout = self._original_stdout

        reporter_codes = []
        for year in self.config.years:
            last_updated = self.get_last_download_date(year)
            if last_updated > datetime(1900, 1, 1) and not self.config.force_full_download:
                updated_reporters = self.get_reporters_by_data_availability(
                    year, last_updated
                )
                year_path = os.path.join(self.config.latest_path, str(year))
                self.config.logger.info(
                    f"Downloading reporter {self.config.classification_code} - {year} "
                    f"files updated since {last_updated}."
                )
            else:
                last_updated = None
                year_path = os.path.join(self.config.raw_files_path, str(year))
                self.config.logger.info(f"Downloading all {self.config.classification_code} - {year}.")
            os.makedirs(year_path, exist_ok=True)
            max_retries = 5
            attempt = 0
            while attempt < max_retries:
                # as-reported, by-classification
                #APIDownloader(call_type, year_path, last_updated)
                try:
                    if self.suppress_print:
                        with HiddenPrints():
                            comtradeapicall.bulkDownloadFinalClassicFile(
                                self.config.api_key,
                                year_path,
                                typeCode="C",
                                freqCode="A",
                                clCode=self.config.classification_code,
                                period=str(year),
                                # updated reporter codes to match classic
                                reporterCode=None,
                                decompress=False,
                                publishedDateFrom=last_updated.strftime("%Y-%m-%d")
                                # publishedDateTo='2018-01-01'
                            )
                    else:
                        comtradeapicall.bulkDownloadFinalClassicFile(
                            self.config.api_key,
                            year_path,
                            typeCode="C",
                            freqCode="A",
                            clCode=self.config.classification_code,
                            period=str(year),
                            # updated reporter codes to match classic
                            reporterCode=None,
                            decompress=False,
                            publishedDateFrom=last_updated.strftime("%Y-%m-%d")
                            # publishedDateTo='2018-01-01'
                        )
                    self.config.logger.info(f"Completed apicall for year {year}.")
                    break
                except ConnectionResetError as e:
                    self.config.logger.info(f"Connection Reset Error: {e}")
                    attempt += 1
                    time.sleep(2**attempt)
                except KeyError as e:
                    self.config.logger.info(f"An error occurred: {str(e)}")

            relocated_files = [] if last_updated is None or self.config.force_full_download else self.replace_raw_files_with_updated_reports(year, year_path)        
            
            self.generate_download_report(
                    year_path, relocated_files
                )
            self.config.logger.info(f"Generated download report for {year}.")
            self.config.logger.info(f"Filtering and exporting data for year {year}.")
            corrupted_files = self.find_corrupt_files(year)
            attempts = 1
            remove_from_corrupted = set()
            corrupted = False
            while corrupted_files and attempts < 4:
                corrupted = True
                logging.info(f"Found corrupted files")
                for corrupted_file in corrupted_files:
                    year = ComtradeFile(corrupted_file).year
                    reporter_code = ComtradeFile(corrupted_file).reporter_code
                    self.config.logger.info(f"... requesting from api {year}-{reporter_code}.")
                    comtradeapicall.bulkDownloadFinalClassicFile(
                        self.config.api_key,
                        year_path,
                        typeCode="C",
                        freqCode="A",
                        clCode=self.config.classification_code,
                        period=year,
                        reporterCode=reporter_code,
                        decompress=False,
                    )
                    # attempt to read in all re-downloaded file using reporter code
                    try:
                        df = pd.read_csv(
                            corrupted_file,
                            sep="\t",
                            compression="gzip",
                            usecols=list(self.config.columns.keys()),
                            dtype=self.config.columns,
                        )
                        # if successful read then remove from corrupted_files
                        remove_from_corrupted.add(corrupted_file)
                    except:
                        self.config.logger.info(
                            f"{corrupted_file} on attempt {attempts} after initial failure  is still corrupted"
                        )
                        continue
                for file in remove_from_corrupted:
                    self.config.logger.info(f"remove {remove_from_corrupted}")
                    corruped_files.remove(file)
                attempts += 1
            if corrupted_files:
                for f in corrupted_files:
                    self.config.logger.info(
                        f"download failed, removing from raw downloaded folder {f}"
                    )
                    shutil.move(f, os.path.join(self.config.output_dir, "corrupted", f.split('/')[-1]))
            
            df = self.transform_data(year)
            self.save_combined_comtrade_year(df, year)

    def get_last_download_date(self, year):
        """
        Get information about last date raw files for the classification code
        and year were last downloaded
        """
        year_path = Path(self.config.raw_files_path) / str(year)
        min_date = datetime(1900, 1, 1)
        if not year_path.exists():
            return min_date
        
        files = list(year_path.glob("*.gz"))
        if not files:
            return min_date
        
        self.config.logger.info(f"{self.config.classification_code} - {year}: {len(files)} reporter files found")
        comtrade_files = [ComtradeFile(f) for f in files]
        latest_date = max(file.published_date for file in comtrade_files)
        return latest_date

        
    def get_reporters_by_data_availability(self, year, latest_date):
        df = comtradeapicall.getFinalClassicDataBulkAvailability(
            self.config.api_key,
            typeCode="C",
            freqCode="A",
            clCode=self.config.classification_code,
            period=str(year),
            reporterCode=None,
        )
        if df.empty:
            return []
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df_since_download = df[df['timestamp'].dt.date > latest_date.date()]
            reporter_codes = df_since_download["reporterCode"].unique()
            return reporter_codes

    def replace_raw_files_with_updated_reports(self, year, year_path):
        """ """
        # most recently downloaded files
        updated_files = list(Path(year_path).glob("*.gz"))
        if not updated_files:
            return []

        raw_year_path = Path(self.config.raw_files_path) / str(year)
        archive_path = Path(self.config.archived_path) / str(year)
        archive_path.mkdir(parents=True, exist_ok=True)
        
        relocated = []
        raw_files_map = {ComtradeFile(f).reporter_code: f for f in raw_year_path.glob("*.gz")}

        for updated_file in updated_files:
            self.config.logger.debug("updated file: ", updated_file)
            updated = ComtradeFile(updated_file)
            try:
                if outdated_file := raw_files_map.get(updated.reporter_code):
                    shutil.move(outdated_file, archive_path)
                    relocated.append(outdated_file)
            except shutil.Error as e:
                self.config.logger.error(f"Failed to move {outdated_file}: {e}")
            try:
                shutil.move(updated_file, raw_year_path)
                relocated.append(updated_file)
            except shutil.Error as e:
                self.config.logger.error(f"Failed to move {updated_file}: {e}")
        
        self.config.logger.info(f"Replaced outdated raw files with latest data")
        self.config.logger.info(f"These reporters updated their data {ComtradeFile(file).reporter for file in relocated}")
        return relocated
    
    
    def find_corrupt_files(self, year):
        """return any empty or corrupted files."""
        dfs = []
        year_path = os.path.join(self.config.raw_files_path, str(year))
        corrupted_files = set()

        for f in glob.glob(os.path.join(year_path, "*.gz")):
            try:
                df = pd.read_csv(
                    f,
                    sep="\t",
                    compression="gzip",
                    usecols=list(self.config.columns.keys()),
                    dtype=self.config.columns,
                    nrows=1
                )

            except EOFError as e:
                self.config.logger.info(f"downloaded corrupted file: {f}")
                corrupted_files.add(f)

            except pd.errors.EmptyDataError as e:
                self.config.logger.info(f"downloaded empty file: {f}")
                corrupted_files.add(f)
        return corrupted_files 


    def transform_data(self, year):
        """"""
        year_path = Path(self.config.raw_files_path) / str(year)
        df = pd.concat((pd.read_csv(f, 
                                    compression='gzip', 
                                    sep='\t',
                                    usecols=list(self.config.columns.keys()),
                                    dtype=self.config.columns,) 
                        for f in glob.glob(os.path.join(year_path, "*.gz"))), ignore_index=True)
        
        # Merge reporter and partner reference tables for ISO3 codes
        df = df.merge(self.reporters, on="reporterCode", how="left")
        df = df.merge(self.partners, on="partnerCode", how="left")

        if self.config.partner_iso3_codes:
            df = df[df.partnerISO3.isin(self.config.partner_iso3_codes)]

        if not self.config.drop_secondary_partners:
            df = df.merge(
                self.partners.rename(
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
        """"""
        self.config.logger.info(f"Saving transformed data file for {year}.")
        df.to_stata(
            os.path.join(
                self.config.output_dir, 'as_reported_output',
                f"comtrade_{self.config.classification_code}_{year}.dta",
            ),
            write_index=False,
        )

        df.to_parquet(
            os.path.join(
                self.config.output_dir, 'as_reported_output',
                f"comtrade_{self.config.classification_code}_{year}.parquet",
            ), 
            compression='snappy',
            index=False,
        )

        del df

    def generate_download_report(self, year_path: Path, replaced_files: list[Path]) -> None:
        """Generate detailed download report with file and processing metadata."""

        report_data = {
           'report_time': datetime.now(),
           'classification': self.config.classification_code,
           'files': []
        }

        for file in Path(year_path).glob("*.gz"):
            comtrade_file = ComtradeFile(file)
            file_data = {
               'reporter_code': comtrade_file.reporter_code,
               'published_date': comtrade_file.published_date,
               'year': comtrade_file.year,
               'file_size': file.stat().st_size,
               'replaced': file in replaced_files,
               'status': 'replaced' if file in replaced_files else 'new'
            }
            report_data['files'].append(file_data)

        df = pd.DataFrame(report_data['files'])
        df['download_time'] = report_data['report_time']
        df['classification'] = report_data['classification'] 

        try:
            existing = pd.read_csv(self.config.download_report_path)
            df = pd.concat([existing, df], ignore_index=True)
        except FileNotFoundError:
            pass
        df.to_csv(self.config.download_report_path, index=False)        
        
    # def output_requested_format(self, year_path) -> None:
    #     files = glob.glob(year_path)
    #     df = pd.read_csv(self.config.input_path, compression='gzip', sep='\t')
    #     if self.config.file_format == 'parquet':
    #         df.to_parquet(self.config.output_path, compression='snappy')
    #     elif self.config.file_format == 'dta':
    #         df.to_stata(self.config.output_path, compression='gzip')
    #     elif self.config.file_format not in ['parquet', 'csv', 'dta']:
    #         raise ValueError(f"Unsupported file format: {self.config.file_format}")


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
