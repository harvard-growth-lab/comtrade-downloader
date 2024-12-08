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
from pathlib import Path

class ComtradeDownloader(object):
    columns = {
        "period": "int16",
        "reporterCode": "int16",
        "flowCode": "category",
        "partnerCode": "int16",
        # "partner2Code": "int16",
        "classificationCode": "string",
        "cmdCode": "string",
        # "customsCode": "string",
        # "mosCode": "int16",
        # "motCode": "int16",
        "qtyUnitCode": "int8",
        "qty": "float64",
        "isQtyEstimated": "int8",
        "CIFValue": "float64",
        "FOBValue": "float64",
        "primaryValue": "float64",
    }

    def __init__(
        self,
        api_key,
        output_dir,
        classification_code,
        file_format,
        log_level,
        start_year,
        end_year,
        reporter_iso3_codes=[],
        partner_iso3_codes=[],
        commodity_codes=[],
        flow_codes=["M", "X"],
        mot_codes=[0],
        mos_codes=[0],
        customs_codes=["C00"],
        drop_world_partner=False,
        drop_secondary_partners=True,
        delete_tmp_files=True,
        compress_output=True,
        suppress_print=True,
        force_full_download=False,
    ):
        self.logger = self.setup_logger(log_level)
        
        self.api_key = api_key
        self.output_dir = output_dir
        self.run_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.gmtime())
        self.download_report_path = os.path.join(
            self.output_dir, 'atlas_download_reports', f"download_report_{self.run_time}.csv"
        )

        self.classification_code = classification_code
        self.years = range(start_year, end_year + 1)
        self.reporter_iso3_codes = reporter_iso3_codes
        self.partner_iso3_codes = partner_iso3_codes
        self.commodity_codes = commodity_codes
        self.flow_codes = flow_codes
        self.mot_codes = mot_codes
        self.mos_codes = mos_codes
        self.customs_codes = customs_codes

        self.drop_world_partner = drop_world_partner
        self.drop_secondary_partners = drop_secondary_partners

        self.delete_tmp_files = delete_tmp_files
        self.suppress_print = suppress_print
        self.force_full_download = force_full_download
        
        self.file_format = file_format
        self.validate_inputs()

        if compress_output:
            self.file_extension = "gz"
        else:
            self.file_extension = "csv"

        ###############################################################

        self.reporters = comtradeapicall.getReference("reporter")[
            ["reporterCode", "reporterCodeIsoAlpha3"]
        ].rename(columns={"reporterCodeIsoAlpha3": "reporterISO3"})

        self.partners = comtradeapicall.getReference("partner")[
            ["PartnerCode", "PartnerCodeIsoAlpha3"]
        ].rename(
            columns={
                "PartnerCode": "partnerCode",
                "PartnerCodeIsoAlpha3": "partnerISO3",
            }
        )

        ###############################################################

        # Make directory for most recently updated raw files
        self.latest_path = os.path.join(
            self.output_dir, "latest_raw_as_reported", self.classification_code
        )
        os.makedirs(self.latest_path, exist_ok=True)

        # Make directory for raw files used by extractor script
        self.raw_files_path = os.path.join(
            self.output_dir, "raw_as_reported", self.classification_code
        )
        os.makedirs(self.raw_files_path, exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "corrupted_as_reported"), exist_ok=True)

        # Make directory for archiving out of date raw files
        self.archived_path = os.path.join(
            self.output_dir, "archived_raw_as_reported", self.classification_code
        )
        os.makedirs(self.archived_path, exist_ok=True)

        # # Remove temporary directory
        # if self.delete_tmp_files:
        #     self.remove_tmp_dir(self.tmp_path)
    
    def validate_inputs(self):
        """Validate configuration values"""
        if not self.api_key:
            raise ValueError(f"Requires an API KEY for Comtrade")

        start_year, end_year = self.years[0], self.years[-1]
        if not 1962 <= start_year <= datetime.now().year:
            raise ValueError(f"Invalid start_year: {start_year}")
        if not 1962 <= end_year <= datetime.now().year:
            raise ValueError(f"Invalid end_year: {end_year}")
        if start_year > end_year:
            raise ValueError(f"start_year ({start_year}) must be <= end_year ({end_year})")
                
        if self.file_format not in ['parquet', 'csv', 'dta']:
            raise ValueError(f"Unsupported file format: {self.file_format}")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def setup_logger(self, log_level) -> logging.Logger:
        logger = logging.getLogger('ComtradeDownloader')
        logger.setLevel(log_level)
        return logger
    
    
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
        for year in self.years:
            last_updated = self.get_last_download_date(year)
            if last_updated > datetime(1900, 1, 1) and not self.force_full_download:
                updated_reporters = self.get_reporters_by_data_availability(
                    year, last_updated
                )
                year_path = os.path.join(self.latest_path, str(year))
                self.logger.info(
                    f"Downloading reporter {self.classification_code} - {year} "
                    f"files updated since {last_updated}."
                )
            else:
                last_updated = None
                year_path = os.path.join(self.raw_files_path, str(year))
                self.logger.info(f"Downloading all {self.classification_code} - {year}.")
            os.makedirs(year_path, exist_ok=True)
            max_retries = 5
            attempt = 0
            while attempt < max_retries:
                try:
                    if self.suppress_print:
                        with HiddenPrints():
                            comtradeapicall.bulkDownloadFinalClassicFile(
                                self.api_key,
                                year_path,
                                typeCode="C",
                                freqCode="A",
                                clCode=self.classification_code,
                                period=str(year),
                                # updated reporter codes to match classic
                                reporterCode=None,
                                decompress=False,
                                publishedDateFrom=last_updated.strftime("%Y-%m-%d")
                                # publishedDateTo='2018-01-01'
                            )
                    else:
                        comtradeapicall.bulkDownloadFinalClassicFile(
                            self.api_key,
                            year_path,
                            typeCode="C",
                            freqCode="A",
                            clCode=self.classification_code,
                            period=str(year),
                            # updated reporter codes to match classic
                            reporterCode=None,
                            decompress=False,
                            publishedDateFrom=last_updated.strftime("%Y-%m-%d")
                            # publishedDateTo='2018-01-01'
                        )
                    self.logger.info(f"Completed apicall for year {year}.")
                    break
                except ConnectionResetError as e:
                    self.logger.info(f"Connection Reset Error: {e}")
                    attempt += 1
                    time.sleep(2**attempt)
                except KeyError as e:
                    self.logger.info(f"An error occurred: {str(e)}")

            relocated_files = [] if last_updated is None or self.force_full_download else self.replace_raw_files_with_updated_reports(year, year_path)        
            
            self.generate_download_report(
                    year_path, relocated_files
                )
            self.logger.info(f"Generated download report for {year}.")
            self.logger.info(f"Filtering and exporting data for year {year}.")
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
                    self.logger.info(f"... requesting from api {year}-{reporter_code}.")
                    comtradeapicall.bulkDownloadFinalClassicFile(
                        self.api_key,
                        year_path,
                        typeCode="C",
                        freqCode="A",
                        clCode=self.classification_code,
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
                            usecols=list(self.columns.keys()),
                            dtype=self.columns,
                        )
                        # if successful read then remove from corrupted_files
                        remove_from_corrupted.add(corrupted_file)
                    except:
                        self.logger.info(
                            f"{corrupted_file} on attempt {attempts} after initial failure  is still corrupted"
                        )
                        continue
                for file in remove_from_corrupted:
                    self.logger.info(f"remove {remove_from_corrupted}")
                    corruped_files.remove(file)
                attempts += 1
            if corrupted_files:
                for f in corrupted_files:
                    self.logger.info(
                        f"download failed, removing from raw downloaded folder {f}"
                    )
                    shutil.move(f, os.path.join(self.output_dir, "corrupted", f.split('/')[-1]))
            
            df = self.transform_data(year)
            self.save_combined_comtrade_year(df, year)

    def get_last_download_date(self, year):
        """
        Get information about last date raw files for the classification code
        and year were last downloaded
        """
        year_path = Path(self.raw_files_path) / str(year)
        min_date = datetime(1900, 1, 1)
        if not year_path.exists():
            return min_date
        
        files = list(year_path.glob("*.gz"))
        if not files:
            return min_date
        
        self.logger.info(f"{self.classification_code} - {year}: {len(files)} reporter files found")
        comtrade_files = [ComtradeFile(f) for f in files]
        latest_date = max(file.published_date for file in comtrade_files)
        return latest_date

        
    def get_reporters_by_data_availability(self, year, latest_date):
        df = comtradeapicall.getFinalClassicDataBulkAvailability(
            self.api_key,
            typeCode="C",
            freqCode="A",
            clCode=self.classification_code,
            period=str(year),
            reporterCode=None,
        )
        if df.empty:
            return []
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df_since_download = df[df['timestamp'].dt.date > latest_date.date()]
            reporter_codes = df_since_download["reporterCode"].unique()
            import pdb
            pdb.set_trace()
            return reporter_codes

    def replace_raw_files_with_updated_reports(self, year, year_path):
        """ """
        # most recently downloaded files
        updated_files = list(Path(year_path).glob("*.gz"))
        if not updated_files:
            return []

        raw_year_path = Path(self.raw_files_path) / str(year)
        archive_path = Path(self.archived_path) / str(year)
        archive_path.mkdir(parents=True, exist_ok=True)
        
        relocated = []
        raw_files_map = {ComtradeFile(f).reporter_code: f for f in raw_year_path.glob("*.gz")}

        for updated_file in updated_files:
            self.logger.debug("updated file: ", updated_file)
            updated = ComtradeFile(updated_file)
            try:
                if outdated_file := raw_files_map.get(updated.reporter_code):
                    shutil.move(outdated_file, archive_path)
                    relocated.append(outdated_file)
            except shutil.Error as e:
                self.logger.error(f"Failed to move {outdated_file}: {e}")
            try:
                shutil.move(updated_file, raw_year_path)
                relocated.append(updated_file)
            except shutil.Error as e:
                self.logger.error(f"Failed to move {updated_file}: {e}")
        
        self.logger.info(f"Replaced outdated raw files with latest data")
        self.logger.info(f"These reporters updated their data {ComtradeFile(file).reporter for file in relocated}")
        return relocated
    
    
    def find_corrupt_files(self, year):
        """return any empty or corrupted files."""
        dfs = []
        year_path = os.path.join(self.raw_files_path, str(year))
        corrupted_files = set()

        for f in glob.glob(os.path.join(year_path, "*.gz")):
            try:
                df = pd.read_csv(
                    f,
                    sep="\t",
                    compression="gzip",
                    usecols=list(self.columns.keys()),
                    dtype=self.columns,
                    nrows=1
                )

            except EOFError as e:
                self.logger.info(f"downloaded corrupted file: {f}")
                corrupted_files.add(f)

            except pd.errors.EmptyDataError as e:
                self.logger.info(f"downloaded empty file: {f}")
                corrupted_files.add(f)
        return corrupted_files 


    def transform_data(self, year):
        """"""
        year_path = Path(self.raw_files_path) / str(year)
        df = pd.concat((pd.read_csv(f, 
                                    compression='gzip', 
                                    sep='\t',
                                    usecols=list(self.columns.keys()),
                                    dtype=self.columns,) 
                        for f in glob.glob(os.path.join(year_path, "*.gz"))), ignore_index=True)
        
        # Merge reporter and partner reference tables for ISO3 codes
        df = df.merge(self.reporters, on="reporterCode", how="left")
        df = df.merge(self.partners, on="partnerCode", how="left")

        if self.partner_iso3_codes:
            df = df[df.partnerISO3.isin(self.partner_iso3_codes)]

        if not self.drop_secondary_partners:
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
        self.logger.info(f"Saving transformed data file for {year}.")
        df.to_stata(
            os.path.join(
                self.output_dir, 'as_reported_output',
                f"comtrade_{self.classification_code}_{year}.dta",
            ),
            write_index=False,
        )

        df.to_parquet(
            os.path.join(
                self.output_dir, 'as_reported_output',
                f"comtrade_{self.classification_code}_{year}.parquet",
            ), 
            compression='snappy',
            index=False,
        )

        del df

    def generate_download_report(self, year_path: Path, replaced_files: list[Path]) -> None:
        """Generate detailed download report with file and processing metadata."""

        report_data = {
           'report_time': datetime.now(),
           'classification': self.classification_code,
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
           existing = pd.read_csv(self.download_report_path)
           df = pd.concat([existing, df], ignore_index=True)
        except FileNotFoundError:
           pass
        df.to_csv(self.download_report_path, index=False)        
        
    # def output_requested_format(self, year_path) -> None:
    #     files = glob.glob(year_path)
    #     df = pd.read_csv(self.input_path, compression='gzip', sep='\t')
    #     if self.file_format == 'parquet':
    #         df.to_parquet(self.output_path, compression='snappy')
    #     elif self.file_format == 'dta':
    #         df.to_stata(self.output_path, compression='gzip')
    #     elif self.file_format not in ['parquet', 'csv', 'dta']:
    #         raise ValueError(f"Unsupported file format: {self.file_format}")


    def remove_tmp_dir(self, tmp_path):
        """ """
        for f in glob.glob(os.path.join(tmp_path, "*.gz")):
            try:
                os.move(f)
            except OSError as e:
                self.logger.info(f"Error: {f} : {e.strerror}")

        try:
            os.rmdir(tmp_path)
        except OSError as e:
            self.logger.info(f"Error: {tmp_path} : {e.strerror}")
