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

class ComtradeDownloader(object):
    columns = {
        "period": "int16",
        "reporterCode": "int16",
        "flowCode": "category",
        "partnerCode": "int16",
        "partner2Code": "int16",
        "classificationCode": "string",
        "cmdCode": "string",
        "customsCode": "string",
        "mosCode": "int16",
        "motCode": "int16",
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
        
        # api
        if not self.api_key:
            raise ValueError(f"Requires an API KEY for Comtrade")
        # Validate years
        start_year, end_year = self.years[0], self.years[-1]
        if not 1962 <= start_year <= datetime.now().year:
            raise ValueError(f"Invalid start_year: {start_year}")
        if not 1962 <= end_year <= datetime.now().year:
            raise ValueError(f"Invalid end_year: {end_year}")
        if start_year > end_year:
            raise ValueError(f"start_year ({start_year}) must be <= end_year ({end_year})")
                
        # Validate file format
        if self.file_format not in ['parquet', 'csv', 'dta']:
            raise ValueError(f"Unsupported file format: {self.file_format}")

        os.makedirs(self.output_dir, exist_ok=True)
    
    def setup_logger(self, log_level) -> logging.Logger:
        """Setup and configure logger"""
        # Create logger instance
        logger = logging.getLogger('ComtradeDownloader')
        logger.setLevel(log_level)
        return logger
    

    def download_comtrade_bilateral_totals(self):
        """ """
        dfs = []

        for year in self.years:
            year_data = comtradeapicall.getFinalData(
                self.api_key,
                typeCode="C",
                freqCode="A",
                clCode=self.classification_code,
                period=str(year),
                reporterCode=None,
                cmdCode="TOTAL",
                breakdownMode='classic',
                flowCode=None,
                partnerCode=None,
                # update for classic mode
                partner2Code=None,
                customsCode="C00",
                motCode=0,
            )
            try:
                year_data = year_data.rename(
                    columns={"cifvalue": "CIFValue", "fobvalue": "FOBValue"}
                )
                year_data = year_data[self.columns.keys()]
            except:
                year_data = pd.DataFrame(columns=self.columns.keys())

            dfs.append(year_data)
            self.logger.info(f"Completed downloading Commodity Totals for {year}")
        df = pd.concat(dfs)

        # Merge reporter and partner reference tables for ISO3 codes
        df = df.merge(self.reporters, on="reporterCode", how="left")
        df = df.merge(self.partners, on="partnerCode", how="left")

        df.to_csv(
            os.path.join(
                self.output_dir, 'output', f"comtrade_HS_totals.{self.file_extension}"
            ),
            index=False,
        )
        # df.to_parquet(
        #     os.path.join(
        #         self.output_dir, 'output_parquet', f"comtrade_HS_totals.parquet"
        #     ),
        #     index=False,
        # )

        self.logger.info("Completed downloading Commodity Totals for {}".format(self.years))

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
            last_updated = self.get_date_of_last_download(year)
            if last_updated is not None and not self.force_full_download:
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
                                publishedDateFrom=last_updated
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
                            publishedDateFrom=last_updated
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

            if last_updated is not None and not self.force_full_download:
                relocated_files = self.replace_raw_files_with_updated_reports(
                    year, year_path
                )
                self.generate_comtrade_commodity_download_report(
                    year_path, relocated_files
                )
            else:
                self.generate_comtrade_commodity_download_report(year_path, [])
            self.logger.info(f"Generated download report for {year}.")

            self.logger.info(f"Filtering and exporting data for year {year}.")
            corrupted_files = self.combine_clean_comtrade_commodity_year(year)
            attempts = 1
            remove_from_corrupted = set()
            corrupted = False
            while corrupted_files and attempts < 4:
                corrupted = True
                logging.info(f"Found corrupted files")
                for corrupted_file in corrupted_files:
                    year = corrupted_file.split("/")[-1][20:24]
                    reporter_code = corrupted_file.split("/")[-1][17:20]
                    self.logger.info(f"... requesting from api {year}-{reporter_code}.")
                    comtradeapicall.bulkDownloadFinalClassicFile(
                        self.api_key,
                        year_path,
                        typeCode="C",
                        freqCode="A",
                        clCode=self.classification_code,
                        period=year,
                        reporterCode=[reporter_code],
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
                    
            if self.file_format not in ['gzip', 'csv']:
                self.output_requested_format(year_path)

    def get_date_of_last_download(self, year):
        """
        Get information about last date raw files for the classification code
        and year were last downloaded
        """
        raw_file_by_year_path = os.path.join(self.raw_files_path, str(year))
        raw_file_names = []
        if os.path.exists(raw_file_by_year_path):
            raw_file_names = os.listdir(raw_file_by_year_path)
            filtered_files = [
                file for file in raw_file_names if not file.startswith(".")
            ]
            number_files = len(filtered_files)

            if number_files > 0:
                self.logger.info(
                    f"For {self.classification_code} - {year} {number_files} country reporter files are downloaded"
                )
                latest_updated_date = "0000-00-00"
                for file in glob.glob(os.path.join(raw_file_by_year_path, "*.gz")):
                    # from file title, char 27:37 extract most recently updated date
                    updated_date = file[-14:-4]
                    if updated_date > latest_updated_date:
                        latest_updated_date = updated_date
                date = latest_updated_date.split("-")
                latest_date = datetime(int(date[0]), int(date[1]), int(date[2]))
                latest_date = latest_date + timedelta(1)
                return str(latest_date).split(" ")[0]
        else:
            latest_date = None
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
            return {}
        else:
            df_since_download = df[df["timestamp"] > str(latest_date)]
            reporter_codes = df_since_download["reporterCode"].unique()
            return reporter_codes

    def replace_raw_files_with_updated_reports(self, year, year_path):
        """ """
        relocated_files = []
        updated_file_names = os.listdir(year_path)
        if not updated_file_names:
            return relocated_files

        raw_file_by_year_path = os.path.join(self.raw_files_path, str(year))
        raw_file_names = os.listdir(raw_file_by_year_path)

        archive_by_year_dir = os.path.join(self.archived_path, str(year))

        file_name_to_index = {
            file_name[15:20]: index for index, file_name in enumerate(raw_file_names)
        }

        for file_name in updated_file_names:
            self.logger.debug("file name in updated file: ", file_name)
            latest_file = os.path.join(year_path, file_name)
            try:
                outdated_file_name = raw_file_names[
                    file_name_to_index[file_name[15:20]]
                ]
                self.logger.debug("file to replace: ", outdated_file_name)
                outdated_file = os.path.join(raw_file_by_year_path, outdated_file_name)
            except:
                # add the data to the raw output file
                outdated_file = None
                self.logger.debug("not in raw file", file_name)

            try:
                shutil.move(latest_file, raw_file_by_year_path)
                self.logger.debug(f"moved {latest_file} over")
                relocated_files.append(latest_file)
            except shutil.Error as e:
                self.logger.debug(f"did not move over {latest_file}")

            if not os.path.exists(archive_by_year_dir):
                os.makedirs(archive_by_year_dir)

            if outdated_file:
                try:
                    shutil.move(outdated_file, archive_by_year_dir)
                    self.logger.debug(f"archiving {outdated_file}")
                    relocated_files.append(outdated_file)
                except shutil.Error as e:
                    self.logger.debug("Error: ", e)
        self.logger.info(f"Replacing any outdated raw files with latest data")
        return relocated_files

    def combine_clean_comtrade_commodity_year(self, year):
        """ """
        dfs = []

        year_path = os.path.join(self.raw_files_path, str(year))
        corrupted_files = set()
        for col in ['partner2Code', 'motCode', 'customsCode', 'mosCode']:
            self.columns.pop(col, None)   
        for f in glob.glob(os.path.join(year_path, "*.gz")):
            try:
                df = pd.read_csv(
                    f,
                    sep="\t",
                    compression="gzip",
                    usecols=list(self.columns.keys()),
                    dtype=self.columns,
                )

                if self.drop_world_partner:
                    df = df[df.partnerCode != 0]
                dfs.append(df)

            except EOFError as e:
                self.logger.info("downloaded corrupted file: ", f)
                corrupted_files.add(f)
                # year = f.split("/")[-1][20:24]
                # reporter_code = f.split("/")[-1][17:20]
            except pd.errors.EmptyDataError as e:
                self.logger.info("downloaded empty file: ", f)
                corrupted_files.add(f)

        if corrupted_files:
            return corrupted_files

        try:
            df = pd.concat(dfs)

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
        except:
            df = pd.DataFrame()
            self.logger.info(f"No data was downloaded for {year}")
            return

        self.logger.info(f"Saving transformed data file for {year}.")
        df.to_csv(
            os.path.join(
                self.output_dir, 'output',
                f"comtrade_{self.classification_code}_{year}.{self.file_extension}",
            ),
            index=False,
        )
        
        df.to_parquet(
            os.path.join(
                self.output_dir, 'output_parquet',
                f"comtrade_{self.classification_code}_{year}.parquet",
            ), compression='zippy',
            index=False,
        )


        del df
        return {}

    def generate_comtrade_commodity_download_report(self, year_path, replaced_files):
        """
        Generates a download log report as a csv
        """

        data = []

        try:
            report = pd.read_csv(self.download_report_path)
        except:
            report = None

        download_time = time.gmtime()
        if not replaced_files:
            for f in glob.glob(os.path.join(year_path, "*.gz")):
                m = re.match(
                    # updated for CLASSIC
                    "COMTRADE-FINALCLASSIC-CA(?P<reporterCode>\d{3})(?P<year>\d{4})"
                    "(?P<classificationCode>\w+)"
                    "\[(?P<lastUpdateYear>\d{4})-(?P<lastUpdateMonth>\d{2})-"
                    "(?P<lastUpdateDay>\d{2})\]\.gz",
                    f.split("/")[-1],
                )
                data.append(m.groupdict())
        else:
            for file in replaced_files:
                m = re.match(
                    "COMTRADE-FINALCLASSIC-CA(?P<reporterCode>\d{3})(?P<year>\d{4})"
                    "(?P<classificationCode>\w+)"
                    "\[(?P<lastUpdateYear>\d{4})-(?P<lastUpdateMonth>\d{2})-"
                    "(?P<lastUpdateDay>\d{2})\]\.gz",
                    file.split("/")[-1],
                )
                data.append(m.groupdict())
                
        df = pd.DataFrame(data)
        if df.empty:
            self.logger.info("No updated reports were downloaded")
        else:
            df["lastUpdate"] = pd.to_datetime(
                df[["lastUpdateYear", "lastUpdateMonth", "lastUpdateDay"]].rename(
                    columns={
                        "lastUpdateYear": "year",
                        "lastUpdateMonth": "month",
                        "lastUpdateDay": "day",
                    }
                )
            )
            df = df.drop(columns=["lastUpdateYear", "lastUpdateMonth", "lastUpdateDay"])
            df["downloadTime"] = time.strftime("%Y-%m-%d %H:%M:%S", download_time)

        if report is not None:
            report = pd.concat([report, df])
        else:
            report = df

        report.to_csv(self.download_report_path, index=False)
        del report
        
        
    def output_requested_format(self, year_path) -> None:
        files = glob.glob(year_path)
        df = pd.read_csv(self.input_path, compression='gzip', sep='\t')
        if self.file_format == 'parquet':
            df.to_parquet(self.output_path, compression='snappy')
        elif self.file_format == 'dta':
            df.to_stata(self.output_path, compression='gzip')
        elif self.file_format not in ['parquet', 'csv', 'dta']:
            raise ValueError(f"Unsupported file format: {self.file_format}")


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
