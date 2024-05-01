#!/usr/bin/env python
# coding: utf-8


"""
ComtradeDownloader object uses apicomtradecall to output reporter data 
by Classification Code by Year by Reporter
"""

import requests
import pandas as pd
import requests
import comtradeapicall
import glob
import os
import shutil
import sys
import re
import time
from datetime import date, timedelta, datetime
import logging

logging.basicConfig(level=logging.INFO)

API_KEYS = {
    "Brendan": "ecd16be0c5ec4ed5b95dd7eb23d98fbf",
    "Ellie": "fa60b196283d493c9f65ad7acfa3d76d",
}

logging.basicConfig(level=logging.INFO)


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
        self.api_key = api_key
        self.output_dir = output_dir
        self.run_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.gmtime())
        self.download_report_path = os.path.join(
            self.output_dir, f"download_report_{self.run_time}.csv"
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
            self.output_dir, "latest_raw", self.classification_code
        )
        os.makedirs(self.latest_path, exist_ok=True)

        # Make directory for raw files used by extractor script
        self.raw_files_path = os.path.join(
            self.output_dir, "raw", self.classification_code
        )
        os.makedirs(self.raw_files_path, exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "corrupted"), exist_ok=True)

        # Make directory for archiving out of date raw files
        self.archived_path = os.path.join(
            self.output_dir, "archived_raw", self.classification_code
        )
        os.makedirs(self.archived_path, exist_ok=True)

        # # Remove temporary directory
        # if self.delete_tmp_files:
        #     self.remove_tmp_dir(self.tmp_path)

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
                flowCode=None,
                partnerCode=None,
                partner2Code=[0],
                customsCode="C00",
                motCode=0,
            )
            year_data = year_data.rename(
                columns={"cifvalue": "CIFValue", "fobvalue": "FOBValue"}
            )
            year_data = year_data[self.columns.keys()]

            dfs.append(year_data)
            logging.info("Completed downloading Commodity Totals for {}".format(year))
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
        logging.info("Completed downloading Commodity Totals for {}".format(self.years))

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
                logging.info(
                    f"Downloading reporter {self.classification_code} - {year} "
                    f"files updated since {last_updated}."
                )
            else:
                last_updated = None
                year_path = os.path.join(self.raw_files_path, str(year))
                logging.info(f"Downloading all {self.classification_code} - {year}.")
            os.makedirs(year_path, exist_ok=True)
            max_retries = 5
            attempt = 0
            while attempt < max_retries:
                try:
                    if self.suppress_print:
                        with HiddenPrints():
                            comtradeapicall.bulkDownloadFinalFile(
                                self.api_key,
                                year_path,
                                typeCode="C",
                                freqCode="A",
                                clCode=self.classification_code,
                                period=str(year),
                                reporterCode=reporter_codes,
                                decompress=False,
                                publishedDateFrom=last_updated
                                # publishedDateTo='2018-01-01'
                            )
                    else:
                        comtradeapicall.bulkDownloadFinalFile(
                            self.api_key,
                            year_path,
                            typeCode="C",
                            freqCode="A",
                            clCode=self.classification_code,
                            period=str(year),
                            reporterCode=reporter_codes,
                            decompress=False,
                            publishedDateFrom=last_updated
                            # publishedDateTo='2018-01-01'
                        )
                    logging.info(f"Completed apicall for year {year}.")
                    break
                except ConnectionResetError as e:
                    logging.info(f"Connection Reset Error: {e}")
                    attempt += 1
                    time.sleep(2**attempt)
                except KeyError as e:
                    logging.info(f"An error occurred: {str(e)}")

            if last_updated is not None and not self.force_full_download:
                relocated_files = self.replace_raw_files_with_updated_reports(
                    year, year_path
                )
                self.generate_comtrade_commodity_download_report(
                    year_path, relocated_files
                )
            else:
                self.generate_comtrade_commodity_download_report(year_path, [])
            logging.info(f"Generated download report for {year}.")

            logging.info(f"Filtering and exporting data for year {year}.")
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
                    logging.info(f"... requesting from api {year}-{reporter_code}.")
                    comtradeapicall.bulkDownloadFinalFile(
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
                        logging.info(
                            f"{corrupted_file} on attempt {attempts} after initial failure  is still corrupted"
                        )
                        continue
                for file in remove_from_corrupted:
                    logging.info(f"remove {remove_from_corrupted}")
                    corruped_files.remove(file)
                attempts += 1
            for f in corrupted_files:
                logging.info(
                    f"download failed, removing from raw downloaded folder {f}"
                )
                # year = f.split("/")[-1][20:24]
                # reporter_code = f.split("/")[-1][17:20]
                # year_path = os.path.join(self.raw_files_path, year)
                # logging.info(f"year path: {year_path}")
                # place in a corrupted files folder for follow-up with comtrade
                shutil.move(f, os.path.join(self.output_dir, "corrupted", f.split('/')[-1]))
            # logging.info(f"Cleaning up year {year}.")
            # if self.delete_tmp_files:
            #     self.remove_tmp_dir(year_path)

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
                logging.info(
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
        df = comtradeapicall.getFinalDataBulkAvailability(
            self.api_key,
            typeCode="C",
            freqCode="A",
            clCode=self.classification_code,
            period=str(year),
            reporterCode=None,
        )
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
            logging.debug("file name in updated file: ", file_name)
            latest_file = os.path.join(year_path, file_name)
            try:
                outdated_file_name = raw_file_names[
                    file_name_to_index[file_name[15:20]]
                ]
                logging.debug("file to replace: ", outdated_file_name)
                outdated_file = os.path.join(raw_file_by_year_path, outdated_file_name)
            except:
                # add the data to the raw output file
                outdated_file = None
                logging.debug("not in raw file", file_name)

            try:
                shutil.move(latest_file, raw_file_by_year_path)
                logging.debug(f"moved {latest_file} over")
                relocated_files.append(latest_file)
            except shutil.Error as e:
                logging.debug(f"did not move over {latest_file}")

            if not os.path.exists(archive_by_year_dir):
                os.makedirs(archive_by_year_dir)

            if outdated_file:
                try:
                    shutil.move(outdated_file, archive_by_year_dir)
                    logging.debug(f"archiving {outdated_file}")
                    relocated_files.append(outdated_file)
                except shutil.Error as e:
                    logging.debug("Error: ", e)
        logging.info(f"Replacing any outdated raw files with latest data")
        return relocated_files

    def combine_clean_comtrade_commodity_year(self, year):
        """ """
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
                )

                if self.drop_world_partner:
                    df = df[df.partnerCode != 0]

                if self.drop_secondary_partners:
                    df = df[df.partner2Code == 0]
                    df = df.drop(columns=["partner2Code"])

                dfs.append(df)

            except EOFError as e:
                logging.info("downloaded corrupted file: ", f)
                corrupted_files.add(f)
                # year = f.split("/")[-1][20:24]
                # reporter_code = f.split("/")[-1][17:20]

        if corrupted_files:
            return corrupted_files

            # Filter unneeded data before appending to keep what is stored in memory
            # as low as possible.
            #             if self.commodity_codes:
            #                 df = df[df.cmdCode.isin(self.commodity_codes)]
            #             if self.flow_codes:
            #                 df = df[df.flowCode.isin(self.flow_codes)]
            #             if self.mot_codes:
            #                 df = df[df.motCode.isin(self.mot_codes)]
            #             if self.mos_codes:
            #                 df = df[df.mosCode.isin(self.mos_codes)]
            #             if self.customs_codes:
            #                 df = df[df.customsCode.isin(self.customs_codes)]

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
            logging.info(f"No data was downloaded for {year}")
            return

        logging.info(f"Saving transformed data file for {year}.")
        df.to_csv(
            os.path.join(
                self.output_dir, 'output',
                f"comtrade_{self.classification_code}_{year}.{self.file_extension}",
            ),
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
                    "COMTRADE-FINAL-CA(?P<reporterCode>\d{3})(?P<year>\d{4})"
                    "(?P<classificationCode>\w+)"
                    "\[(?P<lastUpdateYear>\d{4})-(?P<lastUpdateMonth>\d{2})-"
                    "(?P<lastUpdateDay>\d{2})\]\.gz",
                    f.split("/")[-1],
                )
                data.append(m.groupdict())
        else:
            for file in replaced_files:
                m = re.match(
                    "COMTRADE-FINAL-CA(?P<reporterCode>\d{3})(?P<year>\d{4})"
                    "(?P<classificationCode>\w+)"
                    "\[(?P<lastUpdateYear>\d{4})-(?P<lastUpdateMonth>\d{2})-"
                    "(?P<lastUpdateDay>\d{2})\]\.gz",
                    file.split("/")[-1],
                )
                data.append(m.groupdict())

        df = pd.DataFrame(data)
        if df.empty:
            logging.info("No updated reports were downloaded")
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

    def remove_tmp_dir(self, tmp_path):
        """ """
        for f in glob.glob(os.path.join(tmp_path, "*.gz")):
            try:
                os.move(f)
            except OSError as e:
                logging.info(f"Error: {f} : {e.strerror}")

        try:
            os.rmdir(tmp_path)
        except OSError as e:
            logging.info(f"Error: {tmp_path} : {e.strerror}")
