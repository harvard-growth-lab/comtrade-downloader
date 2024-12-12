from dataclasses import dataclass
from pathlib import Path
import regex as re
from datetime import datetime
from src.configure_downloader import ComtradeConfig
from src.comtrade_file import ComtradeFile
from contextlib import contextmanager, nullcontext
import os, sys
import comtradeapicall
import glob
import pandas as pd


class BaseDownloader:
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
        # "qtyUnitCode": "int8",
        # "qty": "float64",
        # "isQtyEstimated": "int8",
        "CIFValue": "float64",
        "FOBValue": "float64",
        "primaryValue": "float64",
    }
    
    NES_COUNTRIES = ['_AC','_X ','X1 ', 'XX ','R91','A49','E29','R20','MCO','X2 ', 'A79','A59','F49','O19','F19','E19','F97']

    EXCL_REPORTER_GROUPS = {'ASEAN': 'R4', 
                           'European Union': 'EUR', 
                           }

    earliest_date = datetime(1962, 1, 1)

    def __init__(self, config):
        self.config = config
        self._setup_reference_data()
        
        
    def _setup_reference_data(self):
        self.reporters = comtradeapicall.getReference("reporter")[
           ["reporterCode", "reporterCodeIsoAlpha3"]
        ].rename(columns={"reporterCodeIsoAlpha3": "reporterISO3"})
        self.reporters = self.reporters[~self.reporters.reporterISO3.isin(self.EXCL_REPORTER_GROUPS.values())]
        self.reporters = self.reporters[~self.reporters.reporterISO3.isin([self.NES_COUNTRIES])]

        self.partners = comtradeapicall.getReference("partner")[
           ["PartnerCode", "PartnerCodeIsoAlpha3"]
        ].rename(columns={
           "PartnerCode": "partnerCode",
           "PartnerCodeIsoAlpha3": "partnerISO3",
        })
        self.partners = self.partners[~self.partners.partnerISO3.isin([self.NES_COUNTRIES])]
        

    @contextmanager
    def suppress_stdout(self):
        with open(os.devnull, "w") as devnull:
            old_stdout = sys.stdout
            sys.stdout = devnull
            try:
                yield
            finally:
                sys.stdout = old_stdout

    @staticmethod
    def create_downloader(config):
        downloaders = {"classic": ClassicDownloader, "final": BulkDownloader}
        return downloaders[config.download_type](config)

    def execute_download(self, year: int, year_path: Path, last_updated: datetime):
        pass

    def atlas_data_filter(self, df: pd.DataFrame):
        query_statement = "(flowCode == 'M' or flowCode == 'X' or flowCode == 'RM' or flowCode == 'RX')"
        df = df.query(query_statement)
        return self._handle_digit_level(df)
    
    def download_with_retries(
        self, year: int, year_path: Path, last_updated: datetime, num_attempts=3
    ):
        self.year_path = year_path
        if self.config.reporter_iso3_codes:
            requested_reporters = comtradeapicall.convertCountryIso3ToCode(self.config.reporter_iso3_codes[0])
        attempt = 0
        while attempt < num_attempts:
            try:
                self.execute_download(year, last_updated, reporter_code=requested_reporters if requested_reporters else [])
                return
            except ConnectionResetError as e:
                wait = 2**attempt
                attempt += 1
                self.config.logger.warning(
                    f"Attempt {attempt+1}/{num_attempts} failed: {e}, waiting {wait}s"
                )
                time.sleep(wait)
            except KeyError as e:
                self.config.logger.info(f"An error occurred: {str(e)}")
                attempt += 1
        self.config.logger.warning(f"reached max attempts {num_attempts}")
        
    def clean_data(self, df):
        """
        Adds ISOCode columns for reporter and partner countries
        """
        # WLD is partner_code 0 
        df.loc[df.partnerCode==0, 'partnerISO3'] = "WLD"
        # iso_code S19 as reported by Comtrade is Taiwan
        df.loc[df.partnerCode==490, 'partnerISO3'] = "TWN"
        df.loc[df.reporterCode==490, 'reporterISO3'] = "TWN"
        
        df = df[~df.partnerISO3.isin(self.NES_COUNTRIES)]
        
        # South Africa represented in Comtrade as ZA1 and ZAF
        df.loc[df.reporterISO3=="ZA1", "reporterISO3"] = "ZAF"
        df.loc[df.partnerISO3=="ZA1", "partnerISO3"] = "ZAF"
        return df


    def _find_corrupt_files(self, year):
        """return any empty or corrupted files."""
        dfs = []
        year_path = Path(self.config.raw_files_path / str(year))
        corrupted_files = set()

        for f in glob.glob(os.path.join(year_path, "*.gz")):
            try:
                df = pd.read_csv(
                    f,
                    sep="\t",
                    compression="gzip",
                    usecols=list(self.columns.keys()),
                    dtype=self.columns,
                    nrows=1,
                )

            except EOFError as e:
                self.config.logger.info(f"downloaded corrupted file: {f}")
                corrupted_files.add(f)

            except pd.errors.EmptyDataError as e:
                self.config.logger.info(f"downloaded empty file: {f}")
                corrupted_files.add(f)
        return corrupted_files

    def handle_corrupt_files(self, year):
        corrupted_files = self._find_corrupt_files(year)
        attempts = 1
        remove_from_corrupted = set()
        # corrupted = False
        while corrupted_files and attempts < 3:
            corrupted = True
            logging.info(f"Found corrupted files")
            for corrupted_file in corrupted_files:
                year = ComtradeFile(corrupted_file).year
                reporter_code = ComtradeFile(corrupted_file).reporter_code
                self.config.logger.info(
                    f"... requesting from api {year}-{reporter_code}."
                )
                self.execute_download(year, last_updated, reporter_code)
                # attempt to read in all re-downloaded file using reporter code
                try:
                    df = pd.read_csv(
                        corrupted_file,
                        sep="\t",
                        compression="gzip",
                        usecols=list(self.columns.keys()),
                        dtype=self.columns,
                        nrows=1,
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
            import pdb

            pdb.set_trace()
            for f in corrupted_files:
                self.config.logger.info(
                    f"download failed, removing from raw downloaded folder {f}"
                )
                shutil.move(f, Path(self.config.corrupted_path / ComtradeFile(f).name))
                
                
    def _handle_digit_level(self, df: pd.DataFrame):
        # create product digitlevel column based on commodity code
        df = df.assign(
            digitLevel=df["cmdCode"].str.len()
        )
        # zero digit value replaces the word TOTAL
        df.loc[df.cmdCode == "TOTAL", "digitLevel"] = 0
        return df

        

class ClassicDownloader(BaseDownloader):
    """
    Comtrade APIs call for as reported data, provided in the classification
    code reported in
    """

    def __init__(self, config: ComtradeConfig):
        super().__init__(config)

    def execute_download(self, year: int, last_updated, reporter_code):
        params = {
            "subscription_key": self.config.api_key,
            "directory": self.year_path,
            "typeCode": "C",
            "freqCode": "A",
            "clCode": self.config.classification_code,
            "period": str(year),
            # reporterCode format is the only params difference for classic
            "reporterCode": reporter_code,
            "decompress": False,
        }
        if last_updated:
            params["publishedDateFrom"] = last_updated.strftime("%Y-%m-%d")

        with self.suppress_stdout() if self.config.suppress_print else nullcontext():
            comtradeapicall.bulkDownloadFinalClassicFile(**params)
            
            
    def get_reporters_by_data_availability(self, year: int, latest_date: datetime):
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
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df_since_download = df[df["timestamp"].dt.date > latest_date.date()]
            reporter_codes = df_since_download["reporterCode"].unique()
            return reporter_codes


class BulkDownloader(BaseDownloader):

    def __init__(self, config: ComtradeConfig):
        super().__init__(config)
        # self.columns |={
        #     "partner2Code": "int16",
        #     # "cmdCode": "string",
        #     "customsCode": "string",
        #     "mosCode": "int16",
        #     "motCode": "int16",
        # }


    def execute_download(self, year: int, last_updated, reporter_code):
        params = {
            "subscription_key": self.config.api_key,
            "directory": self.year_path,
            "typeCode": "C",
            "freqCode": "A",
            "clCode": self.config.classification_code,
            "period": str(year),
            "reporterCode": reporter_code,
            "decompress": False,
        }
        if last_updated:
            params["publishedDateFrom"] = last_updated.strftime("%Y-%m-%d")
            
        with self.suppress_stdout() if self.config.suppress_print else nullcontext():
            comtradeapicall.bulkDownloadFinalFile(**params)
            
    
    def get_reporters_by_data_availability(self, year: int, latest_date: datetime):
        df = comtradeapicall.getFinalDataBulkAvailability(
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
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df_since_download = df[df["timestamp"].dt.date > latest_date.date()]
            reporter_codes = df_since_download["reporterCode"].unique()
            return reporter_codes

