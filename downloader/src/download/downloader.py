from dataclasses import dataclass
from pathlib import Path
import re
from datetime import datetime, timedelta
from src.download.configure_downloader import ComtradeConfig
from src.download.comtrade_file import ComtradeFile
from contextlib import contextmanager, nullcontext
import os, sys
import comtradeapicall
import glob
import pandas as pd
import shutil
import time


class BaseDownloader:
    NES_COUNTRIES = [
        "_AC",
        "_X ",
        "X1 ",
        "XX ",
        "R91",
        "A49",
        "E29",
        "R20",
        "MCO",
        "X2 ",
        "A79",
        "A59",
        "F49",
        "O19",
        "F19",
        "E19",
        "F97",
    ]

    EXCL_REPORTER_GROUPS = {
        "ASEAN": "R4 ",  # trailing space
        "European Union": "EUR",
    }

    earliest_date = datetime(1962, 1, 1)

    def __init__(self, config):
        self.config = config
        self.reporters = self._setup_reference_data("reporter")
        self.partners = self._setup_reference_data("partner")

    def _setup_reference_data(self, reference):
        # Comtrade API return Partner uppercase P and Reporter lowercase r
        if reference == "partner":
            api_field_name = "Partner"
        else:
            api_field_name = reference

        ref = comtradeapicall.getReference(reference)[
            [f"{api_field_name}Code", f"{api_field_name}CodeIsoAlpha3"]
        ].rename(
            columns={
                f"{api_field_name}CodeIsoAlpha3": f"{reference}ISO3",
                f"{api_field_name}Code": f"{reference}Code",
            }
        )

        if reference == "reporter":
            ref = ref[~ref[f"reporterISO3"].isin(self.EXCL_REPORTER_GROUPS.values())]
        ref = ref[~ref[f"{reference}ISO3"].isin([self.NES_COUNTRIES])]
        ref[f"{reference}Code"] = ref[f"{reference}Code"].astype(
            self.columns[f"{reference}Code"]
        )
        ref[f"{reference}ISO3"] = ref[f"{reference}ISO3"].astype("string")
        return ref

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

    def atlas_data_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters out Not Elsewhere Specified locations

        Returns:
            pd.DataFrame: Data with NES countries filtered out
        """
        if self.config.partner_iso3_codes:
            df = df[df.partnerISO3.isin(self.config.partner_iso3_codes)]

        return df[~df.partnerISO3.isin(self.NES_COUNTRIES)]

    def download_with_retries(
        self, year: int, year_path: Path, last_updated: datetime, num_attempts=2
    ):
        self.year_path = year_path
        requested_reporters = []
        if self.config.reporter_iso3_codes:
            requested_reporters = comtradeapicall.convertCountryIso3ToCode(
                ",".join(self.config.reporter_iso3_codes)
            )
        attempt = 0
        while attempt < num_attempts:
            try:
                self.execute_download(
                    year,
                    last_updated,
                    reporter_code=requested_reporters if requested_reporters else None,
                )
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

    def handle_iso_codes_recoding(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds ISOCode columns for reporter and partner countries
        """
        # WLD is partner_code 0
        df.loc[df.partnerCode == 0, "partnerISO3"] = "WLD"
        # iso_code S19 as reported by Comtrade is Taiwan
        df.loc[df.partnerCode == 490, "partnerISO3"] = "TWN"
        df.loc[df.reporterCode == 490, "reporterISO3"] = "TWN"

        # South Africa represented in Comtrade as ZA1 and ZAF
        df.loc[df.reporterISO3 == "ZA1", "reporterISO3"] = "ZAF"
        df.loc[df.partnerISO3 == "ZA1", "partnerISO3"] = "ZAF"
        return df.drop(columns=["reporterCode", "partnerCode"])

    def process_downloaded_files(
        self, year, convert_to_parquet=False, save_all_parquet_files=False
    ):
        """
        Validate Data files and Convert files to Parquet
        """
        year_path = Path(self.config.raw_files_path / str(year))
        corrupted_files = set()
        files = glob.glob(
            os.path.join(self.config.raw_files_parquet_path, str(year), "*.parquet")
        )
        parquet_files = [file.split("/")[-1].split(".")[0] for file in files]

        for f in glob.glob(os.path.join(year_path, "*.gz")):
            try:
                # complete file read needed to catch all corrupted files
                df = pd.read_csv(
                    f,
                    sep="\t",
                    compression="gzip",
                    usecols=list(self.columns.keys()),
                    dtype=self.columns,
                )
                del df

            except EOFError as e:
                self.config.logger.info(f"downloaded corrupted file: {f}")
                corrupted_files.add(f)
                continue

            except pd.errors.EmptyDataError as e:
                self.config.logger.info(f"downloaded empty file: {f}")
                corrupted_files.add(f)
                continue
        if corrupted_files:
            self.handle_corrupt_files(year, corrupted_files)
        for f in glob.glob(os.path.join(year_path, "*.gz")):
            if convert_to_parquet:
                file_name = f.split("/")[-1].split(".")[0]
                if file_name not in parquet_files or save_all_parquet_files:
                    df = pd.read_csv(
                        f,
                        sep="\t",
                        compression="gzip",
                        usecols=list(self.columns.keys()),
                        dtype=self.columns,
                    )

                    df.to_parquet(
                        Path(
                            self.config.raw_files_parquet_path
                            / str(year)
                            / f"{file_name}.parquet",
                        ),
                        compression="snappy",
                        index=False,
                    )
                    del df

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

    def handle_corrupt_files(self, year, corrupted_files):
        remove_from_corrupted = set()
        for corrupted_file in corrupted_files:
            self.config.logger.info(f"Found corrupted files")
            year = ComtradeFile(corrupted_file).year
            reporter_code = ComtradeFile(corrupted_file).reporter_code
            self.config.logger.info(f"... requesting from api {year}-{reporter_code}.")
            attempts = 1
            while attempts < 3:
                try:
                    self.config.logger.info(f"Trying {attempts} attempt")
                    self.execute_download(year, self.earliest_date, reporter_code)
                    df = pd.read_csv(
                        corrupted_file,
                        sep="\t",
                        compression="gzip",
                        usecols=list(self.columns.keys()),
                        dtype=self.columns,
                    )
                    # don't delete from list being iterated over
                    remove_from_corrupted.add(corrupted_file)
                    break
                except:
                    attempts += 1
                    self.config.logger.info(
                        f"{corrupted_file} on attempt {attempts} after initial failure  is still corrupted"
                    )

        for file in remove_from_corrupted:
            self.config.logger.info(f"remove {remove_from_corrupted}")
            corrupted_files.remove(file)

        for f in corrupted_files:
            self.config.logger.info(
                f"download failed, removing from raw downloaded folder {f}"
            )
            if os.path.isfile(Path(self.config.corrupted_path / ComtradeFile(f).name)):
                os.remove(Path(self.config.corrupted_path / ComtradeFile(f).name))
            shutil.move(f, Path(self.config.corrupted_path / ComtradeFile(f).name))

    def handle_digit_level(self, df: pd.DataFrame) -> pd.DataFrame:
        # create product digitlevel column based on commodity code
        df = df.assign(digitLevel=df["cmdCode"].str.len())
        # zero digit value replaces the word TOTAL
        df.loc[df.cmdCode == "TOTAL", "digitLevel"] = 0
        return df


class ClassicDownloader(BaseDownloader):
    """
    Comtrade APIs call for as reported data, provided in the classification
    code reported in
    """

    columns = {
        # "period": "int16",
        "reporterCode": "category",
        "flowCode": "category",
        "partnerCode": "category",
        # "partner2Code": "int16",
        # "classificationCode": "string",
        "cmdCode": "string",
        # "customsCode": "category",
        # "mosCode": "category",
        # "motCode": "category",
        # "qtyUnitCode": "int8",
        "qty": "float64",
        # "isQtyEstimated": "int8",
        # Comtrade passes NAN values
        "CIFValue": "float64",
        "FOBValue": "float64",
        "primaryValue": "float64",
        "isAggregate": "category",
    }

    def __init__(self, config: ComtradeConfig):
        super().__init__(config)

    def execute_download(self, year: int, last_updated, reporter_code):
        params = {
            "subscription_key": self.config.api_key,
            "directory": str(self.year_path),
            "typeCode": "C",
            "freqCode": "A",
            "clCode": self.config.classification_code,
            "period": str(year),
            # reporterCode format is the only params difference for classic
            "reporterCode": reporter_code,
            "decompress": False,
        }
        if last_updated != self.earliest_date:
            params["publishedDateFrom"] = (last_updated + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
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
    columns = {
        # "period": "int16",
        "reporterCode": "category",
        "flowCode": "category",
        "partnerCode": "category",
        "partner2Code": "int16",
        # "classificationCode": "string",
        "cmdCode": "string",
        "customsCode": "category",
        "mosCode": "category",
        "motCode": "category",
        # "qtyUnitCode": "int8",
        "qty": "float64",
        # "isQtyEstimated": "int8",
        # Comtrade passes NAN values
        "CIFValue": "float64",
        "FOBValue": "float64",
        "primaryValue": "float64",
        "isAggregate": "category",
    }

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
            "reporterCode": reporter_code,
            "decompress": False,
        }
        if last_updated != self.earliest_date:
            params["publishedDateFrom"] = (last_updated + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )

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
