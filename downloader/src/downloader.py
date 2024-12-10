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
        downloaders = {"classic": ClassicDownloader, "bulk": BulkDownloader}
        return downloaders[config.download_type](config)

    def execute_download(self, year: int, year_path: Path, last_updated: datetime):
        pass

    def download_with_retries(
        self, year: int, year_path: Path, last_updated: datetime, num_attempts=3
    ):
        self.year_path = year_path
        attempt = 0
        while attempt < num_attempts:
            try:
                self.execute_download(year, last_updated)
                break
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
            import pdb

            pdb.set_trace()

            for corrupted_file in corrupted_files:
                year = ComtradeFile(corrupted_file).year
                reporter_code = ComtradeFile(corrupted_file).reporter_code
                self.config.logger.info(
                    f"... requesting from api {year}-{reporter_code}."
                )
                import pdb

                pdb.set_trace()

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


class ClassicDownloader(BaseDownloader):
    """
    Comtrade APIs call for as reported data, provided in the classification
    code reported in
    """

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

    def __init__(self, config: ComtradeConfig):
        self.config = config

    def execute_download(self, year: int, last_updated=None, reporter_code=None):
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


class BulkDownloader(BaseDownloader):
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

    def __init__(self, config: ComtradeConfig):
        self.config = config

    def execute_download(self, year: int, last_updated=None, reporter_code=[]):
        params = {
            "subscription_key": self.config.api_key,
            "directory": self.year_path,
            "dataset": "trade",
            "period": str(year),
            "reporterCode": reporter_code,
            "decompress": False,
        }
        if last_updated:
            params["publishedDateFrom"] = last_updated.strftime("%Y-%m-%d")

        with self.suppress_stdout() if self.config.suppress_print else nullcontext():
            comtradeapicall.bulkDownloadFile(**params)
