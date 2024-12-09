from dataclasses import dataclass
from pathlib import Path
import regex as re
from datetime import datetime
from src.configure_downloader import ComtradeConfig
from contextlib import contextmanager, nullcontext
import os, sys
import comtradeapicall


class BaseDownloader:
    
    @contextmanager
    def suppress_stdout(self):
        with open(os.devnull, 'w') as devnull:
            old_stdout = sys.stdout
            sys.stdout = devnull
            try:
                yield
            finally:
                sys.stdout = old_stdout

    @staticmethod
    def create_downloader(config):
        downloaders = {
           "classic": ClassicDownloader,
           "bulk": BulkDownloader
        }
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
                wait = 2 ** attempt
                attempt += 1
                self.config.logger.warning(f"Attempt {attempt+1}/{num_attempts} failed: {e}, waiting {wait}s")
                time.sleep(wait)
            except KeyError as e:
                self.config.logger.info(f"An error occurred: {str(e)}")
                attempt += 1
        self.config.logger.warning(f"reached max attempts {num_attempts}")

class ClassicDownloader(BaseDownloader):
    """ 
    Comtrade APIs call for as reported data, provided in the classification 
    code reported in
    """
    def __init__(self, config: ComtradeConfig):
        self.config = config


    def execute_download(self, year: int, last_updated: datetime):
        params = {
           "subscription_key": self.config.api_key,
           "directory": self.year_path,
           "typeCode": "C",
           "freqCode": "A", 
           "clCode": self.config.classification_code,
           "period": str(year),
            # reporterCode format is the only params difference for classic
            "reporterCode": None,
           "decompress": False, 
            "publishedDateFrom": last_updated.strftime("%Y-%m-%d")
        }
        with self.suppress_stdout() if self.config.suppress_print else nullcontext():
            comtradeapicall.bulkDownloadFinalClassicFile(**params)

class BulkDownloader(BaseDownloader):
    def __init__(self, config: ComtradeConfig):
        self.config = config

    def execute_download(self, year: int, last_updated: datetime):
        params = {
           "subscription_key": self.config.api_key,
           "directory": self.year_path,
           "dataset": "trade",
           "period": str(year),
            "reporterCode": [],
           "decompress": False,
            "publishedDateFrom": last_updated.strftime("%Y-%m-%d"),
        }

        with self.suppress_stdout() if self.config.suppress_print else nullcontext():
            comtradeapicall.bulkDownloadFile(**params)

# # Usage
# config = DownloaderConfig(download_type="classic", ...)
# downloader = BaseDownloader.create_downloader(config)
# downloader.download_with_retries(year=2020) CopyRetryClaude does not have the ability to run the code it generates yet.Claude can make mistakes. Please double-check responses.

# class ClassicDownloader():
#     """Parses and stores Comtrade file metadata."""
#     api_callers = {
#         "as_reported": 
#     }
    
#     data_request = {
#     }
    
    
    
#     def __init__(
#         self,
#         call_type,
#         year_path,
#         last_updated
#         ):
        
#         self.year_path = year_path
#         self.last_udpated = last_updated
        
        
#         # reporters / partners 
#         self.reporters = comtradeapicall.getReference("reporter")[
#             ["reporterCode", "reporterCodeIsoAlpha3"]
#         ].rename(columns={"reporterCodeIsoAlpha3": "reporterISO3"})

#         self.partners = comtradeapicall.getReference("partner")[
#             ["PartnerCode", "PartnerCodeIsoAlpha3"]
#         ].rename(
#             columns={
#                 "PartnerCode": "partnerCode",
#                 "PartnerCodeIsoAlpha3": "partnerISO3",
#             }
#         )


#     def _call_as_reported(self) -> None:
        
#         comtradeapicall.bulkDownloadFinalClassicFile(
#             self.api_key,
#             year_path,
#             typeCode="C",
#             freqCode="A",
#             clCode=self.classification_code,
#             period=str(year),
#             # updated reporter codes to match classic
#             reporterCode=None,
#             decompress=False,
#             publishedDateFrom=last_updated.strftime("%Y-%m-%d")
#             # publishedDateTo='2018-01-01'
#         )

        
        
#         raise ValueError(f"File format has not been handled: {self.name}")



