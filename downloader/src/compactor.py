import logging
import gzip
import glob
import os.path
import pandas as pd
import comtradeapicall
import time
from datetime import date, timedelta, datetime
import numpy as np

# import dask.dataframe as dd
# import dask.array as da
# from dask.diagnostics import ProgressBar
# from dask.diagnostics import ResourceProfiler


class ComtradeCompactor(ComtradeConfig):
    GROUP_REPORTERS = {"EU": "097", "ASEAN": "975"} #R4: ASEAN
    CLASSIFICATION_CODES = {"H0 (HS-92)": "H0", "H4 (HS-12)": "H4", "H5 (HS-17)": "H5", "SITC": "SITC"}

    def __init__(
        self,
        config: ComtradeConfig
        downloader: BaseDownloader
    ):
        
        self.columns = [
            "period",
            "reporterCode",
            "partnerCode",
            "partner2Code",
            "flowCode",
            "classificationCode",
            "cmdCode",
            "motCode",
            "customsCode",
            "qty",
            "primaryValue",
            "CIFValue": "float",
            "FOBValue": "float",
        ] 

        self.dtypes_dict = {
            "period": np.int16,
            "reporterCode": np.int16,
            "flowCode": "category",
            "partnerCode": np.int16,
            "classificationCode": "category",
            "cmdCode": "str",
            "motCode": "category",
            "customsCode": "category",
            "qty": np.float32,
            "primaryValue": np.float32,
            "CIFValue": np.float32,
            "FOBValue": np.float32,
        }

        self.run_time = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S")

        # transform iso3_codes into reporterCodes
        reporter_df = self.downloader.reporters[
            ~self.downloader.reporters.reporterISO3.isin(EXCL_REPORTER_GROUPS.values())
        ]
        # remove not else specified
        reporter_df = reporter_df[~reporter_df.reporterISO3.isin([NES_COUNTRIES])]

        self.reporter_iso3s = [reporter_df["reporterISO3"].tolist()]
        self.reporter_codes = [reporter_df["reporterCode"].tolist()]

        partners_df = self.downloader.partners[~self.downloader.partners.partnerISO3.isin([NES_COUNTRIES])]

        logging.info("All partners and world. World duplicates the total primary value")
        self.partner_iso3s = partners_df["partnerISO3"].tolist()
        self.partner_codes = partners_df["partnerCode"].tolist()
        self.requests_all_and_world_partners = True
        self.requests_all_partners = True

    def compact(self):
        """
        Runs steps to extract user requested data from the raw Comtrade data files
        on the cluster. Writes output file to a user output file in requested data format

        Input:
            ComtradeCompactor (obj)
        """
        for classification_code in CLASSIFICATION_CODES.values():
            all_years_df = pd.DataFrame(columns=self.columns)
            all_years_df = all_years_df.astype(self.dtypes_dict)
            logging.info(f"Requested data at {datetime.now()}")
            logging.info("Querying the data for")

            for year in self.config.years:
                logging.info(f"starting data gathering for year: {year}")
                df = self.get_df_by_year(year, query_statement)

                if df.empty:
                    logging.info(
                        f"No requested {self.classification_code} data for {year}."
                    )
                    continue
                year_df = self.clean(df)
                if self.output_by_year:
                    self.write_output_file(year_df, year)
                else:
                    all_years_df = pd.concat([all_years_df, year_df], axis=0)
            if not self.output_by_year:
                self.write_output_file(all_years_df)
            logging.info(f"Requested file(s) available at {datetime.now()}")

    def write_output_file(self, df, year=None):
        """ """
        if self.output_by_year:
            os.makedirs(
                os.path.join(
                    self.output_dir, f"{self.classification_code}_[{self.run_time}]"
                ),
                exist_ok=True,
            )
            outpath = os.path.join(
                self.output_dir,
                f"{self.classification_code}_[{self.run_time}]",
                f"{self.classification_code}_" + f"{year}" + f".{self.data_format}",
            )
        else:
            outpath = os.path.join(
                self.output_dir,
                f"{os.environ.get('USER')}_"
                + f"{self.classification_code}_"
                + f"{self.start_year}-{self.end_year}_"
                + f"{self.run_time}"
                f".{self.data_format}",
            )

        if self.atlas_cleaning:
            # remove default columns to reduce file size
            cols_to_drop = [
                "reporterCode",
                "partnerCode",
                "motCode",
                "customsCode",
                "partner2Code",
            ]
            for col in cols_to_drop:
                try:
                    df = df.drop(columns=[col])
                except:
                    logging.info(f"{col} not in df columns, failed to drop")

        # handle requested data format
        if self.data_format == "parquet":
            df.to_parquet(outpath, index=False)

        elif self.data_format == "dta":
            # requirement to cast ints to floats for stata files
            cast_fields = [
                "period",
                "reporterCode",
                "partnerCode",
                "motCode",
                "altQtyUnitCode",
            ]
            for cast_field in cast_fields:
                if cast_field in df.columns:
                    df[cast_field] = df[cast_field].astype(float)
            if not self.atlas_cleaning:
                df["partner2Code"] = df.partner2Code.astype(str)
            df.reset_index(drop=True, inplace=True)
            df.to_stata(outpath)

        elif self.data_format == "csv":
            if self.requests_all_reporters or len(self.reporter_codes) > 15:
                self.data_format = "gzip"
                df.to_csv(outpath, compression="gzip", index=False)
            else:
                df.to_csv(outpath, index=False)

        else:
            logging.info(
                f"Selected data format {self.data_format} is not yet supported."
            )
            logging.info("Defaulting to a parquet file")
            df.to_parquet(outpath, index=False)
        logging.info(f"File downloaded to {outpath} as a {self.data_format} file")

    def read_and_filter_files(self, year, query_statement) -> pd.DataFrame:
        """
        Generator function iterates through requested classification code's year directory
        for requested reporter codes and then filters the reporter's dataframe

        Yields dataframe
        """
        # handle case when years requested cover both S1 and S2
        if self.classification_code == "SITC":
            if year <= 1975:
                self.src_dir = os.path.join("/n/hausmann_lab/lab/atlas/data/raw", "S1")
            if year >= 1976:
                self.src_dir = os.path.join("/n/hausmann_lab/lab/atlas/data/raw", "S2")

        if self.requests_all_reporters:
            all_reporters = glob.glob(os.path.join(self.src_dir, str(year), "*.gz"))
            year_df = pd.DataFrame()
            for file in all_reporters:
                if ComtradeFile(file).reporterCode not in self.GROUP_REPORTERS.values():
                    reporter_dfs = dd.read_csv(
                        file,
                        compression="gzip",
                        sep="\t",
                        usecols=self.columns,
                        dtype=self.dtypes_dict,
                        blocksize=None,
                    )

                    year_df = dd.concat([year_df, reporter_dfs])
            logging.info("concatenated all reporter dataframes")

            filtered_df = year_df.query(query_statement)
            filtered_df = filtered_df.compute()

            # create product digitlevel column based on commodity code
            filtered_df = filtered_df.assign(
                digitLevel=filtered_df["cmdCode"].str.len()
            )
            # zero digit value replaces the word TOTAL
            filtered_df.loc[filtered_df.cmdCode == "TOTAL", "digitLevel"] = 0
            filtered_df = filtered_df[filtered_df.digitLevel.isin(self.digit_levels)]
            filtered_df["digitLevel"] = filtered_df.digitLevel.astype("category")
            yield filtered_df

        else:
            for src_name in glob.glob(os.path.join(self.src_dir, str(year), "*.gz")):
                # extracts reporter code based on outputted file naming convention
                reporter_code = src_name.split("/")[-1][17:20]
                # do not include group reporters
                if ComtradeFile(file).reporterCode not in self.GROUP_REPORTERS.values():
                    continue
                if int(reporter_code) in self.reporter_codes:
                    reporter_df = pd.read_csv(
                        src_name,
                        compression="gzip",
                        sep="\t",
                        usecols=self.columns,
                        dtype=self.dtypes_dict,
                    )

                    filtered_df = reporter_df.query(query_statement)

                    # create product digitlevel column based on commodity code
                    filtered_df = filtered_df.assign(
                        digitLevel=filtered_df["cmdCode"].str.len()
                    )
                    # zero digit value replaces the word TOTAL
                    filtered_df.loc[filtered_df.cmdCode == "TOTAL", "digitLevel"] = 0
                    filtered_df = filtered_df[
                        filtered_df.digitLevel.isin(self.digit_levels)
                    ]
                    yield filtered_df

    def get_df_by_year(self, year, query_statement) -> pd.DataFrame:
        """
        Calls generator function and concatenates returned filtered dataframes

        Returns a filter data frame for one year
        """
        year_df = pd.DataFrame(columns=self.columns)
        year_df.astype(self.dtypes_dict)
        # Concatenate DataFrames using the generator
        for df in self.read_and_filter_files(year, query_statement):
            year_df = pd.concat([year_df, df], axis=0, ignore_index=True)
        return year_df
    
    
    def filter_data(self):
        """
        """
        # by year
        # remove all group reporters
        # filter
        # product digitlevel column based on commodity code
        # zero digit value replaces the word TOTAL
        pass

    def generate_filter(self) -> str:
        """
        Returns query statement based on requested filters
        """
        conditions = []
        for field, values in self.filters.items():
            # no filtering is applied if value is empty list or requesting all partners
            if values == [] or (
                field == "partnerCode"
                and (self.requests_all_partners | self.requests_all_and_world_partners)
            ):
                continue
            elif field == "partnerCode" and self.requests_all_partners:
                condition_string = f"({field} != 0)"
                conditions.append(condition_string)
                continue
            condition_string = "("
            for value in values:
                if field in [
                    "cmdCode",
                    "flowCode",
                    "motCode",
                    "customsCode",
                ]:  # types are strings
                    condition = f"{field} == '{value}' or "
                else:
                    condition = f"{field} == {value} or "
                condition_string += condition
            condition_string = condition_string[0:-4]
            condition_string += ")"
            conditions.append(condition_string)
        if conditions:
            query_statement = " and ".join(conditions)
        else:
            query_statement = ""
        return query_statement

    def clean(self, df):
        """
        Adds ISOCode columns for reporter and partner countries
        """
        df_copy = df.copy()

        # reporterCodes => ISO3Codes
        reporter_code_df = comtradeapicall.getReference("reporter")
        reporter_code_df = reporter_code_df[
            ["reporterCode", "reporterCodeIsoAlpha3"]
        ].rename(columns={"reporterCodeIsoAlpha3": "reporterISO3"})
        reporter_codes = reporter_code_df[
            reporter_code_df["reporterCode"].isin(df["reporterCode"].unique().tolist())
        ]
        mapping_dict = dict(
            zip(reporter_codes["reporterCode"], reporter_codes["reporterISO3"])
        )
        df_copy["reporterISO3"] = df_copy["reporterCode"].map(mapping_dict)

        # partnerCodes => ISO3Codes
        partner_code_df = comtradeapicall.getReference("partner")[
            ["PartnerCode", "PartnerCodeIsoAlpha3"]
        ].rename(
            columns={
                "PartnerCode": "partnerCode",
                "PartnerCodeIsoAlpha3": "partnerISO3",
            }
        )
        partner_codes = partner_code_df[
            partner_code_df["partnerCode"].isin(df["partnerCode"].unique().tolist())
        ]
        mapping_dict = dict(
            zip(partner_codes["partnerCode"], partner_codes["partnerISO3"])
        )
        mapping_dict[0] = "WLD"
        mapping_dict[490] = "TWN"
        df_copy["partnerISO3"] = df_copy["partnerCode"].map(mapping_dict)

        # remove Not Elsewhere Specified Partner Countries
        df_copy = df_copy[~df_copy.partnerISO3.isin(NES_COUNTRIES)]

        # handles south africa
        df_copy.loc[df_copy.reporterISO3 == "ZA1", "reporterISO3"] = "ZAF"
        df_copy.loc[df_copy.partnerISO3 == "ZA1", "partnerISO3"] = "ZAF"

        return df_copy
