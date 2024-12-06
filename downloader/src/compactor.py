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

class ComtradeCompactor(object):
    GROUP_REPORTERS = {"EU": "097", "ASEAN": "975"}

    def __init__(
        self,
        classification_code,
        start_year,
        end_year,
        reporter_iso3_codes=[],
        partner_iso3_codes=[],
        is_show_reexport=[],
        commodity_codes=[],
        modes_of_transport=[],
        customs_codes=[],
        flow_codes=[],
        digit_level=0,
        additional_requested_cols=[],
        output_by_year=True,
        atlas_cleaning=False,
        data_format="csv",
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
        ] + list(additional_requested_cols)

        additional_col_dtypes = {
            "typeCode": "str",
            "freqCode": "str",
            "mosCode": "str",
            "qtyUnitCode": "str",
            "isQtyEstimated": "int",
            "atlQtyUnitCode": "str",
            "altQty": "float",
            "isAltQtyEstimated": "int",
            "netWgt": "float",
            "isNetWgtEstimated": "int",
            "grossWgt": "float",
            "isGrossWgtEstimated": "int",
            "CIFValue": "float",
            "FOBValue": "float",
            "legacyEstimationFlag": "int",
            "isReported": "int",
            "isAggregate": "int",
        }

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
        }

        for col in additional_requested_cols:
            dtype = additional_col_dtypes.get(col)
            if dtype:
                self.dtypes_dict[col] = dtype

        self.classification_code = CLASSIFICATION_CODES[classification_code]
        # TODO: ADD CASES BASED ON CLASSIFICATION CODE SELECTED FOR EACH YEAR
        self.start_year = start_year
        self.end_year = end_year
        self.years = range(self.start_year, self.end_year + 1)

        self.src_dir = os.path.join(
            "/n/hausmann_lab/lab/atlas/data/raw",
            self.classification_code,
        )

        self.output_dir = os.path.join(
            "/n/hausmann_lab/lab/_shared_dev_data/compactor_output",
            os.environ.get("USER"),
            self.classification_code,
        )
        os.makedirs(self.output_dir, exist_ok=True)

        self.run_time = time.strftime("%Y-%m-%d_%H_%M_%S", time.gmtime())

        # transform iso3_codes into reporterCodes
        reporter_df = comtradeapicall.getReference("reporter")
        reporter_df = reporter_df[~reporter_df.reporterCodeIsoAlpha3.isin(EXCL_REPORTER_GROUPS.values())]
        reporter_df = reporter_df[["reporterCode", "reporterCodeIsoAlpha3"]].rename(columns={"reporterCodeIsoAlpha3": "reporterISO3"})
        # remove not else specified
        reporter_df = reporter_df[~reporter_df.reporterISO3.isin([NES_COUNTRIES])]
        
        
        if "All" in reporter_iso3_codes:
            self.reporter_iso3s = [reporter_df["reporterISO3"].tolist()]
            self.reporter_codes = [reporter_df["reporterCode"].tolist()]
            self.requests_all_reporters = True
        else:
            reporter_codes = reporter_df[
                reporter_df["reporterISO3"].isin(reporter_iso3_codes)
            ]
            self.reporter_iso3s = reporter_codes["reporterISO3"].tolist()
            self.reporter_codes = reporter_codes["reporterCode"].tolist()
            self.requests_all_reporters = False

        # transform given iso3_codes into partnerCodes
        partners_df = comtradeapicall.getReference("partner")
        # removes WORLD
        partners_df = partners_df[["PartnerCode", "PartnerCodeIsoAlpha3"]].rename(columns={"PartnerCode": "partnerCode","PartnerCodeIsoAlpha3": "partnerISO3"})
        partners_df = partners_df[~partners_df.partnerISO3.isin([NES_COUNTRIES])]
        
        
        if "All" in partner_iso3_codes and "World" in partner_iso3_codes:
            logging.info(
                "Requested All partners and world. World duplicates the total primary value"
            )
            self.partner_iso3s = partners_df["partnerISO3"].tolist()
            self.partner_codes = partners_df["partnerCode"].tolist()
            self.requests_all_and_world_partners = True
            self.requests_all_partners = True
        
        elif "All" in partner_iso3_codes:
            logging.info("requested All partners")
            self.partner_iso3s = partners_df["partnerISO3"].tolist()
            # drop world
            self.partner_codes = partners_df[partners_df.partnerCode != 0][
                "partnerCode"
            ].tolist()
            self.requests_all_and_world_partners = False
            self.requests_all_partners = True
            
        else:
            # map Taiwan back to comtrade iso code definition S19    
            partner_iso3_codes = list(map(lambda x: x.replace('TWN', 'S19'), partner_iso3_codes))
            partner_codes = partners_df[partners_df["partnerISO3"].isin(partner_iso3_codes)]
            self.partner_iso3s = partner_codes["partnerISO3"].tolist()
            self.partner_codes = partner_codes["partnerCode"].tolist()
            self.requests_all_and_world_partners = False
            self.requests_all_partners = False
        
        # setup filter parameters
        self.filters = {}
        # if partner2Code detail is not requested filter to world
        if is_show_reexport == "Yes":
            self.filters["partner2Code"] = partners_df[
                partners_df.partnerCode != 0
            ]["partnerCode"].tolist()
        else:
            self.filters["partner2Code"] = [0]
        self.filters["partnerCode"] = self.partner_codes
        if commodity_codes == "":
            self.filters["cmdCode"] = []
        elif len(commodity_codes) == 1:
            self.filters["cmdCode"] = [commodity_codes]
        else:
            commodity_codes = commodity_codes.split(",")
            commodity_codes = [cmd.strip() for cmd in commodity_codes]
            self.filters["cmdCode"] = commodity_codes
        modes_of_transport = [item for item in modes_of_transport if item != " "]

        if modes_of_transport:
            mot_ids = []
            for id in list(modes_of_transport):
                mot_ids.append(MOT_OPTIONS[id])
                self.filters["motCode"] = mot_ids
        else:
            # default to filter for total
            self.filters["motCode"] = ["0"]

        customs_codes = [item for item in customs_codes if item != " "]
        if customs_codes:
            customs_ids = []
            for id in list(customs_codes):
                customs_ids.append(CUSTOMS_OPTIONS[id])
                self.filters["customsCode"] = customs_ids
        else:
            # default to filter for total
            self.filters["customsCode"] = ["C00"]

        flow_codes = [item for item in flow_codes if item != " "]
        if flow_codes:
            flow_codes_abbrv = []
            for flow_code in list(flow_codes):
                flow_codes_abbrv.append(FLOW_CODE_OPTIONS[flow_code])
                self.filters["flowCode"] = flow_codes_abbrv
        else:
            self.filters["flowCode"] = []

        self.digit_levels = list(digit_level)
        self.output_by_year = output_by_year
        self.atlas_cleaning = atlas_cleaning
        self.data_format = data_format

    def compact(self):
        """
        Runs steps to extract user requested data from the raw Comtrade data files
        on the cluster. Writes output file to a user output file in requested data format

        Input:
            ComtradeCompactor (obj)
        """
        all_years_df = pd.DataFrame(columns=self.columns)
        all_years_df = all_years_df.astype(self.dtypes_dict)
        logging.info(f"Requested data at {datetime.now()}")
        logging.info("Querying the data for")
        if self.requests_all_reporters:
            logging.info("all Reporters")
        else:
            logging.info(f"Reporters: {self.reporter_iso3s}")

        if self.requests_all_partners and self.requests_all_and_world_partners:
            logging.info("all Partners and the World")
        elif self.requests_all_partners:
            logging.info("all Partners")
        else:
            logging.info(f"Partners: {self.partner_iso3s}")
        show_filters = {
            key: value for key, value in self.filters.items() if key != "partnerCode"
        }
        logging.info(f"Filtering for {show_filters}")
        logging.info(f"In the following years {self.start_year} - {self.end_year}.")

        query_statement = self.generate_filter()

        for year in self.years:
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
                f"{self.classification_code}_"
                + f"{year}"
                + f".{self.data_format}",
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
            cols_to_drop = ['reporterCode', 'partnerCode', 'motCode','customsCode', 'partner2Code']
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
                df['partner2Code'] = df.partner2Code.astype(str)
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
            # year_df = dd.read_csv(os.path.join(self.src_dir, str(year), "*.gz"),
            #           compression='gzip',
            #           sep='\t',
            #           usecols=self.columns,
            #           dtype=self.dtypes_dict,
            #           blocksize=None,
            #          )
            all_reporters = glob.glob(os.path.join(self.src_dir, str(year), "*.gz"))
            year_df = pd.DataFrame()
            for file in all_reporters:
                if file.split("/")[-1][17:20] not in [self.GROUP_REPORTERS["ASEAN"], self.GROUP_REPORTERS["EU"]]:
                    reporter_dfs = dd.read_csv(
                            file,
                            compression="gzip",
                            sep="\t",
                            usecols=self.columns,
                            dtype=self.dtypes_dict,
                            blocksize=None
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
                if reporter_code in [
                    self.GROUP_REPORTERS["ASEAN"],
                    self.GROUP_REPORTERS["EU"],
                ]:
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
        df_copy.loc[df_copy.reporterISO3=="ZA1", "reporterISO3"] = "ZAF"
        df_copy.loc[df_copy.partnerISO3=="ZA1", "partnerISO3"] = "ZAF"

        return df_copy