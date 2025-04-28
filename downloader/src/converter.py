from pathlib import Path
import re
from datetime import datetime, timedelta
from src.configure_downloader import ComtradeConfig
from src.comtrade_file import ComtradeFile
import pandas as pd
import dask.dataframe as dd
import glob
import os
import shutil

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.set_option("max_colwidth", 400)
pd.set_option("display.precision", 10)


class ClassificationConverter(object):
    CONVERTED_FILES_DTYPES = {
        "reporterCode": str,
        "flowCode": str,
        "partnerCode": str,
        "cmdCode": str,
        "qty": float,
        "CIFValue": float,
        "FOBValue": float,
        "primaryValue": float,
    }
    CLASSIFICATIONS = {
        "S1": 1962,
        "S2": 1976,
        "S3": 1988,
        # "S4": 2007,
        "H0": 1988,
        "H1": 1996,
        "H2": 2002,
        "H3": 2007,
        "H4": 2012,
        "H5": 2017,
        "H6": 2022,
    }

    conversion_links = [
        "S1", 
        "S2", 
        "S3",
        "H0",
        "H1",
        "H2",
        "H3",
        "H4",
        "H5",
        "H6",
    ]
    


    def __init__(self, config: ComtradeConfig, target_class):
        self.config = config
        self.target_class = target_class
        

    # clear out as_converted data

    def run(self):
        """
        """
        for source_class in self.CLASSIFICATIONS.keys():
            if source_class.startswith("H"):
                self.source_product_level = 6
            else: 
                self.source_product_level = 4
                
            if self.target_class.startswith("H"):
                self.target_product_level = 6
            else: 
                self.target_product_level = 4

            self.config.logger.info(f"Starting conversion for source {source_class}")
            if source_class == self.target_class:
                # moves already reported
                # TODO: need to handle newly downloaded files, probably clear out converted
                self.handle_as_reported(source_class)
                continue
            weight = self.apply_weights(source_class)
            # multiply weight by trade value
            self.run_conversion(weight, source_class)
            del weight

                
    def run_conversion(self, weight, source_class):
        """Run conversion in specified direction for given years"""
        for as_reported_year in range(self.config.start_year, self.config.end_year + 1):
            self.config.logger.info(f"Beginning conversion for {as_reported_year}...")

            # Find all files in source classification for conversion
            # self.config.classification_code = source_class
            raw_parquet_path = Path(self.config.raw_files_parquet_path.parent / source_class / str(as_reported_year))
            # raw_parquet_path = Path(self.config.raw_files_parquet_path) / str(as_reported_year)
            as_reported_files = list(raw_parquet_path.glob("*.parquet"))

            # Process each file
            for file in as_reported_files:

                df = self.convert_file(file, weight, source_class)
                result = self.add_product_levels(df)
                result["isAggregate"] = 1
                result = result.drop(columns="level")

                result = result.astype(self.CONVERTED_FILES_DTYPES)
                for col in result.select_dtypes(
                    include=["string", "object", "category"]
                ).columns:
                    result[col] = result[col].astype(str)

                file_obj = ComtradeFile(str(file).split('/')[-1])
                file_obj.swap_classification(self.target_class)
                
                self.save_converted_data(as_reported_year, file_obj, result)
                del result

                
    def convert_file(self, file, weight, source_class):
        """Convert a single reporter file from source to next classification"""

        as_reported_trade = pd.read_parquet(file)
        as_reported_trade["cmdCode"] = as_reported_trade.cmdCode.astype(str)
        try:
            as_reported_trade = as_reported_trade[
                (as_reported_trade.motCode == "0")
                & (as_reported_trade.mosCode == "0")
                & (as_reported_trade.customsCode == "C00")
                & (as_reported_trade.flowCode.isin(["M", "X", "RM", "RX"]))
                & (as_reported_trade.partner2Code == 0)
            ]
            
            as_reported_trade = as_reported_trade.drop(
                columns=[
                    "customsCode",
                    "motCode",
                    "mosCode",
                    "partner2Code",
                ]
            )
        except:
            as_reported_trade = as_reported_trade[(as_reported_trade.flowCode.isin(["M", "X", "RM", "RX"]))]
                                                   
        as_reported_trade.groupby(
            ["reporterCode", "partnerCode", "flowCode", "cmdCode"], observed=False
        ).agg({
            "qty":"sum",
            "CIFValue":"sum",
            "FOBValue":"sum",
            "primaryValue":"sum"}).reset_index()


        reporter_code = ComtradeFile(file).reporter_code
        self.config.logger.info(f"Converting file for reporter {reporter_code}: {file}")

        # Process conversion groups (n:m relationships)
        converted_df = (
            as_reported_trade.merge(
                weight, left_on="cmdCode", right_on=source_class, how="left"
            )
            .assign(target_trade_val=lambda x: x["primaryValue"] * x["weight"])
            .assign(target_FOBval=lambda x: x["FOBValue"] * x["weight"])
            .assign(target_CIFval=lambda x: x["CIFValue"] * x["weight"])
            .groupby(["reporterCode", "flowCode", "partnerCode", source_class], observed=True)
            .agg(
                {
                    "qty": "sum",
                    "target_trade_val": "sum",
                    "target_FOBval": "sum",
                    "target_CIFval": "sum",
                }
            )
            .reset_index()
        )
        
        converted_df = converted_df[~((converted_df.target_trade_val==0)&(converted_df.target_FOBval==0)&(converted_df.target_CIFval==0))]

        converted_df = converted_df.rename(
            columns={
                source_class : "cmdCode",
                "target_trade_val": "primaryValue",
                "target_FOBval": "FOBValue",
                "target_CIFval": "CIFValue",
            }
        )
        converted_df = converted_df[~((converted_df.primaryValue==0)&(converted_df.FOBValue==0)&(converted_df.CIFValue==0))]
                
        converted_df["cmdCode"] = converted_df["cmdCode"].astype(str)
        converted_df["cmdCode"] = converted_df["cmdCode"].apply(
            lambda x: x.zfill(self.target_product_level) if len(x) < self.target_product_level and x != "TOTAL" else x)
        if converted_df.empty:
            if len(as_reported_trade.cmdCode.unique()) == 1:
                self.config.logger.error(f"Country {reporter_code} only reporting totals")
            else:
                self.config.logger.error(f"Check file: {file}, returning empty after converting")
        return converted_df


    def apply_weights(self, source_class):
        """
        Generate weight table

        Returns:
            DataFrame: weight matrix for given source / target class
        """
        # forward
        # move from H0 to H3, H3 target
        # how do I enforce close to zero, close to one?
        source_position = self.conversion_links.index(source_class)
        target_position = self.conversion_links.index(self.target_class)
        if source_position < target_position:
            direction = "forward"
        else:
            direction = "backward"

        if direction == "forward":
            next_class = self.conversion_links[target_position - 1]
            conversion_link = self.conversion_links[source_position : target_position - 1]
            conversion_link.reverse()
        else:
            next_class = self.conversion_links[target_position + 1]
            conversion_link = self.conversion_links[
                target_position + 2 : source_position + 1
            ]

        weights = pd.read_csv(self.config.conversion_weights_path / f"{next_class}:{self.target_class}.csv")#, dtype={next_class: str, self.target_class: str})
        weights = self.handle_product_code_string(weights, next_class)
        weights = self.handle_product_code_string(weights, self.target_class)
        weights = weights.rename(
            columns={"weight": f"weight_{next_class}_{self.target_class}"}
        )

        for seq_class in conversion_link:
            next_class_weights = pd.read_csv(
                self.config.conversion_weights_path / f"{seq_class}:{next_class}.csv")#,
                #dtype={seq_class: str, next_class: str}
            #)
            next_class_weights = self.handle_product_code_string(next_class_weights, seq_class)
            next_class_weights = self.handle_product_code_string(next_class_weights, next_class)

            next_class_weights = next_class_weights.rename(
                columns={"weight": f"weight_{seq_class}_{next_class}"}
            )
            weights = weights.merge(next_class_weights, on=next_class, how="right")

            weights[f"weight_{seq_class}_{self.target_class}"] = (
                weights[f"weight_{seq_class}_{next_class}"]
                * weights[f"weight_{next_class}_{self.target_class}"]
            )
            

            weights = weights.drop(
                columns=[
                    f"weight_{seq_class}_{next_class}",
                    next_class,
                    f"weight_{next_class}_{self.target_class}",
                ]
            )

            weights = weights.drop_duplicates()
            next_class = seq_class
            
        weights = weights[weights[f"weight_{source_class}_{self.target_class}"]>0]
        weights = weights.rename(columns={f"weight_{source_class}_{self.target_class}":"weight"})
        
        # force product conversions to equal one
        weights['sum'] = weights.groupby(source_class)['weight'].transform('sum')
        weights['weight'] = weights['weight'] / weights['sum']
        weights = weights.drop(columns='sum')
        
        weights_sum = weights.groupby(source_class)['weight'].sum()
        assert all(abs(weights_sum - 1.0) < 0.01), self.config.logger.error(f"Not all weight sums are within 0.01 of 1. Found: {weights_sum[abs(weights_sum - 1.0) >= 0.01]}")
        del weights_sum
        return weights
    
    def handle_product_code_string(self, df, classification):
        if classification.startswith("H"):
            detailed_product_level = 6
        else:
            detailed_product_level = 4
        if not df[df[classification].isna()].empty:
            df = df[~(df[classification].isna())]
            self.config.logger.error(f"dropping nans from conversion table: \n {df[df[classification].isna()].head()}")
        df[classification] = df[classification].astype(int).astype(str)
        df.loc[:, classification] = df[classification].apply(lambda x: x.zfill(detailed_product_level) if len(x) < detailed_product_level and x != "TOTAL" else x)

        return df
                
        
    def handle_as_reported(self, classification):
        """
        trade data reported in the classification is relocated to converted folder
        """
        for year in range(self.config.start_year, self.config.end_year + 1):
            # self.config.classification_code = classification
            destination_path = Path(self.config.converted_final_path.parent / classification / str(year))
            original_paths = Path(self.config.raw_files_parquet_path.parent / classification / str(year)).glob("*.parquet")
            # destination_path = Path(
            #     self.config.converted_final_path / str(year)
            # )
            # original_paths = Path(
            #     self.config.raw_files_parquet_path / str(year) ).glob("*.parquet")
            
            destination_path.mkdir(parents=True, exist_ok=True)
            for file in original_paths:
                filename = str(file).split("/")[-1]
                destination_file = os.path.join(destination_path, filename)
                self.config.logger.info(f"original paths {file}")
                self.config.logger.info(f"destination {destination_file}")
                shutil.copy2(file, destination_file)

    def add_product_levels(self, df):
        agg_levels = [
            ("TOTAL", lambda x: ""),  # Country total
            # ('1digit', lambda x: x[:1]),          # 1-digit
            ("2digit", lambda x: x[:2]),  # 2-digit
            ("4digit", lambda x: x[:4]),  # 4-digit
              
        ]
        if self.target_product_level==6:
            agg_levels.append(("6digit", lambda x: x)) # Original 6-digit (for completeness)

        group_cols = ["reporterCode", "flowCode", "partnerCode"]
        value_cols = ["qty", "primaryValue", "CIFValue", "FOBValue"]

        results = []

        for level_name, level_func in agg_levels:
            # For country totals, group only by reporter/flow/partner
            if level_name == "TOTAL":
                level_df = df.groupby(group_cols, observed=True)[value_cols].sum().reset_index()
                level_df["cmdCode"] = "TOTAL"
            else:
                df[f"level_{level_name}"] = df["cmdCode"].apply(level_func)

                level_df = (
                    df.groupby(group_cols + [f"level_{level_name}"], observed=True)[value_cols]
                    .sum()
                    .reset_index()
                )

                level_df["cmdCode"] = level_df[f"level_{level_name}"]
                level_df = level_df.drop(f"level_{level_name}", axis=1)

                # avoid duplication)
                if level_name == f"{self.target_product_level}digit":
                    continue

            level_df["level"] = level_name
            results.append(level_df)

        df["level"] = f"{self.target_product_level}digit"
        results.append(df[group_cols + ["cmdCode", "level"] + value_cols])
        return pd.concat(results, ignore_index=True)

    def save_converted_data(self, year, file_obj, result):
        """Save converted data to intermediate and final locations"""
        # self.config.classification_code = self.target_class
        final_path = Path(self.config.converted_final_path.parent / self.target_class / str(year))
        # final_path = Path(f"{self.config.converted_final_path}/{year}")
        final_path.mkdir(parents=True, exist_ok=True)

        final_file = f"{final_path}/{file_obj.name}"
        result.to_parquet(final_file, index=False)
        self.config.logger.info(f"Saved to final file: {final_file}")
