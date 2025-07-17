from pathlib import Path
import re
from datetime import datetime, timedelta
from src.download.configure_downloader import ComtradeConfig
from src.download.comtrade_file import ComtradeFile
from src.download.api_downloader import ComtradeDownloader
import pandas as pd
import os
import shutil
from src.utils.file_management import cleanup_files_from_dir


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
        self.downloader = ComtradeDownloader(config)


    def run(self) -> None:
        """
        Copies the as reported data for the requested classification to the
        converted folder

        Then, converts the data to the target classification
        """
        for classification in self.conversion_links:
            self.handle_as_reported(classification)

        for source_class in self.CLASSIFICATIONS.keys():
            if source_class.startswith("H"):
                self.source_product_level = 6
            else:
                self.source_product_level = 4

            if self.target_class.startswith("H"):
                self.target_product_level = 6
            else:
                self.target_product_level = 4

            if source_class == self.target_class:
                continue
            weight = self.apply_weights(source_class)
            # multiply weight by trade value
            self.run_conversion(weight, source_class)
            del weight

    def run_conversion(self, weight: pd.DataFrame, source_class: str) -> None:
        """Run conversion in specified direction for given years"""
        self.config.logger.info(
            f"Beginning conversion for {source_class} to {self.target_class}..."
        )
        for as_reported_year in range(self.config.start_year, self.config.end_year + 1):
            self.config.logger.info(f"for {as_reported_year}...")

            # Find all files in source classification for conversion
            raw_parquet_path = Path(
                self.config.raw_files_parquet_path.parent
                / source_class
                / str(as_reported_year)
            )
            as_reported_files = list(raw_parquet_path.glob("*.parquet"))

            # Process each file
            for file in as_reported_files:

                df = self.convert_file(file, weight, source_class)
                if df.empty:
                    continue
                result = self.add_product_levels(df)
                result["isAggregate"] = 1
                result = result.drop(columns="level")

                result = result.astype(self.CONVERTED_FILES_DTYPES)
                for col in result.select_dtypes(
                    include=["string", "object", "category"]
                ).columns:
                    result[col] = result[col].astype(str)

                file_obj = ComtradeFile(str(file).split("/")[-1])
                file_obj.swap_classification(self.target_class)

                self.save_converted_data(as_reported_year, file_obj, result)
                del result

    def convert_file(
        self, file: Path, weight: pd.DataFrame, source_class: str
    ) -> pd.DataFrame:
        """Convert a single reporter file from source to next classification"""

        as_reported_trade = pd.read_parquet(file)
        as_reported_trade["cmdCode"] = as_reported_trade.cmdCode.astype(str)

        as_reported_trade = self.downloader.handle_known_errors(as_reported_trade, file)
        as_reported_trade = self.downloader.enforce_unique_partner_product_level(
            as_reported_trade, file
        )
        if as_reported_trade.empty:
            return as_reported_trade

        reporter_code = ComtradeFile(file).reporter_code
        self.config.logger.debug(
            f"Converting file for reporter {reporter_code}: {file}"
        )

        # conversion works at most detailed source data product level
        as_reported_trade = as_reported_trade[
            as_reported_trade.cmdCode.str.len() == self.source_product_level
        ]

        # Process conversion groups (n:m relationships)
        converted_df = (
            as_reported_trade.merge(
                weight, left_on="cmdCode", right_on=source_class, how="left"
            )
            .assign(target_trade_val=lambda x: x["primaryValue"] * x["weight"])
            .assign(target_FOBval=lambda x: x["FOBValue"] * x["weight"])
            .assign(target_CIFval=lambda x: x["CIFValue"] * x["weight"])
            .groupby(
                ["reporterCode", "flowCode", "partnerCode", self.target_class],
                observed=True,
            )
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
        converted_df = converted_df[
            ~(
                (converted_df.target_trade_val == 0)
                & (converted_df.target_FOBval == 0)
                & (converted_df.target_CIFval == 0)
            )
        ]

        converted_df = converted_df.rename(
            columns={
                self.target_class: "cmdCode",
                "target_trade_val": "primaryValue",
                "target_FOBval": "FOBValue",
                "target_CIFval": "CIFValue",
            }
        )
        converted_df = converted_df[
            ~(
                (converted_df.primaryValue == 0)
                & (converted_df.FOBValue == 0)
                & (converted_df.CIFValue == 0)
            )
        ]
        if converted_df.empty:
            if len(as_reported_trade.cmdCode.unique()) == 1:
                self.config.logger.warning(
                    f"Country {reporter_code} only reporting totals"
                )
            elif (
                bool(
                    as_reported_trade.cmdCode.str.len().max()
                    != self.source_product_level
                )
                or as_reported_trade[
                    as_reported_trade.cmdCode.str.len() == self.source_product_level
                ]["cmdCode"].nunique()
                == 1
            ):
                self.config.logger.warning(
                    f"Country {reporter_code} insufficient product level detail reported {as_reported_trade.cmdCode.str.len().unique()} for {source_class}"
                )
            else:
                self.config.logger.warning(
                    f"Check file: {file}, returning empty after converting"
                )
                self.config.logger.warning(
                    f"dropping data with an unknown issue with source class: {source_class} and {self.target_class}"
                )
            return converted_df
        return converted_df

    def apply_weights(self, source_class: str) -> pd.DataFrame:
        """
        Generate weight table

        Returns:
            DataFrame: weight matrix for given source / target class
        """
        # forward
        # move from H0 to H3, H3 target
        source_position = self.conversion_links.index(source_class)
        target_position = self.conversion_links.index(self.target_class)
        if source_position < target_position:
            direction = "forward"
        else:
            direction = "backward"

        if direction == "forward":
            next_class = self.conversion_links[target_position - 1]
            conversion_link = self.conversion_links[
                source_position : target_position - 1
            ]
            conversion_link.reverse()
        else:
            next_class = self.conversion_links[target_position + 1]
            conversion_link = self.conversion_links[
                target_position + 2 : source_position + 1
            ]

        weights = pd.read_csv(
            self.config.conversion_weights_path
            / f"conversion_weights_{next_class}_to_{self.target_class}.csv"
        )
        weights = self.handle_product_code_string(weights, next_class)
        weights = self.handle_product_code_string(weights, self.target_class)
        weights = weights[weights.weight != 0]
        weights = weights.rename(
            columns={"weight": f"weight_{next_class}_{self.target_class}"}
        )
        weights = weights.drop(columns="group_id")

        counter = 0
        for seq_class in conversion_link:
            next_class_weights = pd.read_csv(
                self.config.conversion_weights_path
                / f"conversion_weights_{seq_class}_to_{next_class}.csv"
            )
            next_class_weights = self.handle_product_code_string(
                next_class_weights, seq_class
            )
            next_class_weights = self.handle_product_code_string(
                next_class_weights, next_class
            )

            next_class_weights = next_class_weights.rename(
                columns={"weight": f"weight_{seq_class}_{next_class}"}
            )
            next_class_weights = next_class_weights.drop(columns="group_id")

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
            counter += 1
            weights = weights.rename(
                columns={
                    f"weight_{seq_class}_{next_class}": f"weight_{seq_class}_{next_class}_{counter}",
                    next_class: f"next_class_temp_{counter}",
                    f"weight_{next_class}_{self.target_class}": f"weight_{next_class}_{self.target_class}_{counter}",
                }
            )

            weights = weights.drop_duplicates()
            del next_class_weights
            next_class = seq_class

        weights = weights[weights[f"weight_{source_class}_{self.target_class}"] > 0]
        weights = weights.rename(
            columns={f"weight_{source_class}_{self.target_class}": "weight"}
        )

        # force product conversions to equal one
        weights["sum"] = weights.groupby(source_class)["weight"].transform("sum")
        weights["weight"] = weights["weight"] / weights["sum"]
        weights = weights.drop(columns="sum")

        weights_sum = weights.groupby(source_class)["weight"].sum()
        assert all(abs(weights_sum - 1.0) < 0.01), self.config.logger.warning(
            f"Not all weight sums are within 0.01 of 1. Found: {weights_sum[abs(weights_sum - 1.0) >= 0.01]}"
        )
        del weights_sum
        return weights

    def handle_product_code_string(
        self, df: pd.DataFrame, classification: str
    ) -> pd.DataFrame:
        if classification.startswith("H"):
            detailed_product_level = 6
        else:
            detailed_product_level = 4
        if not df[df[classification].isna()].empty:
            df = df[~(df[classification].isna())]
            self.config.logger.warning(
                f"dropping nans from conversion table: \n {df[df[classification].isna()].head()}"
            )
        df[classification] = df[classification].astype(int).astype(str)
        df.loc[:, classification] = df[classification].apply(
            lambda x: (
                x.zfill(detailed_product_level)
                if len(x) < detailed_product_level and x != "TOTAL"
                else x
            )
        )
        return df

    def handle_as_reported(self, classification: str) -> None:
        """
        trade data reported in the classification is copied to converted folder
        """
        for year in range(self.config.start_year, self.config.end_year + 1):
            destination_path = Path(
                self.config.converted_final_path.parent / classification / str(year)
            )
            original_paths = Path(
                self.config.raw_files_parquet_path.parent / classification / str(year)
            ).glob("*.parquet")

            destination_path.mkdir(parents=True, exist_ok=True)
            cleanup_files_from_dir(destination_path)
            
            for file in original_paths:
                filename = str(file.name)
                destination_file = os.path.join(destination_path, filename)
                self.config.logger.debug(f"original paths {file}")
                self.config.logger.debug(f"destination {destination_file}")
                shutil.copy2(file, destination_file)

    def add_product_levels(self, df: pd.DataFrame) -> pd.DataFrame:
        agg_levels = [
            ("TOTAL", lambda x: ""),  # Country total
            ("2digit", lambda x: x[:2]),  # 2-digit
            ("4digit", lambda x: x[:4]),  # 4-digit
        ]
        if self.target_product_level == 6:
            agg_levels.append(
                ("6digit", lambda x: x)
            )  # Original 6-digit (for completeness)

        group_cols = ["reporterCode", "flowCode", "partnerCode"]
        value_cols = ["qty", "primaryValue", "CIFValue", "FOBValue"]

        results = []

        for level_name, level_func in agg_levels:
            # For country totals, group only by reporter/flow/partner
            if level_name == "TOTAL":
                level_df = (
                    df.groupby(group_cols, observed=True)[value_cols]
                    .sum()
                    .reset_index()
                )
                level_df["cmdCode"] = "TOTAL"
            else:
                df[f"level_{level_name}"] = df["cmdCode"].apply(level_func)

                level_df = (
                    df.groupby(group_cols + [f"level_{level_name}"], observed=True)[
                        value_cols
                    ]
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

    def save_converted_data(
        self, year: int, file_obj: ComtradeFile, result: pd.DataFrame
    ) -> None:
        """Save converted data to final locations"""
        final_path = Path(
            self.config.converted_final_path.parent / self.target_class / str(year)
        )
        final_path.mkdir(parents=True, exist_ok=True)
        
        final_file = f"{final_path}/{file_obj.name}"
        result.to_parquet(final_file, index=False)
        self.config.logger.debug(f"Saved to final file: {final_file}")
