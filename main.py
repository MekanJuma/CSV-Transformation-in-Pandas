import pandas as pd
import numpy as np
import glob
import os
import re
import datetime
from collections import defaultdict
import argparse


class CSVTransformer:
    def __init__(self, path):
        self.path = path

    def extract_month_year_from_filename(self, filename):
        month = filename.split("_")[-1].split(".")[0]
        month = month.lower()

        if month.startswith("jly"):
            month = "jul"

        month = (
            month.replace("24", "/2024") if month.endswith("24") else month + "/2023"
        )
        try:
            date_obj = datetime.datetime.strptime(month, "%b/%Y")
            return date_obj.date()
        except Exception as e:
            print("error while converting to date", e)
            return None

    def extract_total_from_task_details(self, task_details):
        if pd.isna(task_details) or not isinstance(task_details, str):
            return 0.0

        pattern = r"\btotal\b[^\d]*\s*\:\s*\$([\d,]+\.\d{2})"
        match = re.search(pattern, task_details, re.IGNORECASE)

        if match:
            total_amount = match.group(1)
            return float(total_amount.replace(",", ""))

        return 0.0

    def transform_columns(self, df):
        date_columns = [
            "completeBeforeTime",
            "completeAfterTime",
            "creationTime",
            "startTime",
            "completionTime",
            "departureTime",
            "arrivalTime",
        ]
        for col in date_columns:
            if col in df.columns and df[col].dtype != "datetime64[ns]":
                df[col] = pd.to_datetime(df[col])

        object_columns = ["forceCompletedBy", "dependencies", "metadata"]
        for col in object_columns:
            if col in df.columns and df[col].dtype != "object":
                df[col] = df[col].astype("object")

        lonlat_columns = [
            "destinationLonLat",
            "completionLonLat",
            "startLonLat",
            "departureLonLat",
            "arrivalLonLat",
        ]
        for col in lonlat_columns:
            if col in df.columns:
                df[col] = df[col].str.lstrip("`")
                new_col = col.replace("LonLat", "")
                df[[new_col + "_Longitude", new_col + "_Latitude"]] = (
                    df[col].str.split(",", expand=True).astype(float)
                )

        if "recipientsNumbers" in df.columns:
            df["recipientsNumbers"] = df["recipientsNumbers"].astype(str)
            df["recipientsNumbers"] = df["recipientsNumbers"].str.lstrip("`")
            df["recipientsNumbers"].replace("nan", np.nan, inplace=True)

        df.drop(columns=lonlat_columns, inplace=True)

        return df

    def transform_and_union(self, output_path):
        all_data = []

        for filename in glob.glob(self.path):
            df = pd.read_csv(filename)
            base_filename = os.path.basename(filename)
            month_year = self.extract_month_year_from_filename(filename)

            df = self.transform_columns(df)

            df["Filename"] = base_filename
            df["Month/Year"] = month_year
            df["TotalAmount"] = df["taskDetails"].apply(
                self.extract_total_from_task_details
            )

            all_data.append(df)

        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.sort_values(by="Month/Year")

        combined_df.to_excel(output_path, index=False)


class CSVValidator(CSVTransformer):
    def __init__(self, path):
        super().__init__(path)
        self.path = path
        self.all_columns = defaultdict(lambda: defaultdict(set))
        self.top_values = defaultdict(lambda: defaultdict(str))

    def format_top_values(self, series):
        counts = series.value_counts(dropna=False)
        if pd.isna(series).any():
            counts.index = counts.index.fillna("NA")
        return ", ".join([f"{val} ({count})" for val, count in counts.head(5).items()])

    def get_file_stats(self):
        file_stats = []

        for filename in glob.glob(self.path):
            df = pd.read_csv(filename)
            base_filename = os.path.basename(filename)
            month_year = self.extract_month_year_from_filename(filename)
            df["Month/Year"] = month_year

            for col in df.columns:
                self.all_columns[col][str(df[col].dtype)].add(base_filename)
                self.top_values[col][base_filename] = self.format_top_values(df[col])

            file_stats.append(
                {
                    "filename": os.path.basename(filename),
                    "month_year": month_year,
                    "num_columns": len(df.columns),
                    "num_records": len(df),
                    "columns": df.columns.tolist(),
                }
            )

        return pd.DataFrame(file_stats).sort_values(by="month_year")

    def get_discrepancies(self):
        discrepancies_flat = []
        for col, type_info in self.all_columns.items():
            if len(type_info) > 1:
                for data_type, files in type_info.items():
                    top_values_str = ", ".join(
                        [f"{file}: [{self.top_values[col][file]}]" for file in files]
                    )
                    discrepancy = {
                        "Column": col,
                        "Data Type": data_type,
                        "Files": ", ".join(files),
                        "Top 5 Repeated Values": top_values_str,
                    }
                    discrepancies_flat.append(discrepancy)
        return pd.DataFrame(discrepancies_flat)

    def generate_report(self, output_path):
        stats_df = self.get_file_stats()
        discrepancies_df = self.get_discrepancies()

        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            stats_df.to_excel(writer, sheet_name="File_Stats", index=False)
            discrepancies_df.to_excel(writer, sheet_name="Discrepancies", index=False)


if __name__ == "__main__":
    path = "data/*.csv"

    parser = argparse.ArgumentParser(description="CSV Transformation and Validation")
    parser.add_argument("--validate", action="store_true", help="Validate CSV files")
    parser.add_argument(
        "--transform", action="store_true", help="Transform and combine CSV files"
    )

    args = parser.parse_args()

    if args.validate:
        validator = CSVValidator(path)
        validator.generate_report("validation/report.xlsx")
        print("Validation completed.")

    if args.transform:
        transformer = CSVTransformer(path)
        transformer.transform_and_union("output/combined_data.xlsx")
        print("Transformation completed.")
