import requests
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
import os


# Define config as a global variable
config = {}


def load_config(config_path):
    global config
    with open(config_path) as file:
        config = json.load(file)


def json_to_df(fields, json_data):
    df = pd.DataFrame()

    for station in json_data["Station"]:
        row = {}

        for field in fields:
            value = station

            for path in field.split("-"):
                value = value[path]

                if path == "Coordinates":
                    value = value[1]  # [0]: TWD67, [1]: WGS84

            row[field] = value

        df = pd.concat([df, pd.DataFrame([row])])

    return df


def json_to_df_uv(json_data):
    df = pd.DataFrame()

    for location in json_data["weatherElement"]["location"]:
        row = {
            "StationID": location["StationID"],
            "UVIndex": location["UVIndex"],
            "Date": json_data["weatherElement"]["Date"],
        }

        df = pd.concat([df, pd.DataFrame([row])])

    return df


def process_99(df):
    df.reset_index(drop=True, inplace=True)

    for index, row in df.iterrows():
        for col in df.columns:
            if str(row[col]) in ["-99", "-99.0"]:
                print(
                    f"Invalid: Row index: {index}, Column name: {col}, Value: {row[col]}"
                )
                df.at[index, col] = None

    return df


def process_date(df):
    df.reset_index(drop=True, inplace=True)
    date_columns = [col for col in df.columns if "Date" in col]

    for index, row in df.iterrows():
        for col in date_columns:
            try:
                pd.to_datetime(row[col].split("T")[0], format="%Y-%m-%d")

            except (ValueError, AttributeError):
                print(f"Invalid date: Row {index}, Column {col}: {row[col]}\n")
                df.at[index, col] = None

    return df


def process_col_names(df, length_limit=30):
    new_columns = []

    for col in df.columns:
        if len(col) >= length_limit:
            new_col_name = "-".join(col.split("-")[1:])
            new_columns.append(new_col_name)

        else:
            new_columns.append(col)

    df.columns = new_columns

    return df


def crawler(config_path, dataset_name_ch):
    load_config(config_path)

    base_url = config["base_url"]
    authorization = config["authorization"]
    dataset_info = config["datasets"][dataset_name_ch]

    name = dataset_info["name"]
    resource_id = dataset_info["resource_id"]
    fields = dataset_info["fields"]

    url = f"{base_url}/{resource_id}?Authorization={authorization}"
    print("URL: ", url, "\n")

    response = requests.get(url)

    if response.status_code == 200:
        if dataset_name_ch in [
            "自動氣象站-氣象觀測資料",
            "自動雨量站-雨量觀測資料",
            "現在天氣觀測報告-現在天氣觀測報告",
        ]:
            df = json_to_df(fields, response.json()["records"])

        elif dataset_name_ch == "紫外線指數-每日紫外線指數最大值":
            df = json_to_df_uv(response.json()["records"])

        df = process_col_names(df)
        df = process_99(df)
        df = process_date(df)

        dir_path = config["base_dir"]
        now = datetime.now(timezone(timedelta(hours=8))).strftime('%Y%m%d%H%M%S')
        file_name = f"{name}_{now}.csv"
        file_path = os.path.join(dir_path, name, file_name)

        df.to_csv(file_path, index=False)
        print(f"Data saved: {file_path}\n")

    else:
        print(f"Failed to retrieve data: {response.status_code}")
