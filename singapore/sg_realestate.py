import requests
import pandas as pd
from datetime import datetime


data_dir = "."


def get_df(dataset_id):
    base_url = "https://data.gov.sg/api/action/datastore_search"
    limit = 5000
    offset = 0
    all_records = []

    while True:
        url = f"{base_url}?resource_id={dataset_id}&limit={limit}&offset={offset}"
        print("URL:", url)
        response = requests.get(url)
        data = response.json()

        records = data["result"]["records"]
        if not records:
            break

        all_records.extend(records)
        offset += limit

    df = pd.DataFrame(all_records)
    df = df.drop(columns=["_id"])

    return df


def save_df(df, dataset_name):
    today = datetime.today().strftime("%Y%m%d")
    csv_path = f"{data_dir}/{dataset_name}_{today}.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saved {csv_path}")


def resale_flat_prices_1990_to_1999():
    df = get_df("d_ebc5ab87086db484f88045b47411ebc5")

    # Column names 轉成可讀的文字
    df.columns = [
        "Month",
        "Town",
        "Flat Type",
        "Block",
        "Street Name",
        "Storey Range",
        "Floor Area",
        "Flat Model",
        "Lease Commence Date",
        "Resale Price",
    ]

    # 插入新欄位
    df.insert(9, "Remaining Lease Years", "")
    df.insert(10, "Remaining Lease Months", "")

    # 存檔
    save_df(df, "resale_flat_prices_1990_to_1999")


def resale_flat_prices_2000_to_201202():
    df = get_df("d_43f493c6c50d54243cc1eab0df142d6a")

    # Column names 轉成可讀的文字
    df.columns = [
        "Month",
        "Town",
        "Flat Type",
        "Block",
        "Street Name",
        "Storey Range",
        "Floor Area",
        "Flat Model",
        "Lease Commence Date",
        "Resale Price",
    ]

    # 插入新欄位
    df.insert(9, "Remaining Lease Years", "")
    df.insert(10, "Remaining Lease Months", "")

    # 存檔
    save_df(df, "resale_flat_prices_2000_to_201202")


def resale_flat_prices_201202_to_2014():
    df = get_df("d_2d5ff9ea31397b66239f245f57751537")

    # Column names 轉成可讀的文字
    df.columns = [
        "Month",
        "Town",
        "Flat Type",
        "Block",
        "Street Name",
        "Storey Range",
        "Floor Area",
        "Flat Model",
        "Lease Commence Date",
        "Resale Price",
    ]

    # 插入新欄位
    df.insert(9, "Remaining Lease Years", "")
    df.insert(10, "Remaining Lease Months", "")

    # 存檔
    save_df(df, "resale_flat_prices_201202_to_2014")


def resale_flat_prices_2015_to_2016():
    df = get_df("d_ea9ed51da2787afaf8e51f827c304208")

    # Column names 轉成可讀的文字
    df.columns = [
        "Month",
        "Town",
        "Flat Type",
        "Block",
        "Street Name",
        "Storey Range",
        "Floor Area",
        "Flat Model",
        "Lease Commence Date",
        "Remaining Lease Years",
        "Resale Price",
    ]

    # 插入新欄位
    df.insert(10, "Remaining Lease Months", "")

    # 存檔
    save_df(df, "resale_flat_prices_2015_to_2016")


def resale_flat_prices():
    df = get_df("d_8b84c4ee58e3cfc0ece0d773c8ca6abc")

    # Column names 轉成可讀的文字
    df.columns = [
        "Month",
        "Town",
        "Flat Type",
        "Block",
        "Street Name",
        "Storey Range",
        "Floor Area",
        "Flat Model",
        "Lease Commence Date",
        "Remaining Lease",
        "Resale Price",
    ]

    # 插入新欄位
    df.insert(9, "Remaining Lease Years", "")
    df.insert(10, "Remaining Lease Months", "")

    # 將 "Remaining Lease" 年月拆開
    df[["Remaining Lease Years", "Remaining Lease Months"]] = (
        df["Remaining Lease"].str.extract(r"(\d+) years(?: (\d+) month)?").fillna(0)
    )
    df["Remaining Lease Years"] = df["Remaining Lease Years"].astype(int)
    df["Remaining Lease Months"] = df["Remaining Lease Months"].astype(int)

    # 移除 "Remaining Lease" 欄位
    df = df.drop(columns=["Remaining Lease"])

    # 存檔
    save_df(df, "resale_flat_prices")


def renting_out_of_flats():
    df = get_df("d_c9f57187485a850908655db0e8cfe651")

    # 取代 "0001-01"
    df = df.replace("0001-01", "", regex=False)

    # Column names 轉成可讀的文字
    df.columns = [
        "Rent Approval Date",
        "Town",
        "Block",
        "Street Name",
        "Flat Type",
        "Monthly Rent",
    ]

    # 存檔
    save_df(df, "renting_out_of_flats")


def hdb_property_information():
    map_dict = {
        "AMK": "ANG MO KIO",
        "BB": "BUKIT BATOK",
        "BD": "BEDOK",
        "BH": "BISHAN",
        "BM": "BUKIT MERAH",
        "BP": "BUKIT PANJANG",
        "BT": "BUKIT TIMAH",
        "CCK": "CHOA CHU KANG",
        "CL": "CLEMENTI",
        "CT": "CENTRAL AREA",
        "GL": "GEYLANG",
        "HG": "HOUGANG",
        "JE": "JURONG EAST",
        "JW": "JURONG WEST",
        "KWN": "KALLANG/WHAMPOA",
        "MP": "MARINE PARADE",
        "PG": "PUNGGOL",
        "PRC": "PASIR RIS",
        "QT": "QUEENSTOWN",
        "SB": "SEMBAWANG",
        "SGN": "SERANGOON",
        "SK": "SENGKANG",
        "TAP": "TAMPINES",
        "TG": "TENGAH",
        "TP": "TOA PAYOH",
        "WL": "WOODLANDS",
        "YS": "YISHUN",
    }

    # csv 下載連結: https://data.gov.sg/datasets/d_17f5382f26140b1fdae0ba2ef6239d2f/view
    df = get_df("d_17f5382f26140b1fdae0ba2ef6239d2f")

    # town 縮寫轉全名
    df["bldg_contract_town"] = df["bldg_contract_town"].map(map_dict)

    # Column names 轉成可讀的文字
    df.columns = [
        "Block Number",
        "Street",
        "Max Floor Level",
        "Year Completed",
        "Residential",
        "Commercial",
        "Market Hawker",
        "Miscellaneous",
        "Multistorey Carpark",
        "Precinct Pavilion",
        "Building Contract Town",
        "Total Dwelling Units",
        "1room Sold",
        "2room Sold",
        "3room Sold",
        "4room Sold",
        "5room Sold",
        "Exec Sold",
        "Multigen Sold",
        "Studio Apartment Sold",
        "1room Rental",
        "2room Rental",
        "3room Rental",
        "Other Room Rental",
    ]

    # 存檔
    save_df(df, "hdb_property_information")


def price_range_of_hdb_flats_offered():
    df = get_df("d_2d493bdcc1d9a44828b6e71cb095b88d")

    # 取代 "-"
    df = df.replace("-", "", regex=False)

    # Column names 轉成可讀的文字
    df.columns = [
        "Financial Year",
        "Town",
        "Room Type",
        "Min Selling Price",
        "Max Selling Price",
        "Min Selling Price Less Ahg Shg",
        "Max Selling Price Less Ahg Shg",
    ]

    # 存檔
    save_df(df, "price_range_of_hdb_flats_offered")


if __name__ == "__main__":
    pass
    # resale_flat_prices_1990_to_1999()
    # resale_flat_prices_2000_to_201202()
    # resale_flat_prices_201202_to_2014()
    # resale_flat_prices_2015_to_2016()
    # resale_flat_prices()
    # renting_out_of_flats()
    # hdb_property_information()
    # price_range_of_hdb_flats_offered()
