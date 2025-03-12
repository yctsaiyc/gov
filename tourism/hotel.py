import os
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import numpy as np


class Hotel:
    def __init__(self, data_dir="data"):
        self.base_url = f"https://admin.taiwan.net.tw"
        self.data_id = self.get_data_id()
        self.columns = self.get_columns()

        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    def save_all(self):
        link_dict = self.get_links()
        # self.save_json(link_dict, "hotel.json")

        for name in link_dict:
            if "XLSX" in link_dict[name]:
                url = link_dict[name]["XLSX"]

            elif "XLS" in link_dict[name]:
                url = link_dict[name]["XLS"]

            elif "ODS" in link_dict[name]:
                url = link_dict[name]["ODS"]

            else:
                print(f"\nSkip: {name}.")
                continue

            print("\nURL:", url)
            df = self.get_df(name, url)

            if df.empty:
                print(f"Skip: {name}.")
                continue

            csv_path = f"{self.data_dir}/{name}.csv".replace(" ", "")
            df.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}.")

    def get_links(self):
        file_page_url = f"{self.base_url}/businessinfo/FilePage?a={self.data_id}"
        link_dict = {}
        page = 1  ###

        while True:
            url = f"{file_page_url}&P={page}"
            print("URL:", url)

            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            tbody = soup.find("tbody")
            trs = tbody.find_all("tr")

            if not trs:
                return link_dict

            for tr in trs:
                tds = tr.find_all("td")
                name = tds[1].text
                link_dict[name] = {}

                for a in tds[2].find_all("a"):
                    link = a.get("href")
                    data_format = a.find("span").text.split("：")[-1]
                    link_dict[name][data_format] = f"{self.base_url}{link}"

            # return link_dict  ###
            page += 1

    def save_json(self, content, filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=4)
        print(f"Saved {filename}.")


# 觀光旅館合法家數統計表
class TouristHotel(Hotel):
    def get_data_id(self):
        return "10812"

    def get_columns(self):
        return [
            "地區",
            "國際觀光旅館家數",
            "國際觀光旅館單人房數",
            "國際觀光旅館雙人房數",
            "國際觀光旅館套房數",
            "國際觀光旅館小計",
            "一般觀光旅館家數",
            "一般觀光旅館單人房數",
            "一般觀光旅館雙人房數",
            "一般觀光旅館套房數",
            "一般觀光旅館小計",
            "家數合計",
            "單人房數合計",
            "雙人房數合計",
            "套房數合計",
            "小計合計",
        ]

    def get_df(self, name, url):
        # 1. 讀取excel
        try:
            df = pd.read_excel(url)

        except Exception as e:
            print(e)
            return pd.DataFrame()

        # 2. 檢查是否為3行表頭+22個縣市+1合計
        print("DataFrame length:", len(df))
        # file_id = int(url.split("=")[-1])
        # if file_id >= 20000 and len(df) != 26:
        #     print("DataFrame length is not correct.")
        #     return pd.DataFrame()
        # elif file_id < 20000 and len(df) != 23:
        #     print("DataFrame length is not correct.")
        #     return pd.DataFrame()

        # 3. 取得年月
        year_month = df.iloc[0, 0]

        if year_month:
            year_month = year_month.split("：")[-1]

        else:
            print(df)
            year_month = df.iloc[0, 1]

        # 4. 刪除表頭和合計
        df = df.iloc[3:-1]

        # 5. 重新命名欄位
        df.columns = self.columns

        # 6. 新增年月欄位
        df.insert(0, "年月", year_month)

        return df


# 觀光旅館營運報表
class TouristHotelReport(Hotel):
    def get_data_id(self):
        return "10425"

    def get_columns(self):
        return [
            "地區名稱",
            "分類",
            "客房住用數",
            "住用率",
            "平均房價",
            "房租收入",
            "餐飲收入",
            "總營業收入",
            "客房部(男) ",
            "客房部(女) ",
            "客房部人數",
            "餐飲部(男) ",
            "餐飲部(女) ",
            "餐飲部人數",
            "管理部(男) ",
            "管理部(女) ",
            "管理部人數",
            "其他部門(男)",
            "其他部門(女)",
            "其他部門人數",
            "員工合計(男)",
            "員工合計(女)",
            "員工合計人數",
        ]

    def get_df(self, name, url):
        # 0. 檢查是否是單月資料
        if "-" in name:
            return pd.DataFrame()

        # 1. 讀取excel
        try:
            df = pd.read_excel(url)

        except Exception as e:
            print(e)
            return pd.DataFrame()

        # 1.1 舊資料另外處理
        if name.replace(" ", "")[:6] < "202006":
            return self.get_df_before_202006(df, name)

        elif name.replace(" ", "")[:6] < "202101":
            return self.get_df_202006_to_2021(df, name)

        else:
            return pd.DataFrame()

        # 2. 取得年月
        year_month = df.iloc[0, 12].split("月")[0].replace("年", "-")

        # 3. 刪除多餘欄位
        df = df.iloc[:, [0, 3, 5, 8, 9] + list(range(11, 29))]

        # 4. 重新命名欄位
        df.columns = self.columns

        # 5. 新增年月欄位
        df.insert(0, "年月", year_month)

        # 6. 刪除 "總計" 及其以下的列
        df = df.iloc[: df[df["分類"].eq("總計")].index.min()]

        # 7. 刪除 "小計" 所在的列，刪除表頭
        df = df[~df["分類"].isin(["小計", None, np.nan])]

        # 8. 將住用率轉換成百分比
        df["住用率"] = df["住用率"].astype(float).mul(100).round(2)

        # 9. 印出資料長度
        print("DataFrame length:", len(df))

        # 10. 刪除男女合計欄位
        df = df.drop(
            columns=[
                col for col in df.columns if any(x in col for x in ["男", "女", "合計"])
            ]
        )

        return df

    def get_df_202006_to_2021(self, df, name):
        # 1. 取得年月
        if "202301" in name:
            year_month = "2023-01"

        elif "202006" in name:
            year_month = "2020-06"

        else:
            year_month = df.iloc[0, 12].split("月")[0].replace("年", "-")

        # 2. 刪除多餘欄位
        df = df.iloc[:, [0, 3, 5, 8, 9] + list(range(11, 19))]

        # 3. 新增缺少欄位
        for i in [8, 9, 11, 12, 14, 15, 17, 18, 20, 21]:
            df.insert(i, f"new_col{i}", "")

        # 4. 重新命名欄位
        df.columns = self.columns

        # 5. 新增年月欄位
        df.insert(0, "年月", year_month)

        # 6. 刪除 "總計" 及其以下的列
        df = df.iloc[: df[df["分類"].eq("總計")].index.min()]

        # 7. 刪除 "小計" 所在的列，刪除表頭
        df = df[~df["分類"].isin(["小計", None, np.nan])]

        # 8. 將住用率轉換成百分比
        df["住用率"] = df["住用率"].astype(float).mul(100).round(2)

        # 9. 印出資料長度
        print("DataFrame length:", len(df))

        # 10. 刪除男女合計欄位
        df = df.drop(
            columns=[
                col for col in df.columns if any(x in col for x in ["男", "女", "合計"])
            ]
        )

        return df

    def get_df_before_202006(self, df, name):
        return pd.DataFrame()


class StandardHotel(Hotel):
    def get_data_id(self):
        return "9248"

    def get_columns(self):
        return [
            "縣市別",
            "家數",
            "房間數",
            "員工人數",
        ]

    def get_df(self, name, url):
        # 1. 讀取excel
        try:
            df = pd.read_excel(url)

        except Exception as e:
            print(e)
            return pd.DataFrame()

        # 2. 取得年月
        year_month = df.iloc[0, 0].split("月")[0].replace("年", "-")

        # 3. 新增員工人數欄位
        if len(df.columns) == 3:
            df["員工人數"] = None

        # 4. 重新命名欄位
        df.columns = self.columns

        # 5. 新增年月欄位
        df.insert(0, "年月", year_month)

        # 6. 刪除表頭、"總計" 及其以下的列
        df = df.iloc[2 : df[df["縣市別"].str.contains("總", na=False)].index.min()]

        # 7. 印出資料長度
        print("DataFrame length:", len(df))

        return df


if __name__ == "__main__":
    # # 觀光旅館合法家數統計表
    # tourist_hotel = TouristHotel("data/tourist_hotel")
    # tourist_hotel.save_all()

    # # 觀光旅館營運報表
    # tourist_hotel_report = TouristHotelReport("data/tourist_hotel_report")
    # tourist_hotel_report.save_all()

    # 一般旅館家數及房間數
    standard_hotel = StandardHotel("data/standard_hotel")
    standard_hotel.save_all()
