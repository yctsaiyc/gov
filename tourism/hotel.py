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

            csv_path = f"{self.data_dir}/{name}.csv"
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
                name = tds[1].text.replace(" ", "")
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

    def get_year_month(self, name):
        split = name.split("月")[0].split("年")
        year = split[0]
        month = split[1].zfill(2)
        return f"{year}-{month}"


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

        # 2. 刪除表頭和合計
        df = df.iloc[3:-1]

        # 2-1. 舊資料另外處理
        year_month = self.get_year_month(name)

        if year_month < "2014-10" and year_month not in ["2007-10", "2011-02"]:
            df = self.process_old_df(df, year_month)

        # 3. 重新命名欄位
        df.columns = self.columns

        # 4. 新增年月欄位
        df.insert(0, "年月", year_month)

        # 5. 刪除 "小計" 的 column
        df = df.loc[:, ~df.columns.str.contains("計")]

        # 6. 印出資料長度
        print("DataFrame length:", len(df))

        return df

    def process_old_df(self, df, year_month):
        # 1. 合併縣市欄位
        mask = df.iloc[:, 0].isna() | df.iloc[:, 0].isin(["臺灣省", "台灣省"])
        df.iloc[:, 0] = df.iloc[:, 0].where(~mask, df.iloc[:, 1])

        # 2. 刪除多餘縣市欄位
        df = df.drop(df.columns[1], axis=1)

        # 3. 刪除 "小計" 的 row
        mask = (df.iloc[:, 0] != "小　計") & ~df.iloc[:, 0].isna()
        df = df[mask]

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
        if name[:6] < "202006":
            return self.get_df_before_202006(df, name)

        elif name[:6] < "202101":
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


# 一般旅館家數及房間數統計表
class StandardHotel(Hotel):
    def get_data_id(self):
        return "9248"

    def get_columns(self):
        return [
            "縣市別",
            "合法旅館家數",
            "合法旅館房間數",
            "合法旅館員工人數",
            "未合法旅館家數",
            "未合法旅館房間數",
            "未合法旅館員工人數",
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

        # 3. 新增缺少欄位
        if len(df.columns) == 3:  # 2023-08以後
            df["合法旅館員工人數"] = ""
            df["未合法旅館家數"] = ""
            df["未合法旅館房間數"] = ""
            df["未合法旅館員工人數"] = ""

        elif len(df.columns) == 4:  # 2019 到 2023-07
            df["未合法旅館家數"] = ""
            df["未合法旅館房間數"] = ""
            df["未合法旅館員工人數"] = ""

        elif len(df.columns) == 10:  # 2011-03 到 2018
            # 刪除小計
            df = df.iloc[:, :7]

        # 4. 重新命名欄位
        df.columns = self.columns

        # 5. 新增年月欄位
        df.insert(0, "年月", year_month)

        # 6. 刪除表頭、"總計" 及其以下的列
        df = df.iloc[2 : df[df["縣市別"].str.contains("總", na=False)].index.min()]

        # 7. 印出資料長度
        print("DataFrame length:", len(df))

        return df


# 一般旅館營運報表
class StandardHotelReport(Hotel):
    def get_data_id(self):
        return "9711"

    def get_columns(self):
        return [
            "縣市",  # "地區名稱",
            "填報率",
            "未報家數",
            "住用及營收概況-客房住用數",
            "住用及營收概況-住用率",
            "住用及營收概況-平均房價",
            "住用及營收概況-房租收入 ",
            "住用及營收概況-餐飲收入 ",
            "住用及營收概況-總營業收入",
            "各部門職工概況-客房部(男)",
            "各部門職工概況-客房部(女)",
            "各部門職工概況-客房部人數",
            "各部門職工概況-餐飲部(男)",
            "各部門職工概況-餐飲部(女)",
            "各部門職工概況-餐飲部人數",
            "各部門職工概況-管理部(男)",
            "各部門職工概況-管理部(女)",
            "各部門職工概況-管理部人數",
            "各部門職工概況-其他部門(男)",
            "各部門職工概況-其他部門(女)",
            "各部門職工概況-其他部門人數",
            "各部門職工概況-員工合計(男)",
            "各部門職工概況-員工合計(女)",
            "各部門職工概況-員工合計人數",
        ]

    def get_df(self, name, url):
        if "1-12月" not in name and "1~12月" not in name:
            return pd.DataFrame()

        # 1. 讀取excel
        try:
            df_dict = pd.read_excel(url, sheet_name=None)

        except Exception as e:
            print(e)
            return pd.DataFrame()

        # 2. 創建空DataFrame
        df = pd.DataFrame(columns=["年月"] + self.columns)

        # 3. 合併所有sheet
        year = name[:4]

        for sheet_name in df_dict:
            # 3-1. 檢查是否為單月資料
            if "-" in sheet_name:
                continue

            df2 = df_dict[sheet_name]

            # 3-1-1. 舊資料另外處理
            if year <= "2021":
                df2 = self.get_df_before_2021(df2)
                df = pd.concat([df, df2], ignore_index=True)
                continue

            # 3-2. 重新命名欄位
            df2.columns = self.columns

            # 3-3. 新增年月欄位
            month = sheet_name.replace("月", "").zfill(2)
            year_month = f"{year}-{month}"
            print(year_month)
            df2.insert(0, "年月", year_month)

            # 3-4. 刪除表頭、"合計" 及其以下的列
            df2 = df2.iloc[
                3 : df2[df2["縣市"].str.contains("計", na=False)].index.min()
            ]

            # 3-5. 百分率轉換
            mask = df2.columns.str.contains("率")
            df2.loc[:, mask] = df2.loc[:, mask].astype(float).mul(100).round(2)

            # 3-6. 印出資料長度
            print("Number of Records:", len(df2))

            # 3-7. 合併DataFrame
            df = pd.concat([df, df2], ignore_index=True)

        # 4. 刪除男女合計欄位
        df = df.drop(
            columns=[
                col for col in df.columns if any(x in col for x in ["男", "女", "合計"])
            ]
        )

        return df

    def get_df_before_2021(self, df):
        return pd.DataFrame()


class HomeStay(Hotel):
    def get_data_id(self):
        return "10173"

    def get_columns(self):
        return [
            "縣市別",
            "合法民宿家數",
            "合法民宿房間數",
            "合法民宿員工人數",
            "未合法民宿家數",
            "未合法民宿房間數",
            "未合法民宿員工人數",
        ]

    def get_df(self, name, url):
        # 1. 讀取excel
        try:
            df = pd.read_excel(url)

        except Exception as e:
            print(e)
            return pd.DataFrame()

        # 2. 新增缺少欄位
        if len(df.columns) == 3:  # 2023-08以後
            df["合法民宿員工人數"] = ""
            df["未合法民宿家數"] = ""
            df["未合法民宿房間數"] = ""
            df["未合法民宿員工人數"] = ""

        elif len(df.columns) == 4:  # 2019 到 2023-07
            df["未合法民宿家數"] = ""
            df["未合法民宿房間數"] = ""
            df["未合法民宿員工人數"] = ""

        elif len(df.columns) == 10:  # 2014-10 到 2018
            # 刪除小計
            df = df.iloc[:, :7]

        elif len(df.columns) == 7:  # 2011-03 到 2014-09
            # 刪除小計
            df = df.iloc[:, :5]

            # 新增員工數
            df.insert(3, "合法民宿員工人數", "")
            df["未合法民宿員工人數"] = ""

        # 3. 重新命名欄位
        df.columns = self.columns

        # 4. 新增年月欄位
        split = name.split("月")[0].split("年")
        year = split[0]
        month = split[1].zfill(2)
        year_month = f"{year}-{month}"
        df.insert(0, "年月", year_month)

        # 5. 刪除表頭、"總計" 及其以下的列
        df = df.iloc[2 : df[df["縣市別"].str.contains("總", na=False)].index.min()]

        # 6. 印出資料長度
        print("Number of Records:", len(df))

        return df


class HomeStayReport(StandardHotelReport):
    def get_data_id(self):
        return "9969"

    def get_columns(self):
        return [
            "填報率",
            "縣市",
            "總出租客房數",
            "客房住用數",
            "客房住用率",
            "住宿人次",
            "平均房價",
            "客房收入",
            "餐飲收入",
            "其他收入",
            "收入合計",
            "裝修及設備",
            "登記經營者(男)",
            "登記經營者(女)",
            "登記經營者小計",
            "員工人數(男)",
            "員工人數(女)",
            "員工人數小計",
            "經營者及員工人數總計",
            "未報家數",
        ]

    def get_df_before_2021(self, df):
        return pd.DataFrame()


if __name__ == "__main__":
    # 觀光旅館合法家數統計表
    tourist_hotel = TouristHotel("data/tourist_hotel")
    tourist_hotel.save_all()

    # # 觀光旅館營運報表
    # tourist_hotel_report = TouristHotelReport("data/tourist_hotel_report")
    # tourist_hotel_report.save_all()

    # # 一般旅館家數及房間數統計表
    # standard_hotel = StandardHotel("data/standard_hotel")
    # standard_hotel.save_all()

    # # 一般旅館營運報表
    # standard_hotel_report = StandardHotelReport("data/standard_hotel_report")
    # standard_hotel_report.save_all()

    # # 民宿家數及房間數統計表
    # home_stay = HomeStay("data/home_stay")
    # home_stay.save_all()

    # # 民宿營運報表
    # home_stay_report = HomeStayReport("data/home_stay_report")
    # home_stay_report.save_all()
