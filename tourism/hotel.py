import os
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import numpy as np
from io import BytesIO, StringIO
import pdfplumber


class Hotel:
    def __init__(self, data_dir="data"):
        self.base_url = f"https://admin.taiwan.net.tw"
        self.data_id = self.get_data_id()
        self.columns = self.get_columns()

        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    def update_data(self):
        # 取得檔名和下載連結
        link_dict = self.get_links()
        name, url = next(iter(link_dict.items()))
        url = next(iter(url.values()))
        print(f"{name}: {url}")

        # 轉成DataFrame
        df = self.get_df(url)
        df = self.process_df(df, name)

        # 刪除不必要的欄位
        df = self.drop_columns(df)

        # 存檔
        csv_path = f"{self.data_dir}/{name}.csv"
        df.to_csv(csv_path, index=False)
        print(f"Saved: {csv_path}.")

    def save_all(self):
        # 取得所有資料連結
        link_dict = self.get_links(get_all=True)

        # 將連結存成json
        # self.save_json(link_dict, "hotel.json")

        # 創建空DataFrame
        df = pd.DataFrame(columns=["年月"] + self.columns)

        # 合併歷年資料
        for name in link_dict:
            file_type = "EXCEL"

            if "XLSX" in link_dict[name]:
                url = link_dict[name]["XLSX"]

            elif "XLS" in link_dict[name]:
                url = link_dict[name]["XLS"]

            elif "ODS" in link_dict[name]:
                url = link_dict[name]["ODS"]

            elif "PDF" in link_dict[name]:
                url = link_dict[name]["PDF"]
                file_type = "PDF"

            else:
                print(f"Skip: {name}.")
                continue

            print(f"\n{name}")
            print("URL:", url)

            if file_type == "EXCEL":
                df2 = self.get_df(url)

            else:
                df2 = self.pdf_to_df(url)

            if df2.empty:
                print(f"Skip: {name}. DataFrame is empty.")
                continue

            df2 = self.process_df(df2, name)
            df = pd.concat([df, df2], ignore_index=True)

        # 刪除不必要的欄位
        df = self.drop_columns(df)

        # 依時間排序
        df = df.sort_values(by="年月", ascending=False, kind="stable")

        # 存檔
        csv_path = f"{self.data_dir}/all.csv"
        df.to_csv(csv_path, index=False)
        print(f"Saved: {csv_path}.")

    def get_links(self, get_all=False):
        file_page_url = f"{self.base_url}/businessinfo/FilePage?a={self.data_id}"
        link_dict = {}
        page = 1

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
                name = tds[1].text.replace(" ", "").replace("(", "").replace(")", "")
                link_dict[name] = {}

                for a in tds[2].find_all("a"):
                    link = a.get("href")
                    data_format = a.find("span").text.split("：")[-1]
                    link_dict[name][data_format] = f"{self.base_url}{link}"

                    if not get_all:
                        return link_dict

            page += 1

    def save_json(self, content, filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=4)
        print(f"Saved {filename}.")

    def drop_columns(self, df):
        return df

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

    def get_df(self, url):
        # 1. 讀取excel
        try:
            df = pd.read_excel(url)

        except Exception as e:
            print(e)
            return pd.DataFrame()

        # 2. 刪除表頭和合計
        df = df.iloc[3:-1]

        return df

    def process_df(self, df, name):
        # 1. 取得年月
        year_month = self.get_year_month(name)

        # 1-1. 舊資料另外處理
        if (
            year_month > "2011-02"
            and year_month < "2014-10"
            and year_month != "2007-10"
        ):
            df = self.process_old_df(df, year_month)

        # 2. 重新命名欄位
        df.columns = self.columns

        # 3. 新增年月欄位
        df.insert(0, "年月", year_month)

        # 4. 印出資料長度
        print("Number of Records:", len(df))

        return df

    def drop_columns(self, df):
        # 刪除 "小計" 的 columns
        df = df.loc[:, ~df.columns.str.contains("計")]
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

    def pdf_to_df(self, url):
        if "16047" in url:
            return pd.DataFrame()

        response = requests.get(url)
        pdf_file = BytesIO(response.content)
        text = ""

        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text += page.extract_text() + "\n"

        text = (
            text.replace("臺北市 小 計", "臺北市")
            .replace("高雄市 小 計", "高雄市")
            .replace("台灣省 基隆市", "基隆市")
            .replace("小 計", "小計")
            .replace("合 計", "合計")
        )

        if "臺北市" in text:
            text = "臺北市" + text.split("臺北市")[1]

        elif "台北市" in text:
            text = "台北市" + text.split("台北市")[1]

        try:
            df = pd.read_csv(StringIO(text), sep=" ", engine="python", header=None)
            df = df[~df.iloc[:, 0].astype(str).str.contains("計", na=False)]

        except Exception as e:
            print(text)
            print(e)
            return pd.DataFrame()

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
            "FIT類別",
            "GROUP類別",
            "類別合計",
            "本國",
            "中國大陸",
            "日本",
            "南韓",
            "港澳",
            "新加坡",
            "馬來西亞",
            "泰國",
            "印尼",
            "越南",
            "菲律賓",
            "汶萊",
            "緬甸",
            "寮國",
            "柬埔寨",
            "印度",
            "中東",
            "俄羅斯",
            "其他亞洲地區",  # 舊版欄位
            "美國",
            "加拿大",
            "北美",  # 舊版欄位
            "中南美洲",
            "英國",
            "歐洲其他地區",
            "紐澳",
            "非洲",
            "其他地區",
            "華僑",  # 舊版欄位
            "合計",
        ]

    def get_year_month(self, name):
        return f"{name[:4]}-{name[4:6]}"

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

        # 2. 取得年月
        year_month = self.get_year_month(name)

        # 3. 分離出主表
        if year_month >= "2021-01":
            df_main = self.get_df_main(df)

            # 4. 分離出國家表
            df_country = self.get_df_country(df)

        # 舊資料另外處理
        else:
            df_main = self.get_old_df_main(df, year_month)
            df_country = self.get_df_old_country(df, year_month)

        # 5. 合併表格
        df = df_main.merge(df_country, on=["地區名稱", "分類"], how="left")

        # 6. 確認欄位名稱正確
        if list(df.columns) != list(self.columns):
            print(df.columns)

        # 7. 新增年月欄位
        df.insert(0, "年月", year_month)

        # 8. 刪除男、女、合計欄位
        df = df.drop(
            columns=[
                col for col in df.columns if any(x in col for x in ["男", "女", "合計"])
            ]
        )

        # 9. 印出資料長度
        print("Number of Records:", len(df))

        return df

    def get_df_main(self, df):
        # 1. 刪除多餘欄位
        df = df.iloc[:, [0, 3, 5, 8, 9] + list(range(11, 29))]

        # 2. 重新命名欄位
        df.columns = self.columns[:23]

        # 3. 刪除 "總計" 及其以下的row
        df = df.iloc[: df[df["分類"].eq("總計")].index.min()]

        # 4. 刪除 "小計" 所在的row，刪除表頭
        df = df[~df["分類"].isin(["小計", None, np.nan])]

        # 5. 將住用率轉換成百分比
        df["住用率"] = df["住用率"].astype(float).mul(100).round(2)

        # 6. 印出資料長度
        print("Number of Records:", len(df))

        return df

    def get_df_country(self, df):
        # 1. 找到初始點
        mask = df.iloc[:, 2].eq("FIT類別")
        df = df.loc[mask.idxmax() :].reset_index(drop=True)

        # 2. 新增舊欄位
        df.insert(23, "其他亞洲地區", "")
        df.insert(26, "北美", "")
        df.insert(33, "華僑", "")

        # # 3. 檢查國家順序
        # if df.iloc[0].tolist()[2:] == self.columns[23:]:
        #     df = df.iloc[1:].reset_index(drop=True)

        # else:
        #     print("Country order error")
        #     print(df.iloc[0].tolist()[2:])
        #     print(self.columns[23:])
        #     raise

        # 4. 重新命名欄位
        df.columns = self.columns[:2] + self.columns[23:]

        # 5. 刪除 "總計" 及其以下的row
        df = df.iloc[: df[df["分類"].eq("總計")].index.min()]

        # 6. 刪除 "小計" 所在的row
        df = df[~df["分類"].isin(["小計", None, np.nan])]

        # 7. 印出資料長度
        print("Number of Records:", len(df))

        return df

    def get_old_df_main(self, df, year_month):
        # 1. 刪除多餘欄位
        df = df.iloc[:, [0, 3, 5, 8, 9] + list(range(11, 19))]

        # 2. 新增缺少欄位
        for i in [8, 9, 11, 12, 14, 15, 17, 18, 20, 21]:
            df.insert(i, f"new_col{i}", "")

        # 3. 重新命名欄位
        df.columns = self.columns

        # 4. 刪除 "總計" 及其以下的列
        df = df.iloc[: df[df["分類"].eq("總計")].index.min()]

        # 5. 刪除 "小計" 所在的列，刪除表頭
        df = df[~df["分類"].isin(["小計", None, np.nan])]

        # 6. 將住用率轉換成百分比
        df["住用率"] = df["住用率"].astype(float).mul(100).round(2)

        # 7. 印出資料長度
        print("Number of Records:", len(df))

        return df

    def get_old_df_country(self, df, year_month):
        # 1. 找到初始點
        mask = df.iloc[:, 2].eq("FIT類別")
        df = df.loc[mask.idxmax() :].reset_index(drop=True)

        # 2. 新增缺少國家
        # 本國,大陸,日本,韓國,港澳,新加坡,馬來西亞,其他亞洲地區,北美,歐洲,紐澳,其它,華僑,合計
        df.insert(12, "泰國", "")
        df.insert(13, "印尼", "")
        df.insert(14, "越南", "")
        df.insert(15, "菲律賓", "")
        df.insert(16, "汶萊", "")
        df.insert(17, "緬甸", "")
        df.insert(18, "寮國", "")
        df.insert(19, "柬埔寨", "")
        df.insert(20, "印度", "")
        df.insert(21, "中東", "")
        df.insert(22, "俄羅斯", "")
        df.insert(24, "美國", "")
        df.insert(25, "加拿大", "")
        df.insert(27, "中南美洲", "")
        df.insert(28, "英國", "")
        df.insert(31, "非洲", "")

        # 3. 重新命名欄位
        df.rename(
            columns={
                df.columns[0]: "地區名稱",
                df.columns[1]: "分類",
                "大陸": "中國大陸",
                "韓國": "南韓",
                "歐洲": "歐洲其他地區",
                "其它": "其他地區",
            },
            inplace=True,
        )

        # 4. 刪除 "總計" 及其以下的row
        df = df.iloc[: df[df["分類"].eq("總計")].index.min()]

        # 5. 刪除 "小計" 所在的row
        df = df[~df["分類"].isin(["小計", None, np.nan])]

        # 6. 印出資料長度
        print("Number of Records:", len(df))

        return df


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
            "家數小計",
            "房間數小計",
            "員工人數小計",
        ]

    def get_df(self, url):
        # 1. 讀取excel
        try:
            df = pd.read_excel(url)

        except Exception as e:
            print(e)
            return pd.DataFrame()

        # 2. 新增缺少欄位
        if len(df.columns) == 3:  # 2023-08以後
            df["合法旅館員工人數"] = ""
            df["未合法旅館家數"] = ""
            df["未合法旅館房間數"] = ""
            df["未合法旅館員工人數"] = ""
            df["家數小計"] = ""
            df["房間數小計"] = ""
            df["員工人數小計"] = ""

        elif len(df.columns) == 4:  # 2019 到 2023-07
            df["未合法旅館家數"] = ""
            df["未合法旅館房間數"] = ""
            df["未合法旅館員工人數"] = ""
            df["家數小計"] = ""
            df["房間數小計"] = ""
            df["員工人數小計"] = ""

        # 3. 重新命名欄位
        df.columns = self.columns

        return df

    def process_df(self, df, name):
        # 1. 新增年月欄位
        year_month = self.get_year_month(name)
        df.insert(0, "年月", year_month)

        # 2. 刪除表頭、"總計" 及其以下的列
        if year_month in ["2013-10", "2013-11", "2013-12"]:
            skip_rows = 3

        else:
            skip_rows = 2

        filtered_index = df[df["縣市別"].str.contains("總", na=False)].index

        if not filtered_index.empty:
            df = df.iloc[skip_rows : filtered_index.min()]

        # 3. 印出資料長度
        print("Number of Records:", len(df))

        return df

    def drop_columns(self, df):
        # 刪除小計
        return df.iloc[:, :-3]

    def pdf_to_df(self, url):
        response = requests.get(url)
        pdf_file = BytesIO(response.content)
        text = ""

        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text += page.extract_text() + "\n"

        try:
            text = (
                "台北市"
                + text.split("新北市")[-1]
                .split("台北市")[-1]
                .split("總 計")[0]
                .split("合 計")[0]
            )
            df = pd.read_csv(StringIO(text), sep=" ", engine="python", header=None)
            df.columns = self.columns

        except Exception as e:
            print(text)
            print(e)

        return df


# 一般旅館營運報表
class StandardHotelReport(Hotel):
    def get_data_id(self):
        return "9711"

    def get_columns(self):
        return [
            "年月",  # 原始資料無
            "縣市",  # "地區名稱",
            "總家數",  # 2019 以前
            "填報率",
            "未報家數",
            "住用及營收概況-總出租客房數",  # 2021 以前
            "住用及營收概況-客房住用數",
            "住用及營收概況-住用率",
            "住用及營收概況-住宿人數",  # 2020 以前
            "住用及營收概況-平均房價",
            "住用及營收概況-房租收入",
            "住用及營收概況-餐飲收入",
            "住用及營收概況-其他收入",  # 2021 以前
            "住用及營收概況-總營業收入",
            "住用及營收概況-裝修及設備支出",  # 2020 以前
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

    def get_rename_dict(self):
        return {
            # 2024
            "地區名稱Region": "縣市",
            "填報率%": "填報率",
            "未報家數Count": "未報家數",
            "客房住用數No. of Rooms Occupied": "住用及營收概況-客房住用數",
            "住用率Occupancy Rate": "住用及營收概況-住用率",
            "平均房價Average Room Rate": "住用及營收概況-平均房價",
            "房租收入Room Revenue": "住用及營收概況-房租收入",
            "餐飲收入F & B Revenue": "住用及營收概況-餐飲收入",
            "總營業收入Total Revenue": "住用及營收概況-總營業收入",
            "客房部(男) Room Dep.": "各部門職工概況-客房部(男)",
            "客房部(女) Room Dep.": "各部門職工概況-客房部(女)",
            "客房部人數Room Dep.": "各部門職工概況-客房部人數",
            "餐飲部(男) F&B Dep.": "各部門職工概況-餐飲部(男)",
            "餐飲部(女) F&B Dep.": "各部門職工概況-餐飲部(女)",
            "餐飲部人數F&B Dep.": "各部門職工概況-餐飲部人數",
            "管理部(男) Adm. Dep.": "各部門職工概況-管理部(男)",
            "管理部(女) Adm. Dep.": "各部門職工概況-管理部(女)",
            "管理部人數Adm. Dep.": "各部門職工概況-管理部人數",
            "其他部門(男) Other Dep.": "各部門職工概況-其他部門(男)",
            "其他部門(女) Other Dep.": "各部門職工概況-其他部門(女)",
            "其他部門人數Other Dep.": "各部門職工概況-其他部門人數",
            "員工合計(男) Total": "各部門職工概況-員工合計(男)",
            "員工合計(女) Total": "各部門職工概況-員工合計(女)",
            "員工合計人數Total": "各部門職工概況-員工合計人數",
            # 2023
            "地區名稱": "縣市",
            "填報率": "填報率",
            "未報家數": "未報家數",
            "客房住用數": "住用及營收概況-客房住用數",
            "住用率": "住用及營收概況-住用率",
            "平均房價": "住用及營收概況-平均房價",
            "客房收入": "住用及營收概況-房租收入",
            "餐飲收入": "住用及營收概況-餐飲收入",
            "總營業收入": "住用及營收概況-總營業收入",
            "客房部(男) ": "各部門職工概況-客房部(男)",
            "客房部(女) ": "各部門職工概況-客房部(女)",
            "客房部人數": "各部門職工概況-客房部人數",
            "餐飲部(男) ": "各部門職工概況-餐飲部(男)",
            "餐飲部(女) ": "各部門職工概況-餐飲部(女)",
            "餐飲部人數": "各部門職工概況-餐飲部人數",
            "管理部(男) ": "各部門職工概況-管理部(男)",
            "管理部(女) ": "各部門職工概況-管理部(女)",
            "管理部人數": "各部門職工概況-管理部人數",
            "其他部門(男) ": "各部門職工概況-其他部門(男)",
            "其他部門(女) ": "各部門職工概況-其他部門(女)",
            "其他部門人數": "各部門職工概況-其他部門人數",
            "員工合計(男) ": "各部門職工概況-員工合計(男)",
            "員工合計(女) ": "各部門職工概況-員工合計(女)",
            "員工合計人數": "各部門職工概況-員工合計人數",
            # 2021
            "縣市": "縣市",
            "填報率": "填報率",
            "客房住用率": "住用及營收概況-住用率",
            "其他收入": "住用及營收概況-其他收入",
            "收入合計": "住用及營收概況-總營業收入",
            "員工人數": "各部門職工概況-員工合計人數",
            "總出租客房數": "住用及營收概況-總出租客房數",
            # 2020
            "住宿人數": "住用及營收概況-住宿人數",
            "裝修及設備支出": "住用及營收概況-裝修及設備支出",
            # 2019
            "1月總家數": "總家數",
            "2月總家數": "總家數",
            "3月總家數": "總家數",
            "4月總家數": "總家數",
            "5月總家數": "總家數",
            "6月總家數": "總家數",
            "7月總家數": "總家數",
            "8月總家數": "總家數",
            "9月總家數": "總家數",
            "10月總家數": "總家數",
            "11月總家數": "總家數",
            "12月總家數": "總家數",
            # 2015
            "回報率": "填報率",
        }

    def get_df(self, name, url):
        year = name[:4]

        if year >= "2017":
            if "1-12月" not in name and "1~12月" not in name:
                return pd.DataFrame()

        else:
            if "年6月" not in name and "7~12月" not in name:
                return pd.DataFrame()

        # 1. 讀取excel
        try:
            df_dict = pd.read_excel(url, sheet_name=None)

        except Exception as e:
            print(e)
            return pd.DataFrame()

        # 2. 創建空DataFrame
        df = pd.DataFrame(columns=self.columns)

        # 3. 合併所有sheet
        for sheet_name in df_dict:
            # 3-1. 檢查是否為單月資料
            if "-" in sheet_name:
                continue

            df2 = df_dict[sheet_name]

            # 3-2. 刪除表頭
            mask = (
                df2.iloc[:, 0]
                .astype(str)
                .isin(["縣市", "地區名稱", "填報率", "回報率", "地區名稱Region"])
            )

            header_idx = mask.idxmax()
            df2.columns = df2.iloc[header_idx]
            df2 = df2.iloc[header_idx + 1 :].reset_index(drop=True)

            # 3-3. 重新命名欄位
            for col in df2.columns:
                if col not in self.get_rename_dict() and not pd.isna(col):
                    print(f"發現新欄位: {col}")

            df2 = df2.rename(columns=self.get_rename_dict())

            # 3-4. 重新排序欄位
            df2 = df2.reindex(columns=self.columns)

            # 3-5. 填入年月
            month = sheet_name.replace("月", "").zfill(2)
            year_month = f"{year}-{month}"
            print(year_month)
            df2["年月"] = year_month

            # 3-6. 刪除 "合計" 及其以下的row
            df2 = df2.iloc[
                0 : df2[df2["縣市"].str.contains("計", na=False)].index.min()
            ]

            # 3-7. 百分率轉換
            mask = df2.columns.str.contains("率")
            df2.loc[:, mask] = df2.loc[:, mask].astype(float).mul(100).round(2)

            # 3-8. 印出資料長度
            print("Number of Records:", len(df2))

            # 3-9. 合併DataFrame
            df = pd.concat([df, df2], ignore_index=True)

        return df

    def drop_columns(self, df):
        return df.drop(
            columns=[col for col in df.columns if any(x in col for x in ["男", "女"])]
        )


class HomeStay(StandardHotel):
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
            "家數小計",
            "房間數小計",
            "員工人數小計",
        ]

    def get_df(self, url):
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
            df["家數小計"] = ""
            df["房間數小計"] = ""
            df["員工人數小計"] = ""

        elif len(df.columns) == 4:  # 2019 到 2023-07
            df["未合法民宿家數"] = ""
            df["未合法民宿房間數"] = ""
            df["未合法民宿員工人數"] = ""
            df["家數小計"] = ""
            df["房間數小計"] = ""
            df["員工人數小計"] = ""

        elif len(df.columns) == 7:  # 2011-03 到 2014-09
            # 新增員工數
            df.insert(3, "合法民宿員工人數", "")
            df.insert(6, "未合法民宿員工人數", "")
            df["家數小計"] = ""
            df["房間數小計"] = ""
            df["員工人數小計"] = ""

        # 3. 重新命名欄位
        df.columns = self.columns

        return df

    def process_df(self, df, name):
        # 1. 新增年月欄位
        year_month = self.get_year_month(name)
        df.insert(0, "年月", year_month)

        # 2. 刪除表頭、"總計" 及其以下的列
        if year_month in ["2013-10", "2013-11", "2013-12"]:
            skip_rows = 3

        else:
            skip_rows = 2

        filtered_index = df[df["縣市別"].str.contains("總", na=False)].index

        if not filtered_index.empty:
            df = df.iloc[skip_rows : filtered_index.min()]

        # 3. 印出資料長度
        print("Number of Records:", len(df))

        return df


class HomeStayReport(StandardHotelReport):
    def get_data_id(self):
        return "9969"

    def get_columns(self):
        return [
            "年月",
            "縣市",
            "總家數",
            "填報率",
            "未報家數",
            "總出租客房數",
            "客房住用數",
            "客房住用率",
            "住宿人次",
            "平均房價",
            "客房收入",
            "餐飲收入",
            "其他收入",
            "收入合計",
            "裝修及設備支出",
            "登記經營者(男)",
            "登記經營者(女)",
            "登記經營者小計",
            "員工人數(男)",
            "員工人數(女)",
            "員工人數",
            "經營者及員工人數總計",
        ]

    def get_rename_dict(self):
        return {
            "填報率": "填報率",
            "縣市": "縣市",
            "總出租客房數": "總出租客房數",
            "客房住用數": "客房住用數",
            "客房住用率": "客房住用率",
            "住宿人次": "住宿人次",
            "平均房價": "平均房價",
            "客房收入": "客房收入",
            "餐飲收入": "餐飲收入",
            "其他收入": "其他收入",
            "收入合計": "收入合計",
            "裝修及設備": "裝修及設備支出",
            "登記經營者(男) ": "登記經營者(男)",
            "登記經營者(女) ": "登記經營者(女)",
            "登記經營者小計": "登記經營者小計",
            "員工人數(男) ": "員工人數(男)",
            "員工人數(女) ": "員工人數(女)",
            "員工人數小計": "員工人數",
            "經營者及員工人數總計": "經營者及員工人數總計",
            "未報家數": "未報家數",
            # 2020
            "住宿人數": "住宿人次",
            "裝修及設備支出": "裝修及設備支出",
            "員工人數": "員工人數",
            # 2019
            "1月總家數": "總家數",
            "2月總家數": "總家數",
            "3月總家數": "總家數",
            "4月總家數": "總家數",
            "5月總家數": "總家數",
            "6月總家數": "總家數",
            "7月總家數": "總家數",
            "8月總家數": "總家數",
            "9月總家數": "總家數",
            "10月總家數": "總家數",
            "11月總家數": "總家數",
            "12月總家數": "總家數",
            # 2017
            "經營人數": "登記經營者小計",
            # 2015
            "回報率": "填報率",
        }


if __name__ == "__main__":
    # # 觀光旅館合法家數統計表
    # tourist_hotel = TouristHotel("data/tourist_hotel")
    # tourist_hotel.update_data()
    # # tourist_hotel.save_all()

    # # 觀光旅館營運報表
    # tourist_hotel_report = TouristHotelReport("data/tourist_hotel_report")
    # tourist_hotel_report.save_all()

    # # 一般旅館家數及房間數統計表
    # standard_hotel = StandardHotel("data/standard_hotel")
    # standard_hotel.save_all()

    # # 一般旅館營運報表
    # standard_hotel_report = StandardHotelReport("data/standard_hotel_report")
    # standard_hotel_report.save_all()

    # 民宿家數及房間數統計表
    home_stay = HomeStay("data/home_stay")
    home_stay.save_all()

    # # 民宿營運報表
    # home_stay_report = HomeStayReport("data/home_stay_report")
    # home_stay_report.save_all()
