import os
import requests
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


class Tourism:
    def __init__(self, data_dir="data", checkpoint_path="checkpoint.txt"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

        self.url = (
            "https://stat.taiwan.net.tw/data/api/statistics/scenicSpot/month/single"
        )

        self.checkpoint_path = checkpoint_path
        self.checkpoint = self.get_checkpoint()

        self.columns = [
            "類型",
            "觀光風景區",
            "觀光遊憩區",
            "縣市",
            "年月",
            "遊客人次",
            "去年同月遊客人次",
            "差值",
            "成長率",
            "遊客人次計算方式",
            "遊客人次計算類型",
            "資料來源",  # 2023年6月後才有
        ]

    def get_checkpoint(self):

        # 檢查checkpoint檔案是否存在
        if not os.path.exists(self.checkpoint_path):

            # 如果檔案不存在，創建並寫入初始內容
            year_month = "2024-11"

            with open(self.checkpoint_path, "w", encoding="utf-8") as f:
                f.write(year_month)

            print(f"Checkpoint file created with default content: {year_month}")

        with open(self.checkpoint_path, "r", encoding="utf-8") as f:
            return f.read()

    def update_checkpoint(self, year_month):
        self.checkpoint = year_month

        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            f.write(self.checkpoint)

        print(f"Updated checkpoint: {self.checkpoint}")

    def get_df(self, year, month):

        # 西元年轉民國年
        year = int(year) - 1911

        url = f"{self.url}?year={year}&monthStart={month}&monthEnd={month}"
        print(f"URL: {url}")

        headers = {
            # "Accept": "application/json, text/plain, */*",
            # "Accept-Encoding": "gzip, deflate, br, zstd",
            # "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
            "Authorization": "MTczNzAwOTAwOTMwMzo6NjkxMDc1",
            # "Connection": "keep-alive",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            # "Cookie": "_ga=GA1.1.347167713.1734921111; _ga_FQMGBMMQ6H=GS1.1.1736992239.24.0.1736992239.0.0.0; _ga_6B5V7950JG=GS1.1.1736992798.4.0.1736992798.60.0.93308755",
            # "Host": "stat.taiwan.net.tw",
            # "Referer": "https://stat.taiwan.net.tw/statistics/month/scenicSpot/single",
            # "Sec-CH-UA": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            # "Sec-CH-UA-Mobile": "?0",
            # "Sec-CH-UA-Platform": '"Linux"',
            # "Sec-Fetch-Dest": "empty",
            # "Sec-Fetch-Mode": "cors",
            # "Sec-Fetch-Site": "same-origin",
            # "User-Agent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            excel_data = BytesIO(response.content)

            df = pd.read_excel(excel_data, skiprows=2)

            return df

        else:
            print(f"請求失敗，狀態碼: {response.status_code}")
            return pd.DataFrame()

    def process_df(self, df, year, month):
        df.columns = [
            "類型",
            "觀光遊憩區",
            "縣市",
            "遊客人次",
            "去年同月遊客人次",
            "差值",
            "成長率",
            "遊客人次計算方式",
            "資料來源",
        ]

        # 刪除英文
        cols = ["類型", "觀光遊憩區", "縣市"]
        df[cols] = df[cols].map(lambda x: x.split("\n")[0] if isinstance(x, str) else x)

        # 新增「觀光風景區」欄位
        del_idx = []

        for idx, row in df.iterrows():
            if row["類型"] not in ["國家公園", "國家級風景特定區"]:
                break

            if pd.isna(row["縣市"]):
                park = row["觀光遊憩區"]
                del_idx.append(idx)
                continue

            elif row["觀光遊憩區"].startswith(" "):
                df.at[idx, "觀光風景區"] = park

        df = df.map(lambda x: x.replace("  ", "") if isinstance(x, str) else x)
        df = df.drop(del_idx, axis=0).reset_index(drop=True)

        # 處理nan
        df = df.replace("nan", "")

        # 刪除tab
        df = df.replace("\t", "", regex=True)

        # 刪除資料來源備註
        df = df[:-2]

        # 新增遊客人次計算類型欄位
        df["遊客人次計算類型"] = df["遊客人次計算方式"].map(
            lambda x: self.get_count_type(x) if isinstance(x, str) else ""
        )

        # 計算差值
        df["差值"] = df["遊客人次"] - df["去年同月遊客人次"]

        # 計算成長率
        df["成長率"] = np.where(
            df["去年同月遊客人次"] == 0.0,
            "",
            round(df["差值"] / df["去年同月遊客人次"] * 100, 2),
        )

        # 新增年月欄位
        df["年月"] = f"{year}-{month:02}"

        # 排序欄位
        df = df.reindex(columns=self.columns)

        return df

    def get_count_type(self, count_method):

        keywords = {
            "門票": "門票數",  # 電子計數器及門票數計算
            "人工": "人工估算",  # 人工計數器
            "計數器": "電子感應",  # 紅外線計數器自動偵測
            "電子計數": "電子感應",  # 紅外線計數器自動偵測
            "感應器": "電子感應",
            "估算": "人工估算",
            "系統自動偵測": "AI辨識",
            "登記": "登記數量",
            "核准進入": "人工估算",
            "監測系統": "AI辨識",
            "AI": "AI辨識",
            "電信": "電信數據",
            "推估": "人工估算",
            "人力統計": "人工估算",
            "行動裝置": "電信數據",
            "推算": "人工估算",
            "入境人數計算": "人工估算",
            "概估": "人工估算",
            "數計算": "人工估算",
            "數位人流計數": "電子感應",
            "人流": "人工估算",
            "停車數": "人工估算",
            "人數": "人工估算",
            "自動": "AI辨識",
            "電子": "電子感應",
            "住宿人次": "人工估算",
            "人數": "人工估算",
            "參照觀光大平臺之數據": "人工估算",
            "觀光大數據": "人工估算",
        }

        for key in keywords.keys():
            if key in count_method:
                return keywords[key]

        # raise Exception(f"需定義遊客人次計算類型: {count_method}")
        return None

    def save_csv(self):
        today = datetime.today()
        checkpoint_split = self.checkpoint.split("-")
        checkpoint_date = datetime(
            int(checkpoint_split[0]), int(checkpoint_split[1]), 1
        ) + timedelta(days=31)

        while checkpoint_date < today:
            year = checkpoint_date.year
            month = checkpoint_date.month

            # 轉dataframe
            df = self.get_df(year, month)

            # if df.empty:
            if len(df) < 5:  # 會有空行使df不為empty
                print(f"No data for {year}-{month:02}.")
                break

            df = self.process_df(df, year, month)

            # 存檔
            csv_name = f"{year}年{month:02}月主要觀光遊憩據點遊客人數.csv"
            csv_path = os.path.join(self.data_dir, csv_name)
            df.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}.")

            # 更新checkpoint
            checkpoint_date = checkpoint_date + timedelta(days=31)
            self.update_checkpoint(f"{year}-{month:02}")


if __name__ == "__main__":
    tourism = Tourism(data_dir="data", checkpoint_path="checkpoint.txt")
    tourism.save_csv()
