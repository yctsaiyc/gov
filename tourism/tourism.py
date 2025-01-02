import os
import requests
from urllib.parse import unquote
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
import re


class Tourism:
    def __init__(self, data_dir="data", checkpoint_path="checkpoint.txt"):
        self.csv_dir = data_dir
        self.xlsx_dir = os.path.join(data_dir, "xlsx")
        self.converted_dir = os.path.join(data_dir, "xlsx", "converted")

        os.makedirs(self.csv_dir, exist_ok=True)
        os.makedirs(self.xlsx_dir, exist_ok=True)
        os.makedirs(self.converted_dir, exist_ok=True)

        self.base_url = "https://admin.taiwan.net.tw"
        self.url = "https://admin.taiwan.net.tw/businessinfo/FilePage?a=12603"

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
            "資料來源",
        ]

    def get_checkpoint(self):

        # 檢查checkpoint檔案是否存在
        if not os.path.exists(self.checkpoint_path):

            # 如果檔案不存在，創建並寫入初始內容
            with open(self.checkpoint_path, "w", encoding="utf-8") as f:
                f.write("114-01-01")

            print(f"Checkpoint file created with default content: 114-01-01")

        with open(self.checkpoint_path, "r", encoding="utf-8") as f:
            return f.read()

    def update_checkpoint(self, checkpoint):
        self.checkpoint = checkpoint

        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            f.write(checkpoint)

    def get_xlsx_link_dict(self):

        # 取得html
        response = requests.get(self.url)
        html = response.text

        # 取得table內容
        soup = BeautifulSoup(html, "html.parser")
        body = soup.body
        tbody = body.find("tbody")

        link_dict = {}

        # 逐行尋找
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")

            # 找checkpoint以後更新的資料
            update_date = tds[-1].text

            if update_date < self.checkpoint:
                continue

            # 取得資料名稱
            name = f"{tds[1].text}_{update_date.replace('-', '')}.xlsx"

            # 取得xlsx檔連結
            a = tds[2].find("a", title=lambda t: t and "檔案格式：XLS" in t)
            link_dict[name] = self.base_url + a.get("href")

        return link_dict

    def download_xlsx(self, xlsx_link_dict):
        for xlsx_name, xlsx_link in xlsx_link_dict.items():
            print(f"URL: {xlsx_link}")
            response = requests.get(xlsx_link)

            if response.status_code == 200:

                # 建立檔案路徑
                xlsx_path = os.path.join(self.xlsx_dir, xlsx_name)

                # 存檔
                with open(xlsx_path, "wb") as f:
                    f.write(response.content)
                    print(f"Saved {xlsx_path}.")

            else:
                raise Exception(
                    f"Failed to download file. Status code: {response.status_code}"
                )

    def xlsx_to_csv(self):
        xlsx_files = [
            file for file in os.listdir(self.xlsx_dir) if file.endswith(".xlsx")
        ]

        for xlsx_file in xlsx_files:
            xlsx_path = os.path.join(self.xlsx_dir, xlsx_file)

            # xlsx轉df
            print(f"\nConverting: {xlsx_path}...")

            # 跳過標頭
            df = pd.read_excel(xlsx_path, skiprows=2)

            # 處理df
            year_month_split = xlsx_file.split("月")[0].split("年")
            year = int(year_month_split[0])
            month = int(year_month_split[1])
            year_month = f"{year}-{month:02}"
            df = self.process_df(df, year_month)

            # 存檔
            csv_name = xlsx_file.replace(".xlsx", ".csv")
            csv_path = os.path.join(self.csv_dir, csv_name)
            df.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}.")

            # 將xlsx壓縮後移到converted資料夾
            zip_path = os.path.join(
                self.converted_dir, xlsx_file.replace(".xlsx", ".zip")
            )

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(csv_path, arcname=csv_name)

            print(f"Compressed: {xlsx_path} to {zip_path}.")

    def process_df(self, df, year_month):

        # 重新命名部份欄位名稱

        df = df.rename(
            columns={
                "類型\nType": "類型",
                "觀光遊憩區\nScenic Spots": "觀光遊憩區",
                "縣市\nCity/Country": "縣市",
                f"{int(year_month.split('-')[0])-1911}年{int(year_month.split('-')[1])}月\n遊客人次": "遊客人次",
                "上年同月\n遊客人次": "去年同月遊客人次",
                "成長率\n(%)": "成長率",
            }
        )

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
            lambda x: self.get_count_type(x)
        )

        # 替換成長率的"-"為空值
        df["成長率"] = df["成長率"].replace("-", "")

        # 新增年月欄位
        df["年月"] = year_month

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

        return None
        # raise Exception(f"需定義遊客人次計算類型: {count_method}")


if __name__ == "__main__":
    tourism = Tourism(data_dir="data", checkpoint_path="checkpoint.txt")
    xlsx_link_dict = tourism.get_xlsx_link_dict()
    tourism.download_xlsx(xlsx_link_dict)
    tourism.xlsx_to_csv()
