import os
import requests
from urllib.parse import unquote
from bs4 import BeautifulSoup
import pandas as pd


class Tourism:
    def __init__(self, data_dir="data", checkpoint_path="checkpoint.txt"):
        self.data_dir = data_dir
        self.xlsx_dir = os.path.join(self.data_dir, "xlsx")
        self.converted_dir = os.path.join(self.data_dir, "xlsx", "converted")

        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.xlsx_dir, exist_ok=True)
        os.makedirs(self.converted_dir, exist_ok=True)

        self.base_url = "https://admin.taiwan.net.tw"
        self.url = "https://admin.taiwan.net.tw/businessinfo/FilePage?a=12603"

        self.checkpoint_path = checkpoint_path
        self.checkpoint = self.get_checkpoint()

    def get_checkpoint(self):

        # 檢查checkpoint檔案是否存在
        if not os.path.exists(self.checkpoint_path):

            # 如果檔案不存在，創建並寫入初始內容
            with open(self.checkpoint_path, "w", encoding="utf-8") as f:
                f.write("113-12-01")

            print(f"Checkpoint file created with default content: 113-12-01")

        with open(self.checkpoint_path, "r", encoding="utf-8") as f:
            return f.read()

    def update_checkpoint(self, checkpoint):
        self.checkpoint = checkpoint

        with open(self.checkpoint_path, "w", encoding="utf-8") as f:
            f.write(checkpoint)

    def get_xlsx_links(self):

        # 取得html
        response = requests.get(self.url)
        html = response.text

        # 取得table內容
        soup = BeautifulSoup(html, "html.parser")
        body = soup.body
        tbody = body.find("tbody")

        links = []

        # 逐行尋找
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")

            # 找checkpoint以後更新的資料
            update_date = tds[-1].text

            if update_date < self.checkpoint:
                continue

            # 取得xlsx檔連結
            a = tds[2].find("a", title=lambda t: t and "檔案格式：XLS" in t)
            links.append(self.base_url + a.get("href"))

        return links

    def download_xlsx(self, xlsx_links):
        for xlsx_link in xlsx_links:
            print(f"URL: {xlsx_link}")
            response = requests.get(xlsx_link)

            if response.status_code == 200:

                # 提取 Content-Disposition 標頭中的檔名
                content_disposition = response.headers.get("Content-Disposition")
                filename = content_disposition.split("filename=")[-1]

                # 解碼檔案名稱
                xlsx_name = unquote(filename)

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
            print(f"Converting {xlsx_path}...")
            df = pd.read_excel(xlsx_path, skiprows=2)
            df = self.process_df(df)

            # 存檔
            csv_path = os.path.join(self.data_dir, xlsx_file.replace(".xlsx", "_2.csv"))
            df.to_csv(csv_path, index=False)
            print(f"Saved {csv_path}.")

    def process_df(self, df):

        # 刪除欄位名稱的換行符號
        df.columns = [col.replace("\n", "") for col in df.columns]

        # 刪除英文
        cols = ["類型Type", "觀光遊憩區Scenic Spots", "縣市City/Country"]
        df[cols] = df[cols].map(lambda x: x.split("\n")[0] if isinstance(x, str) else x)

        # 新增「觀光風景區」欄位
        del_idx = []

        for idx, row in df.iterrows():
            if row["類型Type"] not in ["國家公園", "國家級風景特定區"]:
                break

            if pd.isna(row["縣市City/Country"]):
                park = row["觀光遊憩區Scenic Spots"]
                del_idx.append(idx)
                continue

            elif row["觀光遊憩區Scenic Spots"].startswith(" "):
                df.at[idx, "觀光風景區"] = park

        df = df.map(lambda x: x.replace("  ", "") if isinstance(x, str) else x)
        df = df.drop(del_idx, axis=0).reset_index(drop=True)

        # 處理nan
        df = df.replace("nan", "")

        return df


if __name__ == "__main__":
    tourism = Tourism(data_dir="data", checkpoint_path="checkpoint.txt")
    # xlsx_links = tourism.get_xlsx_links()
    # tourism.download_xlsx(xlsx_links)
    tourism.xlsx_to_csv()
