import os
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd


class Hotel:
    def __init__(self, data_id, data_dir="data"):
        self.base_url = f"https://admin.taiwan.net.tw"
        self.data_id = data_id
        self.columns = self.get_columns()

        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    def save_all(self):
        link_dict = self.get_links()
        # self.save_json(link_dict, "hotel.json")

        for name in link_dict:
            if "-" in name:
                print(f"\nSkip: {name}.")
                continue

            elif "XLSX" in link_dict[name]:
                url = link_dict[name]["XLSX"]

            elif "XLS" in link_dict[name]:
                url = link_dict[name]["XLS"]

            elif "ODS" in link_dict[name]:  # ODS有些連結是錯誤的，例：2020年1月
                url = link_dict[name]["ODS"]

            else:
                print(f"\nSkip: {name}.")
                continue

            print("\nURL:", url)

            if "2014" in name:
                break

            df = self.get_df(url)

            if df.empty:
                print(f"Skip: {name}.")
                continue

            csv_path = f"{self.data_dir}/{name}.csv"
            df.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}.")

    def get_links(self):
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
                name = tds[1].text.replace(" ", "_")
                link_dict[name] = {}

                for a in tds[2].find_all("a"):
                    link = a.get("href")
                    data_format = a.find("span").text.split("：")[-1]
                    link_dict[name][data_format] = f"{self.base_url}{link}"

            page += 1

    def save_json(self, content, filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=4)
        print(f"Saved {filename}.")

    def get_df(self, url):
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
            print(year_month)
            print(type(year_month))
            exit()
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

    def get_columns(self):
        # 觀光旅館合法家數統計表
        if self.data_id == "10812":
            return [
                "地區/客房數",
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

        # 觀光旅館營運報表
        elif self.data_id == "9711":
            return [
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


if __name__ == "__main__":
    # # 觀光旅館合法家數統計表
    # hotel = Hotel("10812")

    # 觀光旅館營運報表
    hotel = Hotel("9711")

    hotel.save_all()
