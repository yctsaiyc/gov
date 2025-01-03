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
            "資料來源",  # 2023年6月後才有
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

    def xlsx_to_df(self, xlsx_path):
        # 跳過標頭
        df = pd.read_excel(xlsx_path, skiprows=2)
        return df

    def xlsx_to_csv(self):
        xlsx_files = [
            file for file in os.listdir(self.xlsx_dir) if file.endswith(".xlsx")
        ]

        for xlsx_file in xlsx_files:
            xlsx_path = os.path.join(self.xlsx_dir, xlsx_file)

            # xlsx轉df
            print(f"\nConverting: {xlsx_path}...")
            df = self.xlsx_to_df(xlsx_path)

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
                zipf.write(xlsx_path, arcname=csv_name.replace(".csv", ".xlsx"))

            print(f"Compressed: {xlsx_path} to {zip_path}.")

            # 刪除xlsx檔案
            os.remove(xlsx_path)
            print(f"Deleted: {xlsx_path}.")

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
            lambda x: self.get_count_type(x) if isinstance(x, str) else ""
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

        # raise Exception(f"需定義遊客人次計算類型: {count_method}")
        return None


class Tourism_2023(Tourism):
    def get_xlsx_link_dict(self):
        return {
            "2023年1月主要觀光遊憩據點遊客人數_1121005.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=30260",
            "2023年2月主要觀光遊憩據點遊客人數_1120501.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=28055",
            "2023年3月主要觀光遊憩據點遊客人數_1130206.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=28859",
            "2023年4月主要觀光遊憩據點遊客人數_1130206.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=30262",
            "2023年5月主要觀光遊憩據點遊客人數_1130206.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=28875",
            "2023年6月主要觀光遊憩據點遊客人數_1130320.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=31847",
            "2023年7月主要觀光遊憩據點遊客人數_1130320.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=31849",
            "2023年8月主要觀光遊憩據點遊客人數_1130320.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=31851",
            "2023年9月主要觀光遊憩據點遊客人數_1130320.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=31853",
            "2023年10月主要觀光遊憩據點遊客人數_1131115.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=35600",
            "2023年11月主要觀光遊憩據點遊客人數_1130315.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=31825",
            "2023年12月主要觀光遊憩據點遊客人數_1130206.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=31359",
        }


class Tourism_2022(Tourism):
    def get_xlsx_link_dict(self):
        return {
            "2022年1月主要觀光遊憩據點遊客人數_1110315.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27492",
            "2022年2月主要觀光遊憩據點遊客人數_1110415.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27490",
            "2022年3月主要觀光遊憩據點遊客人數_1110516.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27488",
            "2022年4月主要觀光遊憩據點遊客人數_1110615.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27486",
            "2022年5月主要觀光遊憩據點遊客人數_1110715.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27484",
            "2022年6月主要觀光遊憩據點遊客人數_1110815.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27481",
            "2022年7月主要觀光遊憩據點遊客人數_1120615.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27480",
            "2022年8月主要觀光遊憩據點遊客人數_1120615.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27478",
            "2022年9月主要觀光遊憩據點遊客人數_1111110.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27476",
            "2022年10月主要觀光遊憩據點遊客人數_1111208.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27474",
            "2022年11月主要觀光遊憩據點遊客人數_1120106.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27472",
            "2022年12月主要觀光遊憩據點遊客人數_1120207.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27469",
        }


class Tourism_2021(Tourism):
    def get_xlsx_link_dict(self):
        return {
            "2021年1月主要觀光遊憩據點遊客人數_1100315.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27505",
            "2021年2月主要觀光遊憩據點遊客人數_1100415.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27504",
            "2021年3月主要觀光遊憩據點遊客人數_1100615.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27503",
            "2021年4月主要觀光遊憩據點遊客人數_1100615.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27502",
            "2021年5月主要觀光遊憩據點遊客人數_1100715.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27501",
            "2021年6月主要觀光遊憩據點遊客人數_1100816.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27500",
            "2021年7月主要觀光遊憩據點遊客人數_1100915.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27499",
            "2021年8月主要觀光遊憩據點遊客人數_1101015.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27498",
            "2021年9月主要觀光遊憩據點遊客人數_1101115.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27497",
            "2021年10月主要觀光遊憩據點遊客人數_1101215.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27496",
            "2021年11月主要觀光遊憩據點遊客人數_1110117.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27495",
            "2021年12月主要觀光遊憩據點遊客人數_1110205.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27494",
        }


class Tourism_2020(Tourism):
    def get_xlsx_link_dict(self):
        return {
            "2020年1月主要觀光遊憩據點遊客人數_1090311.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27517",
            "2020年2月主要觀光遊憩據點遊客人數_1090415.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27516",
            "2020年3月主要觀光遊憩據點遊客人數_1090514.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27515",
            "2020年4月主要觀光遊憩據點遊客人數_1090610.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27514",
            "2020年5月主要觀光遊憩據點遊客人數_1090715.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27513",
            "2020年6月主要觀光遊憩據點遊客人數_1090812.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27512",
            "2020年7月主要觀光遊憩據點遊客人數_1090915.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27511",
            "2020年8月主要觀光遊憩據點遊客人數_1091015.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27510",
            "2020年9月主要觀光遊憩據點遊客人數_1091116.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27509",
            "2020年10月主要觀光遊憩據點遊客人數_1091215.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27508",
            "2020年11月主要觀光遊憩據點遊客人數_1100115.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27507",
            "2020年12月主要觀光遊憩據點遊客人數_1100217.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27506",
        }

    def xlsx_to_df(self, xlsx_path):
        # 跳過標頭
        df = pd.read_excel(xlsx_path, skiprows=1)
        return df

    def process_df(self, df, year_month):
        # 重新命名部份欄位名稱
        df = df.rename(
            columns={
                "類型": "類型",
                "觀 光 遊 憩 區\nScenic Spots": "觀光遊憩區",
                "縣 市 別\nCity/ County": "縣市",
                f"{int(year_month.split('-')[0])-1911}年{int(year_month.split('-')[1])}月\n遊客人次": "遊客人次",
                "上年同月\n遊客人次": "去年同月遊客人次",
                "成長率\n(%)": "成長率",
                "備    註 Endorse\n(計算遊客人數之方法或其他)": "遊客人次計算方式",
            }
        )

        # 刪除英文
        cols = ["類型", "觀光遊憩區", "縣市"]
        df[cols] = df[cols].map(lambda x: x.split("\n")[0] if isinstance(x, str) else x)
        df["縣市"] = df["縣市"].map(
            lambda x: x.split(" ")[0] if isinstance(x, str) else x
        )

        # 新增「觀光風景區」欄位
        del_idx = []

        for idx, row in df.iterrows():
            if row["類型"] not in ["國家公園", "國家級風景特定區"] and not pd.isna(
                row["類型"]
            ):
                break

            # 針對個別資料處理
            if row["觀光遊憩區"] == "野柳海洋世界":
                df.at[idx, "類型"] = "國家級風景特定區"
                df.at[idx, "觀光風景區"] = "野柳海洋世界"
                park = "野柳海洋世界"
                continue

            if pd.isna(row["縣市"]):
                park = row["觀光遊憩區"]
                del_idx.append(idx)
                continue

            else:
                df.at[idx, "觀光風景區"] = park

        df = df.map(lambda x: x.replace("  ", "") if isinstance(x, str) else x)
        df = df.drop(del_idx, axis=0).reset_index(drop=True)

        # 處理nan
        df = df.replace("nan", "")

        # 刪除tab
        df = df.replace("\t", "", regex=True)

        # 刪除資料來源備註
        df = df[:-6]

        # 新增遊客人次計算類型欄位
        df["遊客人次計算類型"] = df["遊客人次計算方式"].map(
            lambda x: self.get_count_type(x) if isinstance(x, str) else ""
        )

        # 替換成長率的"-"為空值
        df["成長率"] = df["成長率"].replace("-", "")

        # 新增年月欄位
        df["年月"] = year_month

        # 排序欄位
        df = df.reindex(columns=self.columns)

        return df


class Tourism_2019(Tourism):
    def get_xlsx_link_dict(self):
        return {
            "2019年1月主要觀光遊憩據點遊客人數_1080315.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27529",
            "2019年2月主要觀光遊憩據點遊客人數_1080415.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27528",
            "2019年3月主要觀光遊憩據點遊客人數_1080515.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27527",
            "2019年4月主要觀光遊憩據點遊客人數_1080617.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27526",
            "2019年5月主要觀光遊憩據點遊客人數_1080712.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=17662",
            "2019年6月主要觀光遊憩據點遊客人數_1080815.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=17661",
            "2019年7月主要觀光遊憩據點遊客人數_1080916.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=17660",
            "2019年8月主要觀光遊憩據點遊客人數_1081015.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=17659",
            "2019年9月主要觀光遊憩據點遊客人數_1081115.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=17658",
            "2019年10月主要觀光遊憩據點遊客人數_1081216.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=17657",
            "2019年11月主要觀光遊憩據點遊客人數_1090117.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=17656",
            "2019年12月主要觀光遊憩據點遊客人數_1090215.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27518",
        }

    def process_df(self, df, year_month):
        # 重新命名部份欄位名稱
        df.columns.values[1] = "觀光遊憩區"
        df.columns.values[2] = "縣市"
        df.columns.values[3] = "遊客人次"
        df.columns.values[4] = "去年同月遊客人次"
        df.columns.values[5] = "成長率"
        df.columns.values[6] = "遊客人次計算方式"

        # 刪除英文
        cols = ["類型", "觀光遊憩區", "縣市"]
        df[cols] = df[cols].map(
            lambda x: x.split("\n")[0].split(" ")[0] if isinstance(x, str) else x
        )

        # 新增「觀光風景區」欄位
        del_idx = []

        for idx, row in df.iterrows():
            if row["類型"] not in ["國家公園", "國家級風景特定區"] and not pd.isna(
                row["類型"]
            ):
                break

            if pd.isna(row["縣市"]):
                park = row["觀光遊憩區"]
                del_idx.append(idx)
                continue

            else:
                df.at[idx, "觀光風景區"] = park

        df = df.map(lambda x: x.replace("  ", "") if isinstance(x, str) else x)
        df = df.drop(del_idx, axis=0).reset_index(drop=True)

        df = super().process_df(df, year_month)
        df = df[:-5]
        return df


class Tourism_2018(Tourism):
    def get_xlsx_link_dict(self):
        return {
            "2018年1月主要觀光遊憩據點遊客人數_1070326.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27545",
            "2018年2月主要觀光遊憩據點遊客人數_1070423.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27544",
            "2018年3月主要觀光遊憩據點遊客人數_1070504.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27543",
            "2018年4月主要觀光遊憩據點遊客人數_1070806.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27541",
            "2018年5月主要觀光遊憩據點遊客人數_1070806.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27540",
            "2018年6月主要觀光遊憩據點遊客人數_1070814.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27539",
            "2018年7月主要觀光遊憩據點遊客人數_1071025.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27538",
            "2018年8月主要觀光遊憩據點遊客人數_1071025.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27536",
            "2018年9月主要觀光遊憩據點遊客人數_1071126.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27535",
            "2018年10月主要觀光遊憩據點遊客人數_1071225.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27533",
            "2018年11月主要觀光遊憩據點遊客人數_1080125.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27532",
            "2018年12月主要觀光遊憩據點遊客人數_1080225.xlsx": "https://admin.taiwan.net.tw/fapi/AttFile?type=AttFile&id=27531",
        }

    def xlsx_to_df(self, xlsx_path):
        # 跳過標頭
        if "11月" in xlsx_path or "12月" in xlsx_path:
            df = pd.read_excel(xlsx_path, skiprows=2)

        else:
            df = pd.read_excel(xlsx_path, skiprows=1)

        return df

    def process_df(self, df, year_month):
        # 重新命名部份欄位名稱
        df.columns.values[0] = "類型"
        df.columns.values[1] = "觀光遊憩區"
        df.columns.values[2] = "縣市"
        df.columns.values[3] = "遊客人次"
        df.columns.values[4] = "去年同月遊客人次"
        df.columns.values[5] = "成長率"
        df.columns.values[6] = "遊客人次計算方式"

        # 刪除英文
        cols = ["類型", "觀光遊憩區", "縣市"]
        df[cols] = df[cols].map(
            lambda x: x.split("\n")[0].split(" ")[0] if isinstance(x, str) else x
        )

        # 新增「觀光風景區」欄位
        del_idx = []
        category = "國家風景區"

        for idx, row in df.iterrows():
            # 填入類型欄位
            if pd.isna(row["類型"]):
                df.at[idx, "類型"] = category

            else:
                category = row["類型"]

            # 只有國家公園和國家風景區有觀光風景區
            if row["類型"] == "公營遊憩區":
                park = None

            if pd.isna(row["縣市"]):
                park = row["觀光遊憩區"]
                del_idx.append(idx)
                continue

            # 填入觀光風景區欄位
            else:
                df.at[idx, "觀光風景區"] = park

        df = df.map(lambda x: x.replace("  ", "") if isinstance(x, str) else x)
        df = df.drop(del_idx, axis=0).reset_index(drop=True)

        # 處理nan
        df = df.replace("nan", "")

        # 刪除tab
        df = df.replace("\t", "", regex=True)

        # 新增遊客人次計算類型欄位
        df["遊客人次計算類型"] = df["遊客人次計算方式"].map(
            lambda x: self.get_count_type(x) if isinstance(x, str) else ""
        )

        # 替換成長率的"-"為空值
        df["成長率"] = df["成長率"].replace("-", "")

        # 新增年月欄位
        df["年月"] = year_month

        # 排序欄位
        df = df.reindex(columns=self.columns)

        # 刪除"◎"符號
        df = df.map(lambda x: x.replace("◎", "") if isinstance(x, str) else x)

        return df


if __name__ == "__main__":
    tourism = Tourism_2018(data_dir="data")
    # tourism = Tourism_2019(data_dir="data")
    # tourism = Tourism_2020(data_dir="data")
    # tourism = Tourism_2021(data_dir="data")
    # tourism = Tourism_2022(data_dir="data")
    # tourism = Tourism_2023(data_dir="data")
    # tourism = Tourism(data_dir="data", checkpoint_path="checkpoint.txt")

    # 取得連結
    xlsx_link_dict = tourism.get_xlsx_link_dict()

    # 下載xlsx
    tourism.download_xlsx(xlsx_link_dict)

    # 轉csv
    tourism.xlsx_to_csv()
