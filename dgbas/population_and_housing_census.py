import requests
import os
import pandas as pd


class PopulationAndHousingCensus:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.base_url = "https://ws.dgbas.gov.tw/001/Upload/463/relfile/11065"

        self.county = {
            "新北市": "230886",
            "臺北市": "230887",
            "桃園市": "230883",
            "基隆市": "230888",
            "新竹市": "230889",
            "宜蘭縣": "230890",
            "新竹縣": "230892",
            "臺中市": "230893",
            "苗栗縣": "230894",
            "彰化縣": "230895",
            "南投縣": "230896",
            "雲林縣": "230897",
            "臺南市": "230898",
            "高雄市": "230899",
            "嘉義市": "230900",
            "嘉義縣": "230901",
            "屏東縣": "230902",
            "澎湖縣": "230903",
            "臺東縣": "230904",
            "花蓮縣": "230905",
            "金門縣": "230906",
            "連江縣": "230907",
        }

        self.table = {
            # "1_常住人口數及人口密度": "t001",
            # "2_常住人口之性比例（不含移工）": "t002",
            "3_常住人口之年齡結構": "t003",
            "4_常住人口之年齡結構（不含移工）": "t004",
            "5_１５歲以上常住人口之婚姻狀況": "t005",
            "6_６歲以上本國籍常住人口使用語言情形": "t006",
            "7_６歲以上本國籍常住人口兒時最早學會語言情形": "t007",
            "8_６歲以上本國籍常住人口之父、母最常使用語言情形": "t008",
            "9_６至３４歲常住人口之在學情形": "t009",
            "10_１５歲以上常住人口之教育程度": "t010",
            "11_１５歲以上常住人口之最高學歷": "t011",
            "12_１５歲以上民間常住人口之工作狀況": "t012",
            "13_１５歲以上民間常住人口有工作者之職業": "t013",
            "14_１５歲以上民間常住人口有工作者之工作地與經常居住地概況": "t014",
            "15_１５歲以上跨鄉鎮市區通勤工作人口之年齡結構": "t015",
            "16_１５歲以上跨鄉鎮市區通勤工作人口之教育程度": "t016",
            "17_６歲以上在學人口之在學地與經常居住地概況": "t017",
            "18_６歲以上跨鄉鎮市區通學人口之年齡結構": "t018",
            "19_６歲以上跨鄉鎮市區通學人口之教育程度": "t019",
            "20_５歲以上常住人口遷徙情形": "t020",
            "21_學齡前兒童幼托及照顧概況": "t021",
            "22_常住人口長期照顧需求者概況": "t022",
            "23_６５歲以上常住人口長期照顧需求者概況": "t023",
            "24_１５歲以上常住人口與現有子女之居住概況": "t024",
            "25_６５歲以上常住人口與現有子女之居住概況": "t025",
            "26_６５歲以上常住人口之居住概況": "t026",
            "27_身心障礙常住人口之性別及年齡結構": "t027",
            "28_原住民族常住人口之性別及年齡結構": "t028",
            "29_常住人口之國籍分布": "t029",
            "30_外國籍與大陸港澳配偶常住人口數": "t030",
            "31_住戶數、常住人口數及平均每戶人口數": "t031",
            "32_普通住戶之戶內人口數": "t032",
            "33_普通住戶之家戶型態": "t033",
            "34_普通住戶之住宅所有權屬": "t034",
            "35_普通住戶住進現宅時間": "t035",
            "36_普通住戶在家上網情形": "t036",
            "37_住宅使用情形": "t037",
            "38_住宅之竣工年份": "t038",
            "39_住宅之樓地板面積": "t039",
            "40_住宅之建築類型": "t040",
            "41_空閒住宅之竣工年份": "t041",
            "42_空閒住宅之樓地板面積": "t042",
            "43_有人經常居住住宅之使用情形": "t043",
            "44_有人經常居住住宅之居住人數": "t044",
            "45_有人經常居住住宅之房廳數": "t045",
            "46_有人經常居住住宅之平均每人使用房廳數及衛浴套數": "t046",
        }

        self.column = {
            "1_常住人口數及人口密度": [
                "年月",
                "縣市",
                "鄉鎮市區",
                "常住人口數(人)",
                "土地面積（平方公里）",
                "人口密度（人 / 平方公里）",
            ],
            "2_常住人口之性比例（不含移工）": [
                "年月",
                "縣市",
                "鄉鎮市區",
                "常住人口數(人)",
                "性比例(女=100)",
            ],
            "3_常住人口之年齡結構": [
                "年月",
                "縣市",
                "鄉鎮市區",
                "總計",
                "未滿１５歲",
                "１５－２４歲",
                "２５－３４歲",
                "３５－４４歲",
                "４５－５４歲",
                "５５－６４歲",
                "６５歲以上",
                "平均年齡（歲）",
            ],
            "4_常住人口之年齡結構（不含移工）": "t004",
            "5_１５歲以上常住人口之婚姻狀況": "t005",
            "6_６歲以上本國籍常住人口使用語言情形": "t006",
            "7_６歲以上本國籍常住人口兒時最早學會語言情形": "t007",
            "8_６歲以上本國籍常住人口之父、母最常使用語言情形": "t008",
            "9_６至３４歲常住人口之在學情形": "t009",
            "10_１５歲以上常住人口之教育程度": "t010",
            "11_１５歲以上常住人口之最高學歷": "t011",
            "12_１５歲以上民間常住人口之工作狀況": "t012",
            "13_１５歲以上民間常住人口有工作者之職業": "t013",
            "14_１５歲以上民間常住人口有工作者之工作地與經常居住地概況": "t014",
            "15_１５歲以上跨鄉鎮市區通勤工作人口之年齡結構": "t015",
            "16_１５歲以上跨鄉鎮市區通勤工作人口之教育程度": "t016",
            "17_６歲以上在學人口之在學地與經常居住地概況": "t017",
            "18_６歲以上跨鄉鎮市區通學人口之年齡結構": "t018",
            "19_６歲以上跨鄉鎮市區通學人口之教育程度": "t019",
            "20_５歲以上常住人口遷徙情形": "t020",
            "21_學齡前兒童幼托及照顧概況": "t021",
            "22_常住人口長期照顧需求者概況": "t022",
            "23_６５歲以上常住人口長期照顧需求者概況": "t023",
            "24_１５歲以上常住人口與現有子女之居住概況": "t024",
            "25_６５歲以上常住人口與現有子女之居住概況": "t025",
            "26_６５歲以上常住人口之居住概況": "t026",
            "27_身心障礙常住人口之性別及年齡結構": "t027",
            "28_原住民族常住人口之性別及年齡結構": "t028",
            "29_常住人口之國籍分布": "t029",
            "30_外國籍與大陸港澳配偶常住人口數": "t030",
            "31_住戶數、常住人口數及平均每戶人口數": "t031",
            "32_普通住戶之戶內人口數": "t032",
            "33_普通住戶之家戶型態": "t033",
            "34_普通住戶之住宅所有權屬": "t034",
            "35_普通住戶住進現宅時間": "t035",
            "36_普通住戶在家上網情形": "t036",
            "37_住宅使用情形": "t037",
            "38_住宅之竣工年份": "t038",
            "39_住宅之樓地板面積": "t039",
            "40_住宅之建築類型": "t040",
            "41_空閒住宅之竣工年份": "t041",
            "42_空閒住宅之樓地板面積": "t042",
            "43_有人經常居住住宅之使用情形": "t043",
            "44_有人經常居住住宅之居住人數": "t044",
            "45_有人經常居住住宅之房廳數": "t045",
            "46_有人經常居住住宅之平均每人使用房廳數及衛浴套數": "t046",
        }

    def save_xlsx(self, table_name, county_name):
        url = (
            f"{self.base_url}/{self.county[county_name]}/{self.table[table_name]}.xlsx"
        )
        print("URL:", url)

        response = requests.get(url)

        if response.status_code == 200:
            dir_path = os.path.join(self.data_dir, table_name, "xlsx")
            os.makedirs(dir_path, exist_ok=True)
            xlsx_path = os.path.join(dir_path, f"{table_name}_{county_name}.xlsx")

            with open(xlsx_path, "wb") as f:
                f.write(response.content)
                print(f"Saved {xlsx_path}.")

        else:
            raise Exception(
                f"Failed to download file. Status code: {response.status_code}"
            )

    def xlsx_to_df(self, table_name, county_name):
        xlsx_name = f"{table_name}_{county_name}.xlsx"
        xlsx_path = os.path.join(self.data_dir, table_name, "xlsx", xlsx_name)

        if table_name == "1_常住人口數及人口密度":
            df = pd.read_excel(xlsx_path, skiprows=18)

            df.columns = [
                "col0",
                "鄉鎮市區",
                "常住人口數(人)",
                "常住人口數-男",
                "常住人口數-女",
                "土地面積（平方公里）",
                "人口密度（人 / 平方公里）",
                "年月",
                "縣市",
                "col9",
                "col10",
                "col11",
                "col12",
            ]

            df = df.dropna(subset=["常住人口數(人)"])
            df = df.replace("　", "", regex=True)

        elif table_name == "2_常住人口之性比例（不含移工）":
            df = pd.read_excel(xlsx_path, skiprows=18)

            df.columns = [
                "col0",
                "鄉鎮市區",
                "常住人口數(人)",
                "常住人口數-男",
                "常住人口數-女",
                "性比例(女=100)",
                "年月",
                "縣市",
                "col8",
                "col9",
                "col10",
                "col11",
            ]

            df = df.dropna(subset=["常住人口數(人)"])
            df = df.replace("　", "", regex=True)

        elif table_name == "3_常住人口之年齡結構":
            df = pd.read_excel(xlsx_path, skiprows=16)

            df.columns = [
                "col0",
                "鄉鎮市區",
                "總計",
                "未滿１５歲",
                "１５－２４歲",
                "２５－３４歲",
                "３５－４４歲",
                "col7",
                "４５－５４歲",
                "５５－６４歲",
                "６５歲以上",
                "平均年齡（歲）",
                "col12",
                "年月",
                "縣市",
                "col15",
                "col16",
                "col17",
            ]

            df = df.replace("　", "", regex=True)

        else:
            raise

        # 填入年月與縣市
        df["年月"] = "109-11"
        df["縣市"] = county_name

        # 選擇需要的欄位
        df = df[self.column[table_name]]

        return df

    def df_to_csv(self, df, table_name, county_name):
        xlsx_dir = os.path.join(self.data_dir, table_name, "xlsx")
        xlsx_name = f"{table_name}_{county_name}.xlsx"
        xlsx_path = os.path.join(xlsx_dir, xlsx_name)

        csv_dir = os.path.join(self.data_dir, table_name)
        csv_name = f"{table_name}_{county_name}.csv"
        csv_path = os.path.join(csv_dir, csv_name)

        # 儲存csv
        df.to_csv(csv_path, index=False)
        print(f"Saved {csv_path}.")

        # # 壓縮xlsx
        # zip_path = xlsx_path.replace(".xlsx", ".zip")
        # os.system(f"zip {zip_path} {xlsx_path}")
        # print(f"Saved {zip_path}.")

        # 將xlsx移到converted資料夾
        converted_dir = os.path.join(self.data_dir, table_name, "xlsx", "converted")
        os.makedirs(converted_dir, exist_ok=True)
        os.system(f"mv {xlsx_path} {converted_dir}")
        print(f"Moved {xlsx_path} to {converted_dir}.")

    def save_all_data(self):
        for table_name in self.table:
            for county_name in self.county:
                self.save_xlsx(table_name, county_name)
                df = self.xlsx_to_df(table_name, county_name)
                self.df_to_csv(df, table_name, county_name)
                exit()


if __name__ == "__main__":
    population_and_housing_census = PopulationAndHousingCensus(data_dir="data")
    population_and_housing_census.save_all_data()
