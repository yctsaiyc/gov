import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import os
import time


class DGBAS:
    def __init__(self, data_dir="data", checkpoint_path="checkpoint.txt"):

        self.base_url = "https://winsta.dgbas.gov.tw/網頁資料查詢/ShowQuery.aspx"

        self.checkpoint_path = checkpoint_path

        self.dir_path = {
            "土地人口": f"{data_dir}/1_population",
            "勞動就業": f"{data_dir}/2_labor",
            "工商經濟": f"{data_dir}/3_economy",
            "財政概況": f"{data_dir}/4_finance",
            "營建管理": f"{data_dir}/5_construction",
            "觀光休閒": f"{data_dir}/6_tourism",
            "交通運輸": f"{data_dir}/7_transportation",
            "公共安全": f"{data_dir}/8_public_safety",
            "社會治安": f"{data_dir}/9_social_security",
            "環境保護": f"{data_dir}/10_environment",
            "交通安全": f"{data_dir}/11_traffic",
        }

        self.schema = {
            "土地人口": [
                "年月",
                "縣市",
                "戶數",
                "戶量",
                "人口密度",
                "人口數",
                "男性人口數",
                "女性人口數",
                "性比例",
                "幼年(0-14歲)人口數",
                "青壯年(15-64歲)人口數",
                "老年(65歲以上)人口數",
                "老化指數",
                "扶養比",
                "原住民人口數",
                "人口增加率",
                "自然增加率",
                "粗出生率",
                "粗死亡率",
                "出生人數",
                "死亡人數",
                "社會增加率",
                "遷入人數",
                "遷出人數",
                "粗結婚率",
                "結婚對數",
                "粗離婚率",
                "離婚對數",
            ],
            "勞動就業": [
                "年月",
                "縣市",
                "勞資爭議件數",
                "勞工職業災害保險給付人次",
                "產業及社福移工人數",
            ],
            "工商經濟": [
                "年月",
                "縣市",
                "公司解散、撤銷及廢止家數",
                "現有公司登記家數",
                "新設立公司登記家數",
                "現有公司登記資本額",
                "商業登記歇業家數",
                "現有商業登記家數",
                "新設立商業登記家數",
                "現有商業登記資本額",
            ],
            "財政概況": [
                "年月",
                "縣市",
                "營利事業家數",
                "營利事業銷售額",
                "賦稅實徵淨額",
                "賦稅實徵淨額-所得稅",
                "賦稅實徵淨額-營業稅",
            ],
            "營建管理": [
                "年月",
                "縣市",
                "核發建築物建造執照-件數",
                "核發建築物建造執照-總樓地板面積",
                "核發建築物建造執照-工程造價",
                "核發建築物建造執照-棟數",
                "核發建築物使用執照統計-件數",
                "核發建築物使用執照統計-總樓地板面積",
                "核發建築物使用執照統計-工程造價",
                "核發建築物使用執照統計-棟數",
                "核發建築物拆除執照統計-件數",
                "核發建築物拆除執照統計-宅數",
                "核發建築物拆除執照統計-總樓地板面積",
                "核發建築物拆除執照統計-棟數",
                "建築物開工統計-件數",
                "建築物開工統計-總樓地板面積",
                "建築物開工統計-工程造價",
                "建築物開工統計-棟數",
            ],
            "觀光休閒": [
                "年月",
                "縣市",
                "觀光旅館家數",
                "觀光旅館房間數",
                "觀光旅館住用率",
            ],
            "交通運輸": [
                "年月",
                "縣市",
                "領有駕駛執照人數－汽車",
                "領有駕駛執照人數－機車",
                "機動車輛登記數－汽車",
                "機動車輛登記數－機車",
            ],
            "公共安全": [
                "年月",
                "縣市",
                "火災發生次數",
                "火災死亡人數",
                "火災受傷人數",
                "火災財物損失估值",
                "火災起火原因次數-電氣因素",
                "火災起火原因次數-縱火",
                "火災起火原因次數-菸蒂",
                "救護出勤次數",
                "急救送醫人次",
                "急救送醫人次-非創傷類",
                "急救送醫人次-創傷類",
            ],
            "社會治安": [
                "年月",
                "縣市",
                "刑事案件發生率",
                "刑事案件發生件數",
                "刑事案件破獲件數",
                "刑事案件破獲率",
                "檢肅毒品統計-毒品數量",
                "竊盜案件發生數",
                "竊盜案件破獲數",
                "暴力犯罪發生率",
                "刑事案件少年嫌疑犯占總嫌疑犯比率",
                "刑事案件少年嫌疑犯人數",
                "暴力犯罪破獲率",
                "處理經濟案件-件數",
                "處理經濟案件-金額",
                "妨害善良風俗人數",
            ],
            "環境保護": [
                "年月",
                "縣市",
                "空氣中總懸浮微粒濃度",
                "懸浮微粒濃度",
                "平均每月落塵量",
                "平均每人每日一般廢棄物產生量",
                "一般廢棄物產生量",
                "執行機關資源回收量",
                "公害陳情受理件數",
                "空氣中臭氧濃度",
            ],
            "交通安全": [
                "年月",
                "縣市",
                "道路交通事故-事故件數",
                "道路交通事故-死亡人數",
                "道路交通事故-受傷人數",
            ],
        }

        self.map = {
            "county": {
                "010000009007": "連江縣",
                "010000009020": "金門縣",
                "010000010002": "宜蘭縣",
                "010000010004": "新竹縣",
                "010000010005": "苗栗縣",
                "010000010007": "彰化縣",
                "010000010008": "南投縣",
                "010000010009": "雲林縣",
                "010000010010": "嘉義縣",
                "010000010013": "屏東縣",
                "010000010014": "臺東縣",
                "010000010015": "花蓮縣",
                "010000010016": "澎湖縣",
                "010000010017": "基隆市",
                "010000010018": "新竹市",
                "010000010020": "嘉義市",
                "010000063000": "臺北市",
                "010000064000": "高雄市",
                "010000065000": "新北市",
                "010000066000": "臺中市",
                "010000067000": "臺南市",
                "010000068000": "桃園市",
            },
            "column": {
                "53": "戶數",
                "54": "戶量",
                "55": "人口密度",
                "56": "人口數",
                "57": "男性人口數",
                "58": "女性人口數",
                "59": "性比例",
                "60": "幼年(0-14歲)人口數",
                "61": "青壯年(15-64歲)人口數",
                "62": "老年(65歲以上)人口數",
                "63": "老化指數",
                "64": "扶養比",
                "65": "原住民人口數",
                "66": "人口增加率",
                "67": "自然增加率",
                "68": "粗出生率",
                "69": "粗死亡率",
                "70": "出生人數",
                "71": "死亡人數",
                "72": "社會增加率",
                "73": "遷入人數",
                "74": "遷出人數",
                "75": "粗結婚率",
                "76": "結婚對數",
                "77": "粗離婚率",
                "78": "離婚對數",
                "79": "勞資爭議件數",
                "81": "勞工職業災害保險給付人次",
                "82": "產業及社福移工人數",
                "83": "公司解散、撤銷及廢止家數",
                "84": "現有公司登記家數",
                "85": "新設立公司登記家數",
                "86": "現有公司登記資本額",
                "87": "商業登記歇業家數",
                "88": "現有商業登記家數",
                "89": "新設立商業登記家數",
                "90": "現有商業登記資本額",
                "91": "營利事業家數",
                "92": "營利事業銷售額",
                "93": "賦稅實徵淨額",
                "94": "賦稅實徵淨額-所得稅",
                "95": "賦稅實徵淨額-營業稅",
                "96": "核發建築物建造執照-件數",
                "97": "核發建築物建造執照-總樓地板面積",
                "98": "核發建築物建造執照-工程造價",
                "99": "核發建築物建造執照-棟數",
                "100": "核發建築物使用執照統計-件數",
                "101": "核發建築物使用執照統計-總樓地板面積",
                "102": "核發建築物使用執照統計-工程造價",
                "103": "核發建築物使用執照統計-棟數",
                "104": "核發建築物拆除執照統計-件數",
                "105": "核發建築物拆除執照統計-宅數",
                "106": "核發建築物拆除執照統計-總樓地板面積",
                "107": "核發建築物拆除執照統計-棟數",
                "108": "建築物開工統計-件數",
                "109": "建築物開工統計-總樓地板面積",
                "110": "建築物開工統計-工程造價",
                "111": "建築物開工統計-棟數",
                "113": "觀光旅館家數",
                "114": "觀光旅館房間數",
                "115": "觀光旅館住用率",
                "116": "領有駕駛執照人數－汽車",
                "117": "領有駕駛執照人數－機車",
                "118": "機動車輛登記數－汽車",
                "119": "機動車輛登記數－機車",
                "122": "火災發生次數",
                "123": "火災死亡人數",
                "124": "火災受傷人數",
                "125": "火災財物損失估值",
                "126": "火災起火原因次數-電氣因素",
                "127": "火災起火原因次數-縱火",
                "128": "火災起火原因次數-菸蒂",
                "129": "救護出勤次數",
                "130": "急救送醫人次",
                "131": "急救送醫人次-非創傷類",
                "132": "急救送醫人次-創傷類",
                "133": "刑事案件發生率",
                "134": "刑事案件發生件數",
                "135": "刑事案件破獲件數",
                "136": "刑事案件破獲率",
                "137": "檢肅毒品統計-毒品數量",
                "138": "竊盜案件發生數",
                "139": "竊盜案件破獲數",
                "140": "暴力犯罪發生率",
                "141": "刑事案件少年嫌疑犯占總嫌疑犯比率",
                "142": "刑事案件少年嫌疑犯人數",
                "143": "暴力犯罪破獲率",
                "144": "處理經濟案件-件數",
                "145": "處理經濟案件-金額",
                "146": "妨害善良風俗人數",
                "147": "空氣中總懸浮微粒濃度",
                "148": "懸浮微粒濃度",
                "149": "平均每月落塵量",
                "150": "平均每人每日一般廢棄物產生量",
                "151": "一般廢棄物產生量",
                "200": "執行機關資源回收量",
                "156": "公害陳情受理件數",
                "157": "空氣中臭氧濃度",
                "158": "道路交通事故-事故件數",
                "160": "道路交通事故-死亡人數",
                "161": "道路交通事故-受傷人數",
            },
        }

    def get_html(self, request_date):
        county_code = ";".join(
            str(county_code) for county_code in self.map["county"].keys()
        )

        column_code = ";".join(str(col_code) for col_code in self.map["column"].keys())

        url = (
            f"{self.base_url}?"
            "mode=2&"
            "period=M&"
            "axX=[Measures]&"
            "axY=[Date];[Place]&"
            f"axDate={request_date}&"
            "axCycle=M月&"
            f"axCode={county_code}&"
            f"axEffect={column_code}&"
            "goon=查詢"
        )

        print("\nURL:", url)

        try:
            response = requests.request("POST", url)

        except Exception as e:
            print(e)
            time.sleep(10)
            response = requests.request("POST", url)

        return response.text

    def html_to_json(self, html):
        soup = BeautifulSoup(html, "html.parser")

        # 取得資料字串
        body = soup.find("body")
        text = body.find("script", {"type": "text/javascript"}).text

        # 保留需要的部份
        text = text.split('"Values":')[1].split("};//")[0]

        # 轉成json
        return json.loads(text)

    def json_to_df(self, json_data):

        # 原：[Measures],[Date],[Place],Value
        df = pd.DataFrame(json_data)

        # 轉成：[Date],[Place],Measures1,Measures2,...
        df = df.pivot_table(
            index=["[Date]", "[Place]"],
            columns="[Measures]",
            values="Value",
            aggfunc="first",
        ).reset_index()

        # 改column name：年月,縣市,Measures1,Measures2,...
        df.rename(columns={"[Date]": "年月", "[Place]": "縣市"}, inplace=True)

        # 改column name：年月,縣市,項目1,項目2,...
        df.rename(columns=self.map["column"], inplace=True)

        # 縣市值代碼轉中文
        df["縣市"] = df["縣市"].replace(self.map["county"])

        return df

    def save_csv(self, request_date=None):

        # default: 上個月
        if not request_date:
            request_date = (
                (pd.Timestamp.now() - pd.DateOffset(months=1))
                .replace(day=1)
                .strftime("%Y/%m/01")
            )

        # 取得df
        html = self.get_html(request_date)
        json_data = self.html_to_json(html)
        df = self.json_to_df(json_data)

        # 歷史資料可能會有整個column都是"..."的情況，將其取代為空字串
        if request_date < "2024/01/01":
            df.replace({"...": ""}, inplace=True)

        # 確認資料是否完整（新資料）
        # if any((df[col] == "...").all() for col in df.columns):  # 某個column全是"..."
        if "..." in df.values:  # df裡有任意一格是"..."
            print(f"Data is not complete on {request_date}")
            return False

        # 年月字串（檔名用）
        year_month = request_date.split("/")[0] + request_date.split("/")[1]

        # 分成11個資料集
        for dataset in self.schema:

            # 路徑
            dir_path = self.dir_path[dataset]
            os.makedirs(dir_path, exist_ok=True)
            csv_path = os.path.join(dir_path, f"{dataset}_{year_month}.csv")

            # 取得資料集欄位
            columns = self.schema[dataset]

            # 存檔
            df[columns].to_csv(csv_path, index=False)
            print("Saved:", csv_path)

        return True

    def update_csv(self):

        # 取得已儲存的最新資料日期
        with open(self.checkpoint_path, "r") as f:
            request_date = f.read()

        # 轉成日期物件，計算下一個月
        request_date_obj = pd.Timestamp(request_date) + pd.DateOffset(months=1)

        # 從下一個月到本月
        while request_date_obj <= pd.Timestamp.now():

            # 轉回日期字串
            request_date = request_date_obj.strftime("%Y/%m/01")

            # 若成功儲存csv（資料是完整的）
            if self.save_csv(request_date):

                # 更新檢查點
                with open(self.checkpoint_path, "w") as f:
                    f.write(request_date)
                    print("Updated checkpoint:", request_date)

                # 下一個月
                request_date_obj = request_date_obj + pd.DateOffset(months=1)

            # 若未儲存代表資料更新不完全
            else:
                break


if __name__ == "__main__":
    dgbas = DGBAS(data_dir="data", checkpoint_path="checkpoint.txt")
    dgbas.update_csv()
