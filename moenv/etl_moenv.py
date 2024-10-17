import os
import requests
import json
from datetime import datetime
import pandas as pd


class ETL_moenv:
    def __init__(self, code, api_key, data_dir_path):
        self.prefix = "moenv"
        self.code = code
        self.api_key = api_key
        self.url = self.get_url()
        self.data_dir_path = data_dir_path
        os.makedirs(os.path.join(self.data_dir_path, "json"), exist_ok=True)

    def get_url(self):
        base_url = "https://data.moenv.gov.tw/api/v2"
        return f"{base_url}/{self.code}?api_key={self.api_key}"

    def get_data_path(self, data_format):
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = f"{self.prefix}_{self.code}_{time}.{data_format}"

        if data_format == "csv":
            return os.path.join(self.data_dir_path, file_name)

        elif data_format == "json":
            return os.path.join(self.data_dir_path, "json", file_name)

    def save_json(self):
        print("URL:", self.url)
        response = requests.get(self.url)

        if response.status_code == 200:
            json_data = response.json()
            json_path = self.get_data_path("json")

            with open(json_path, "w") as f:
                json.dump(json_data, f)

            print(f"json saved: {json_path}")
            return json_data

        else:
            print("Error:", response.status_code)


    def save_csv(self, json_data):
        df = pd.DataFrame(json_data["records"])
        csv_path = self.get_data_path("csv")
        df.to_csv(csv_path, index=False)
        print(f"csv saved: {csv_path}")


if __name__ == "__main__":
    etl_moenv = ETL_moenv(code="aqx_p_04", api_key="", data_dir_path="data")
    json_data = etl_moenv.save_json()
    etl_moenv.save_csv(json_data)
