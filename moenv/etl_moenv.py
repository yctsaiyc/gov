import os
import requests
import json
from datetime import datetime
import pandas as pd


class ETL_moenv:
    def __init__(
        self, code, api_key, data_dir_path="data", checkpoint_path="checkpoint.json"
    ):
        self.prefix = "moenv"
        self.code = code
        self.api_key = api_key
        self.url = self.get_url()
        self.data_dir_path = data_dir_path
        os.makedirs(os.path.join(self.data_dir_path, "json"), exist_ok=True)
        self.checkpoint_path = checkpoint_path

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

    def get_checkpoint(self):
        with open(self.checkpoint_path, "r") as f:
            return json.load(f)

    def update_checkpoint(self, newest_data):
        newest_data = {
            "siteid": newest_data["siteid"],
            "datacreationdate": newest_data["datacreationdate"],
        }

        with open(self.checkpoint_path, "w") as f:
            json.dump(newest_data, f, ensure_ascii=False, indent=4)

    def save_json(self):
        print("URL:", self.url)
        response = requests.get(self.url)

        if response.status_code == 200:
            # 1. get json data
            json_data = response.json()["records"]
            json_path = self.get_data_path("json")
            checkpoint = self.get_checkpoint()

            # 2. drop data before checkpoint
            filtered_data = []
            for item in json_data:
                if (
                    item["siteid"] == checkpoint["siteid"]
                    and item["datacreationdate"] == checkpoint["datacreationdate"]
                ):
                    break
                filtered_data.append(item)

            # 3. save filtered json data
            if len(filtered_data) > 0:
                with open(json_path, "w") as f:
                    json.dump(filtered_data, f, ensure_ascii=False, indent=4)

                print(f"json saved: {json_path}")

                # 4. update checkpoint
                self.update_checkpoint(filtered_data[0])

                return filtered_data

            else:
                print("No new data")
                exit()

        else:
            print("Error:", response.status_code)

    def save_csv(self, json_data):
        df = pd.DataFrame(json_data)
        csv_path = self.get_data_path("csv")
        df.to_csv(csv_path, index=False)
        print(f"csv saved: {csv_path}")


if __name__ == "__main__":
    etl_moenv = ETL_moenv(
        code="aqx_p_04",
        api_key="",
        data_dir_path="data",
        checkpoint_path="checkpoint.json",
    )
    json_data = etl_moenv.save_json()
    etl_moenv.save_csv(json_data)
