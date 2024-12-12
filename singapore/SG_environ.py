import sys
import os
import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
import argparse

# from airflow.exceptions import AirflowFailException


class SGEnviron:
    def __init__(self):
        self.dataset_name = None
        self.api_name = None
        self.set_attrs()

        self.api_url = f"https://api.data.gov.sg/v1/environment/{self.api_name}"
        self.data_dir_path = os.path.join("data/", self.dataset_name)

    def set_attrs(self):
        raise NotImplementedError("Subclasses must implement this method")

    def get_json(self, date_str=None):
        if date_str:
            url = f"{self.api_url}?date={date_str}"

        else:
            url = self.api_url

        print(f"URL: {url}")

        response = requests.get(url)

        if response.status_code == 200:
            return response.json()

        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            raise

    def save_json(self, json_data):
        now = datetime.now(timezone(timedelta(hours=8))).hour

        json_dir_path = os.path.join(self.data_dir_path, "json")
        os.makedirs(json_dir_path, exist_ok=True)

        json_path = os.path.join(
            json_dir_path,
            f"{self.dataset_name}_{now.year}{now.month:02}{now.day:02}_{now.hour:02}.json",
        )

        with open(json_path, "w") as f:
            json.dump(json_path, f)
            print(f"Saved: {json_path}.")

    def json_to_df(self, json_data):
        raise NotImplementedError("Subclasses must implement this method")

    def save_data(self):
        now = datetime.now(timezone(timedelta(hours=8)))

        json_data = self.get_json()
        # self.save_json(json_data)

        df = self.json_to_df(json_data)

        if not df.empty:
            os.makedirs(self.data_dir_path, exist_ok=True)

            csv_path = os.path.join(
                self.data_dir_path,
                f"{self.dataset_name}_{now.year}{now.month:02}{now.day:02}_{now.hour:02}.csv",
            )

            df.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}.")

        else:
            print(f"Data for is empty.")

    def save_history_data(self, start_date_str, end_date_str):
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        while start_date <= end_date:
            date_str = start_date.strftime("%Y-%m-%d")

            json_data = self.get_json(date_str)
            # self.save_json(json_data)

            df = self.json_to_df(json_data)

            if not df.empty:
                os.makedirs(self.data_dir_path, exist_ok=True)

                csv_path = os.path.join(
                    self.data_dir_path,
                    f"{self.dataset_name}_{date_str.replace('-','')}.csv",
                )

                df.to_csv(csv_path, index=False)
                print(f"Saved: {csv_path}.")

            else:
                print(f"Data for is empty.")

            start_date += timedelta(days=1)

    def process_datetime(self, datetime_str):
        return datetime_str.replace("T", " ").replace("+08:00", "")


class SGEnviron24HourWeatherForecast(SGEnviron):
    def set_attrs(self):
        self.dataset_name = "24-hour-weather-forecast"
        self.api_name = "24-hour-weather-forecast"

    def json_to_df(self, json_data):
        columns = [
            # "Update Timestamp",
            "Report Time",
            # "Forecast Start Time",
            # "Forecast End Time",
            "Target Start Time",
            "Target End Time",
            "Weather Forecast (West)",
            "Weather Forecast (East)",
            "Weather Forecast (Central)",
            "Weather Forecast (South)",
            "Weather Forecast (North)",
            "Weather Forecast (General)",
            "Relative Humidity Low (General)",
            "Relative Humidity High (General)",
            "Temperature Low (General)",
            "Temperature High (General)",
            "Wind Speed Low (General)",
            "Wind Speed High (General)",
            "Wind Direction (General)",
        ]

        df = pd.DataFrame(columns=columns)

        for item in json_data["items"]:
            for period in item["periods"]:
                df.loc[len(df)] = [
                    # item["update_timestamp"],
                    self.process_datetime(item["timestamp"]),
                    # item["valid_period"]["start"],
                    # item["valid_period"]["end"],
                    self.process_datetime(period["time"]["start"]),
                    self.process_datetime(period["time"]["end"]),
                    period["regions"]["west"],
                    period["regions"]["east"],
                    period["regions"]["central"],
                    period["regions"]["south"],
                    period["regions"]["north"],
                    item["general"]["forecast"],
                    item["general"]["relative_humidity"]["low"],
                    item["general"]["relative_humidity"]["high"],
                    item["general"]["temperature"]["low"],
                    item["general"]["temperature"]["high"],
                    item["general"]["wind"]["speed"]["low"],
                    item["general"]["wind"]["speed"]["high"],
                    item["general"]["wind"]["direction"],
                ]

        return df


class SGEnviron2HourWeatherForecast(SGEnviron):
    def set_attrs(self):
        self.dataset_name = "2-hour-weather-forecast"
        self.api_name = "2-hour-weather-forecast"

    def json_to_df(self, json_data):
        columns = [
            # "Update Timestamp",
            "Report Time",
            "Target Start Time",
            "Target End Time",
            "Area",
            "Forecast",
            "Longitude",
            "Latitude",
            "WKT",
        ]

        df = pd.DataFrame(columns=columns)

        area_metadata = {
            i["name"]: i["label_location"] for i in json_data["area_metadata"]
        }

        for item in json_data["items"]:
            for forecast in item["forecasts"]:
                df.loc[len(df)] = [
                    # item["update_timestamp"],
                    self.process_datetime(item["timestamp"]),
                    self.process_datetime(item["valid_period"]["start"]),
                    self.process_datetime(item["valid_period"]["end"]),
                    forecast["area"],
                    forecast["forecast"],
                    area_metadata[forecast["area"]]["longitude"],
                    area_metadata[forecast["area"]]["latitude"],
                    f"POINT({area_metadata[forecast['area']]['longitude']} {area_metadata[forecast['area']]['latitude']})",
                ]

        return df


class SGEnviron4DayWeatherForecast(SGEnviron):
    def set_attrs(self):
        self.dataset_name = "4-day-weather-forecast"
        self.api_name = "4-day-weather-forecast"

    def json_to_df(self, json_data):
        columns = [
            # "Update Timestamp",
            "Report Time",
            # "Forecast Timestamp",
            "Target Date",
            "Forecast",
            "Temperature Low",
            "Temperature High",
            "Relative Humidity Low",
            "Relative Humidity High",
            "Wind Speed Low",
            "Wind Speed High",
            "Wind Direction",
        ]

        df = pd.DataFrame(columns=columns)

        for item in json_data["items"]:
            for forecast in item["forecasts"]:
                df.loc[len(df)] = [
                    # item["update_timestamp"],
                    self.process_datetime(item["timestamp"]),
                    # forecast["timestamp"],
                    forecast["date"],
                    forecast["forecast"],
                    forecast["temperature"]["low"],
                    forecast["temperature"]["high"],
                    forecast["relative_humidity"]["low"],
                    forecast["relative_humidity"]["high"],
                    forecast["wind"]["speed"]["low"],
                    forecast["wind"]["speed"]["high"],
                    forecast["wind"]["direction"],
                ]

        return df


class SGEnvironAirTemperature(SGEnviron):
    def set_attrs(self):
        self.dataset_name = "air-temperature-across-singapore"
        self.api_name = "air-temperature"

    def get_json(self, date_str=None):
        json_data = super().get_json(date_str)

        # 歷史資料只保留整點和30分的資料
        if date_str:
            json_data["items"] = [
                item
                for item in json_data["items"]
                if item["timestamp"].split(":")[1] in ["00", "30"]
            ]

        return json_data

    def json_to_df(self, json_data):
        columns = [
            "Time",
            "Station Name",
            "Temperature",
            "Station Longitude",
            "Station Latitude",
            "WKT",
        ]

        df = pd.DataFrame(columns=columns)

        station_dict = {}

        for station in json_data["metadata"]["stations"]:
            station_dict[station["id"]] = {
                "name": station["name"],
                "longitude": station["location"]["longitude"],
                "latitude": station["location"]["latitude"],
            }

        for item in json_data["items"]:
            for reading in item["readings"]:
                df.loc[len(df)] = [
                    self.process_datetime(item["timestamp"]),
                    station_dict[reading["station_id"]]["name"],
                    reading.get("value"),
                    station_dict[reading["station_id"]]["longitude"],
                    station_dict[reading["station_id"]]["latitude"],
                    f"POINT({station_dict[reading['station_id']]['longitude']} {station_dict[reading['station_id']]['latitude']})",
                ]

        return df


class SGEnvironPM25(SGEnviron):
    def set_attrs(self):
        self.dataset_name = "pm25"
        self.api_name = "pm25"

    def json_to_df(self, json_data):
        columns = [
            "Time",
            "Region",
            "PM2.5",
            "Longitude",
            "Latitude",
            "WKT",
        ]

        df = pd.DataFrame(columns=columns)

        region_dict = {
            region["name"]: region["label_location"]
            for region in json_data["region_metadata"]
        }

        for item in json_data["items"]:
            for region, pm25 in item["readings"]["pm25_one_hourly"].items():
                df.loc[len(df)] = [
                    # item["update_timestamp"],
                    self.process_datetime(item["timestamp"]),
                    region,
                    pm25,
                    region_dict[region]["longitude"],
                    region_dict[region]["latitude"],
                    f"POINT({region_dict[region]['longitude']} {region_dict[region]['latitude']})",
                ]

        return df


class SGEnvironPSI(SGEnviron):
    def set_attrs(self):
        self.dataset_name = "pollutant-standards-index"
        self.api_name = "psi"

    def json_to_df(self, json_data):
        columns = [
            # "Update Time",
            "Time",
            "Region",
            "PSI (24 hourly)",
            "PM2.5 (24 hourly)",
            "PM2.5 (subindex)",
            "PM10 (24 hourly)",
            "PM10 (subindex)",
            "CO (8 hour max)",
            "CO (subindex)",
            "O3 (8 hour max)",
            "O3 (subindex)",
            "SO2 (24 hourly)",
            "SO2 (subindex)",
            "NO2 (1 hour max)",
            "Longitude",
            "Latitude",
            "WKT",
        ]

        df = pd.DataFrame(columns=columns)

        region_dict = {
            region["name"]: region["label_location"]
            for region in json_data["region_metadata"]
        }

        for item in json_data["items"]:
            for region in region_dict.keys():
                df.loc[len(df)] = [
                    # item["update_timestamp"],
                    self.process_datetime(item["timestamp"]),
                    region,
                    item["readings"].get("psi_twenty_four_hourly", {}).get(region),
                    item["readings"].get("pm25_twenty_four_hourly", {}).get(region),
                    item["readings"].get("pm25_sub_index", {}).get(region),
                    item["readings"].get("pm10_twenty_four_hourly", {}).get(region),
                    item["readings"].get("pm10_sub_index", {}).get(region),
                    item["readings"].get("co_eight_hour_max", {}).get(region),
                    item["readings"].get("co_sub_index", {}).get(region),
                    item["readings"].get("o3_eight_hour_max", {}).get(region),
                    item["readings"].get("o3_sub_index", {}).get(region),
                    item["readings"].get("so2_twenty_four_hourly", {}).get(region),
                    item["readings"].get("so2_sub_index", {}).get(region),
                    item["readings"].get("no2_one_hour_max", {}).get(region),
                    region_dict[region]["longitude"],
                    region_dict[region]["latitude"],
                    f"POINT({region_dict[region]['longitude']} {region_dict[region]['latitude']})",
                ]

        return df


class SGEnvironRainfall(SGEnvironAirTemperature):
    def set_attrs(self):
        self.dataset_name = "rainfall-across-singapore"
        self.api_name = "rainfall"

    def json_to_df(self, json_data):
        df = super().json_to_df(json_data)
        df.rename(columns={"Temperature": "Rainfall"}, inplace=True)
        return df


class SGEnvironRelativeHumidity(SGEnvironAirTemperature):
    def set_attrs(self):
        self.dataset_name = "relative-humidity-across-singapore"
        self.api_name = "relative-humidity"

    def json_to_df(self, json_data):
        df = super().json_to_df(json_data)
        df.rename(columns={"Temperature": "Relative Humidity"}, inplace=True)
        return df


class SGEnvironWindDirection(SGEnvironAirTemperature):
    def set_attrs(self):
        self.dataset_name = "wind-direction-across-singapore"
        self.api_name = "wind-direction"

    def json_to_df(self, json_data):
        df = super().json_to_df(json_data)
        df.rename(columns={"Temperature": "Wind Direction"}, inplace=True)
        return df


class SGEnvironWindSpeed(SGEnvironAirTemperature):
    def set_attrs(self):
        self.dataset_name = "wind-speed-across-singapore"
        self.api_name = "wind-speed"

    def json_to_df(self, json_data):
        df = super().json_to_df(json_data)
        df.rename(columns={"Temperature": "Wind Speed"}, inplace=True)
        return df


class SGEnvironUltraVioletIndex(SGEnviron):
    def set_attrs(self):
        self.dataset_name = "ultra-violet-index"
        self.api_name = "uv-index"

    def json_to_df(self, json_data):
        columns = [
            "Time",
            "UVI",
        ]

        df = pd.DataFrame(columns=columns)

        for item in json_data["items"]:
            index = item["index"][0]

            df.loc[len(df)] = [
                self.process_datetime(index["timestamp"]),
                index["value"],
            ]

        return df


class SGEnvironAQI(SGEnviron):
    def __init__(self):
        self.dataset_name = "aqi"
        self.data_dir_path = os.path.join("data/", self.dataset_name)

        self.sg_environ_pm25 = SGEnvironPM25()
        self.sg_environ_psi = SGEnvironPSI()

    def get_json(self, date_str=None):
        json_pm25 = self.sg_environ_pm25.get_json(date_str)
        json_psi = self.sg_environ_psi.get_json(date_str)

        return json_pm25, json_psi

    def json_to_df(self, json_data_tuple):
        columns = [
            "Time",
            "Region",
            "PSI (24 hourly)",
            "PM2.5",
            "PM2.5 (24 hourly)",
            "PM2.5 (subindex)",
            "PM10 (24 hourly)",
            "PM10 (subindex)",
            "CO (8 hour max)",
            "CO (subindex)",
            "O3 (8 hour max)",
            "O3 (subindex)",
            "SO2 (24 hourly)",
            "SO2 (subindex)",
            "NO2 (1 hour max)",
            "Longitude",
            "Latitude",
            "WKT",
        ]

        df_pm25 = self.sg_environ_pm25.json_to_df(json_data_tuple[0])
        df_psi = self.sg_environ_psi.json_to_df(json_data_tuple[1])

        # 檢查即時資料時間是否相同
        if set(df_pm25["Time"]) != set(df_psi["Time"]):
            print("Time not match.")
            raise

        df = pd.merge(
            df_pm25,
            df_psi,
            on=["Time", "Region", "Longitude", "Latitude", "WKT"],
            how="outer",
        )

        df = df.reindex(columns=columns)

        return df

    def save_data(self):
        now = datetime.now(timezone(timedelta(hours=8)))

        json_data_tuple = self.get_json()

        df = self.json_to_df(json_data_tuple)

        if not df.empty:
            os.makedirs(self.data_dir_path, exist_ok=True)

            csv_name = f"{self.dataset_name}_{now.year}{now.month:02}{now.day:02}_{now.hour:02}.csv"

            csv_path = os.path.join(self.data_dir_path, csv_name)

            df.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}.")


class SGEnvironWind(SGEnviron):
    def __init__(self):
        self.dataset_name = "wind"
        self.data_dir_path = os.path.join("data/", self.dataset_name)

        self.sg_environ_air_temperature = SGEnvironAirTemperature()
        self.sg_environ_relative_humidity = SGEnvironRelativeHumidity()
        self.sg_environ_wind_direction = SGEnvironWindDirection()
        self.sg_environ_wind_speed = SGEnvironWindSpeed()

    def get_json(self, date_str=None):
        json_air_temperature = self.sg_environ_air_temperature.get_json(date_str)
        json_relative_humidity = self.sg_environ_relative_humidity.get_json(date_str)
        json_wind_direction = self.sg_environ_wind_direction.get_json(date_str)
        json_wind_speed = self.sg_environ_wind_speed.get_json(date_str)

        return (
            json_air_temperature,
            json_relative_humidity,
            json_wind_direction,
            json_wind_speed,
        )

    def json_to_df(self, json_data_tuple):
        columns = [
            "Time",
            "Station Name",
            "Temperature",
            "Relative Humidity",
            "Wind Direction",
            "Wind Speed",
            "Station Longitude",
            "Station Latitude",
            "WKT",
        ]

        df_air_temperature = self.sg_environ_air_temperature.json_to_df(
            json_data_tuple[0]
        )

        df_relative_humidity = self.sg_environ_relative_humidity.json_to_df(
            json_data_tuple[1]
        )

        df_wind_direction = self.sg_environ_wind_direction.json_to_df(
            json_data_tuple[2]
        )

        df_wind_speed = self.sg_environ_wind_speed.json_to_df(json_data_tuple[3])

        # 檢查即時資料時間是否相同
        if (
            set(df_air_temperature["Time"]) != set(df_relative_humidity["Time"])
            or set(df_air_temperature["Time"]) != set(df_wind_direction["Time"])
            or set(df_air_temperature["Time"]) != set(df_wind_speed["Time"])
        ):
            print("Time not match.")
            raise

        df = pd.merge(
            df_air_temperature,
            df_relative_humidity,
            on=["Time", "Station Name", "Station Longitude", "Station Latitude", "WKT"],
            how="outer",
        )

        df = pd.merge(
            df,
            df_wind_direction,
            on=["Time", "Station Name", "Station Longitude", "Station Latitude", "WKT"],
            how="outer",
        )

        df = pd.merge(
            df,
            df_wind_speed,
            on=["Time", "Station Name", "Station Longitude", "Station Latitude", "WKT"],
            how="outer",
        )

        df = df.reindex(columns=columns)

        return df

    def save_data(self):
        now = datetime.now(timezone(timedelta(hours=8)))

        json_data_tuple = self.get_json()

        df = self.json_to_df(json_data_tuple)

        if not df.empty:
            os.makedirs(self.data_dir_path, exist_ok=True)

            csv_name = f"{self.dataset_name}_{now.year}{now.month:02}{now.day:02}_{now.hour:02}{now.minute:02}.csv"

            csv_path = os.path.join(self.data_dir_path, csv_name)

            df.to_csv(csv_path, index=False)
            print(f"Saved: {csv_path}.")


if __name__ == "__main__":
    try:
        # sg_environ = SGEnviron24HourWeatherForecast()

        # sg_environ = SGEnviron2HourWeatherForecast()

        # sg_environ = SGEnviron4DayWeatherForecast()

        # sg_environ = SGEnvironRainfall()

        # sg_environ = SGEnvironUltraVioletIndex()

        # sg_environ = SGEnvironPM25()
        # sg_environ = SGEnvironPSI()
        # sg_environ = SGEnvironAQI()

        # sg_environ = SGEnvironAirTemperature()
        # sg_environ = SGEnvironRelativeHumidity()
        # sg_environ = SGEnvironWindDirection()
        # sg_environ = SGEnvironWindSpeed()
        sg_environ = SGEnvironWind()

        # # 儲存歷史資料
        # sg_environ.save_history_data("2024-12-09", "2024-12-11")

        # 儲存即時資料
        sg_environ.save_data()

    except Exception as e:
        raise  # AirflowFailException(e)
