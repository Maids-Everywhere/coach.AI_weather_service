import openmeteo_requests

import pandas as pd
import datetime as dt
import requests_cache
from retry_requests import retry

cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

URL = "https://api.open-meteo.com/v1/forecast"


class OpenMeteoAPI:
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(
        self, latitude: float, longitude: float, start_date: str, end_date: str
    ):
        self.latitude = latitude
        self.longitude = longitude
        self.start_date = start_date
        self.end_date = end_date

    def _create_params(self) -> dict:
        """Create parameters for the Open-Meteo API request.

        Args:
            latitude (float): The latitude of the location.
            longitude (float): The longitude of the location.
            start_date (str): The start date for the forecast in 'YYYY-MM-DD' format.
            end_date (str): The end date for the forecast in 'YYYY-MM-DD' format.
            is_hourly (bool): Whether to retrieve hourly data.

        Returns:
            dict: The parameters for the Open-Meteo API request.
        """

        _start_date = dt.datetime.strptime(self.start_date, "%Y-%m-%d")
        _end_date = dt.datetime.strptime(self.end_date, "%Y-%m-%d")

        if _start_date > _end_date:
            self.end_date, self.start_date = self.start_date, self.end_date

        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "hourly": [
                "temperature_2m",
                "apparent_temperature",
                "weather_code",
                "rain",
                "snowfall",
                "relative_humidity_2m",
            ],
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

    def fetch_weather_data(self) -> pd.DataFrame:
        """Fetch weather data from the Open-Meteo API.

        Returns:
            pd.DataFrame: A DataFrame containing the weather data.
        """
        params = self._create_params()
        responses = self.openmeteo.weather_api(self.URL, params=params)

        if not responses:
            raise ValueError("No data received from Open-Meteo API.")

        response = responses[0]
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_apparent_temperature = hourly.Variables(1).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(2).ValuesAsNumpy()
        hourly_rain = hourly.Variables(3).ValuesAsNumpy()
        hourly_snowfall = hourly.Variables(4).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(5).ValuesAsNumpy()

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["apparent_temperature"] = hourly_apparent_temperature
        hourly_data["weather_code"] = hourly_weather_code
        hourly_data["rain"] = hourly_rain
        hourly_data["snowfall"] = hourly_snowfall
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m

        return pd.DataFrame(data=hourly_data)
