import datetime as dt
from dataclasses import dataclass
from typing import Tuple

import pandas as pd
import pycountry
from geopy.geocoders import Nominatim
from meteostat import Daily, Point


@dataclass
class DataLoaderConfig:
    country_code: str
    city_name: str
    date_from: dt.date
    date_to: dt.date

    def __post_init__(self):
        self.country_name = DataLoader.get_country_name(self.country_code)
        if self.date_from > self.date_to:
            raise ValueError("date_to parameter must be later than date_from parameter.")


class DataLoader:
    @staticmethod
    def get_country_name(country_code: str) -> str:
        """
        Get country name by ISO country code.

        Args:
            country_code (str): ISO country code

        Returns:
            str: Returns country name by its ISO country code.
        """
        country = pycountry.countries.get(alpha_2=country_code.upper())
        if country == None:
            raise AttributeError(f"Invalid ISO country code: '{country_code}'")
        return country.name

    @staticmethod
    def get_coordinates(country_name: str, city_name: str) -> Point:
        """
        Get coordinates of a country-city.

        Args:
            country_name (str): Name of a country.
            city_name (str): Name of a city.

        Raises:
            ValueError: If country-city not found.

        Returns:
            Point: Latitude, Longitude point.
        """
        geolocator = Nominatim(user_agent="my_weather_app")
        location = geolocator.geocode(f"{city_name}, {country_name}")
        if location:
            return Point(location.latitude, location.longitude)
        else:
            raise ValueError(f"{city_name}, {country_name} not found.")

    @classmethod
    def get_raw_weather_data(cls, data_loader_config: DataLoaderConfig) -> pd.DataFrame:
        """
        Get raw weather data based on data loader config.

        Args:
            data_loader_config (DataLoaderConfig): Data loader config

        Returns:
            pd.DataFrame: Returns raw weather data based on data loader config.
        """
        location = cls.get_coordinates(
            country_name=data_loader_config.country_name,
            city_name=data_loader_config.city_name,
        )
        data = Daily(
            loc=location,
            start=data_loader_config.date_from,
            end=data_loader_config.date_to,
        )
        return data.fetch()

    @classmethod
    def get_weather_data(cls, data_loader_config: DataLoaderConfig) -> pd.DataFrame:
        """
        Get formatted weather data based on data loader config.

        Args:
            data_loader_config (DataLoaderConfig): Data loader config

        Returns:
            pd.DataFrame: Returns formatter weather data based on data loader config.
        """
        df = cls.get_raw_weather_data(data_loader_config=data_loader_config)

        df = df.reset_index()
        df.columns = df.columns.str.upper()
        df = df.rename(columns={"TIME": "DATE"})

        return df
