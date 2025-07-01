import datetime as dt
from unittest.mock import MagicMock

import pandas as pd
import pytest
from meteostat import Point

from weather_forecast.ETL.data_loader import DataLoader, DataLoaderConfig


class TestDataLoader:
    def test_get_country_name(self):
        assert DataLoader.get_country_name("HU") == "Hungary"
        assert DataLoader.get_country_name("DE") == "Germany"

    def test_get_country_name_error(self):
        with pytest.raises(AttributeError):
            DataLoader.get_country_name("XY")

    def test_get_coordinates(self):
        expectation = (47.4978789, 19.0402383)
        result = DataLoader.get_coordinates(country_name="Hungary", city_name="Budapest")
        assert expectation == (result._lat, result._lon)

    def test_get_coordinates_error(self):
        with pytest.raises(ValueError):
            DataLoader.get_coordinates(country_name="Invalid", city_name="Invalid")

    def test_get_raw_weather_data(self, mocker):
        mocker.patch(
            "weather_forecast.ETL.data_loader.DataLoader.get_coordinates",
            return_value=Point(47.4979, 19.0402),
        )

        expectation_df = pd.DataFrame(
            {
                "time": pd.date_range(dt.date(2020, 1, 1), periods=2),
                "tavg": [-0.2, -1.0],
                "tmin": [-2.9, -4.4],
                "tmax": [4.8, 4.0],
            }
        ).set_index("time")

        mock_daily_instance = MagicMock()
        mock_daily_instance.fetch.return_value = expectation_df

        mocker.patch(
            "weather_forecast.ETL.data_loader.Daily",
            return_value=mock_daily_instance,
        )
        data_loader_config = DataLoaderConfig(
            country_code="HU",
            city_name="Budapest",
            date_from=dt.datetime(2020, 1, 1),
            date_to=dt.datetime(2020, 1, 2),
        )
        result = (
            DataLoader.get_raw_weather_data(data_loader_config)
            .reset_index()[["time", "tavg", "tmin", "tmax"]]
            .set_index("time")
        )
        pd.testing.assert_frame_equal(result, expectation_df)

    def test_get_weather_data(self, mocker):
        input_df = pd.DataFrame(
            {
                "time": pd.date_range(dt.date(2020, 1, 1), periods=2),
                "value": [0, 1],
            }
        ).set_index("time")

        mocker.patch(
            "weather_forecast.ETL.data_loader.DataLoader.get_raw_weather_data",
            return_value=input_df,
        )
        data_loader_config = DataLoaderConfig(
            country_code="HU",
            city_name="Budapest",
            date_from=dt.datetime(2020, 1, 1),
            date_to=dt.datetime(2020, 1, 2),
        )
        result = DataLoader.get_weather_data(data_loader_config=data_loader_config)
        expectation_df = pd.DataFrame(
            {
                "DATE": pd.date_range(dt.date(2020, 1, 1), periods=2),
                "VALUE": [0, 1],
            }
        )
        pd.testing.assert_frame_equal(expectation_df, result)
