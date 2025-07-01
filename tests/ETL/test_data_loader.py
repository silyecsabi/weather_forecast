import pytest
from weather_forecast.ETL.data_loader import DataLoader

class TestDataLoader:
    def test_add_numbers(self):
        assert DataLoader.add_numbers(1, 1) == 2
        assert DataLoader.add_numbers(-1, 1) == 0
