from unittest.mock import patch

import pandas as pd
from dagster import materialize

import db
import source
from main import raw_weather, temperature_within_plausible_range, weather_table

FAKE_ALL_CITIES = {
    "Jakarta": {
        "hourly": {
            "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
            "temperature_2m": [28.0, 27.5],
            "precipitation": [0.0, 0.1],
            "wind_speed_10m": [6.0, 5.5],
        }
    },
    "Bandung": {
        "hourly": {
            "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
            "temperature_2m": [20.0, 19.5],
            "precipitation": [0.5, 0.0],
            "wind_speed_10m": [4.0, 3.8],
        }
    },
    "Surabaya": {
        "hourly": {
            "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
            "temperature_2m": [30.0, 29.5],
            "precipitation": [0.0, 0.0],
            "wind_speed_10m": [7.0, 6.5],
        }
    },
}


def test_weather_pipeline_loads_expected_rows():
    loaded = {}

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    with patch.object(
        source, "fetch_all_cities", return_value=FAKE_ALL_CITIES
    ), patch.object(db, "load_table", side_effect=fake_load_table):
        result = materialize(
            [raw_weather, weather_table, temperature_within_plausible_range],
        )

    assert result.success
    assert loaded["weather"].shape[0] == 6  # 3 cities x 2 hourly rows
    assert set(loaded["weather"]["city"]) == {"Jakarta", "Bandung", "Surabaya"}


def test_temperature_check_fails_on_implausible_values():
    bad_data = {
        "Jakarta": {
            "hourly": {
                "time": ["2026-01-01T00:00"],
                "temperature_2m": [999.0],  # implausible reading
                "precipitation": [0.0],
                "wind_speed_10m": [5.0],
            }
        }
    }

    with patch.object(source, "fetch_all_cities", return_value=bad_data):
        result = materialize(
            [raw_weather, temperature_within_plausible_range],
            raise_on_error=False,
        )

    check_evaluations = [
        e for e in result.get_asset_check_evaluations()
        if e.check_name == "temperature_within_plausible_range"
    ]
    assert len(check_evaluations) == 1
    assert check_evaluations[0].passed is False
    assert check_evaluations[0].metadata["num_out_of_range_rows"].value == 1