from unittest.mock import Mock, patch

import pytest
import requests

import source

FAKE_FORECAST = {
    "hourly": {
        "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
        "temperature_2m": [24.5, 24.1],
        "precipitation": [0.0, 0.2],
        "wind_speed_10m": [8.3, 7.9],
    }
}


def test_fetch_weather_returns_parsed_json():
    fake_response = Mock()
    fake_response.json.return_value = FAKE_FORECAST
    fake_response.raise_for_status.return_value = None

    with patch("source.requests.get", return_value=fake_response) as mock_get:
        result = source.fetch_weather("Jakarta")

    assert result == FAKE_FORECAST
    mock_get.assert_called_once()
    called_args, called_kwargs = mock_get.call_args
    assert called_args[0] == source.BASE_URL
    assert called_kwargs["params"]["latitude"] == source.CITIES["Jakarta"]["latitude"]


def test_fetch_weather_raises_value_error_for_unknown_city():
    with pytest.raises(ValueError):
        source.fetch_weather("Atlantis")


def test_fetch_weather_raises_source_unavailable_on_network_error():
    with patch("source.requests.get", side_effect=requests.ConnectionError("boom")):
        with pytest.raises(source.SourceUnavailableError):
            source.fetch_weather("Jakarta")


def test_fetch_all_cities_calls_fetch_weather_for_each_city():
    with patch("source.fetch_weather", return_value=FAKE_FORECAST) as mock_fetch:
        result = source.fetch_all_cities()

    assert set(result.keys()) == set(source.CITIES.keys())
    assert mock_fetch.call_count == len(source.CITIES)
