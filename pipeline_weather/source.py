import requests

BASE_URL = "https://api.open-meteo.com/v1/forecast"

CITIES = {
    "Jakarta": {"latitude": -6.2088, "longitude": 106.8456},
    "Bandung": {"latitude": -6.9175, "longitude": 107.6191},
    "Surabaya": {"latitude": -7.2575, "longitude": 112.7521},
}


class SourceUnavailableError(Exception):
    """Raised when api.open-meteo.com cannot be reached."""


def fetch_weather(city: str) -> dict:
    if city not in CITIES:
        raise ValueError(f"Unknown city: {city}")
    coords = CITIES[city]
    params = {
        "latitude": coords["latitude"],
        "longitude": coords["longitude"],
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "auto",
        "forecast_days": 1,
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise SourceUnavailableError(
            f"Could not reach api.open-meteo.com for {city} — check your internet connection"
        ) from exc
    return response.json()


def fetch_all_cities() -> dict[str, dict]:
    return {city: fetch_weather(city) for city in CITIES}
