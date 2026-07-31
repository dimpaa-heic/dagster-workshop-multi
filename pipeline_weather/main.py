import pandas as pd
from dagster import (
    AssetCheckResult,
    Definitions,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

import db
import source


@asset
def raw_weather() -> pd.DataFrame:
    """Hourly weather forecast for a fixed set of Indonesian cities."""
    all_data = source.fetch_all_cities()
    rows = []
    for city, payload in all_data.items():
        hourly = payload["hourly"]
        times = hourly["time"]
        temps = hourly["temperature_2m"]
        precip = hourly["precipitation"]
        wind = hourly["wind_speed_10m"]
        for i in range(len(times)):
            rows.append(
                {
                    "city": city,
                    "time": times[i],
                    "temperature_c": temps[i],
                    "precipitation_mm": precip[i],
                    "wind_speed_kmh": wind[i],
                }
            )
    return pd.DataFrame(rows)


@asset
def weather_table(raw_weather: pd.DataFrame) -> int:
    return db.load_table(raw_weather, "weather")


@asset_check(asset=raw_weather)
def temperature_within_plausible_range(raw_weather: pd.DataFrame) -> AssetCheckResult:
    """Fails if any temperature reading falls outside a physically plausible
    range for these cities (-10C to 50C) — catches bad API data or parsing
    bugs before they land in the warehouse."""
    out_of_range = raw_weather[
        (raw_weather["temperature_c"] < -10) | (raw_weather["temperature_c"] > 50)
    ]
    return AssetCheckResult(
        passed=out_of_range.empty,
        metadata={"num_out_of_range_rows": len(out_of_range)},
    )


refresh_weather_job = define_asset_job(name="refresh_weather_job")

refresh_weather_hourly = ScheduleDefinition(
    name="refresh_weather_hourly",
    job=refresh_weather_job,
    cron_schedule="0 * * * *",
)

defs = Definitions(
    assets=[raw_weather, weather_table],
    asset_checks=[temperature_within_plausible_range],
    jobs=[refresh_weather_job],
    schedules=[refresh_weather_hourly],
)
