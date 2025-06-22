import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests
from typing import Dict, Union
from numpy import ndarray


def get_air_pollution_history(lat: float, lon: float, start: str, end: str) -> Dict[str, Union[pd.DatetimeIndex, ndarray]]:
    """
    Fetch raw air pollution history data fro API.

    Args:
        lat (float): Latitude
        lon (float): Longitude
        start (str): Start date
        end (str): End date

    Returns:
        Dict[str, Union[pd.DatetimeIndex, np.ndarray]]: Dictionary containing hourly air quality data
    """

    # API client
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": ["pm10", "pm2_5", "sulphur_dioxide", "nitrogen_dioxide", "ozone", "carbon_monoxide", "european_aqi"]
    }
    response = openmeteo.weather_api(url, params=params)[0]

    # Get hourly data
    hourly = response.Hourly()
    hourly_pm10 = hourly.Variables(0).ValuesAsNumpy()
    hourly_pm2_5 = hourly.Variables(1).ValuesAsNumpy()
    hourly_sulphur_dioxide = hourly.Variables(2).ValuesAsNumpy()
    hourly_nitrogen_dioxide = hourly.Variables(3).ValuesAsNumpy()
    hourly_ozone = hourly.Variables(4).ValuesAsNumpy()
    hourly_carbon_monoxide = hourly.Variables(5).ValuesAsNumpy()
    hourly_european_aqi = hourly.Variables(6).ValuesAsNumpy()

    # Get data in JSON format
    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["pm10"] = hourly_pm10
    hourly_data["pm2_5"] = hourly_pm2_5
    hourly_data["sulphur_dioxide"] = hourly_sulphur_dioxide
    hourly_data["nitrogen_dioxide"] = hourly_nitrogen_dioxide
    hourly_data["ozone"] = hourly_ozone
    hourly_data["carbon_monoxide"] = hourly_carbon_monoxide
    hourly_data["european_aqi"] = hourly_european_aqi

    return hourly_data


def get_weather_history(lat: float, lon: float, start: str, end: str) -> Dict[str, Union[pd.DatetimeIndex, ndarray]]:
    """
    Fetch raw weather history data from API.

    Args:
        lat (float): Latitude
        lon (float): Longitude
        start (str): Start date
        end (str): End date

    Returns:
        Dict[str, Union[pd.DatetimeIndex, np.ndarray]]: Dictionary containing hourly weather data
    """
    
    # API client
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "dew_point_2m"]
    }
    response = openmeteo.weather_api(url, params=params)[0]

    # Get hourly historical data for all weather variables
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_rain = hourly.Variables(2).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(3).ValuesAsNumpy()

    # Get the weather data in JSON format
    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["rain"] = hourly_rain
    hourly_data["dew_point_2m"] = hourly_dew_point_2m
    
    return hourly_data
