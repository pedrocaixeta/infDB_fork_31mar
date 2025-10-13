import os
import shutil
from . import config, utils, logger
import logging

import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

import numpy as np

log = logging.getLogger(__name__)

def temperature_2m():
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "start_date": "2025-09-26",
        "end_date": "2025-10-10",
        "hourly": "temperature_2m",
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    print("\nHourly data\n", hourly_dataframe)


def load(log_queue):
    logger.setup_worker_logger(log_queue)

    if not utils.if_active("openmeteo"):
        return

    base_path = config.get_path(["loader", "sources", "openmeteo", "path", "openmeteo"])
    os.makedirs(base_path, exist_ok=True)

    


    log.info(f"LOD2 data loaded successfully")


def grid_midpoints_numpy(xmin, ymin, xmax, ymax, cell_size):
    x_coords = np.arange(xmin + cell_size/2, xmax, cell_size)
    y_coords = np.arange(ymin + cell_size/2, ymax, cell_size)
    xs, ys = np.meshgrid(x_coords, y_coords)
    midpoints = np.column_stack((xs.ravel(), ys.ravel()))
    return midpoints


# Example
midpoints = grid_midpoints_numpy(4100000, 5390000, 4120000, 5410000, 1000)
print(midpoints.shape)
print(midpoints[:5])