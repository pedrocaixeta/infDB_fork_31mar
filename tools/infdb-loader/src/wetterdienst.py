import os
import shutil

from requests import request
from . import config, utils, logger
import logging

import openmeteo_requests

import pandas as pd
import geopandas as gpd
import requests_cache
from retry_requests import retry

import numpy as np
import io
from pyproj import Transformer

from datetime import datetime
from zoneinfo import ZoneInfo

from wetterdienst.provider.dwd.observation import (
    DwdObservationRequest,
)

log = logging.getLogger(__name__)


def load(log_queue):
    logger.setup_worker_logger(log_queue)

    if not utils.if_active("openmeteo"):
        return

    # base_path = config.get_path(["loader", "sources", "openmeteo", "path", "base"])
    # os.makedirs(base_path, exist_ok=True)

    stations_filter_by_examples()


    log.info(f"Openmeteo data loaded successfully")


def stations_filter_by_examples() -> None:
    """Retrieve stations of DWD that measure temperature."""
    request = DwdObservationRequest(
        parameters=("hourly", "temperature_air"),
        periods="recent",
        start_date=datetime(2020, 1, 1, tzinfo=ZoneInfo("UTC")),
        end_date=datetime(2020, 1, 20, tzinfo=ZoneInfo("UTC")),
    )

    print("All stations")
    print(request.all().df)

    # print("Filter by station_id (1048)")
    # station_id = 1048
    # print(request.filter_by_station_id(station_id).df)

    # print("Filter by name (Dresden)")
    # name = "Dresden Klotzsche"
    # print(request.filter_by_name(name).df)

    # frankfurt = (50.11, 8.68)
    # print("Filter by distance (30 km)")
    # print(request.filter_by_distance(latlon=frankfurt, distance=30).df)
    # print("Filter by rank (3 closest stations)")
    # print(request.filter_by_rank(latlon=frankfurt, rank=3).df)

    print("Filter by bbox (Frankfurt)")
    bbox = (8.52, 50.03, 8.80, 50.22)
    envelop = utils.get_envelop()
    bbox = (xmin, ymin, xmax, ymax) = envelop.to_crs(4326).total_bounds
    stations = request.filter_by_bbox(*bbox)
    df = request.filter_by_bbox(*bbox).df
    print(request.filter_by_bbox(*bbox).df)
    values = stations.values.all()  
    # print("Filter by sql (starting with Dre)")
    # sql = "name LIKE 'Dre%'"
    # print(request.filter_by_sql(sql).df)

    frankfurt = (50.11, 8.68)
    values = request.interpolate(frankfurt)
    print(values)
