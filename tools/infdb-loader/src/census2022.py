import logging
import multiprocessing as mp
import os
from typing import Any, Dict, List
import chardet
import pandas as pd
import geopandas as gpd
from charset_normalizer import from_path

from infdb import InfdbClient, InfdbConfig, InfdbLogger
from infdb import InfDB
from . import utils


# ============================== Constants ==============================

LOGGER_NAME: str = __name__
TOOL_NAME: str = "loader"
CONFIG_DIR: str = "configs"
DB_NAME: str = "postgres"
CSV_SEPARATOR: str = ";"
CSV_DECIMAL: str = ","
GPKG_DRIVER: str = "GPKG"
CLIPPED_PREFIX: str = "zensus-2022"

# Module logger
log = logging.getLogger(LOGGER_NAME)


def _init_logger_for_process(cfg: InfdbConfig) -> logging.Logger:
    """Initialize and return a worker logger for this process.

    Args:
        cfg: Shared InfdbConfig to read log path and level.

    Returns:
        A process-local logger wired through InfdbLogger's QueueListener.
    """
    log_path = cfg.get_value([TOOL_NAME, "logging", "path"]) or "loader.log"
    level = cfg.get_value([TOOL_NAME, "logging", "level"]) or "INFO"
    infdb_logger = InfdbLogger(log_path=log_path, level=level, cleanup=False)
    return infdb_logger.setup_worker_logger()


def load(infdb: InfDB) -> None:
    """Entry point to download, validate, and process Zensus 2022 datasets.

    Behavior preserved:
    - Respects `utils.if_active("zensus_2022")`.
    - Validates page links vs YAML list and logs differences.
    - Creates schema if missing.
    - Spawns a process pool with a per-process logger initializer.
    """
    # package config + logger
    cfg = InfdbConfig(tool_name=TOOL_NAME, config_path=CONFIG_DIR)

    global log
    log = _init_logger_for_process(cfg)

    if not utils.if_active("zensus_2022"):
        return

    datasets: List[Dict[str, Any]] = cfg.get_value([TOOL_NAME, "sources", "zensus_2022", "datasets"])

    url = cfg.get_value([TOOL_NAME, "sources", "zensus_2022", "url"])
    zip_links: List[str] = utils.get_website_links(url)

    # validate links
    yaml_links = {entry["url"] for entry in datasets}
    original_set = set(zip_links)
    missing_in_yaml = original_set - yaml_links
    extra_in_yaml = yaml_links - original_set
    if missing_in_yaml:
        log.warning("Links in original list but NOT in YAML:")
        for lnk in sorted(missing_in_yaml):
            log.warning(" - %s", lnk)
    if extra_in_yaml:
        log.warning("Links in YAML but NOT in original list:")
        for lnk in sorted(extra_in_yaml):
            log.warning(" - %s", lnk)

    # create schema (via package client)
    schema = cfg.get_value([TOOL_NAME, "sources", "zensus_2022", "schema"])
    with InfdbClient(cfg, log, db_name=DB_NAME) as db:
        db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # folders
    zip_path = cfg.get_path([TOOL_NAME, "sources", "zensus_2022", "path", "zip"], type="loader")
    os.makedirs(zip_path, exist_ok=True)
    unzip_path = cfg.get_path([TOOL_NAME, "sources", "zensus_2022", "path", "unzip"], type="loader")
    os.makedirs(unzip_path, exist_ok=True)

    number_processes = utils.get_number_processes()
    with mp.Pool(
        processes=number_processes,
        initializer=_init_logger_for_process,
        initargs=(cfg,),
    ) as pool:
        pool.map(process_dataset, datasets)


def process_dataset(dataset: Dict[str, Any]) -> bool:
    """Download, unzip, transform, and load one dataset to PostGIS.

    Args:
        dataset: A dataset record from config (`name`, `url`, `year`, `table_name`, `status`, ...).

    Returns:
        True on success or skip; False when an exception is encountered (logged).
    """
    try:
        infdb = InfDB(tool_name=TOOL_NAME)
        log = infdb.get_worker_logger()

        log.info("Working on %s", dataset["name"])

        # status gate
        if dataset["status"] != "active":
            log.info("%s skips, status not active", dataset["name"])
            return True

        # fresh cfg (per-process)
        cfg = InfdbConfig(tool_name=TOOL_NAME, config_path=CONFIG_DIR)

        years: Iterable[int] = infdb.get_config_value([TOOL_NAME, "sources", "zensus_2022", "years"])
        if dataset["year"] not in years:
            log.info("%s skips, not in years list", dataset["name"])
            return True

        # Download INTO the zip directory and use the returned file path
        zip_dir = infdb.get_config_path([TOOL_NAME, "sources", "zensus_2022", "path", "zip"], type="loader")
        link = dataset["url"]
        downloaded = utils.download_files(link, zip_dir, max_concurrent=1 )  # returns [<zip_file_path>]
        zip_file = downloaded[0]

        # Unzip using the real file path
        unzip_dir = infdb.get_config_path([TOOL_NAME, "sources", "zensus_2022", "path", "unzip"], type="loader")
        folder_path = os.path.join(unzip_dir, dataset["table_name"])
        utils.unzip(zip_file, folder_path)

        # Export to PostGIS for each configured resolution
        resolutions : List[str] = infdb.get_config_value([TOOL_NAME, "sources", "zensus_2022", "resolutions"])
        prefix = infdb.get_config_value([TOOL_NAME, "sources", "zensus_2022", "prefix"])
        schema = infdb.get_config_value([TOOL_NAME, "sources", "zensus_2022", "schema"])
        epsg = cfg.get_db_parameters("postgres")["epsg"]  # target DB SRID

        for resolution in resolutions:
            log.info("Processing %s with %s ...", dataset["name"], resolution)

            # Search for corresponding CSV within the unzipped folder
            csv_path = utils.get_file(folder_path, resolution, ".csv")
            if not csv_path:
                log.warning(f"No file for {dataset['name']} with resolution {resolution} found")
                continue

            # -------------------------
            # FAST LOAD (NEW): COPY + server-side geometry creation
            # This replaces: read CSV -> build GeoDataFrame -> gdf.to_postgis(...)
            # Benefits:
            #   * COPY is much faster than per-row inserts
            #   * ST_MakePoint + ST_Transform happen inside PostGIS (C), not Python
            # -------------------------
            x_col = f"x_mp_{resolution}"  # Zensus CSV columns for X/Y per resolution
            y_col = f"y_mp_{resolution}"
            table_name = f"{prefix}_{dataset['year']}_{resolution}_{dataset['table_name']}"

            utils.fast_copy_points_csv(
                csv_path=csv_path,
                schema=schema,
                table_name=table_name,
                x_col=x_col,
                y_col=y_col,
                srid_src=3035,                 # source X/Y are in EPSG:3035 in the Zensus CSV
                srid_dst=epsg,                 # target SRID from DB config
                drop_existing=True,            # matches old 'replace' behavior
                create_spatial_index=True,     # gives you good query perf right away
            )  # NEW: main speed-up

            log.info(f"Processed successfully {csv_path}")

    except Exception as err:
        log.exception(f"An error occurred while processing file: {dataset['name']} {str(err)}")
        return False

    return True
