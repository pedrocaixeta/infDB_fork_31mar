import logging
import multiprocessing as mp
import os
import sys
from typing import Any, Dict, Iterable, List

import geopandas as gpd
import pandas as pd
from charset_normalizer import from_path

from infdb import InfDB
from . import utils


# ============================== Constants ==============================
CLIPPED_PREFIX: str = "zensus-2022"




def load(infdb: InfDB) -> None:
    """Entry point to download, validate, and process Zensus 2022 datasets.

    Behavior preserved:
    - Respects `utils.if_active("zensus_2022", infdb)`.
    - Validates page links vs YAML list and logs differences.
    - Creates schema if missing.
    - Spawns a process pool with a per-process logger initializer.
    """

    try:
        log = infdb.get_worker_logger()

        if not utils.if_active("zensus_2022", infdb):
            return

        datasets: List[Dict[str, Any]] = infdb.get_config_value([infdb.get_toolname(), "sources", "zensus_2022", "datasets"])

        url = infdb.get_config_value([infdb.get_toolname(), "sources", "zensus_2022", "url"])
        zip_links: List[str] = utils.get_website_links(url, infdb)

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
        schema = infdb.get_config_value([infdb.get_toolname(), "sources", "zensus_2022", "schema"])
        with infdb.connect() as db:
            db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # folders
        zip_path = infdb.get_config_path([infdb.get_toolname(), "sources", "zensus_2022", "path", "zip"], type="loader")
        os.makedirs(zip_path, exist_ok=True)
        unzip_path = infdb.get_config_path([infdb.get_toolname(), "sources", "zensus_2022", "path", "unzip"], type="loader")
        os.makedirs(unzip_path, exist_ok=True)

        number_processes = utils.get_number_processes(infdb)
        with mp.Pool(
            processes=number_processes,
            # initializer=_init_logger_for_process,
            # initargs=(infdb,),
        ) as pool:
            results = pool.starmap(process_dataset, [(dataset, infdb.get_toolname(),) for dataset in datasets])
        
        if not all(results):
            raise RuntimeError("Some datasets failed to process")
        else:
            sys.exit(0)
    except Exception as err:
        log.exception("An error occurred while processing Census: %s", str(err))
        sys.exit(1)

def process_dataset(dataset: Dict[str, Any], tool_name: str) -> bool:
    """Download, unzip, transform, and load one dataset to PostGIS.

    Args:
        dataset: A dataset record from config (`name`, `url`, `year`, `table_name`, `status`, ...).

    Returns:
        True on success or skip; False when an exception is encountered (logged).
    """
    try:
        # Initialize InfDB in each worker process
        infdb = InfDB(tool_name=tool_name)
        log = infdb.get_worker_logger()
        
        log.info("Working on %s", dataset["name"])

        # status gate
        if dataset["status"] != "active":
            log.info("%s skips, status not active", dataset["name"])
            return True

        # fresh cfg (per-process)

        years: Iterable[int] = infdb.get_config_value([infdb.get_toolname(), "sources", "zensus_2022", "years"])
        if dataset["year"] not in years:
            log.info("%s skips, not in years list", dataset["name"])
            return True

        # Download INTO the zip directory and use the returned file path
        zip_dir = infdb.get_config_path([infdb.get_toolname(), "sources", "zensus_2022", "path", "zip"], type="loader")
        link = dataset["url"]
        downloaded = utils.download_files(link, zip_dir, infdb)  # returns [<zip_file_path>]
        zip_file = downloaded[0]

        # Unzip using the real file path
        unzip_dir = infdb.get_config_path([infdb.get_toolname(), "sources", "zensus_2022", "path", "unzip"], type="loader")
        folder_path = os.path.join(unzip_dir, dataset["table_name"])
        utils.unzip(zip_file, folder_path, infdb)

        # Export to PostGIS
        resolutions: List[str] = infdb.get_config_value([infdb.get_toolname(), "sources", "zensus_2022", "resolutions"])
        for resolution in resolutions:
            log.info("Processing %s with %s ...", dataset["name"], resolution)

            file = utils.get_file(folder_path, resolution, ".csv", infdb)
            if not file:
                log.warning("No file for %s with resolution %s found", dataset["name"], resolution)
                continue

            encoding = from_path(file).best().encoding
            log.debug("Detected encoding for file: %s", encoding)

            df = pd.read_csv(
                file,
                sep=";",
                decimal=",",
                low_memory=True,
                encoding=encoding,
            )
            df.fillna(0, inplace=True)
            df.columns = df.columns.str.lower()

            gdf = gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(df[f"x_mp_{resolution}"], df[f"y_mp_{resolution}"]),
                crs="EPSG:3035",
            )

            epsg = infdb.get_config_value(["services", "postgres", "epsg"])
            if epsg is None:
                raise KeyError("Missing 'epsg' in DB parameters for service 'postgres'")
            gdf = gdf.to_crs(epsg=epsg)

            # get engine via client (engine is independent)
            with infdb.connect() as db:
                engine = db.get_db_engine()

            prefix = infdb.get_config_value([infdb.get_toolname(), "sources", "zensus_2022", "prefix"])
            schema = infdb.get_config_value([infdb.get_toolname(), "sources", "zensus_2022", "schema"])

            gdf_envelope = utils.get_envelop(infdb)
            gdf_clipped = gpd.clip(gdf, gdf_envelope) if not gdf_envelope.empty else gdf

            table_name = f"{prefix}_{dataset['year']}_{resolution}_{dataset['table_name']}"
            gdf_clipped = gdf_clipped.rename_geometry("geom")
            gdf_clipped.to_postgis(table_name, engine, if_exists="replace", schema=schema, index=False)

            save_local = infdb.get_config_value([infdb.get_toolname(), "sources", "zensus_2022", "save_local"])
            if save_local == "active":
                out_dir = infdb.get_config_path([infdb.get_toolname(), "sources", "zensus_2022", "path", "processed"], type="loader")
                os.makedirs(out_dir, exist_ok=True)
                gdf_clipped.to_file(
                    os.pyath.join(out_dir, f"{CLIPPED_PREFIX}_{resolution}.gpkg"),
                    layer=table_name,
                    driver="GPKG",
                )
                gdf_clipped.to_csv(
                    os.path.join(out_dir, f"{CLIPPED_PREFIX}_{resolution}_{table_name}.csv"),
                    index=False,
                )

            log.info("Processed sucessfully %s", file)
        return True
    except Exception as err:
        log.exception("An error occurred while processing file: %s %s", dataset.get("name"), str(err))
        return False
