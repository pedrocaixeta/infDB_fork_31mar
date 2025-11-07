import os
import logging
import multiprocessing
import chardet  # kept in case you later add CSV validation/encoding checks
import pandas as pd  # kept (not used now); remove if you want zero pandas footprint
import geopandas as gpd  # kept for get_envelop() usage elsewhere; can be removed if unused
import shutil  # NEW: used for optional local copy of source CSV (save_local)
from . import utils, config, logger

log = logging.getLogger(__name__)


def load(log_queue):
    logger.setup_worker_logger(log_queue)

    if not utils.if_active("zensus_2022"):
        return
    
    datasets = config.get_value(["loader", "sources", "zensus_2022", "datasets"])

    url = config.get_value(["loader", "sources", "zensus_2022", "url"])
    zip_links = utils.get_website_links(url)

    # Validate links vs YAML (helps catch missing/extra links early)
    yaml_links = {entry["url"] for entry in datasets}
    original_set = set(zip_links)

    missing_in_yaml = original_set - yaml_links
    extra_in_yaml = yaml_links - original_set
    if missing_in_yaml:
        log.warning("Links in original list but NOT in YAML:")
        for l in sorted(missing_in_yaml):
            log.warning(f" - {l}")

    if extra_in_yaml:
        log.warning("Links in YAML but NOT in original list:")
        for l in sorted(extra_in_yaml):
            log.warning(f" - {l}")

    # Create schema if it doesn't exist
    schema = config.get_value(["loader", "sources", "zensus_2022", "schema"])
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    utils.sql_query(sql)
    
    # Folders
    zip_path = config.get_path(["loader", "sources", "zensus_2022", "path", "zip"])
    os.makedirs(zip_path, exist_ok=True)
    unzip_path = config.get_path(["loader", "sources", "zensus_2022", "path", "unzip"])
    os.makedirs(unzip_path, exist_ok=True)

    # Parallelize like before (no change)
    number_processes = utils.get_number_processes()
    with multiprocessing.Pool(
        processes=number_processes,
        initializer=logger.setup_worker_logger,
        initargs=(log_queue,),
    ) as pool:
        pool.map(process_dataset, datasets)


def process_dataset(dataset):
    """
    Adapted to the new high-performance utils:
      - utils.download_files(...)  # polite, throttled downloader
      - utils.unzip(...)           # same as before, just called with the new return path
      - utils.fast_copy_points_csv # COPY + server-side geometry (replaces GeoPandas.to_postgis)
    """
    try:
        log.info(f"Working on {dataset['name']}")

        # Status check
        if dataset.get("status") != "active":
            log.info(f"{dataset['name']} skips, status not active")
            return True
        
        # Year filter
        years = config.get_value(["loader", "sources", "zensus_2022", "years"])
        if dataset["year"] not in years:
            log.info(f"{dataset['name']} skips, not in years list")
            return True
            
        # -------------------------
        # DOWNLOAD (NEW: polite downloader)
        # We pass the base directory; downloader picks filename from URL and returns a list of local paths.
        # Limiting concurrency here per-dataset to 1 avoids server throttling (pool already parallelizes datasets).
        # -------------------------
        zip_dir = config.get_path(["loader", "sources", "zensus_2022", "path", "zip"])
        urls_or_path = dataset["url"]
        downloaded = utils.download_files(urls_or_path, base_path=zip_dir, max_concurrent=1)  # NEW
        if not downloaded or not downloaded[0]:
            log.error(f"Download failed for {dataset['name']}: {dataset['url']}")
            return False
        downloaded_zip = downloaded[0]

        # -------------------------
        # UNZIP (same behavior, new call signature)
        # Unzip into a dedicated folder per table_name (matches old structure).
        # -------------------------
        unzip_root = config.get_path(["loader", "sources", "zensus_2022", "path", "unzip"])
        folder_path = os.path.join(unzip_root, dataset["table_name"])
        utils.unzip(downloaded_zip, folder_path)  # NEW: unzip takes (zip_file, dest_dir)

        # Export to PostGIS for each configured resolution
        resolutions = config.get_value(["loader", "sources", "zensus_2022", "resolutions"])
        prefix = config.get_value(["loader", "sources", "zensus_2022", "prefix"])
        schema = config.get_value(["loader", "sources", "zensus_2022", "schema"])
        epsg = utils.get_db_parameters("postgres")["epsg"]  # target DB SRID

        for resolution in resolutions:
            log.info(f"Processing {dataset['name']} with {resolution} ...")

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

            # -------------------------
            # OPTIONAL local save (kept, simplified)
            # Since we now stream directly to DB, there is no GeoDataFrame in memory.
            # If you still want a local artifact, we can copy the source CSV to a processed folder.
            # -------------------------
            save_local = config.get_value(["loader", "sources", "zensus_2022", "save_local"])
            if save_local == "active":
                output_path = config.get_path(["loader", "sources", "zensus_2022", "path", "processed"])
                os.makedirs(output_path, exist_ok=True)
                out_csv = os.path.join(output_path, f"zensus-2022_{resolution}_{table_name}.csv")
                try:
                    shutil.copyfile(csv_path, out_csv)  # NEW: keep a copy for auditing/traceability
                    log.debug(f"Saved local CSV copy to {out_csv}")
                except Exception as e:
                    log.warning(f"Could not save local CSV copy for {table_name}: {e}")

            log.info(f"Processed successfully {csv_path}")

    except Exception as err:
        log.exception(f"An error occurred while processing file: {dataset.get('name')} {str(err)}")
        return False
    
    return True
