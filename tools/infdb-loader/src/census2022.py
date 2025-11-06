import os
import logging
import multiprocessing
import chardet
import pandas as pd
import geopandas as gpd
from . import utils, config, logger
from charset_normalizer import from_path

log = logging.getLogger(__name__)


def load(log_queue):
    logger.setup_worker_logger(log_queue)

    if not utils.if_active("zensus_2022"):
        return
    
    datasets = config.get_value(["loader", "sources", "zensus_2022", "datasets"])

    url = config.get_value(["loader", "sources", "zensus_2022", "url"])
    zip_links = utils.get_website_links(url)

    # Validate links
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
    
    # Create folders
    zip_path = config.get_path(["loader", "sources", "zensus_2022", "path", "zip"])
    os.makedirs(zip_path, exist_ok=True)
    unzip_path = config.get_path(["loader", "sources", "zensus_2022", "path", "unzip"])
    os.makedirs(unzip_path, exist_ok=True)

    # >>> NEW: pre-download all active Zensus archives once (moderate concurrency)
    years = config.get_value(["loader", "sources", "zensus_2022", "years"])
    active_urls = [
        d["url"] for d in datasets
        if d.get("status") == "active" and d.get("year") in years
    ]
    log.info(f"Pre-downloading {len(active_urls)} Zensus archives to {zip_path} with max_concurrent=6 …")
    utils.download_files(active_urls, zip_path, max_concurrent=6)
    log.info("Pre-download complete.")

    # Serial vs Pool (spawn) depending on config
    number_processes = utils.get_number_processes()  # returns 1 when config is 'not-active'
    if number_processes <= 1:
        # Run serially (no nested multiprocessing inside the census process)
        for d in datasets:
            process_dataset(d)
    else:
        # Safe Pool with spawn + short-lived workers to avoid resource buildup
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(
            processes=number_processes,
            initializer=logger.setup_worker_logger,
            initargs=(log_queue,),
            maxtasksperchild=1,
        ) as pool:
            pool.map(process_dataset, datasets)


def process_dataset(dataset):
    try:
        log.info(f"Working on {dataset['name']}")

        # --- Skip inactive or invalid years ---
        if dataset["status"] != "active":
            log.info(f"{dataset['name']} skips, status not active")
            return True

        years = config.get_value(["loader", "sources", "zensus_2022", "years"])
        if dataset["year"] not in years:
            log.info(f"{dataset['name']} skips, not in years list")
            return True

        # --- Unzip (use pre-downloaded file) ---
        zip_path = config.get_path(["loader", "sources", "zensus_2022", "path", "zip"])
        unzip_path = config.get_path(["loader", "sources", "zensus_2022", "path", "unzip"])
        os.makedirs(unzip_path, exist_ok=True)

        zip_file = os.path.join(zip_path, os.path.basename(dataset["url"]))
        if not os.path.exists(zip_file):
            log.error(f"Expected ZIP not found: {zip_file} — did the pre-download succeed?")
            return False

        folder_path = os.path.join(unzip_path, dataset["table_name"])
        log.info(f"Unzipping {zip_file} -> {folder_path}")
        utils.unzip(zip_file, folder_path)

        # --- Process each resolution file ---
        resolutions = config.get_value(["loader", "sources", "zensus_2022", "resolutions"])
        for resolution in resolutions:
            log.info(f"Processing {dataset['name']} with {resolution} …")

            file = utils.get_file(folder_path, resolution, ".csv")
            if not file:
                log.warning(f"No file for {dataset['name']} with resolution {resolution} found")
                continue

            # --- Read CSV (fast sniff: ~1 KB) ---
            with open(file, "rb") as fh:
                encoding = chardet.detect(fh.read(1000)).get("encoding") or "utf-8"
            try:
                df = pd.read_csv(file, sep=";", decimal=",", low_memory=True, encoding=encoding)
            except UnicodeDecodeError:
                with open(file, "r", encoding="latin-1", errors="replace") as fh:
                    df = pd.read_csv(fh, sep=";", decimal=",", low_memory=True)

            df.fillna(0, inplace=True)
            df.columns = df.columns.str.lower()
            log.info(f"Columns in '{dataset['name']}' ({resolution}): {list(df.columns)}")

            # --- Detect coordinate columns dynamically ---
            x_col = next((c for c in df.columns if c.startswith("x_mp_")), None)
            y_col = next((c for c in df.columns if c.startswith("y_mp_")), None)
            if not x_col or not y_col:
                log.warning(f"Missing x_mp_*/y_mp_* in {file}, skipping.")
                continue
            log.info(f"Using columns '{x_col}' and '{y_col}' for geometry.")

            # --- Upload via fast COPY path ---
            engine = utils.get_db_engine("postgres")
            prefix = config.get_value(["loader", "sources", "zensus_2022", "prefix"])
            schema = config.get_value(["loader", "sources", "zensus_2022", "schema"])
            epsg = utils.get_db_parameters("postgres")["epsg"]
            table_name = f"{prefix}_{dataset['year']}_{resolution}_{dataset['table_name']}"

            is_100m = (resolution == "100m")
            is_1km  = (resolution == "1km")
            is_10km = (resolution == "10km")

            utils._upload_to_postgis(
                df, table_name, schema, engine,
                srid=epsg, x_col=x_col, y_col=y_col, src_srid=3035,
                if_exists="replace",
                create_spatial_index=not is_100m,  # skip index for 100m
                analyze_after=False,               # skip analyze for speed
                unlogged=True,
                synchronous_commit_off=True,
            )

            # --- Optional: save processed CSV/GeoPackage locally ---
            save_local = config.get_value(["loader", "sources", "zensus_2022", "save_local"])
            if save_local == "active":
                output_path = config.get_path(["loader", "sources", "zensus_2022", "path", "processed"])
                os.makedirs(output_path, exist_ok=True)
                out_csv = os.path.join(output_path, f"zensus-2022_{resolution}_{table_name}.csv")
                df.to_csv(out_csv, index=False)
                log.info(f"Saved processed CSV: {out_csv}")

            log.info(f"Processed successfully {file}")

    except Exception as err:
        log.exception(f"An error occurred while processing {dataset['name']}: {err}")
        return False

    return True