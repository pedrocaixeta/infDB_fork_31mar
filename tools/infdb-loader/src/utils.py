import os
import io
import time
import logging
import shutil
import subprocess
import multiprocessing

import requests
from zipfile import ZipFile, BadZipFile
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

import pandas as pd
import geopandas as gpd

import sqlalchemy
from sqlalchemy import text

import psycopg2            
import chardet             
from pySmartDL import SmartDL

from concurrent.futures import ThreadPoolExecutor, as_completed
from . import config


log = logging.getLogger(__name__)


def if_multiproccesing():
    status = config.get_value(["loader", "multiproccesing", "status"])
    if status == "active":
        return True
    else:
        return False


def if_active(service):
    status = config.get_value(["loader", "sources", service, "status"])
    if status == "active":
        log.info(f"Loading {service} data...")
        return True
    else:
        log.info("{service} skips, status not active")
        return False


def any_element_in_string(target_string, elements):
    return any(element in target_string for element in elements)


def get_links(url, ending, filter):
    response = requests.get(url)
    html_content = response.content

    # HTML-Seite parsen
    soup = BeautifulSoup(html_content, "html.parser")

    # Alle Links zu ZIP-Dateien finden
    zip_links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.endswith(ending):
            # Check for filters
            if filter in href.lower():
                # Append if url not already in list
                full_url = urljoin(url, link["href"])
                if full_url not in zip_links:
                    zip_links.append(full_url)

    # Gefundene Links ausgeben
    log.debug(zip_links)

    return zip_links


def download_files(urls, base_path, max_concurrent=6, retries=3, delay=2):
    # If base_path looks like a file path (ends with .zip, .gpkg, etc.), use its folder instead
    if os.path.splitext(base_path)[1]:
        base_path = os.path.dirname(base_path) or "."

    os.makedirs(base_path, exist_ok=True)
    if isinstance(urls, str):
        urls = [urls]

    # simple, high-throughput streaming downloader with retry
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    sess = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=delay,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=max_concurrent, pool_maxsize=max_concurrent)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)

    def _stream_download(url):
        dest = os.path.join(base_path, os.path.basename(url))

        # Safety: if a directory exists with same name, remove it
        if os.path.isdir(dest):
            log.warning(f"Found a directory where a file should be ({dest}), removing it.")
            shutil.rmtree(dest, ignore_errors=True)

        if os.path.isfile(dest):
            log.info(f"File {dest} already exists, skipping download.")
            return dest

        log.info(f"Downloading {url} -> {dest}")
        with sess.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 16):  # 64 KB
                    if chunk:
                        f.write(chunk)
        return dest

    results = []
    with ThreadPoolExecutor(max_workers=max_concurrent) as pool:
        futures = {pool.submit(_stream_download, u): u for u in urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                results.append(future.result())
                log.info(f"✅ Downloaded {url}")
            except Exception as e:
                log.error(f"❌ Failed {url}: {e}")
    return results


def unzip(zip_files, unzip_dir):
    os.makedirs(unzip_dir, exist_ok=True)

    if isinstance(zip_files, str):
        zip_files = [zip_files]

    for zip_file in zip_files:
        try:
            with ZipFile(zip_file, "r") as zip_ref:
                members = zip_ref.namelist()
                # Check if all files already exist
                all_exist = all(
                    os.path.exists(os.path.join(unzip_dir, member))
                    for member in members
                )

                if all_exist:
                    log.info(f"Skipping {zip_file} — all files already extracted.")
                    continue

                log.info(f"Unzipping {zip_file}")
                zip_ref.extractall(unzip_dir)
        except BadZipFile as e:
            log.error(f"Error unzipping {zip_file}: {e}")


def sql_query(query):
    try:
        # Connect to the PostgreSQL database-data-import-container
        parameters = get_db_parameters("postgres")
        host = parameters["host"]
        user = parameters["user"]
        password = parameters["password"]
        db = parameters["db"]
        port = parameters["exposed_port"]

        connection = psycopg2.connect(
            dbname=db, user=user, password=password, host=host, port=port
        )
        cursor = connection.cursor()
        # # Create the users table
        cursor.execute(query)
        connection.commit()

        # Close the connection
        cursor.close()
        connection.close()
        log.debug(f"{query} executed successfully.")

    except Exception as error:
        log.error(f"ProgrammingError: {error}")


def do_cmd(cmd: str):
    log.info(f"Executing command: {cmd}")

    process = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # Zeilenweise Puffern
    )

    # Zeilenweise lesen und direkt loggen
    if process.stdout:
        for line in process.stdout:
            log.info(line.rstrip())

    # Warten bis der Prozess beendet ist
    return_code = process.wait()
    if return_code == 0:
        log.info("Command completed successfully.")
    else:
        log.error(f"Command failed with return code {return_code}")


def import_layers(input_file, layers, schema, prefix="", layer_names=None, scope=True):
    # Get connection details from config, then override from .env if present
    parameters = get_db_parameters("postgres") or {}

    # Prefer environment (.env) values for quick testing
    env_user = os.getenv("SERVICES_POSTGRES_USER")
    env_password = os.getenv("SERVICES_POSTGRES_PASSWORD")
    env_db = os.getenv("SERVICES_POSTGRES_DB")
    env_port = os.getenv("SERVICES_POSTGRES_EXPOSED_PORT")
    env_epsg = os.getenv("SERVICES_POSTGRES_EPSG")
    env_host = os.getenv("SERVICES_POSTGRES_HOST")  # optional if you set it

    user = env_user or parameters.get("user")
    password = env_password or parameters.get("password")
    db = env_db or parameters.get("db")
    port = int(env_port) if env_port else parameters.get("exposed_port") or parameters.get("port") or 5432
    host = env_host or parameters.get("host") or "host.docker.internal"
    epsg = int(env_epsg) if env_epsg else parameters.get("epsg")

    # Build PG connection string WITHOUT password; pass password via PGPASSWORD env
    connection_string = f"PG:host={host} port={port} dbname={db} user={user}"
    postgres_engine = get_db_engine("postgres")

    # get the bounding features (GeoDataFrame) that define the area you want to import.
    if scope:
        gdf_scope = get_envelop()
    else:
        gdf_scope = None

    # handle clipping if scope is active
    clipsrc = None
    if gdf_scope is not None and not gdf_scope.empty:
        xmin, ymin, xmax, ymax = gdf_scope.total_bounds
        clipsrc = [str(xmin), str(ymin), str(xmax), str(ymax)]

    if layer_names is None:
        layer_names = layers

    # Add prefix
    if prefix:
        layer_names = [prefix + "_" + name for name in layer_names]

    # prepare env for subprocess with password for libpq
    proc_env = os.environ.copy()
    if password:
        proc_env["PGPASSWORD"] = str(password)

    for layer, layer_name in zip(layers, layer_names):
        log.info(f"Importing layer: {layer} into {schema}")
        cmd = [
            "ogr2ogr", "-f", "PostgreSQL", connection_string,
            input_file,
            "-nln", f"{schema}.{layer_name}",
            "-nlt", "PROMOTE_TO_MULTI",
            "-lco", "GEOMETRY_NAME=geom",
            "-overwrite",
            "-t_srs", f"EPSG:{epsg}",
            "-progress"
        ]
        if clipsrc:
            cmd += ["-clipsrc"] + clipsrc
        cmd += ["-sql", f"SELECT * FROM \"{layer}\""]

        try:
            proc = subprocess.run(cmd, check=True, capture_output=True, text=True, env=proc_env, timeout=1800)
            log.debug("ogr2ogr stdout: %s", proc.stdout)
        except subprocess.CalledProcessError as e:
            log.error("ogr2ogr failed (rc=%s). stderr:\n%s", e.returncode, e.stderr)
            raise


def _upload_to_postgis(
    df,
    table_name: str,
    schema: str,
    engine,
    srid: int = 3035,
    x_col: str = None,
    y_col: str = None,
    src_srid: int = None,
    if_exists: str = "replace",
    create_spatial_index: bool = True,
    analyze_after: bool = False,          # <- set False to save time
    unlogged: bool = True,                # <- speed up bulk load
    synchronous_commit_off: bool = True,  # <- speed up bulk load
):
    if not x_col or not y_col:
        raise ValueError("Provide x_col and y_col for geometry creation.")

    src_srid = src_srid or srid

    df = df.copy()
    df[x_col] = pd.to_numeric(df[x_col], errors="coerce")
    df[y_col] = pd.to_numeric(df[y_col], errors="coerce")

    def sql_type(series: pd.Series) -> str:
        if pd.api.types.is_integer_dtype(series): return "BIGINT"
        if pd.api.types.is_float_dtype(series):   return "DOUBLE PRECISION"
        if pd.api.types.is_bool_dtype(series):    return "BOOLEAN"
        return "TEXT"

    cols = list(df.columns)
    col_defs = ", ".join([f'"{c}" {sql_type(df[c])}' for c in cols])

    target = f'"{schema}"."{table_name}"'
    staging = f'"{schema}"."{table_name}__staging"'

    # CSV buffer
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            # schema and staging
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
            cur.execute(f"DROP TABLE IF EXISTS {staging};")

            unlogged_sql = "UNLOGGED" if unlogged else ""
            cur.execute(f"CREATE {unlogged_sql} TABLE {staging} ({col_defs});")

            if synchronous_commit_off:
                cur.execute("SET LOCAL synchronous_commit = OFF;")

            # COPY into staging
            col_list = ", ".join([f'"{c}"' for c in cols])
            cur.copy_expert(
                f"COPY {staging} ({col_list}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')",
                buf
            )

            # add geom column
            cur.execute(f'ALTER TABLE {staging} ADD COLUMN "geom" geometry(Point, {srid});')
            if src_srid == srid:
                cur.execute(
                    f'UPDATE {staging} SET "geom" = ST_SetSRID(ST_MakePoint("{x_col}", "{y_col}"), {srid});'
                )
            else:
                cur.execute(
                    f'UPDATE {staging} SET "geom" = ST_Transform(ST_SetSRID(ST_MakePoint("{x_col}", "{y_col}"), {src_srid}), {srid});'
                )

            # swap into final
            if if_exists == "replace":
                cur.execute(f"DROP TABLE IF EXISTS {target};")
                cur.execute(f'ALTER TABLE {staging} RENAME TO "{table_name}";')
            else:
                # append mode: copy staging rows into target, then drop staging
                cur.execute(f"INSERT INTO {target} SELECT * FROM {staging};")
                cur.execute(f"DROP TABLE {staging};")

        raw.commit()
    finally:
        raw.close()

    # index + analyze in a separate transaction (optional: skip for 100m)
    with engine.begin() as conn:
        if create_spatial_index:
            conn.execute(text(
                f'CREATE INDEX IF NOT EXISTS "{table_name}_geom_gix" '
                f'ON "{schema}"."{table_name}" USING GIST ("geom");'
            ))
        if analyze_after:
            conn.execute(text(f'ANALYZE {target};'))

def get_envelop():
    scope = config.get_list(["loader", "scope"])
    ags_path = config.get_path(["loader", "sources", "bkg", "path", "unzip"])
    log.debug(f"Envelop Path: {ags_path}")
    path = get_file(ags_path, filename="vg5000", ending=".gpkg")
    log.debug(f"Envelop Path: {path}")
    gdf = gpd.read_file(path, layer="vg5000_gem")

    gdf_scope = gdf[gdf["AGS"].str.startswith(tuple(scope))]

    return gdf_scope


def get_db_parameters(service_name: str):
    parameters_loader = config.get_value(["loader", "hosts", service_name])

    # Adopt settings if config-infdb exists
    dict_config = config.get_config()
    if "services" in dict_config:
        parameters = config.get_value(["services", service_name])
        log.debug(f"Using infdb configuration for: {service_name}")

        # Override config-infdb by config-loader
        keys = parameters_loader.keys()
        for key in keys:
            if key == "host":
                parameters[key] = "host.docker.internal"  # default to localhost

            if parameters_loader[key] != "None":
                parameters[key] = parameters_loader[key]
                log.debug(f"Key overridden: key = {parameters_loader[key]}")
    else:
        # Use settings from config-loader
        parameters = parameters_loader
        log.debug(f"Using loader configuration for: {service_name}")

    # Check if parameters are found
    for key in parameters.keys():
        if parameters[key] is None:
            log.error(f"Service '{service_name}' not found in configuration.")

    return parameters


def get_db_engine(service_name: str):
    parameters = get_db_parameters(service_name)
    host = parameters["host"]
    user = parameters["user"]
    password = parameters["password"]
    db = parameters["db"]
    port = parameters["exposed_port"]

    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = sqlalchemy.create_engine(db_url)

    return engine


def ensure_utf8_encoding(filepath: str) -> str:
    with open(filepath, "rb") as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        source_encoding = result["encoding"]

    if source_encoding is None:
        raise ValueError(f"Could not detect encoding of file: {filepath}")

    if source_encoding.lower() != "utf-8":
        # Re-encode the file to UTF-8
        log.info(f"Re-encoding file from {source_encoding} to UTF-8: {filepath}")
        temp_path = filepath + "_utf8.csv"
        with (
            open(filepath, "r", encoding=source_encoding, errors="replace") as src,
            open(temp_path, "w", encoding="utf-8") as dst,
        ):
            for line in src:
                dst.write(line)
        return temp_path

    return filepath  # already UTF-8


def get_all_files(folder_path, ending):
    """
    Recursively finds all ending files in the given folder and its subfolders.

    Parameters:
        folder_path (str): Path to the top-level folder.

    Returns:
        List[str]: List of full paths to .csv files.
    """
    csv_files = []
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.lower().endswith(ending):
                csv_files.append(os.path.join(dirpath, filename))
    csv_files.sort()
    return csv_files


def get_file(folder_path, filename, ending):
    """
    Recursively finds all ending files in the given folder and its subfolders.

    Parameters:
        folder_path (str): Path to the top-level folder.

    Returns:
        List[str]: List of full paths to .csv files.
    """
    files = get_all_files(folder_path, ending)

    # Filter files that contain the filename
    matching_files = [f for f in files if filename.lower() in f.lower()]

    if not matching_files:
        log.error(
            f"No files found containing '{filename}' with ending '{ending}' in {folder_path}"
        )
        return None

    # Pick the newest by modification time
    newest_file = max(matching_files, key=os.path.getmtime)

    log.debug(f"Envelop Path: {newest_file}")
    return newest_file


def get_website_links(url):
    log.info(f"Fetching Zensus link list from: {url}")
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Failed to fetch {url}: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")

    zip_links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.endswith(".zip"):
            zip_links.append(href)

    log.info(f"Found {len(zip_links)} .zip links on the page")
    for z in zip_links:
        log.debug(z)

    return zip_links


def get_file_from_url(url):
    # Extract filename from url
    path = urlparse(url).path
    filename = os.path.basename(path)
    name, extension = os.path.splitext(filename)
    return filename, name, extension


def get_number_processes():
    """
    Get the maximum number of processes to use based on the configuration.
    """
    number_processes = 1
    max_processes = config.get_value(["loader", "multiproccesing", "max_cores"])

    if config.get_value(["loader", "multiproccesing", "status"]) == "active":
        number_processes = min(multiprocessing.cpu_count(), max_processes)

    log.debug(
        f"Max processes: {max_processes}, Number of processes: {number_processes}"
    )

    return number_processes
