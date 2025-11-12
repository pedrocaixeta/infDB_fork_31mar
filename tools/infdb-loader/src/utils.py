# utils.py
# -----------------------------------------------------------------------------
# Purpose:
#   - Keep the "old" utils behavior but make heavy I/O paths much faster:
#       * import_layers(...)  -> uses GDAL/ogr2ogr with PG_USE_COPY (very fast)
#       * fast_copy_points_csv(...) -> raw PostgreSQL COPY + server-side geometry
#       * download_files(...) -> polite, throttled downloader with retries/backoff
#
# Notes:
#   - Minimal external moving parts; rely on mature tools (GDAL, psycopg2).
#   - Keep function names/signatures so existing code continues to work.
#   - Heavily commented so anyone can read/maintain quickly.
# -----------------------------------------------------------------------------

import os
import io
import csv
import time
import random
import logging
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import chardet
import multiprocessing
import sqlalchemy
import psycopg2
import geopandas as gpd

from zipfile import ZipFile, BadZipFile
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from . import config

log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Basic flags/helpers (kept from old utils)
# -----------------------------------------------------------------------------

def if_multiproccesing():
    """Return True if multiprocessing is enabled in config."""
    status = config.get_value(["loader", "multiproccesing", "status"])
    return status == "active"


def if_active(service):
    """Return True if a given source/service is active; log accordingly."""
    status = config.get_value(["loader", "sources", service, "status"])
    if status == "active":
        log.info(f"Loading {service} data...")
        return True
    else:
        log.info(f"{service} skips, status not active")
        return False


def any_element_in_string(target_string, elements):
    """Check if any element of `elements` is a substring of `target_string`."""
    return any(element in target_string for element in elements)


# -----------------------------------------------------------------------------
# HTML helpers (kept)
# -----------------------------------------------------------------------------

def get_links(url, ending, filter):
    """
    Scrape links ending with `ending` and containing `filter` (case-insensitive).
    Returns absolute URLs.
    """
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    out = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(ending) and filter in href.lower():
            full = urljoin(url, href)
            if full not in out:
                out.append(full)

    log.debug(out)
    return out


def get_website_links(url):
    """
    Scrape all .zip links on a page. Returns the raw hrefs as in the original
    util (so existing callers behave the same).
    """
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    zip_links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.endswith(".zip"):
            zip_links.append(href)

    for z in zip_links:
        log.debug(z)
    return zip_links


def get_file_from_url(url):
    """Return (filename, name_without_ext, extension) from a URL."""
    path = urlparse(url).path
    filename = os.path.basename(path)
    name, extension = os.path.splitext(filename)
    return filename, name, extension


# -----------------------------------------------------------------------------
# Downloader (new: throttled, retried, atomic)
# -----------------------------------------------------------------------------

def download_files(urls, base_path, max_concurrent=3, max_retries=4, backoff_base=1.8, timeout=60):
    """
    Polite, robust downloader that won't get throttled.
    - Limits parallelism with ThreadPoolExecutor (default 3).
    - Retries on 429/5xx with exponential backoff + jitter.
    - Saves atomically (write to .part then os.replace).
    - If `urls` is a single string, it is treated as list [urls].
    - Returns a list of absolute local file paths.

    Backward compatible with previous usage:
      old: download_files(url, base_path_dir)
      now: same signature; you can also pass multiple URLs.
    """
    if isinstance(urls, str):
        urls = [urls]

    os.makedirs(base_path, exist_ok=True)

    session = requests.Session() # Uses a persistent requests.Session() for connection reuse (reduces overhead).
    session.headers.update({"User-Agent": "IDP-Loader/1.0"})

    lock = threading.Lock() # Threading lock ensures multiple threads don’t append to results simultaneously (thread-safe).
    results = []

    def _dest_for(url):
        # Use URL basename as filename (matches historical behavior on many sites)
        name = os.path.basename(urlparse(url).path) or "download"
        return os.path.join(base_path, name)

    def _download_one(url):
        dest = _dest_for(url)
        tmp = dest + ".part"

        # Optionally check size via HEAD to skip re-downloads (best-effort)
        size = None
        try:
            r = session.head(url, timeout=timeout, allow_redirects=True)
            if r.ok and "content-length" in r.headers:
                size = int(r.headers["content-length"])
        except Exception:
            pass

        # Skip if file exists and Content-Length matches (if known)
        if os.path.exists(dest) and size and os.path.getsize(dest) == size:
            log.info(f"File already present: {dest}")
            return dest

        attempt = 0
        while True:
            try:
                with session.get(url, stream=True, timeout=timeout) as r:
                    if r.status_code in (429, 500, 502, 503, 504):
                        raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
                    r.raise_for_status()

                    # Stream chunks to disk
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(chunk_size=128 * 1024):
                            if chunk:
                                f.write(chunk)
                    os.replace(tmp, dest)
                    return dest

            except Exception as e:
                attempt += 1
                if attempt > max_retries:
                    log.error(f"Failed to download {url}: {e}")
                    try:
                        if os.path.exists(tmp):
                            os.remove(tmp)
                    except Exception:
                        pass
                    return None

                # Exponential backoff with a bit of randomness to avoid thundering herd
                sleep_s = (backoff_base ** attempt) + random.uniform(0, 0.25 * backoff_base)
                log.warning(f"Retry {attempt}/{max_retries} for {url} in {sleep_s:.1f}s due to {e}")
                time.sleep(sleep_s)

    with ThreadPoolExecutor(max_workers=max_concurrent) as ex:
        futs = [ex.submit(_download_one, u) for u in urls]
        for fut in as_completed(futs):
            path = fut.result()
            if path:
                with lock:
                    results.append(path)

    return results


# -----------------------------------------------------------------------------
# ZIP handling (kept, with small safety)
# -----------------------------------------------------------------------------

def unzip(zip_files, unzip_dir):
    """
    Unzip one or many ZIP files into `unzip_dir`.
    Skips work if all members already exist.
    """
    os.makedirs(unzip_dir, exist_ok=True)

    if isinstance(zip_files, str):
        zip_files = [zip_files]

    for zip_file in zip_files:
        try:
            with ZipFile(zip_file, "r") as zf:
                members = zf.namelist()
                all_exist = all(os.path.exists(os.path.join(unzip_dir, m)) for m in members)
                if all_exist:
                    log.info(f"Skipping {zip_file} — all files already extracted.")
                    continue

                log.info(f"Unzipping {zip_file}")
                zf.extractall(unzip_dir)

        except BadZipFile as e:
            log.error(f"Error unzipping {zip_file}: {e}")


# -----------------------------------------------------------------------------
# DB helpers (kept)
# -----------------------------------------------------------------------------

def sql_query(query):
    """Execute an arbitrary SQL string against the configured Postgres."""
    try:
        p = get_db_parameters("postgres")
        conn = psycopg2.connect(
            dbname=p["db"], user=p["user"], password=p["password"], host=p["host"], port=p["exposed_port"]
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(query)
        conn.close()
        log.debug(f"{query} executed successfully.")
    except Exception as error:
        log.error(f"ProgrammingError: {error}")


def do_cmd(cmd: str):
    """
    Run a shell command and stream stdout lines to logger.
    Useful for diagnosing external tool calls.
    """
    log.info(f"Executing command: {cmd}")
    process = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    if process.stdout:
        for line in process.stdout:
            log.info(line.rstrip())
    rc = process.wait()
    if rc == 0:
        log.info("Command completed successfully.")
    else:
        log.error(f"Command failed with return code {rc}")


def get_db_parameters(service_name: str):
    """
    Read DB/service parameters from config.
    If an 'infdb' section exists, overlay loader values.
    """
    parameters_loader = config.get_value(["loader", "hosts", service_name])

    dict_config = config.get_config()
    if "services" in dict_config:
        parameters = config.get_value(["services", service_name])
        log.debug(f"Using infdb configuration for: {service_name}")

        # Override with loader entries if present
        for key in parameters_loader.keys():
            if key == "host":
                parameters[key] = "host.docker.internal"  # default to local
            if parameters_loader[key] != "None":
                parameters[key] = parameters_loader[key]
                log.debug(f"Key overridden: key = {parameters_loader[key]}")
    else:
        parameters = parameters_loader
        log.debug(f"Using loader configuration for: {service_name}")

    for key in parameters.keys():
        if parameters[key] is None:
            log.error(f"Service '{service_name}' not found in configuration.")

    return parameters


def get_db_engine(service_name: str):
    """Return a SQLAlchemy engine for the given service."""
    p = get_db_parameters(service_name)
    db_url = f"postgresql://{p['user']}:{p['password']}@{p['host']}:{p['exposed_port']}/{p['db']}"
    return sqlalchemy.create_engine(db_url)


# -----------------------------------------------------------------------------
# File/encoding helpers (kept)
# -----------------------------------------------------------------------------

def ensure_utf8_encoding(filepath: str) -> str:
    """
    Return a UTF-8 path: either the original (if already UTF-8) or a new *_utf8.csv
    created with a safe transcode. Keeps old behavior.
    """
    with open(filepath, "rb") as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        source_encoding = result["encoding"]

    if source_encoding is None:
        raise ValueError(f"Could not detect encoding of file: {filepath}")

    if source_encoding.lower() != "utf-8":
        log.info(f"Re-encoding file from {source_encoding} to UTF-8: {filepath}")
        temp_path = filepath + "_utf8.csv"
        with open(filepath, "r", encoding=source_encoding, errors="replace") as src, \
             open(temp_path, "w", encoding="utf-8") as dst:
            for line in src:
                dst.write(line)
        return temp_path

    return filepath


def get_all_files(folder_path, ending):
    """Recursively collect all files whose names end with `ending`."""
    out = []
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.lower().endswith(ending):
                out.append(os.path.join(dirpath, filename))
    out.sort()
    return out


def get_file(folder_path, filename, ending):
    """
    Find the newest file under `folder_path` that contains `filename` and ends with `ending`.
    Returns a single path or None.
    """
    files = get_all_files(folder_path, ending)
    matching = [f for f in files if filename.lower() in f.lower()]
    if not matching:
        log.error(f"No files found containing '{filename}' with ending '{ending}' in {folder_path}")
        return None
    newest = max(matching, key=os.path.getmtime)
    log.debug(f"Selected file: {newest}")
    return newest


# -----------------------------------------------------------------------------
# Scope / envelope (kept)
# -----------------------------------------------------------------------------

def get_envelop():
    """
    Build a GeoDataFrame for the configured scope (list of AGS prefixes).
    Reads `vg5000_gem` from the GPKG found under the BKG unzip path.
    """
    scope = config.get_list(["loader", "scope"])
    ags_path = config.get_path(["loader", "sources", "bkg", "path", "unzip"])
    path = get_file(ags_path, filename="vg5000", ending=".gpkg")
    gdf = gpd.read_file(path, layer="vg5000_gem")
    gdf_scope = gdf[gdf["AGS"].str.startswith(tuple(scope))]
    return gdf_scope


# -----------------------------------------------------------------------------
# Faster imports
# -----------------------------------------------------------------------------

def _pg_connstring_for_gdal():
    """
    Build a GDAL/OGR PostgreSQL connection string.
    ogr2ogr expects 'dbname', not 'db'.
    """
    p = get_db_parameters("postgres")
    return f"PG:host={p['host']} port={p['exposed_port']} dbname={p['db']} user={p['user']} password={p['password']}"


def _ogr2ogr(cmd_args, env_extra=None):
    """
    Execute ogr2ogr with environment tuned for speed:
      - PG_USE_COPY=YES : streams via COPY (very fast)
      - OGR_ENABLE_PARTIAL_REPROJECTION=TRUE : small perf boost
    Handles spaces in arguments safely (no shell) and logs output line by line.
    Raises RuntimeError if ogr2ogr exits with non-zero code.
    """
    import shlex

    # Normalize input to a proper list of strings
    if isinstance(cmd_args, str):
        cmd_args = shlex.split(cmd_args)
    elif not isinstance(cmd_args, (list, tuple)) or not cmd_args:
        raise ValueError("ogr2ogr expects a non-empty list or command string")

    # Build environment
    env = os.environ.copy()
    env["PG_USE_COPY"] = "YES"
    env["OGR_ENABLE_PARTIAL_REPROJECTION"] = "TRUE"
    if env_extra:
        env.update(env_extra)

    # Log the command for debugging (shell-escaped for readability only)
    import shlex as _shlex
    log.info("Executing ogr2ogr: %s", _shlex.join(map(str, cmd_args)))

    # Run safely (no shell), capture stdout/stderr merged
    proc = subprocess.Popen(
        list(map(str, cmd_args)),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )

    for line in proc.stdout or []:
        log.info(line.rstrip())

    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(f"ogr2ogr failed with code {rc}")
    log.info("ogr2ogr completed successfully.")


def import_layers(input_file, layers, schema, prefix="", layer_names=None, scope=True, overwrite=True):
    """
    High-performance replacement for the old GeoPandas-based import:
      - Uses GDAL/ogr2ogr to load layers directly into PostGIS.
      - Reprojects on the fly to target DB EPSG.
      - Optionally clips to the configured scope (if available).
      - Uses COPY internally (PG_USE_COPY=YES) for fast ingest.

    Parameters:
      input_file : path to GPKG / any OGR-readable vector dataset
      layers     : list of source layer names in `input_file`
      schema     : destination Postgres schema
      prefix     : optional prefix for destination table names
      layer_names: optional override for destination table names (same length as layers)
      scope      : if True, clip to configured envelope (from get_envelop())
      overwrite  : if True, replace tables; else append

    This keeps the same function name so existing callers continue to work,
    but the internals are now much faster and simpler to maintain.
    """
    epsg = get_db_parameters("postgres")["epsg"]
    dst = _pg_connstring_for_gdal()

    # Destination names
    if layer_names is None:
        layer_names = layers
    if prefix:
        layer_names = [f"{prefix}_{name}" for name in layer_names]

    # Prepare a temporary GPKG for clip source if scope=True and envelope is present.
    clipsrc_opt = []
    tmp_gpkg = None
    if scope:
        gdf_scope = get_envelop()
        if gdf_scope is not None and not gdf_scope.empty:
            # Save scope to a temp gpkg in target CRS (avoid double reprojection inside pipeline)
            tmp_gpkg = os.path.splitext(os.path.abspath(input_file))[0] + "_clip_scope_tmp.gpkg"
            try:
                gdf_scope.to_crs(epsg=epsg).to_file(tmp_gpkg, layer="clip_scope", driver="GPKG")
                clipsrc_opt = ["-clipsrc", tmp_gpkg, "-clipsrclayer", "clip_scope"]
            except Exception as e:
                log.warning(f"Clip scope could not be prepared; proceeding without clip. Reason: {e}")
                clipsrc_opt = []
        else:
            log.info("Scope is empty; skipping clip.")

    for src_layer, dst_name in zip(layers, layer_names):
        log.info(f"Importing layer '{src_layer}' -> {schema}.{dst_name}")
        args = [
            "ogr2ogr",
            "-progress",
            "-f", "PostgreSQL", dst,
            input_file,
            "-nln", f"{schema}.{dst_name}",         # destination schema.table
            "-nlt", "PROMOTE_TO_MULTI",             # robust geometry typing
            "-lco", "GEOMETRY_NAME=geom",           # standardize geom column name
            "-t_srs", f"EPSG:{epsg}",               # on-the-fly reprojection
            "-makevalid",                           # cheap geometry fixups
        ] + (["-overwrite"] if overwrite else ["-append"]) \
          + clipsrc_opt \
          + ["-where", "1=1", src_layer]           # '-where 1=1' keeps full layer, but keeps the shape of call
        _ogr2ogr(args)

    if tmp_gpkg and os.path.exists(tmp_gpkg):
        try:
            os.remove(tmp_gpkg)
        except Exception:
            pass


def fast_copy_points_csv(
    csv_path: str,
    schema: str,
    table_name: str,
    x_col: str,
    y_col: str,
    srid_src: int = 3035,
    srid_dst: int = None,
    drop_existing: bool = True,
    create_spatial_index: bool = True,
):
    """
    Super-fast CSV -> PostGIS path for point datasets (e.g., Zensus):
      1) COPY raw CSV into an UNLOGGED staging table with text columns
      2) Create final table with typed columns + geometry built server-side
      3) Reproject in PostGIS (ST_Transform) and add a GiST index
      4) ANALYZE for fresh planner stats

    Why it's fast:
      - COPY is orders of magnitude faster than row-by-row inserts
      - Geometry & reprojection are done in C inside PostGIS (not Python)

    Parameters:
      csv_path  : path to the semicolon-separated CSV file
      schema    : destination schema
      table_name: destination table name (without schema)
      x_col,y_col: column names for source X and Y (e.g., x_mp_100m / y_mp_100m)
      srid_src  : SRID of the raw X/Y values (Zensus uses EPSG:3035)
      srid_dst  : target SRID (defaults to DB EPSG from config)
      drop_existing: drop destination table if it exists
      create_spatial_index: create GiST index on geom
    """
    params = get_db_parameters("postgres")
    srid_dst = srid_dst or params["epsg"]

    # Peek CSV header to build a generic TEXT staging table (robust to weird types) read first line (column names)
    with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
    header_l = [h.strip().lower() for h in header]

    if x_col.lower() not in header_l or y_col.lower() not in header_l:
        raise ValueError(f"Missing {x_col}/{y_col} in CSV header.")

    staging = f"{table_name}__staging"

    conn = psycopg2.connect(
        dbname=params["db"], user=params["user"], password=params["password"],
        host=params["host"], port=params["exposed_port"]
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Clean destination & staging if requested
    if drop_existing:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE;')
    cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{staging}" CASCADE;')

    # Create UNLOGGED staging table (faster, fine for transient step)
    cols_sql = ", ".join(f'"{c}" text' for c in header_l)
    cur.execute(f'CREATE UNLOGGED TABLE "{schema}"."{staging}" ({cols_sql});')

    # COPY directly from file (semicolon-delimited; honor header)
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        cur.copy_expert(
            f'COPY "{schema}"."{staging}" ({", ".join(f"""\"{c}\"""" for c in header_l)}) '
            f"FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true, QUOTE '\"', ESCAPE '\"');",
            f
        )

    # Build final table with geometry in one SQL
    casts = []
    for c in header_l:
        if c in (x_col.lower(), y_col.lower()):
            casts.append(f'"{c}"::double precision as "{c}"')
        else:
            # Keep other columns as-is (text); extend here if you want typed casts
            casts.append(f'"{c}"')

    select_cols = ", ".join(casts)
    cur.execute(f'''
        CREATE TABLE "{schema}"."{table_name}" AS
        SELECT
            {select_cols},
            ST_Transform(ST_SetSRID(ST_MakePoint("{x_col}"::double precision, "{y_col}"::double precision), {srid_src}), {srid_dst})::geometry(Point,{srid_dst}) AS geom
        FROM "{schema}"."{staging}";
    ''')

    # Optional GiST index + ANALYZE for better query performance
    if create_spatial_index:
        cur.execute(f'CREATE INDEX "{table_name}_geom_gix" ON "{schema}"."{table_name}" USING GIST (geom);')
    cur.execute(f'ANALYZE "{schema}"."{table_name}";')

    # Drop staging now that the final table exists
    cur.execute(f'DROP TABLE "{schema}"."{staging}";')

    cur.close()
    conn.close()
    log.info(f'Loaded {csv_path} -> {schema}.{table_name} via COPY + server-side geometry.')


# -----------------------------------------------------------------------------
# CPU count helper (kept)
# -----------------------------------------------------------------------------

def get_number_processes():
    """
    Get the maximum number of processes to use based on the configuration.
    Returns at least 1.
    """
    number_processes = 1
    max_processes = config.get_value(["loader", "multiproccesing", "max_cores"])

    if config.get_value(["loader", "multiproccesing", "status"]) == "active":
        number_processes = min(multiprocessing.cpu_count(), max_processes)

    log.debug(f"Max processes: {max_processes}, Number of processes: {number_processes}")
    return number_processes
