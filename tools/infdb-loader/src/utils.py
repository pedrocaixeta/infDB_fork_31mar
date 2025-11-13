import logging
import multiprocessing
import os
import io
import csv
import time
import random
import logging
import subprocess
import sqlalchemy
import psycopg2
from pathlib import Path
from typing import Iterable, List, Optional

import geopandas as gpd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from pySmartDL import SmartDL
from urllib.parse import urljoin, urlparse
from zipfile import BadZipFile, ZipFile

import chardet

from infdb import InfdbConfig
from infdb.utils import do_cmd, get_db_engine

# ============================== Constants ==============================

LOGGER_NAME: str = __name__
CONFIG_TOOL_NAME: str = "loader"
CONFIG_DIR: str = "configs"

HTTP_TIMEOUT_SECONDS: int = 60
WGET_PROGRESS_BAR: bool = True  # preserve SmartDL progress bar behavior

ZIP_EXT: str = ".zip"
GPKG_EXT: str = ".gpkg"
SQL_SCHEMA_GEOMETRY_COL: str = "geom"
EPSG_FALLBACK_KEY: str = "epsg"

# Module logger
log = logging.getLogger(LOGGER_NAME)

# Single shared config object per process
_cfg = InfdbConfig(tool_name=CONFIG_TOOL_NAME, config_path=CONFIG_DIR)


# ============================== Internal helpers ==============================

def _ensure_list(value) -> List:
    """Return value as list (wrap scalars); pass through lists unchanged."""
    if isinstance(value, list):
        return value
    return [value]


def _fetch_html(url: str) -> BeautifulSoup:
    """Fetch a URL and return a BeautifulSoup parser (html.parser)."""
    resp = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return BeautifulSoup(resp.content, "html.parser")


# ======================== toggles & config helpers ========================

def if_multiproccesing() -> bool:
    """Return True if multiprocessing is enabled via config (original spelling/API)."""
    status = _cfg.get_value([CONFIG_TOOL_NAME, "multiproccesing", "status"])
    return status == "active"


def if_multiprocessing() -> bool:
    """Correctly spelled alias for `if_multiproccesing()`; preserves public API."""
    return if_multiproccesing()


def if_active(service: str) -> bool:
    """Tell whether a given source service is active; logs decision.

    Args:
        service: Service key under `loader.sources`.

    Returns:
        True if active; False otherwise (with informational log).
    """
    status = _cfg.get_value([CONFIG_TOOL_NAME, "sources", service, "status"])
    if status == "active":
        log.info("Loading %s data...", service)
        return True
    log.info("%s skips, status not active", service)
    return False


def any_element_in_string(target_string: str, elements: Iterable[str]) -> bool:
    """Return True if any element is a substring of the target string."""
    return any(element in target_string for element in elements)


# ======================== downloading / scraping ========================

def get_links(url: str, ending: str, flt: str) -> list[str]:
    """Scrape links from a page matching an ending and substring filter.

    Args:
        url: Page URL to scrape.
        ending: Required file suffix (e.g., '.zip').
        flt: Case-insensitive substring that must appear in the href.

    Returns:
        List of absolute link URLs, de-duplicated.
    """
    soup = _fetch_html(url)
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(ending) and flt in href.lower():
            full_url = urljoin(url, href)
            if full_url not in links:
                links.append(full_url)
    log.debug(links)
    return links



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
    - Can also pass multiple URLs.
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


def unzip(zip_files, unzip_dir: str) -> None:
    """Extract one or more zip files into `unzip_dir`, skipping if already extracted.

    Args:
        zip_files: A single .zip path or list of .zip paths.
        unzip_dir: Destination directory for extracted files.
    """
    os.makedirs(unzip_dir, exist_ok=True)
    for zip_file in _ensure_list(zip_files):
        try:
            with ZipFile(zip_file, "r") as zf:
                members = zf.namelist()
                all_exist = all(os.path.exists(os.path.join(unzip_dir, m)) for m in members)
                if all_exist:
                    log.info("Skipping %s — all files already extracted.", zip_file)
                    continue
                log.info("Unzipping %s", zip_file)
                zf.extractall(unzip_dir)
        except BadZipFile as e:
            log.error("Error unzipping %s: %s", zip_file, e)


# =================== geospatial / DB import helpers ===================


def get_envelop():
    """Return the configured administrative envelope (GeoDataFrame filtered by AGS)."""
    scope = _cfg.get_value([CONFIG_TOOL_NAME, "scope"])
    if isinstance(scope, str):
        scope = [scope]
    ags_path = _cfg.get_path([CONFIG_TOOL_NAME, "sources", "bkg", "path", "unzip"], type="loader")
    log.debug("Envelop Path (unzipped): %s", ags_path)
    path = get_file(ags_path, filename="vg5000", ending=GPKG_EXT)
    log.debug("Envelop Path (file): %s", path)
    gdf = gpd.read_file(path, layer="vg5000_gem")
    return gdf[gdf["AGS"].str.startswith(tuple(scope or []))]

def get_all_envelops():
    """Return the configured administrative envelope (GeoDataFrame filtered by AGS)."""
    scope = _cfg.get_value([CONFIG_TOOL_NAME, "scope"])
    if isinstance(scope, str):
        scope = [scope]
    ags_path = _cfg.get_path([CONFIG_TOOL_NAME, "sources", "bkg", "path", "unzip"], type="loader")
    log.debug("Envelop Path (unzipped): %s", ags_path)
    path = get_file(ags_path, filename="vg5000", ending=GPKG_EXT)
    log.debug("Envelop Path (file): %s", path)
    gdf = gpd.read_file(path, layer="vg5000_gem")
    envelop = []
    for s in scope:
        envelop.append(gdf[gdf["AGS"].str.startswith(s)])
    return envelop



# ============================== file helpers ==============================

def get_all_files(folder_path: str, ending: str) -> list[str]:
    """Recursively collect all files under `folder_path` with the given ending."""
    files: list[str] = []
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.lower().endswith(ending):
                files.append(os.path.join(dirpath, filename))
    files.sort()
    return files


def get_file(folder_path: str, filename: str, ending: str) -> Optional[str]:
    """Return the newest file path in `folder_path` containing `filename` and ending with `ending`."""
    files = get_all_files(folder_path, ending)
    matching = [f for f in files if filename.lower() in f.lower()]
    if not matching:
        log.error("No files found containing '%s' with ending '%s' in %s", filename, ending, folder_path)
        return None
    newest = max(matching, key=os.path.getmtime)
    return newest


def get_website_links(url: str) -> list[str]:
    """Return all .zip links found on the given page (absolute or relative hrefs)."""
    soup = _fetch_html(url)
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(ZIP_EXT)]
    for link in links:
        log.debug(link)
    return links


def get_file_from_url(url: str):
    """Split a URL into (filename, stem, extension) triple."""
    path = urlparse(url).path
    filename = os.path.basename(path)
    name, extension = os.path.splitext(filename)
    return filename, name, extension


# ======================= encoding / processes =======================

def ensure_utf8_encoding(filepath: str) -> str:
    """Detect file encoding; if not UTF-8, re-encode to a temp UTF-8 CSV and return its path."""
    with open(filepath, "rb") as f:
        raw = f.read()
        result = chardet.detect(raw)
        source_encoding = result["encoding"]

    if source_encoding is None:
        raise ValueError(f"Could not detect encoding of file: {filepath}")

    if source_encoding.lower() != "utf-8":
        log.info("Re-encoding file from %s to UTF-8: %s", source_encoding, filepath)
        temp_path = filepath + "_utf8.csv"
        with open(filepath, "r", encoding=source_encoding, errors="replace") as src, \
             open(temp_path, "w", encoding="utf-8") as dst:
            for line in src:
                dst.write(line)
        return temp_path

    return filepath

def get_number_processes() -> int:
    """Determine worker process count based on CPU count and config max_cores."""
    number_processes = 1
    max_processes = _cfg.get_value([CONFIG_TOOL_NAME, "multiproccesing", "max_cores"]) or 1
    if _cfg.get_value([CONFIG_TOOL_NAME, "multiproccesing", "status"]) == "active":
        number_processes = min(multiprocessing.cpu_count(), max_processes)
    log.debug("Max processes: %s, Number of processes: %s", max_processes, number_processes)
    return number_processes



# -----------------------------------------------------------------------------
# Faster imports
# -----------------------------------------------------------------------------

def _pg_connstring_for_gdal():
    """
    Build a GDAL/OGR PostgreSQL connection string.
    ogr2ogr expects 'dbname', not 'db'.
    """
    p = _cfg.get_db_parameters("postgres")
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
    epsg = _cfg.get_db_parameters("postgres")["epsg"]
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
    params = _cfg.get_db_parameters("postgres")
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

