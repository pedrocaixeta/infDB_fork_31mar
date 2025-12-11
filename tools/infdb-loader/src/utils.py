import multiprocessing
import os
import csv
import time
import random
import logging
import subprocess
import psycopg2
import shlex
from typing import Iterable, List, Optional
from pathlib import Path

import geopandas as gpd

import requests
from bs4 import BeautifulSoup
from pySmartDL import SmartDL
from urllib.parse import urljoin, urlparse
from zipfile import BadZipFile, ZipFile

import chardet

from infdb import InfDB, InfdbConfig
from infdb.utils import do_cmd as infdb_do_cmd

# ============================== Constants ==============================
HTTP_TIMEOUT_SECONDS: int = 60
WGET_PROGRESS_BAR: bool = True  # preserve SmartDL progress bar behavior

GPKG_EXT: str = ".gpkg"
SQL_SCHEMA_GEOMETRY_COL: str = "geom"

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

def _pg_connstring_for_gdal(infdb: InfDB) -> str:
    """
    Build a GDAL/OGR PostgreSQL connection string.
    ogr2ogr expects 'dbname', not 'db'.
    """
    # use InfDB helper that returns merged DB params
    params = infdb.get_db_parameters_dict()
    return (
        f"PG:host={params['host']} "
        f"port={params['exposed_port']} "
        f"dbname={params['db']} "
        f"user={params['user']} "
        f"password={params['password']}"
    )


def _pg_connstring_for_psql(infdb: InfDB) -> str:
    """
    Build a PostgreSQL connection string for psql/libpq tools (URI format).
    Used by: psql, raster2pgsql, pg_dump, pg_restore, etc.
    """
    params = infdb.get_db_parameters_dict()
    return (
        f"postgresql://{params['user']}:{params['password']}"
        f"@{params['host']}:{params['exposed_port']}/{params['db']}"
    )

def _ogr2ogr(cmd_args, infdb, env_extra=None):
    """
    Execute ogr2ogr with environment tuned for speed:
      - PG_USE_COPY=YES : streams via COPY (very fast)
      - OGR_ENABLE_PARTIAL_REPROJECTION=TRUE : small perf boost
    Handles spaces in arguments safely (no shell) and logs output line by line.
    Raises RuntimeError if ogr2ogr exits with non-zero code.
    """
    log = infdb.get_worker_logger()

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
    log.info("Executing ogr2ogr: %s", shlex.join(map(str, cmd_args)))

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



# ======================== toggles & config helpers ========================


def if_multiprocesing(infdb: InfDB) -> bool:
    """Return True if multiprocessing is enabled via config (original spelling/API)."""
    status = infdb.get_config_value([infdb.get_toolname(), "multiproccesing", "status"])
    return status == "active"


def if_active(service: str, infdb: InfDB) -> bool:
    """Tell whether a given source service is active; logs decision.

    Args:
        service: Service key under `loader.sources`.

    Returns:
        True if active; False otherwise (with informational log).
    """
    status = infdb.get_config_value([infdb.get_toolname(), "sources", service, "status"])
    log = infdb.get_worker_logger()
    if status == "active":
        log.info("Loading %s data...", service)
        return True
    log.info("%s skips, status not active", service)
    return False


def any_element_in_string(target_string: str, elements: Iterable[str]) -> bool:
    """Return True if any element is a substring of the target string."""
    return any(element in target_string for element in elements)


# ======================== downloading / scraping ========================


def get_links(url: str, ending: str, flt: str, infdb: InfDB) -> list[str]:
    """Scrape links from a page matching an ending and substring filter.

    Args:
        url: Page URL to scrape.
        ending: Required file suffix (e.g., '.zip').
        flt: Case-insensitive substring that must appear in the href.

    Returns:
        List of absolute link URLs, de-duplicated.
    """
    log = infdb.get_worker_logger()
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


def _requests_download(
    url: str,
    dest_dir: str,
    infdb: InfDB,
    username: str,
    access_token: str,
    timeout=60,
    max_retries=5,
    backoff_base=1.5,
    chunk=1024 * 1024,
) -> str:
    """HEAD (size if available) → streamed GET with retries/backoff."""
    os.makedirs(dest_dir, exist_ok=True)

    # filename from URL path
    filename = os.path.basename(urlparse(url).path) or "download"
    dest = os.path.join(dest_dir, filename)
    log = infdb.get_worker_logger()

    auth = (username, access_token)
    session = requests.Session()
    session.auth = auth

    # HEAD: get size if available
    size = None
    try:
        with session.head(url, allow_redirects=True, timeout=timeout) as r:
            if r.ok and "content-length" in r.headers:
                size = int(r.headers["content-length"])
    except Exception as exc:
        log.exception("Exception occurred during _requests_download(): %s", exc)

    # short-circuit if already present with same size
    if size and os.path.exists(dest) and os.path.getsize(dest) == size:
        log.info("File %s already exists (size match).", dest)
        return dest
    if os.path.exists(dest):
        log.info("File %s already exists (size unknown); skipping re-download.", dest)
        return dest

    # GET with retries
    for attempt in range(max_retries + 1):
        try:
            with session.get(url, stream=True, timeout=timeout) as resp:
                resp.raise_for_status()
                tmp = dest + ".part"
                with open(tmp, "wb") as f:
                    for b in resp.iter_content(chunk_size=chunk):
                        if b:
                            f.write(b)
                if size and os.path.getsize(tmp) != size:
                    raise IOError(f"Size mismatch: expected {size}, got {os.path.getsize(tmp)}")
                os.replace(tmp, dest)
                log.info("Downloaded %s", dest)
                return dest
        except Exception as e:
            if attempt >= max_retries:
                # don’t leak creds in logs
                log.error("Download failed for %s: %s", url, e.__class__.__name__)
                raise
            sleep_s = (backoff_base**attempt) + random.uniform(0, 0.25 * backoff_base)
            log.warning("Retry %d/%d for %s in %.1fs", attempt + 1, max_retries, url, sleep_s)
            time.sleep(sleep_s)

    session.close()


def download_files(
    urls, file_path: str, infdb: InfDB, protocol: str = "http", username: str = None, access_token: str = None
) -> list[str]:
    """
    If `webdav` provided → use requests (supports WebDAV basic auth).
    Else → use SmartDL (your current async flow).
    """
    # Create base path if base_path is supposed to be a directory
    filename, name, extension = get_file_from_url(file_path)
    log = infdb.get_worker_logger()
    if extension:
        base_path = os.path.dirname(file_path)
    else:
        base_path = file_path
    os.makedirs(base_path, exist_ok=True)

    url_list = _ensure_list(urls)

    # Auth path (WebDAV or protected HTTP)
    if protocol == "webdav":
        if not username or not access_token:
            raise ValueError("Username and access_token required when protocol=webdav")
        results = []
        for url in url_list:
            results.append(_requests_download(url, base_path, infdb, username=username, access_token=access_token))
        return results

    # Original SmartDL path (no auth)
    objs = []
    files = []
    for url in url_list:
        obj = SmartDL(url, file_path, progress_bar=WGET_PROGRESS_BAR)
        target_path = obj.get_dest()
        if os.path.exists(target_path):
            log.info("File %s already exists.", target_path)
        else:
            log.info("File %s downloading ...", target_path)
            obj.start(blocking=False)

        objs.append(obj)

    files: list[str] = []
    for obj in objs:
        obj.wait()
        files.append(obj.get_dest())
    return files


def unzip(zip_files, unzip_dir: str, infdb: InfDB) -> None:
    """Extract one or more zip files into `unzip_dir`, skipping if already extracted.

    Args:
        zip_files: A single .zip path or list of .zip paths.
        unzip_dir: Destination directory for extracted files.
    """
    log = infdb.get_worker_logger()
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

def download_aria2c(
    url: str,
    output_dir: str | Path,
    output_filename: str = None,
    connections: int = 4,
    max_connection_per_server: int = 4,
    continue_download: bool = True,
    allow_overwrite: bool = False,
    auto_file_renaming: bool = False,
    quiet: bool = True
) -> None:
    """
    Download files using aria2c with configurable options."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Build command
    cmd_parts = ["aria2c"]
    
    # Connection settings
    if continue_download:
        cmd_parts.append("-c")
    
    if connections > 1:
        cmd_parts.extend(["-x", str(connections)])
    
    if max_connection_per_server > 1:
        cmd_parts.extend(["-s", str(max_connection_per_server)])
    
    # Overwrite behavior
    if not allow_overwrite:
        cmd_parts.append("--allow-overwrite=false")
    
    if not auto_file_renaming:
        cmd_parts.append("--auto-file-renaming=false")
    
    # Logging
    if quiet:
        cmd_parts.extend(["--summary-interval=60", "--console-log-level=warn"])
    
    # Output location
    cmd_parts.extend(["-d", str(output_dir)])
    
    if output_filename:
        cmd_parts.extend(["-o", output_filename])
    
    # URL (last argument)
    cmd_parts.append(f'"{url}"')
    
    # Execute
    cmd = " ".join(cmd_parts)
    do_cmd(cmd)




# =================== geospatial / DB import helpers ===================


def get_envelop(infdb: InfDB):
    """Return the configured administrative envelope (GeoDataFrame filtered by AGS)."""
    scope = infdb.get_config_value([infdb.get_toolname(), "scope"])
    log = infdb.get_worker_logger()
    if isinstance(scope, str):
        scope = [scope]
    ags_path = infdb.get_config_path([infdb.get_toolname(), "sources", "bkg", "path", "unzip"], type="loader")
    log.debug("Envelop Path (unzipped): %s", ags_path)
    path = get_file(ags_path, filename="vg5000", ending=GPKG_EXT, infdb=infdb)
    log.debug("Envelop Path (file): %s", path)
    gdf = gpd.read_file(path, layer="vg5000_gem")
    gdf_scope = gdf[gdf["AGS"].str.startswith(tuple(scope or []))]
    gdf_scope.to_postgis("scope", infdb.get_db_engine(), if_exists="replace", schema="opendata", index=False)
    return gdf_scope


def get_all_envelops(infdb: InfDB):
    """Return the configured administrative envelope (GeoDataFrame filtered by AGS)."""
    scope = infdb.get_config_value([infdb.get_toolname(), "scope"])
    log = infdb.get_worker_logger()
    if isinstance(scope, str):
        scope = [scope]
    ags_path = infdb.get_config_path([infdb.get_toolname(), "sources", "bkg", "path", "unzip"], type="loader")
    log.debug("Envelop Path (unzipped): %s", ags_path)
    path = get_file(ags_path, filename="vg5000", ending=GPKG_EXT, infdb=infdb)
    log.debug("Envelop Path (file): %s", path)
    gdf = gpd.read_file(path, layer="vg5000_gem")
    envelop = []
    for s in scope:
        envelop.append(gdf[gdf["AGS"].str.startswith(s)])
    return envelop



# ============================== file helpers ==============================


def get_subdirectories_by_suffix(folder, suffix):
    """Return all subdirectories in `folder` whose names end with `suffix`."""
    folder = Path(folder)
    return [str(p) for p in folder.iterdir() if p.is_dir() and p.name.endswith(suffix)]


def get_all_files(folder_path: str, ending: str) -> list[str]:
    """Recursively collect all files under `folder_path` with the given ending."""
    files: list[str] = []
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.lower().endswith(ending):
                files.append(os.path.join(dirpath, filename))
    files.sort()
    return files


def get_file(folder_path: str, filename: str, ending: str, infdb: InfDB) -> Optional[str]:
    """Return the newest file path in `folder_path` containing `filename` and ending with `ending`.
    Necessary for data that was updated by provider:
    All data is saved in files -> selects newest to save in database."""
    files = get_all_files(folder_path, ending)
    log = infdb.get_worker_logger()
    matching = [f for f in files if filename.lower() in Path(f).stem.lower()]
    if not matching:
        log.error("No files found containing '%s' with ending '%s' in %s", filename, ending, folder_path)
        return None
    newest = max(matching, key=os.path.getmtime)
    return newest


def get_website_links(url: str, infdb: InfDB) -> list[str]:
    """Return all .zip links found on the given page (absolute or relative hrefs)."""
    soup = _fetch_html(url)
    log = infdb.get_worker_logger()
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".zip")]
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


def ensure_utf8_encoding(filepath: str, infdb: InfDB) -> str:
    """Detect file encoding; if not UTF-8, re-encode to a temp UTF-8 CSV and return its path."""
    log = infdb.get_worker_logger()
    with open(filepath, "rb") as f:
        raw = f.read()
        result = chardet.detect(raw)
        source_encoding = result["encoding"]

    if source_encoding is None:
        raise ValueError(f"Could not detect encoding of file: {filepath}")

    if source_encoding.lower() != "utf-8":
        log.info("Re-encoding file from %s to UTF-8: %s", source_encoding, filepath)
        temp_path = filepath + "_utf8.csv"
        with (
            open(filepath, "r", encoding=source_encoding, errors="replace") as src,
            open(temp_path, "w", encoding="utf-8") as dst,
        ):
            for line in src:
                dst.write(line)
        return temp_path

    return filepath


def get_number_processes(infdb: InfDB) -> int:
    """Determine worker process count based on CPU count and config max_cores."""
    log = infdb.get_worker_logger()
    number_processes = 1
    max_processes = infdb.get_config_value([infdb.get_toolname(), "multiproccesing", "max_cores"]) or 1
    if infdb.get_config_value([infdb.get_toolname(), "multiproccesing", "status"]) == "active":
        number_processes = min(multiprocessing.cpu_count(), max_processes)
    log.debug("Max processes: %s, Number of processes: %s", max_processes, number_processes)
    return number_processes

def do_cmd(cmd: str | List[str]) -> int:
    """
    Execute a shell command without shell interpretation.
    """
    return infdb_do_cmd(cmd)

# ======================= import / export to postgis =======================-----------------------------------------------------------------------------

def import_layers(input_file, layers, schema, infdb: InfDB, prefix="", layer_names=None,
                  scope=True, overwrite=True):
    
    epsg = (infdb.get_db_parameters_dict() or {}).get("epsg")
    dst = _pg_connstring_for_gdal(infdb)
    log = infdb.get_worker_logger()

    # Prepare destination table names
    if layer_names is None:
        layer_names = layers
    if prefix:
        layer_names = [f"{prefix}_{name}" for name in layer_names]

    # Get clipping geometry if requested
    clipsrc_opt = []
    tmp_gpkg = None
    
    if scope:
        clip_wkt, clip_method, _ = get_clip_geometry(target_crs=epsg, infdb=infdb)

        if clip_wkt:
            tmp_gpkg = os.path.splitext(os.path.abspath(input_file))[0] + "_clip_tmp.gpkg"
            try:
                from shapely import wkt as shapely_wkt
                clip_geom = shapely_wkt.loads(clip_wkt)
                clip_gdf = gpd.GeoDataFrame([{'geom': clip_geom}], geometry='geom', crs=f"EPSG:{epsg}")
                clip_gdf.to_file(tmp_gpkg, layer="clip_boundary", driver="GPKG")
                clipsrc_opt = ["-clipsrc", tmp_gpkg, "-clipsrclayer", "clip_boundary"]
                log.info(f"Clipping enabled: {clip_method} method")
            except Exception as e:
                log.warning(f"Could not prepare clip geometry; proceeding without clip. Error: {e}")
                clipsrc_opt = []

    # Import each layer
    first = True
    for src_layer, dst_name in zip(layers, layer_names):
        log.info(f"Importing '{src_layer}' → {schema}.{dst_name}")
        
        # Only the first layer uses -overwrite; subsequent ones append
        layer_overwrite = overwrite and first
        first = False

        args = [
            "ogr2ogr",
            "-progress",
            "-f", "PostgreSQL", dst,
            input_file,
            "-nln", f"{schema}.{dst_name}",
            "-nlt", "PROMOTE_TO_MULTI",
            "-lco", "GEOMETRY_NAME=geom",
            "-t_srs", f"EPSG:{epsg}",
            # "-makevalid",
        ] + (["-overwrite"] if layer_overwrite else ["-append"]) \
          + clipsrc_opt \
          + [src_layer]
        
        _ogr2ogr(args, infdb)

    # Cleanup temp file
    if tmp_gpkg and os.path.exists(tmp_gpkg):
        try:
            os.remove(tmp_gpkg)
        except Exception:
            pass


def fast_copy_points_csv(
    infdb: InfDB,
    csv_path: str,
    schema: str,
    table_name: str,
    x_col: str,
    y_col: str,
    srid_src: int = 3035,
    epsg: int = None,
    drop_existing: bool = True,
    create_spatial_index: bool = True,
    clip_to_scope: bool = True,
):
    log = infdb.get_worker_logger()
    params = infdb.get_db_parameters_dict()
    epsg = (infdb.get_db_parameters_dict() or {}).get("epsg")

    # Read CSV header
    with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = [h.strip().lower() for h in next(reader)]

    if x_col.lower() not in header or y_col.lower() not in header:
        raise ValueError(f"Missing coordinate columns: {x_col}, {y_col}")

    # Connect to database
    conn = psycopg2.connect(
        dbname=params["db"],
        user=params["user"],
        password=params["password"],
        host=params["host"],
        port=params["exposed_port"],
    )
    conn.autocommit = True
    cur = conn.cursor()

    staging = f"{table_name}__staging"

    try:
        # Step 1: Drop existing tables
        if drop_existing:
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE;')
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{staging}" CASCADE;')

        # Step 2: Create UNLOGGED staging table (all TEXT columns)
        cols_sql = ", ".join(f'"{c}" text' for c in header)
        cur.execute(f'CREATE UNLOGGED TABLE "{schema}"."{staging}" ({cols_sql});')

        # Step 3: COPY data from CSV
        log.info(f"Importing {csv_path} → staging table...")
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            cur.copy_expert(
                f'COPY "{schema}"."{staging}" ({", ".join(f"""\"{c}\"""" for c in header)}) '
                f"FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true);",
                f
            )

        # Step 4: Build SELECT with type casts
        select_cols = ", ".join(
            f'"{c}"::double precision' if c in (x_col.lower(), y_col.lower()) else f'"{c}"'
            for c in header
        )

        # Step 5: Get clipping WHERE clause
        where_clause = ""
        if clip_to_scope:
            clip_wkt, clip_method, _ = get_clip_geometry(target_crs=epsg, infdb=infdb)

            if clip_wkt:
                where_clause = f"""
                    WHERE ST_Intersects(
                        ST_Transform(
                            ST_SetSRID(ST_MakePoint("{x_col}"::double precision, "{y_col}"::double precision), {srid_src}),
                            {epsg}
                        ),
                        ST_GeomFromText('{clip_wkt}', {epsg})
                    )
                """
                log.info(f"Clipping enabled: {clip_method} method")

        # Step 6: Create final table with geometry + clipping in one SQL
        log.info(f"Creating final table{' with clipping' if where_clause else ''}...")
        cur.execute(f"""
            CREATE TABLE "{schema}"."{table_name}" AS
            SELECT
                {select_cols},
                ST_Transform(
                    ST_SetSRID(ST_MakePoint("{x_col}"::double precision, "{y_col}"::double precision), {srid_src}),
                    {epsg}
                )::geometry(Point, {epsg}) AS geom
            FROM "{schema}"."{staging}"
            {where_clause};
        """)

        # Step 8: Create spatial index
        if create_spatial_index:
            log.info("Creating spatial index...")
            cur.execute(f'CREATE INDEX "{table_name}_geom_gix" ON "{schema}"."{table_name}" USING GIST (geom);')

    finally:
        # Cleanup: Drop staging table
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{staging}" CASCADE;')
        cur.close()
        conn.close()


def get_clip_geometry(target_crs: int, infdb: InfDB):
    """
    Get clipping geometry for the configured scope.
    
    This is the single source of truth for spatial clipping used by both
    import_layers() and fast_copy_points_csv().
    
    Parameters:
        target_crs: Target EPSG code (e.g., 25832 for UTM32N)
    
    Returns:
        tuple: (geometry_wkt, method_used, row_count_estimate) or (None, None, None) if no clipping
    """
    gdf_envelope = get_envelop(infdb)
    log = infdb.get_worker_logger()
    
    if gdf_envelope is None or gdf_envelope.empty:
        log.info("Scope envelope is empty; no clipping applied.")
        return None, None, None
    
    # Transform to target CRS
    gdf_transformed = gdf_envelope.to_crs(epsg=target_crs)
    
    # Use actual boundary polygons (most accurate)
    clip_geom = gdf_transformed.unary_union
    clip_wkt = clip_geom.wkt
    area_km2 = clip_geom.area / 1_000_000
    log.info(f"Using exact geometry clip: {len(gdf_transformed)} polygons, {area_km2:.1f} km²")
    return clip_wkt, 'exact', len(gdf_transformed)


def get_clip_geometries_per_scope(target_crs: int, infdb: InfDB):
    """
    Return one exact clipping geometry per configured scope.

    Returns a list of dicts:
      {
        "scope": "<AGS prefix from config>",
        "landkreis": "<first 5 digits>",
        "geom": shapely geometry (in target_crs),
        "bbox": (minx, miny, maxx, maxy),
      }
    Intended for cases where you need to process each scope separately:
    For DGM1: build a separate -te + -cutline + output raster for each AGS.
    That avoids one huge raster with a lot of NoData; you get one small raster per municipality.
    """
    scope_cfg = infdb.get_config_value([infdb.get_toolname(), "scope"]) or []
    log = infdb.get_worker_logger()

    if isinstance(scope_cfg, str):
        scope_cfg = [scope_cfg]

    # get_all_envelops() already returns a list of GDFs per scope prefix, in the same order
    envelopes = get_all_envelops(infdb)

    results = []
    for scope_prefix, gdf in zip(scope_cfg, envelopes):
        if gdf is None or gdf.empty:
            log.warning("No envelope polygons found for scope %s", scope_prefix)
            continue

        gdf_tr = gdf.to_crs(epsg=target_crs)
        geom = gdf_tr.unary_union
        minx, miny, maxx, maxy = geom.bounds

        results.append({
            "scope": scope_prefix,
            "landkreis": scope_prefix[:5],
            "geom": geom,
            "bbox": (minx, miny, maxx, maxy),
        })

    if not results:
        log.info("No valid per-scope clip geometries; no clipping will be applied.")
    else:
        log.info(
            "Prepared %d per-scope clip geometries (exact).", len(results)
        )

    return results