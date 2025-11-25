import logging
import multiprocessing
import os, time, random
from pathlib import Path
from typing import Iterable, List, Optional

import geopandas as gpd
import requests
from bs4 import BeautifulSoup
from pySmartDL import SmartDL
from urllib.parse import urljoin, urlparse
from zipfile import BadZipFile, ZipFile

import chardet

from infdb import InfDB
from infdb.utils import do_cmd

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


# ======================== toggles & config helpers ========================

def if_multiprocesing(infdb:InfDB) -> bool:
    """Return True if multiprocessing is enabled via config (original spelling/API)."""
    status = infdb.get_config_value([infdb.get_toolname(), "multiproccesing", "status"])
    return status == "active"

def if_active(service: str, infdb:InfDB) -> bool:
    """Tell whether a given source service is active; logs decision.

    Args:
        service: Service key under `loader.sources`.

    Returns:
        True if active; False otherwise (with informational log).
    """
    status =  infdb.get_config_value([infdb.get_toolname(), "sources", service, "status"])
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

def get_links(url: str, ending: str, flt: str, infdb:InfDB) -> list[str]:
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

def _requests_download(url: str, dest_dir: str, infdb: InfDB, username: str, access_token: str,
                       timeout=60, max_retries=5, backoff_base=1.5, chunk=1024*1024) -> str:
    """HEAD (size if available) → streamed GET with retries/backoff."""
    os.makedirs(dest_dir, exist_ok=True)

    # filename from URL path
    filename = os.path.basename(urlparse(url).path) or "download"
    dest = os.path.join(dest_dir, filename)
    log = infdb.get_worker_logger()

    auth = (username, access_token)

    # HEAD: get size if available
    size = None
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, auth=auth)
        if r.ok and "content-length" in r.headers:
            size = int(r.headers["content-length"])
    except Exception:
        pass  # server may not support HEAD properly

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
            with requests.get(url, stream=True, timeout=timeout, auth=auth) as resp:
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
            sleep_s = (backoff_base ** attempt) + random.uniform(0, 0.25 * backoff_base)
            log.warning("Retry %d/%d for %s in %.1fs", attempt + 1, max_retries, url, sleep_s)
            time.sleep(sleep_s)

def download_files(urls, file_path: str, infdb: InfDB, protocol: str = "http", username: str = None, access_token: str = None) -> list[str]:
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


# =================== geospatial / DB import helpers ===================

def import_layers(
    input_file: str,
    layers: List[str],
    schema: str,
    infdb: InfDB,
    prefix: str = "",
    layer_names: Optional[List[str]] = None,
    scope: bool = True,
    if_exists: str = "replace"
) -> None:
    """Import vector data into PostGIS.

    If the source supports multiple named layers (e.g., .gpkg/.gdb/.sqlite),
    import each requested layer. Otherwise, import the file once with no
    'layer' argument and store it under the target name.

    Args:
        input_file: Path/URL to a vector dataset.
        layers: Source layer names to import (for multi-layer files).
        schema: Target DB schema.
        prefix: Optional prefix for target tables.
        layer_names: Optional explicit target table names (aligned with layers).
        scope: If True, spatially mask reads to the configured envelope.

    Raises:
        KeyError: If EPSG is missing in DB parameters.
    """
    log = infdb.get_worker_logger()
    gdf_scope = get_envelop(infdb) if scope else None
    epsg = (infdb.get_db_parameters_dict() or {}).get("epsg")
    if epsg is None:
        raise KeyError("Missing 'epsg' in DB parameters for service 'postgres'")
    engine = infdb.get_db_engine()

    # Desired target table names
    target_names = list(layer_names) if layer_names is not None else list(layers)
    if prefix:
        target_names = [f"{prefix}_{name}" for name in target_names]

    # Detect if source is multi-layer
    ext = Path(input_file).suffix.lower()
    is_multilayer = ext in {GPKG_EXT, ".gdb", ".sqlite"}  # extend if needed

    if not is_multilayer:
        # Single-layer sources: read once and write under first target name (or fallback)
        target_name = target_names[0] if target_names else (prefix or Path(input_file).stem)
        log.info("Importing single-layer source into %s.%s", schema, target_name)
        gdf = gpd.read_file(input_file, mask=gdf_scope)
        gdf.to_crs(epsg=epsg, inplace=True)
        gdf = gdf.rename_geometry(SQL_SCHEMA_GEOMETRY_COL)
        gdf.to_postgis(target_name, engine, if_exists=if_exists, schema=schema, index=False)
        return

    # Multi-layer path
    for layer, layer_name in zip(layers, target_names):
        log.info("Importing layer: %s into %s.%s", layer, schema, layer_name)
        gdf = gpd.read_file(input_file, layer=layer, mask=gdf_scope)
        gdf.to_crs(epsg=epsg, inplace=True)
        gdf = gdf.rename_geometry(SQL_SCHEMA_GEOMETRY_COL)
        gdf.to_postgis(layer_name, engine, if_exists=if_exists, schema=schema, index=False)


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
        with open(filepath, "r", encoding=source_encoding, errors="replace") as src, \
             open(temp_path, "w", encoding="utf-8") as dst:
            for line in src:
                dst.write(line)
        return temp_path

    return filepath


def get_number_processes(infdb: InfDB) -> int:
    """Determine worker process count based on CPU count and config max_cores."""
    log = infdb.get_worker_logger()
    number_processes = 1
    max_processes =  infdb.get_config_value([infdb.get_toolname(), "multiproccesing", "max_cores"]) or 1
    if  infdb.get_config_value([infdb.get_toolname(), "multiproccesing", "status"]) == "active":
        number_processes = min(multiprocessing.cpu_count(), max_processes)
    log.debug("Max processes: %s, Number of processes: %s", max_processes, number_processes)
    return number_processes
