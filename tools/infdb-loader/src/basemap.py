import os
import time
from typing import List, Sequence, Optional

from infdb import InfDB
from . import utils

def _wait_for_file(path: str, *, min_size: int = 5_000, timeout: float = 90.0, step: float = 2.0) -> bool:
    """Poll for a file that is being downloaded asynchronously (e.g. pySmartDL).

    Returns True as soon as the file exists **and** is bigger than `min_size`.
    Returns False if `timeout` expires.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            try:
                if os.path.getsize(path) >= min_size:
                    return True
            except OSError:
                pass
        time.sleep(step)
    return False


def load(infdb: InfDB) -> None:
    log = infdb.get_worker_logger()
    if not utils.if_active("basemap"):
        log.info("Basemap loader inactive → skipping.")
        return

    base_path = infdb.get_config_path(["loader", "sources", "basemap", "path", "base"], type="loader")
    os.makedirs(base_path, exist_ok=True)

    site_url = infdb.get_config_value(["loader", "sources", "basemap", "url"])
    ending = infdb.get_config_value(["loader", "sources", "basemap", "ending"])
    filters: Sequence[str] = infdb.get_config_value(["loader", "sources", "basemap", "filter"]) or []

    schema = infdb.get_config_value(["loader", "sources", "basemap", "schema"])
    prefix = infdb.get_config_value(["loader", "sources", "basemap", "prefix"])
    layer_names: Sequence[str] = infdb.get_config_value(["loader", "sources", "basemap", "layer"]) or []

    # make sure schema exists
    with infdb.connect() as db:
        db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    for flt in filters:
        urls: List[str] = utils.get_links(site_url, ending, flt)
        if len(urls) != 1:
            log.warning("Basemap: filter '%s' produced %d links → %s (skipping)", flt, len(urls), urls)
            continue
        url = urls[0]
        log.info("Basemap: selected %s for filter '%s'", url, flt)

        filename, name, extension = utils.get_file_from_url(url)
        name_no_day = name.rsplit("-", 1)[0]
        expected_monthly_path = os.path.join(base_path, name_no_day + extension)

        downloaded_paths: List[str] = utils.download_files(url, base_path)

        input_file: Optional[str] = None

        # wait for the file to appear
        if downloaded_paths:
            candidate = downloaded_paths[0]

            if _wait_for_file(candidate, min_size=5_000, timeout=120.0):
                input_file = candidate
            else:
                log.error(
                    "Basemap: downloaded file not found on disk (%s) → skipping to fallback",
                    candidate,
                )

        # skip if no file found
        if input_file is None:
            log.error(
                "Basemap: no usable .gpkg for filter '%s' (download not ready and no fallback). Skipping.",
                flt,
            )
            continue

        log.info("Basemap: importing %s into schema %s", input_file, schema)
        layers = [layer + "_bdlm" for layer in layer_names]

        try:
            utils.import_layers(input_file, layers, schema, prefix=prefix, layer_names=layer_names)
        except Exception as exc:
            log.error("Basemap: import of %s failed → %s", input_file, exc)
            continue

    log.info("Basemap data loaded successfully")
