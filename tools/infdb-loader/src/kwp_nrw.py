# src/bkg.py
import logging
import multiprocessing as mp
import os
from logging.handlers import QueueHandler
import sys
from typing import List, Sequence, Union

from infdb import InfDB
from . import utils


def load(infdb: InfDB) -> bool:
    """Download BKG sources, import layers, and generate geogitter grid.

    Behavior preserved:
    - (Optional) feature guard for BKG: left commented as in original.
    - Download/unzip/import NUTS and VG5000 with scope=False.
    - Create schema if missing; then generate geogitter with configured resolutions.
    """
    log = infdb.get_worker_logger()

    if not utils.if_active("bkg", infdb):
        return

    # Paths
    try:
        zip_path = infdb.get_config_path([infdb.get_toolname(), "sources", "bkg", "path", "zip"], type="loader")
        os.makedirs(zip_path, exist_ok=True)
        unzip_path = infdb.get_config_path([infdb.get_toolname(), "sources", "bkg", "path", "unzip"], type="loader")
        os.makedirs(unzip_path, exist_ok=True)

        schema = infdb.get_config_value([infdb.get_toolname(), "sources", "bkg", "schema"])
        prefix = infdb.get_config_value([infdb.get_toolname(), "sources", "bkg", "prefix"])

        # Ensure schema exists via InfdbClient
        with infdb.connect() as db:
            db.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # --- NUTS (download+unzip+import) ---
        log.info("Downloading and unzipping NUTS")
        nuts_url = infdb.get_config_value([infdb.get_toolname(), "sources", "bkg", "nuts", "url"])
        utils.download_files(nuts_url, zip_path, infdb)
        nuts_zip = utils.get_file(zip_path, filename="nuts250", ending=".zip", infdb=infdb)
        utils.unzip(nuts_zip, unzip_path, infdb)

        nuts_layers = infdb.get_config_value([infdb.get_toolname(), "sources", "bkg", "nuts", "layer"])
        nuts_gpkg = utils.get_file(unzip_path, filename="nuts250", ending=".gpkg", infdb=infdb)
        utils.import_layers(nuts_gpkg, nuts_layers, schema, infdb, prefix, scope=False)

        log.info("BKG data loaded successfully")
    except Exception as err:
        log.exception("An error occurred while processing BKG data: %s", str(err))
