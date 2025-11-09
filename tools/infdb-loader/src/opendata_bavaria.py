import os
import shutil
from . import config, utils, logger
import logging
from infdb import InfDB

log = logging.getLogger(__name__)


def load(log_queue):
    logger.setup_worker_logger(log_queue)

    if not utils.if_active("opendata_bavaria"):
        return

    # Create base directory
    base_path = config.get_path(["loader", "sources", "opendata_bavaria", "path", "base"])
    os.makedirs(base_path, exist_ok=True)

    schema = config.get_value(["loader", "sources", "opendata_bavaria", "schema"])
    prefix = config.get_value(["loader", "sources", "opendata_bavaria", "prefix"])
    
    datasets = config.get_value(["loader", "sources", "opendata_bavaria", "datasets"])

    # Geländemodell Bayern 1m
    if datasets["gelaendemodell_1m"]["status"] == "active":
        log.info("Loading Geländemodell Bayern 1m data from Opendata Bavaria")
        # todo: add gelaendemodell loading here

    # Building LOD2
    if datasets["lod2"]["status"] == "active":
        log.info("Loading LOD2 data from Opendata Bavaria")
        # todo: add lod2 loading here

    # Tatsächliche Nutzung
    if datasets["tatsaechliche_nutzung"]["status"] == "active":
        log.info("Loading Tatsächliche Nutzung data from Opendata Bavaria")
        # add tatsaechliche_nutzung loading here

    log.info(f"Opendata Bavaria data loaded successfully")
