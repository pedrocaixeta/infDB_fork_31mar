import logging
import multiprocessing as mp
import os
from typing import Any, Dict, List

from infdb import InfDB
from src import utils
from src import (
    bkg,
    basemap,
    lod2,
    lod2_nrw,
    census2022,
    plz,
    tabula,
    package,
    need,
    openmeteo,
    # wetterdienst,
)

# ============================== Constants ==============================

TOOL_NAME: str = "loader"
CONFIG_DIR: str = "configs"
DB_NAME: str = "postgres"


# ============================== Entry Point ============================

def main() -> None:
    """Bootstrap loader, drop dev schema, and spawn data-loading processes.

    Behavior preserved:
    - Uses InfDB for config and centralized logging (queue listener).
    - Optionally downloads the 'opendata' package when active.
    - Drops schema 'opendata' for clean development runs.
    - Launches the original set of processes; respects utils.if_multiproccesing() to serialize.
    - Stops the queue listener at the end.
    """
    # Bootstrap InfDB (provides package config + central logging)
    infdb = InfDB(tool_name=TOOL_NAME, config_path=CONFIG_DIR)

    # Root logger and the running QueueListener (started by InfdbLogger internally)
    log = infdb.get_log()
    log_queue = infdb.infdblogger.log_queue
    listener = infdb.infdblogger.listener

    log.info("Starting loader.............................................")
    log.info("-------------------------------------------------------------")
    log.info("-------------------------------------------------------------")

    # Download opendata package for development directly (original guard)
    # if utils.if_active("package"):
    #     package.load(infdb)

    # Drop schema "opendata" for clean development runs
    with infdb.connect() as db:  # InfdbClient context
        db.execute_query("DROP SCHEMA IF EXISTS opendata CASCADE;")

    # Ensure that administrative areas are loaded for scope
    #bkg.load(infdb)

    # Launch data loading in parallel
    mp.freeze_support()
    processes: List[mp.Process] = []
    processes.append(mp.Process(target=need.load,       args=(infdb,), name="need"))
    processes.append(mp.Process(target=tabula.load,     args=(infdb,), name="tabula"))
    processes.append(mp.Process(target=lod2.load,       args=(infdb,), name="lod2"))
    processes.append(mp.Process(target=lod2_nrw.load,   args=(infdb,), name="lod2-nrw"))
    processes.append(mp.Process(target=plz.load,        args=(infdb,), name="plz"))
    processes.append(mp.Process(target=basemap.load,    args=(infdb,), name="basemap"))
    processes.append(mp.Process(target=census2022.load, args=(infdb,), name="census2022"))
    processes.append(mp.Process(target=openmeteo.load,  args=(infdb,), name="openmeteo"))
    # processes.append(mp.Process(target=wetterdienst.load, args=(log_queue,), name="wetterdienst"))

    for process in processes:
        process.start()
        if not utils.if_multiproccesing():
            process.join()
            log.info("Process %s done", process.name)
    log.info("Processes started")

    # Wait for all processes to finish
    for cnt, process in enumerate(processes, 1):
        process.join()
        log.info("Process %s done (%d out of %d)", process.name, cnt, len(processes))

    # Stop the central listener explicitly
    if listener:
        listener.stop()

    log.info("Processes done")


if __name__ == "__main__":
    main()
