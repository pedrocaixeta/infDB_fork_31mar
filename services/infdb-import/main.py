import multiprocessing as mp
from typing import Callable, List
import sys
from infdb import InfDB

from src import (
    basemap,
    bkg,
    census2022,
    gebaeude_neuburg,
    kwp_nrw,
    kwp_nrw_oberhausen,
    lod2_nrw,
    need,
    opendata_bavaria,
    openmeteo,
    plz,
    tabula,
    utils,
    waermeatlas_hessen_bensheim,
    # wetterdienst,
)

# ============================== Entry Point ============================


def _run_loader(load_fn: Callable[[InfDB], None]) -> None:
    infdb = InfDB(tool_name="infdb-import")
    try:
        load_fn(infdb)
    finally:
        # prevents QueueListener/_monitor thread exceptions on process exit
        try:
            infdb.stop_logger()
        except Exception:
            pass


def main() -> None:
    """Bootstraps loader, drops dev schema, and spawns data-loading processes.

    Behavior preserved:
    - Uses InfDB for config and centralized logging (queue listener).
    - Optionally downloads the 'opendata' package when active.
    - Drops schema 'opendata' for clean development runs.
    - Launches the original set of processes; respects utils.if_multiprocesing() to serialize.
    - Stops the queue listener at the end.
    """

    # Bootstrap InfDB (provides package config + central logging)
    infdb = InfDB(tool_name="infdb-import")

    # Root logger and the running QueueListener (started by InfdbLogger internally)
    log = infdb.get_logger()
    # log_queue = infdb.infdblogger.log_queue # Uncomment when wetterdienst is supported again

    log.info("Starting loader.............................................")
    log.info("-------------------------------------------------------------")
    log.info("-------------------------------------------------------------")

    # Download opendata package for development directly (original guard)
    # if utils.if_active("package", infdb):
    #     package.load(infdb)

    # # Drop schema "opendata" for clean development runs
    # with infdb.connect() as db:  # InfdbClient context
    #     db.execute_query("DROP SCHEMA IF EXISTS opendata CASCADE;")

    # Ensure that administrative areas are loaded for scope
    bkg.load(infdb)
    # Launch data loading in parallel
    mp.freeze_support()
    processes: List[mp.Process] = []
    # processes.append(mp.Process(target=_run_loader, args=(need.load,), name="need"))
    # processes.append(mp.Process(target=_run_loader, args=(tabula.load,), name="tabula"))
    # processes.append(mp.Process(target=_run_loader, args=(plz.load,), name="plz"))
    # processes.append(mp.Process(target=_run_loader, args=(basemap.load,), name="basemap"))
    # processes.append(mp.Process(target=_run_loader, args=(census2022.load,), name="census2022"))
    # processes.append(mp.Process(target=_run_loader, args=(openmeteo.load,), name="openmeteo"))
    # processes.append(mp.Process(target=_run_loader, args=(kwp_nrw.load,), name="kwp_nrw"))
    # processes.append(mp.Process(target=_run_loader, args=(kwp_nrw_oberhausen.load,), name="kwp_nrw_oberhausen"))
    # processes.append(mp.Process(target=_run_loader, args=(gebaeude_neuburg.load,), name="gebaeude-neuburg"))
    # processes.append(
    #     mp.Process(target=_run_loader, args=(waermeatlas_hessen_bensheim.load,), name="waermeatlas_hessen_bensheim"))

    # processes.append(mp.Process(target=_run_loader, args=(wetterdienst.load,), name="wetterdienst"))
    processes.append(mp.Process(target=_run_loader, args=(opendata_bavaria.load,), name="opendata_bavaria"))
    # processes.append(mp.Process(target=_run_loader, args=(lod2_nrw.load,), name="lod2-nrw"))

    for process in processes:
        process.start()
        if not utils.if_multiprocesing(infdb):
            process.join()
            log.info("Process %s done", process.name)
    log.info("Processes started")

    # Wait for all processes to finish and collect status
    for cnt, process in enumerate(processes, 1):
        process.join()
        status = "OK" if process.exitcode == 0 else "FAILED"
        log.info("Process %s done (%d out of %d) - status: %s", process.name, cnt, len(processes), status)

    # Run buildings_lod2.sql ONCE here (after all joins to prevent race conditions)



    # Summarize successes and failures
    successful = [p.name for p in processes if p.exitcode == 0]
    failed = [p.name for p in processes if p.exitcode != 0]

    if successful:
        log.info("Successful processes (%d/%d): %s", len(successful), len(processes), ", ".join(successful))
    else:
        log.warning("No processes completed successfully.")

    if failed:
        log.error("Failed processes (%d/%d): %s", len(failed), len(processes), ", ".join(failed))
    else:
        log.info("No processes failed.")

    # Stop the central listener explicitly
    log.info("Processes done")
    infdb.stop_logger()


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    mp.freeze_support()
    main()
