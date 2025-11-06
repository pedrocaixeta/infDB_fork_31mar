import multiprocessing as mp
from src import utils, config
from src import bkg, basemap, lod2, census2022, plz, tabula, package, need, openmeteo, wetterdienst
from src.logger import setup_main_logger
import logging

log = logging.getLogger(__name__)

if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    log_queue = mp.Queue()
    listener = setup_main_logger(log_queue)

    log.info("Starting loader.............................................")
    log.info("-------------------------------------------------------------")
    log.info("-------------------------------------------------------------")

    # Download opendata package for development directly
    if utils.if_active("package"):
        package.load()

    # Drop schema "opendata" for development purposes on clean runs
    sql = f"DROP SCHEMA IF EXISTS opendata CASCADE;"
    utils.sql_query(sql)
    
    # # Ensure that administrative areas are loaded for scope of other datasets
    bkg.load(log_queue)

    # Load data in parallel
    mp.freeze_support()
    processes = []
    processes.append(mp.Process(target=need.load, args=(log_queue,), name="need"))
    processes.append(mp.Process(target=tabula.load, args=(log_queue,), name="tabula"))
    processes.append(mp.Process(target=lod2.load, args=(log_queue,), name="lod2"))
    processes.append(mp.Process(target=plz.load, args=(log_queue,), name="plz"))
    processes.append(mp.Process(target=basemap.load, args=(log_queue,), name="basemap"))
    processes.append(
        mp.Process(target=census2022.load, args=(log_queue,), name="census2022")
    )
    processes.append(mp.Process(target=openmeteo.load, args=(log_queue,), name="openmeteo"))
    # processes.append(mp.Process(target=wetterdienst.load, args=(log_queue,), name="wetterdienst"))

    for process in processes:
        process.start()
        if not utils.if_multiproccesing():
            process.join()  # Only one process at a time
            log.info("Process %s done", process.name)
    log.info("Processes started")

    # Wait for processes
    for cnt, process in enumerate(processes, 1):
        process.join()
        log.info("Process %s done (%d out of %d)", process.name, cnt, len(processes))

    listener.stop()

    log.info("Processes done")
