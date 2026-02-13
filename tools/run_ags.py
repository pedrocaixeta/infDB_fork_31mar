import logging
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import geopandas as gpd
from infdb import InfDB

infdb = InfDB(tool_name="infdb-data")
engine = infdb.get_db_engine()
sql = """SELECT *
            FROM opendata.bkg_vg5000_gem
            -- WHERE ags like '05%' OR ags like '09%'
            WHERE ags IN ('05119000', '09185149')
            ORDER BY ags
            LIMIT 10;
        """
ags_list = gpd.read_postgis(sql, engine)

# Setup logging
SCRIPT_DIR = Path(__file__).parent
log_file = SCRIPT_DIR / "tools.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# PROFILE = "linear"
# PROFILE = "basedata"
# PROFILE = "basedata-buildings"



PROFILE = sys.argv[1] if len(sys.argv) > 1 else "linear"
logger.info(f"Using profile: {PROFILE}")

todo_ags = ags_list["ags"].tolist()

num_workers = 1
logger.info(f"AGS to process: {', '.join(todo_ags)}")
running_processes = set()
running_lock = threading.Lock()
stop_event = threading.Event()


def run_ags(ags):
    if stop_event.is_set():
        return

    process = subprocess.Popen(
        ["bash", SCRIPT_DIR / "run-profile.sh", PROFILE, ags],
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    with running_lock:
        running_processes.add(process)

    try:
        for line in process.stdout:
            logger.info(f"[{ags}] {line.rstrip()}")
        process.wait()
        if process.returncode != 0:
            logger.error(f"Process failed with return code {process.returncode} for AGS {ags}")
            raise subprocess.CalledProcessError(process.returncode, process.args)
    finally:
        with running_lock:
            running_processes.discard(process)


def signal_handler(sig, frame):
    logger.info("\nInterrupt received, stopping Docker...")
    stop_event.set()
    with running_lock:
        for process in list(running_processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGINT)
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    try:
        start_time = time.time()
        failed_ags = []

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Map futures to their AGS for better error reporting
            future_to_ags = {executor.submit(run_ags, ags): ags for ags in todo_ags}

            for future in as_completed(future_to_ags):
                ags = future_to_ags[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"AGS {ags} generated an exception: {exc}")
                    failed_ags.append(ags)

        end_time = time.time()
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")

        if failed_ags:
            logger.warning(f"The following AGS failed: {', '.join(failed_ags)}")

    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(0)
