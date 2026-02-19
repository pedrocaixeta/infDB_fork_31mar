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
import psycopg2
# from infdb import InfDB
# from sqlalchemy import create_engine

# infdb = InfDB(tool_name="infdb-data")
user = "infdb_user"
password = "infdb"
host = "localhost"
port = "54328"
db = "infdb"
db_connection_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
conn = psycopg2.connect(
    dbname=db,
    user=user,
    password=password,
    host=host,
    port=port
)

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

with conn.cursor() as cur:  # InfdbClient context
    logger.info("Terminating other connections to avoid deadlocks during schema drop...")
    cur.execute("""SELECT pg_terminate_backend(pid)
                            FROM pg_stat_activity
                            WHERE pid <> pg_backend_pid() ;""")
    
    logger.info("Rolling back any open transactions to prevent locks...")
    cur.execute("ROLLBACK;")
    
    logger.info("Dropping schemas for clean development run...")
    cur.execute("DROP SCHEMA IF EXISTS basedata CASCADE;")
    cur.execute("DROP SCHEMA IF EXISTS buildings_to_street CASCADE;")
    cur.execute("DROP SCHEMA IF EXISTS linear_heat_density CASCADE;")
    cur.execute("DROP SCHEMA IF EXISTS ro_heat CASCADE;")

sql = """SELECT *
            FROM opendata.scope
            WHERE ags LIKE '09%'
            ORDER BY ags;
        """
ags_list = gpd.read_postgis(sql, conn, geom_col='geom')



# PROFILE = "linear"
# PROFILE = "basedata"
# PROFILE = "basedata-buildings"
# PROFILE = "basedata-ways"



PROFILE = sys.argv[1] if len(sys.argv) > 1 else "linear"
logger.info(f"Using profile: {PROFILE}")

todo_ags = ags_list["ags"].tolist()
logger.info(f"Total AGS to process: {len(todo_ags)}")
logger.info(f"AGS to process: {', '.join(todo_ags)}")

num_workers = 5
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
