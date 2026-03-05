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
import argparse
# from infdb import InfDB
# from sqlalchemy import create_engine

# infdb = InfDB(tool_name="infdb-data")

# Parameters
user = "infdb_user"
password = "infdb"
host = "localhost"
port = "54328"
db = "infdb"


def parse_arguments():
    """
    Parse and return command-line arguments for AGS processing.
    Returns:
        argparse.Namespace: An object containing the following attributes:
            - profile (str, optional): Profile name to use for processing
            - tool (str, optional): Tool name to use for processing
            - ags_list (str, optional): Comma-separated list of AGS codes
            - clean (bool): Flag to clean schemas before running (default: False)
            - num_workers (int): Number of parallel workers to use (default: 5)
    """

    parser = argparse.ArgumentParser(description="Run AGS processing with specified profile or tool")
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "-p", "--profile",
        type=str,
        help="Profile name to use"
    )
    mode_group.add_argument(
        "-t", "--tool",
        type=str,
        help="Tool name to use"
    )
    parser.add_argument(
        "-a", "--ags-list",
        type=str,
        default=None,
        dest="ags_list",
        help="Comma-separated AGS codes (optional, use this for manual AGS input)"
    )
    parser.add_argument(
        "-c", "--clean",
        action="store_true",
        help="Clean schemas before running"
    )
    parser.add_argument(
        "-n",
        type=int,
        default=5,
        dest="num_workers",
        help="Number of parallel workers (default: 5)"
    )

    args = parser.parse_args()

    if args.ags_list and not any(item.strip() for item in args.ags_list.split(",")):
        parser.error("--ags-list must contain at least one AGS code.")

    return args


def run_ags(command, ags):
    """Run the specified command for a given AGS code in a subprocess, while managing the process lifecycle and logging output.
    Args:
        command (str): The command to run (e.g., profile or tool name)
        ags (str): The AGS code to process
     Raises:
        subprocess.CalledProcessError: If the subprocess returns a non-zero exit code, indicating failure.
    """

    if stop_event.is_set():
        return

    process = subprocess.Popen(
        ["bash", str(SCRIPT_DIR / "tools.sh"), command[0], command[1], ags],
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
    """
    Handle interrupt signals (e.g., SIGINT from Ctrl+C).
    Logs the interrupt, sets the stop event to signal shutdown, and gracefully
    terminates all running child processes by sending SIGINT to their process groups.
    Args:
        sig (int): The signal number received.
        frame (FrameType): The current stack frame.
    Raises:
        KeyboardInterrupt: Always raised to propagate the interrupt.
    """

    logger.info("\nInterrupt received, stopping Docker...")
    stop_event.set()
    with running_lock:
        for process in list(running_processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGINT)
    raise KeyboardInterrupt


if __name__ == "__main__":

    # Determine the script directory for relative paths
    SCRIPT_DIR = Path(__file__).parent

    # Setup logging
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

    # Parse command-line arguments
    args = parse_arguments()

    # Connect to the database
    db_connection_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    conn = psycopg2.connect(
        dbname=db,
        user=user,
        password=password,
        host=host,
        port=port
    )

    # Terminate other connections and rollback any open transactions to avoid deadlocks
    with conn.cursor() as cur:
        logger.info(
            "Terminating other connections to avoid deadlocks..."
        )
        cur.execute("""SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE pid <> pg_backend_pid();""")

        logger.info("Rolling back any open transactions...")
        cur.execute("ROLLBACK;")

    # Drop schemas if -c flag is provided for clean development run
    if args.clean:
        with conn.cursor() as cur:
            logger.info("Dropping schemas for clean development run...")
            cur.execute("DROP SCHEMA IF EXISTS basedata CASCADE;")
            cur.execute(
                "DROP SCHEMA IF EXISTS buildings_to_street CASCADE;"
            )
            cur.execute(
                "DROP SCHEMA IF EXISTS linear_heat_density CASCADE;"
            )
            cur.execute("DROP SCHEMA IF EXISTS ro_heat CASCADE;")

    # Determine the command to run based on command-line arguments
    if args.profile:
        command = ("-p", args.profile)
    elif args.tool:
        command = ("-t", args.tool)
    else:
        logger.error("Either profile or tool must be specified")
        sys.exit(1)

    # Get AGS list from --ags-list option or database
    if args.ags_list:
        todo_ags = [item.strip() for item in args.ags_list.split(",") if item.strip()]
    else:
        # Fetch available AGS list from infDB
        sql = """SELECT *
                    FROM opendata.scope
                    -- WHERE ags LIKE '09%'
                    ORDER BY ags;
                """
        ags_list = gpd.read_postgis(sql, conn, geom_col='geom')
        todo_ags = ags_list["ags"].tolist()
    logger.info(f"Total AGS to process: {len(todo_ags)}")
    if len(todo_ags) < 20:
        logger.info(f"AGS to process: {', '.join(todo_ags)}")

    # Setup signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    running_processes = set()
    running_lock = threading.Lock()
    stop_event = threading.Event()

    # Run AGS processing in parallel
    start_time = time.time()
    failed_ags = []
    try:
        with ThreadPoolExecutor(max_workers=args.num_workers) as executor:
            # Map futures to their AGS for better error reporting
            future_to_ags = {executor.submit(run_ags, command, ags): ags for ags in todo_ags}

            for future in as_completed(future_to_ags):
                ags = future_to_ags[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"AGS {ags} generated an exception: {exc}")
                    failed_ags.append(ags)

    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(0)

    # Log total execution time and any failed AGS
    end_time = time.time()
    logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
    if failed_ags:
        logger.warning(f"The following AGS failed: {', '.join(failed_ags)}")
