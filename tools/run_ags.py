import logging
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Setup logging
SCRIPT_DIR = Path(__file__).parent
log_file = SCRIPT_DIR / "tools.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# PROFILE = "linear"
# PROFILE = "basedata"
PROFILE = "basedata-buildings"

PROFILE = sys.argv[1] if len(sys.argv) > 1 else "linear"
logger.info(f"Using profile: {PROFILE}")

num_workers = 5
ags_list = (
# Top Municipalities by Building Count (Cumulative 50% of Bavaria)
"09162000", # München
# "09564000", # Nürnberg
# "09761000", # Augsburg
# "09362000", # Regensburg
# "09161000", # Ingolstadt
# "09663000", # Würzburg
# "09563000", # Fürth
# "09562000", # Erlangen
# "09261000", # Landshut
# "09461000", # Bamberg
# "09462000", # Bayreuth
# "09163000", # Rosenheim
# "09661000", # Aschaffenburg
# "09762000", # Kempten (Allgäu)
# "09662000", # Schweinfurt
# "09363000", # Straubing
# "09763000", # Memmingen
# "09463000", # Hof
# "09361000", # Amberg
# "09263000", # Passau
# "09184148", # Schweinfurt
# "09775169", # Neu-Ulm
# "09181113", # Dachau
# "09179118", # Fürstenfeldbruck
# "09184113", # Aschaffenburg
# "09177121", # Erding
# "09178115", # Freising
# "09184119", # Hanau (beispielhaft für Pendlergürtel)
# "09262000", # Straubing
# "09179147", # Germering
# "09375113", # Beilngries
# "09189155", # Traunstein
# "09179121", # Garching b.München
# "09184120", # Karlstein a.Main
# "09772115", # Altenmünster
# "09576132", # Hilpoltstein
# "09376161", # Schwandorf
# "09188117", # Ettal
# "09180121", # Garmisch-Partenkirchen, Markt
# "09180124", # Lenggries
# # ... [List continues through middle-sized towns] ...
# "09573115", # Ansbach
# "09779121", # Buchloe
# "09181140", # Karlsfeld
# "09187148", # Neuburg a.d.Donau
# "09279112", # Dingolfing
# "09174115", # Bad Tölz
# "09182114", # Ebersberg
# "09571114", # Altdorf b.Nürnberg
# "09778129", # Günzburg
# "09184131", # Mainaschaff
# "09678146", # Karlstadt
# "09175115", # Dachau
# [Approx. Rank 155 - Threshold for 50%]
"09177113", # Berglern
)
logger.info(f"AGS to process: {', '.join(ags_list)}")
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
            future_to_ags = {executor.submit(run_ags, ags): ags for ags in ags_list}

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
