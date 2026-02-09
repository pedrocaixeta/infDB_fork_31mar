import os
import signal
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timezone

# PROFILE = "linear"
# PROFILE = "basedata"
PROFILE = sys.argv[1] if len(sys.argv) > 1 else "basedata"

<<<<<<<< HEAD:tools/run_ags.py
# PROFILE = "linear"
# PROFILE = "basedata"
PROFILE = sys.argv[1] if len(sys.argv) > 1 else "basedata"
print(f"Using profile: {PROFILE}")

num_workers = 1
ags_list = (
    # Top 10 Biggest Cities in Bavaria
    # "09162000", # München
    # "09564000", # Nürnberg
    # "09761000", # Augsburg
    # "09362000", # Regensburg
    # "09161000", # Ingolstadt
    # "09663000", # Würzburg
    # "09563000", # Fürth
    # "09562000", # Erlangen
    # "09461000", # Bamberg
    # "09462000", # Bayreuth
    # Additional Requested Municipalities
    "09780139",  # Sonthofen (BY)
    "09185149",  # Neuburg a. d. Donau (BY)
    "09163000",  # Rosenheim (BY)
    # # Additional Requested Municipalities
    # "09276111", # Bayerisch Eisenstein
    # "09179111", # Adelshofen (Oberbayern)
    # "09675112", # Albertshofen
    # "09774111", # Aletshausen
    # "09772114", # Allmannshofen
    # "09273119", # Biburg
    # "09272116", # Eppenschlag
    # "09271126", # Hunding
    # "09272140", # Ringelai
    # "09272152", # Zenting
)
print(f"AGS to process: {', '.join(ags_list)}")
========
num_workers = 3
#ags_list = {"09780139", "05119000", "09185149"}
ags_list = {"09162000", "09564000", "09761000"}
# - "09780139"  # Sonthofen (BY)
# - "05119000" # Oberhausen (NRW)
# - "09185149" # Neuburg a. d. Donau (BY)
>>>>>>>> 5521146 (infdb-basedata-ways is implemented for parallel ags processing):tools/run_profile.py


SCRIPT_DIR = Path(__file__).parent
running_processes = set()
running_lock = threading.Lock()
stop_event = threading.Event()


def run_ags(ags):
    if stop_event.is_set():
        return

    process = subprocess.Popen(
<<<<<<<< HEAD:tools/run_ags.py
        ["bash", SCRIPT_DIR / "run-profile.sh", PROFILE, ags],
========
        ["bash", SCRIPT_DIR / "run.sh", PROFILE, ags],
>>>>>>>> 5521146 (infdb-basedata-ways is implemented for parallel ags processing):tools/run_profile.py
        start_new_session=True,
    )

    with running_lock:
        running_processes.add(process)

    try:
        process.wait()
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, process.args)
    finally:
        with running_lock:
            running_processes.discard(process)


def signal_handler(sig, frame):
    print("\nInterrupt received, stopping Docker...")
    stop_event.set()
    with running_lock:
        for process in list(running_processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGINT)
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(run_ags, ags) for ags in ags_list]
            for future in as_completed(futures):
                future.result()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0)
