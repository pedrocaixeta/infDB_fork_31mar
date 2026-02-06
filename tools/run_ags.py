import os
import signal
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# PROFILE = "linear"
# PROFILE = "basedata"
PROFILE = sys.argv[1] if len(sys.argv) > 1 else "basedata"
print(f"Using profile: {PROFILE}")

num_workers = 1
ags_list = {
    "09276111",
    "09179111",
    "09675112",
    "09774111",
    "09772114",
    "09273119",
    "09272116",
    "09271126",
    "09272140",
    "09272152",
}
"09276111", # Achslach
"09179111", # Adelshofen
"09675112", # Albertshofen
"09774111", # Aletshausen
"09772114", # Allmannshofen
"09273119", # Biburg
"09272116", # Eppenschlag
"09271126", # Hunding
"09272140", # Ringelai
"09272152"  # Zenting
print(f"AGS to process: {', '.join(sorted(ags_list))}")

SCRIPT_DIR = Path(__file__).parent
running_processes = set()
running_lock = threading.Lock()
stop_event = threading.Event()


def run_ags(ags):
    if stop_event.is_set():
        return

    process = subprocess.Popen(
        ["bash", SCRIPT_DIR / "run-profile.sh", PROFILE, ags],
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
