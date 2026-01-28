import subprocess
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import freeze_support
from pathlib import Path

num_workers = 1
ags_list = {"09780139", "05119000", "09185149"}
# - "09780139"  # Sonthofen (BY)
# - "05119000" # Oberhausen (NRW)
# - "09185149" # Neuburg a. d. Donau (BY)

SCRIPT_DIR = Path(__file__).parent


def run_ags(ags):
    subprocess.run(["bash", SCRIPT_DIR / "run.sh", "linear", ags], check=True)


if __name__ == '__main__':
    freeze_support()
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        executor.map(run_ags, ags_list)
