import subprocess
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import freeze_support

num_workers = 3
ags_list = ("09780139", "05119000", "09185149")
# - "09780139"  # Sonthofen (BY)
# - "05119000" # Oberhausen (NRW)
# - "09185149" # Neuburg a. d. Donau (BY)


def run_ags(ags):
    subprocess.run(["bash", "run.sh", "linear", ags], check=True)


if __name__ == '__main__':
    freeze_support()
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        executor.map(run_ags, ags_list)
