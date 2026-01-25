import subprocess

num_workers = 3
ags_list = ("09780139", "05119000", "09185149")
# - "09780139"  # Sonthofen (BY)
# - "05119000" # Oberhausen (NRW)
# - "09185149" # Neuburg a. d. Donau (BY)

ags_worker = tuple(ags_list[i::num_workers] for i in range(num_workers))

for i in range(num_workers):
    subprocess.run(["bash", "run.sh", "--ags", ",".join(ags_worker[i])], check=True)