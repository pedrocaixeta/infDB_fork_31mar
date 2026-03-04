# Run infDB Tools
If you want to run a profile (multiple linked tools) or a single tool, you can use the bash script `tools.sh`:
```bash
# Profile
# bash tools.sh -p PROFILE AGS
bash tools.sh -p linear 09185149

# Single Tool
# bash tools.sh -t TOOL AGS
bash tools.sh -t ro-heat 09185149
```

## Run multiple AGS
```bash
# Profile
uv run python3 run_ags.py -p linear [-a AGS1,AGS2,... -n NUM_WORKERS -c]

# Single Tool
uv run python3 run_ags.py -t ro-heat [-a AGS1,AGS2,... -n NUM_WORKERS -c]

```

There are optional parameters:
- `-a AGS1,AGS2,...`: Comma-separated list of AGS
- `-n NUM_WORKERS`: Number of parallel workers to use (default: 5)
- `-c`: Clean database before running

AGS:
- 09780139 Sonthofen
- 09185149 Neuburg a. d. Donau
- 05119000 Oberhausen (NRW)

Profiles available:
- linear