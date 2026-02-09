# Run infDB Tools
AGS:
- 09780139 Sonthofen

Profiles available:
- linear
- buildings-to-street
- pylovo
- basedata

## Run Linear Heat Density Toolchain
```bash
# bash tools/run-profile.sh PROFILE AGS_ID
bash tools/run-profile.sh linear 09780139
```
Run several profiles in parallel:
```bash
# python3 tools/run_ags.py PROFILE
python3 tools/run_ags.py linear
```


## Run Tool Single Service
```bash
# bash tools/run-service.sh TOOL AGS
bash tools/run-service.sh buildings-to-street 09780139
```