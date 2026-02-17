# Rerun Opendata from Scratch
bash infdb-stop.sh
docker volume rm infdb-demo-db-data
git pull
git clean -fdx
bash infdb-start.sh -d
bash infdb-import.sh
python3 tools/run_ags.py linear