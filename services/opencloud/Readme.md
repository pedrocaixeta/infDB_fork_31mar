# Quick setup guide for infDB opencloud configuration

Mor information on https://docs.opencloud.eu/docs/admin/getting-started/container/docker-compose/external-proxy

## Change Enviroment File
You can also take the already adjusted env file under services/opencloud/.env or follow these steps:

Copy env template.

```bash
cp .env.example .env
```


Change domains in .env
```bash
# Config compose for collabora (office) + external proxy (NGINX)
COMPOSE_FILE=docker-compose.yml:weboffice/collabora.yml:external-proxy/opencloud.yml:external-proxy/collabora.yml

# Set domains
OC_DOMAIN=cloud.ocd.need.energy
COLLABORA_DOMAIN=collabora.ocd.need.energy
WOPISERVER_DOMAIN=wopiserver.ocd.need.energy

# Set initial password
INITIAL_ADMIN_PASSWORD=YourSecurePassword

# Set path for data storage
OC_CONFIG_DIR=/data/opencloud/config
OC_DATA_DIR=/data/opencloud/data
```

Set permissions for data storage:
```bash
sudo mkdir -p /data/opencloud/{config,data}
sudo chown -R 1000:1000 /data/opencloud
```
## NGINX Reverse Proxy Manager

Add three subdomains with SSL and Cache Assets + Websockets Support
- https://cloud.ocd.need.energy to http://10.162.28.78:9200
- https://collabora.ocd.need.energy/ http://10.162.28.78:9980
- https://wopiserver.ocd.need.energy/ to http://10.162.28.78:9300


## Start Opencloud
Finally, start opencloud:
```bash
docker compose up -d
```
Open it [https://cloud.ocd.need.energy](https://cloud.ocd.need.energy)
with `admin` and the choosen password above.

## Stop Opencloud
```bash
docker compose down
```