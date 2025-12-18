---
icon: material/cogs
---

The infDB provides you can preconfigured services:

![alt text](services.png)

## Jupyter
The relevant configuration in parameters for Jupyter Service
``` bash title=".env"
COMPOSE_PROFILES=...,notebook,...  # (1)
SERVICES_JUPYTER_EXPOSED_PORT=8888 # (2)
SERVICES_JUPYTER_TOKEN=infdb # (3)
```

1. Profile "notebook" must be within the list to activate Jupyter service
2. Port on which the Jupyter is available 
3. Token for identification to Jupyter Notebook

Open in your browser the following address:
=== "Local"
    http://localhost/8888

=== "Remote"
    http://IP-ADDRESS-OF-HOST/8888



## QGIS Webclient (QWC)

## pgAdmin


## Visualize infDB data in QWC Web Client
1. In [.env](.env) make sure profiles `core` and `qwc`to `COMPOSE_PROFILES`
2. Restart infDB with new profile to start services including QWC Web Client:
```bash
bash infdb-startup.sh
```
3. Open http://localhost:80/ in your web browser.

## Inspect infDB Data in Database with Postgres Admin UI
1. In [.env](.env) make sure profiles `core` and `admin`to `COMPOSE_PROFILES`
2. Restart infDB with new profile to start services including QWC Web Client:
```bash
bash infdb-startup.sh
```
3. Open http://localhost:82/ in your web browser.