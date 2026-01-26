---
icon: material/map-legend
---
# QGIS Webclient (QWC2) :material-map-legend:

The **QGIS Web Client 2 (QWC2)** service provides a modern, responsive web application for visualizing and interacting with QGIS projects. It allows users to publish geospatial data stored in the infDB database to the web, offering capabilities such as layer management, feature information, searching, and map printing. More information can be found on the official [Github Repo](https://github.com/qgis/qwc2/).

## Configuration

The configuration is managed via environment variables:

```bash title=".env"
# ==============================================================================
# SERVICE ACTIVATION
# ==============================================================================
# Select profiles to activate
COMPOSE_PROFILES=...,qwc,...  # (1)

# ==============================================================================
# QWC2 SEVICE
# ==============================================================================
# Profile: qwc

# Port on which the QWC2 is available on the host machine
SERVICES_QWC_EXPOSED_PORT=8088 # (2)
```

1.  **Activate service**: The `qwc` profile must be included to activate the QWC service.
2.  **Port**: The port on which QWC2 is available.

## Access

If you activate the service, it should be available on the default port `SERVICES_QWC_EXPOSED_PORT=8088` via your browser:

=== "Local"
    http://localhost:8088

=== "Remote"
    http://IP-ADDRESS-OF-HOST:8088

###Review: I think we need a more detailed description here to enable others to upload their projects?###