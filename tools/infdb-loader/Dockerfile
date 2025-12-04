FROM python:3.12-slim

WORKDIR /app
USER root
ENV DEBIAN_FRONTEND=noninteractive

# Base tooling + GDAL + aria2 + unzip
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    aria2 unzip \
    gdal-bin \
 && rm -rf /var/lib/apt/lists/*

# PostgreSQL client (PGDG) + PostGIS necessary for shp2pgsql / raster2pgsql
RUN install -d /usr/share/postgresql-common/pgdg && \
    curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
    . /etc/os-release && \
    bash -c "echo 'deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $VERSION_CODENAME-pgdg main' > /etc/apt/sources.list.d/pgdg.list" && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
      postgresql-client-17 \
      postgis \ 
    && rm -rf /var/lib/apt/lists/*

# Install uv (Python package manager)
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"

# COPY requiremets into the container
COPY pyproject.toml /app/pyproject.toml
# COPY uv.lock /app/uv.lock

## Install venv with uv
# RUN uv sync

## Install infdb package in editable mode
# COPY infdb_package /app/infdb_package
# RUN uv pip install -e /app/infdb_package

# Copy the source code into the container
COPY src /app/src
COPY sql /app/sql
COPY main.py /app/main.py

# Run main.py with uv and environment defined in pyproject.toml
CMD ["uv", "run", "python", "main.py"]
