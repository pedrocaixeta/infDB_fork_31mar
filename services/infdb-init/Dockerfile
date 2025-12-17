FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

WORKDIR /app


# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Install postgresql-client for pg_dump and psql commands
RUN install -d /usr/share/postgresql-common/pgdg && \
curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
. /etc/os-release && \
bash -c "echo 'deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $VERSION_CODENAME-pgdg main' > /etc/apt/sources.list.d/pgdg.list" && \
apt update && \
apt install -y postgresql-client-17

# COPY requiremets into the container
COPY pyproject.toml /app/pyproject.toml

## Install infdb package in editable mode
# COPY ../../src/infdb_package /app/mnt/infdb_package
# RUN uv pip install -e /app/mnt/infdb_package

# Copy the source code into the container
COPY src /app/src
COPY sql /app/sql
COPY main.py /app/main.py
COPY 3dcitydb-5.1.0 /tmp/3dcitydb

# Run main.py with uv and environment defined in pyproject.toml
CMD ["uv", "run", "python", "main.py"]
