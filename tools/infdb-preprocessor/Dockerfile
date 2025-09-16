FROM python:latest

WORKDIR /app

## Install uv on Ubuntu
# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Download the latest uv installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Install postgresql-client for pg_dump and psql commands
RUN apt install curl ca-certificates &&\
install -d /usr/share/postgresql-common/pgdg && \
curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
. /etc/os-release && \
bash -c "echo 'deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $VERSION_CODENAME-pgdg main' > /etc/apt/sources.list.d/pgdg.list" && \
apt update && \
apt install -y postgresql-client-17

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# COPY requiremets into the container
COPY pyproject.toml /app/pyproject.toml

## Install venv with uv
RUN uv sync

## Install infdb package in editable mode
COPY infdb_package /app/infdb_package
RUN uv pip install -e /app/infdb_package

# Copy the source code into the container
COPY src /app/src
COPY sql /app/sql
COPY main.py /app/main.py

# Run main.py with uv and environment defined in pyproject.toml
CMD ["uv", "run", "python", "main.py"]
