FROM python:3.12-slim

WORKDIR /app

## Install uv on Ubuntu
# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Download the latest uv installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Install GDAL required for some geo-transformations
RUN apt install gdal-bin -y

# Add build dependencies to avoid scikit-learn-extra error
RUN apt install build-essential python3-dev -y

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# COPY requiremets into the container
COPY pyproject.toml /app/pyproject.toml
# COPY uv.lock /app/uv.lock

## Install venv with uv
# RUN uv sync

## Install infdb package in editable mode
# COPY infdb-package /app/infdb-package
# RUN uv pip install -e /app/infdb-package

# Copy the source code into the container
COPY src /app/src

# Create pylovo database and generate grid files for the hardcoded ags in pylovo/config/config_generation.yaml
CMD ["sh", "-c", "cd /app/src/pylovo && uv run python -m runme.main_constructor && uv run python -m runme.main_generation"]

