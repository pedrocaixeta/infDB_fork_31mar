FROM python:3.12-slim
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gdal-bin \
    build-essential \
    python3-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*
# Install uv package manager
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH="/root/.local/bin/:$PATH"

## Copy pylovo package, pyproject for infdb and main.py into app
COPY src/pylovo /app/src/pylovo
COPY pyproject.toml /app/pyproject.toml
COPY main.py /app/main.py

# Run script and autoinstall dependencies via uv
CMD ["uv", "run", "python", "main.py"]
