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
# Copy and install pylovo package
COPY src/pylovo /app/pylovo
WORKDIR /app/pylovo
RUN uv sync
# Copy main.py
WORKDIR /app
COPY main.py /app/main.py
# Install infdb_package and run main.py (simple and clean)
CMD ["sh", "-c", "pip install -q /app/mnt/infdb_package && python3 /app/main.py"]
