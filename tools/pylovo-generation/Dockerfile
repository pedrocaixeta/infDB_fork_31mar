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

# Copy pylovo package source code
COPY src/pylovo /app/pylovo

# Install pylovo package with uv
WORKDIR /app/pylovo
RUN uv sync

# Go back to app root
WORKDIR /app

# Create entrypoint script that uses pylovo CLI commands directly
RUN echo '#!/bin/bash\n\
set -e\n\
echo "Installing infdb package..."\n\
cd /app/pylovo\n\
uv pip install /app/mnt/infdb-package\n\
\n\
echo "Getting AGS from InfDB config..."\n\
AGS=$(uv run python -c "\n\
from infdb import InfDB\n\
infdb = InfDB(tool_name='"'"'pylovo-generation'"'"')\n\
ags = infdb.get_value(['"'"'base'"'"', '"'"'scope'"'"'])[0]\n\
print(ags)\n\
")\n\
\n\
echo "AGS from config: $AGS"\n\
\n\
echo "Setting up pylovo database..."\n\
uv run pylovo-setup\n\
\n\
echo "Generating synthetic grids for AGS: $AGS..."\n\
uv run pylovo-generate --ags $AGS\n\
\n\
echo "Grid generation completed successfully!"\n\
' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Run the entrypoint script
CMD ["/app/entrypoint.sh"]

