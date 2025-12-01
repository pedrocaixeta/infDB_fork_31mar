FROM 3dcitydb/citydb-tool
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root
RUN apt-get update && apt-get install -y aria2 && rm -rf /var/lib/apt/lists/*

WORKDIR /workspaces/infdb-loader

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# COPY requiremets into the container
COPY pyproject.toml /workspaces/infdb-loader/pyproject.toml
# COPY uv.lock /workspaces/infdb-loader/uv.lock

# Copy the source code into the container
COPY src src
COPY sql sql
COPY main.py main.py

# Reset entrypoint to the default because of citydb-tool image
ENTRYPOINT []

# Run main.py with uv and environment defined in pyproject.toml
CMD ["uv", "run", "python", "main.py"]