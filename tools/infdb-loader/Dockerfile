FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /workspaces/infdb-loader

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# COPY requiremets into the container
COPY pyproject.toml /workspaces/infdb-loader/pyproject.toml
# COPY uv.lock /workspaces/infdb-loader/uv.lock

# Copy the source code into the container
COPY src /workspaces/infdb-loader/src
COPY sql /workspaces/infdb-loader/sql
COPY main.py /workspaces/infdb-loader/main.py

# Run main.py with uv and environment defined in pyproject.toml
CMD ["uv", "run", "python", "main.py"]
