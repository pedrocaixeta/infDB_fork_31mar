FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /workspaces/choose-a-name

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# COPY requiremets into the container
COPY pyproject.toml /workspaces/choose-a-name/pyproject.toml
# COPY uv.lock /workspaces/choose-a-name/uv.lock

# Copy the source code into the container
COPY src /workspaces/choose-a-name/src
COPY sql /workspaces/choose-a-name/sql
COPY main.py /workspaces/choose-a-name/main.py

# Run main.py with uv and environment defined in pyproject.toml
CMD ["uv", "run", "python", "main.py"]