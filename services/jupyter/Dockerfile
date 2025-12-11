FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

# Workdir for notebooks
WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
COPY notebooks/ /app/notebooks/

# Install JupyterLab
RUN uv venv --seed
RUN uv pip install -e .

# Expose Jupyter port
EXPOSE 8888

# Jupyter config file
COPY jupyter_notebook_config.py /root/.jupyter/jupyter_notebook_config.py

# Start JupyterLab
CMD [ "uv","run","--with","jupyter","jupyter","lab"]
