FROM python:latest

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install JupyterLab
RUN pip install --no-cache-dir jupyterlab

# Workdir for notebooks
WORKDIR /app

# # Expose Jupyter port
# EXPOSE 8888

# Start JupyterLab
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token="${SERVICES_JUPYTER_TOKEN}" --NotebookApp.notebook_dir='/app/'
