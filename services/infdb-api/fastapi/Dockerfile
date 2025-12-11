FROM python:3.12-slim
# migrate to ghcr.io/astral-sh/uv:python3.12-trixie-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir fastapi uvicorn[standard] httpx shapely

COPY main.py /app/main.py


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]