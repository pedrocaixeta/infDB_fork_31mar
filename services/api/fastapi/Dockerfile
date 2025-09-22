FROM python:3.12-slim

WORKDIR /app

# system deps (optional but nice to have for httpx performance)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir fastapi uvicorn[standard] httpx

# copy only the app file you actually have
COPY main.py /app/main.py

EXPOSE 8000

# run the app found in main.py
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]