# docker/db/Dockerfile (example path)
FROM timescale/timescaledb-ha:pg17

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      postgresql-17-pgrouting \
    && rm -rf /var/lib/apt/lists/*
USER postgres
