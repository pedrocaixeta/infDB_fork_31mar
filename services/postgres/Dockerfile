FROM timescale/timescaledb-ha:pg17

# Install pgRouting
USER root
RUN apt-get update && \
    apt-get install -y postgresql-17-pgrouting && \
    rm -rf /var/lib/apt/lists/*
USER postgres