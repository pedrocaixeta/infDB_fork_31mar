FROM timescale/timescaledb-ha:pg17

# Install pgRouting
USER root
RUN apt-get update && \
    apt-get install -y postgresql-17-pgrouting && \
    rm -rf /var/lib/apt/lists/*

# Copy 3DCityDB files and initialization script
COPY 3dcitydb-5.1.0 /tmp/3dcitydb
# RUN chmod +x /tmp/3dcitydb/postgresql/shell-scripts/unix/create-db.sh
COPY 020_init-3dcitydb.sh /docker-entrypoint-initdb.d/020_init-3dcitydb.sh
RUN chmod +x /docker-entrypoint-initdb.d/020_init-3dcitydb.sh
USER postgres