# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Stage 1 — build the dbrestore wheel
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

WORKDIR /build
RUN pip install --no-cache-dir build
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m build --wheel --outdir /dist

# ---------------------------------------------------------------------------
# Stage 2 — runtime image with the native DB client tools baked in
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Native client tools required by the adapters:
#   postgres      -> pg_dump / pg_restore     (PGDG client, version 17)
#   mysql/mariadb -> mysqldump / mysql        (default-mysql-client)
#   mongodb       -> mongodump / mongorestore (mongodb-database-tools)
# SQLite uses Python's stdlib sqlite3, so it needs no extra binary.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg; \
    \
    # PostgreSQL APT repository (recent pg_dump/pg_restore)
    install -d /usr/share/postgresql-common/pgdg; \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc; \
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list; \
    \
    # MongoDB APT repository (mongodb-database-tools)
    curl -fsSL https://pgp.mongodb.com/server-7.0.asc \
        | gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg] https://repo.mongodb.org/apt/debian bookworm/mongodb-org/7.0 main" \
        > /etc/apt/sources.list.d/mongodb-org-7.0.list; \
    \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        postgresql-client-17 \
        default-mysql-client \
        mongodb-database-tools; \
    \
    # ca-certificates stays (TLS for S3/Slack); build-only tools are removed
    apt-get purge -y --auto-remove curl gnupg; \
    rm -rf /var/lib/apt/lists/*

# Install the application from the built wheel
COPY --from=builder /dist/*.whl /tmp/
RUN pip install /tmp/*.whl && rm -rf /tmp/*.whl

# Drop privileges
RUN useradd --create-home --uid 10001 dbrestore
USER dbrestore
WORKDIR /work

# The CLI is the entrypoint: `docker run <image> backup --profile ...`
ENTRYPOINT ["dbrestore"]
CMD ["--help"]
