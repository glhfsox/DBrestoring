IDEA : https://roadmap.sh/projects/database-backup-utility

# dbrestore

`dbrestore` is a Python CLI/GUI for full database backup and restore workflows across PostgreSQL, MySQL/MariaDB, MongoDB, and SQLite.

## Features


- Full, incremental, and differential backup modes (`--mode full|differential|incremental`) with content addressed removal of chunk duplicates 
- Optional AES-256-GCM encryption of backup artifacts (`--passphrase` or `encryption.passphrase` in config)
- Data masking to produce sanitized copies (`dbrestore sanitize`) — deterministic PII anonymization
- Full backup and restore commands
- Desktop GUI for profile editing, backup/restore actions, readiness dashboards, schedule management, env-file editing, and history browsing
- Linux systemd and macOS launchd schedule installation for unattended backups
- Local artifact storage with optional gzip compression
- Optional S3-compatible artifact storage
- Automatic retention cleanup for old backup runs
- Optional Slack webhook notifications for backup and verification outcomes
- Preflight validation for config, output paths, and required native tools
- Configurable verification targets plus scheduled backup+verification cycles
- Status/readiness and preflight commands for operational checks

## Under construction

- Richer cross-platform scheduling presets and custom calendar expressions
- Variable size chunking
- GPG
- Web-UI

## Selective restore

- PostgreSQL supports selective table restore via repeated `--table`.
- MongoDB supports selective collection restore via repeated `--collection`.
- MySQL/MariaDB and SQLite selective restore are intentionally rejected with the current backup formats.

## Encryption

Artifacts can be encrypted at rest with AES-256-GCM (scrypt-derived key, random
salt and nonce per file). Encrypted files get a `.enc` extension and are detected
automatically on restore; a wrong passphrase is rejected by the GCM tag.

Pass it on the CLI or via `DBRESTORE_PASSPHRASE`:

```bash
dbrestore backup --profile local_pg --passphrase secret
dbrestore restore --profile local_pg --input ./backups/local_pg/<run> --passphrase secret
```

Or in config:

```yaml
defaults:
  encryption:
    passphrase: ${DBRESTORE_PASSPHRASE}
```

Works with `--mode full` only, not the chunked differential/incremental modes.

## Data masking (sanitized copies)

`dbrestore sanitize` pulls a snapshot of a database and applies PII masking rules
to produce a safe, anonymized copy — useful for seeding dev/staging from prod.
Masking is deterministic (the same value always maps to the same masked value, so
joins stay consistent) and irreversible. The source database is never modified.

```yaml
profiles:
  prod:
    db_type: sqlite
    database: ./prod.sqlite
    masking:
      salt: ${MASKING_SALT}          # optional; fixes outputs across runs
      target_profile: staging        # optional default destination (for scheduling)
      output: ./sanitized.sqlite      # optional default output path
      rules:
        - { table: users, column: email, strategy: email }
        - { table: users, column: full_name, strategy: name }
        - { table: users, column: phone, strategy: phone }
        - { table: cards, column: pan, strategy: redact }
```

```bash
# SQLite: mask the dumped file directly
dbrestore sanitize --profile prod --output ./sanitized.sqlite
dbrestore sanitize --profile prod --output ./sanitized.sqlite --target-profile staging

# Postgres/MySQL/MariaDB: mask via a scratch DB, then dump the sanitized result
dbrestore sanitize --profile prod_pg --target-profile scratch_pg --output ./sanitized.dump
```

Strategies: `email`, `name`, `phone`, `hash`, `redact`, `constant`, `null`.

SQLite is masked directly on the dumped file. Postgres/MySQL/MariaDB go through a
scratch `--target-profile`: dbrestore backs up the source, restores it into the
scratch database, masks it there with SQL, and (with `--output`) dumps the
sanitized result. The source database is never modified.

**Scheduling:** put the destination in the `masking` block (`output:` and/or
`target_profile:`) so the unattended command is just `dbrestore sanitize --profile X`.
[deploy/](deploy/) ships `dbrestore-sanitize@.{service,timer}` to run it nightly
(e.g. a fresh anonymized dev DB every morning).

## Installation

```bash
pip install .
```

Or for isolated CLI usage:

```bash
pipx install .
```

For development:

```bash
pip install -e '.[dev]'
```

## Docker

The release workflow publishes an image with the DB client tools (`pg_dump`,
`mysqldump`, `mongodump`, …) included:

```bash
docker pull ghcr.io/glhfsox/dbrestoring:latest
```

The CLI is the entrypoint, so arguments after the image name are `dbrestore`
subcommands. Mount the config and an artifacts directory:

```bash
docker run --rm \
  -v "$(pwd)/dbrestore.yaml:/work/dbrestore.yaml:ro" \
  -v "$(pwd)/backups:/work/backups" \
  ghcr.io/glhfsox/dbrestoring:latest backup --profile local_pg
```

Add `-e DBRESTORE_PASSPHRASE` to pass an encryption passphrase, and `--network host`
to reach a database on the host. The GUI is not in the image.

## Central reporting (control plane)

A server can report each backup to a central dashboard. Add a `control_plane`
block and every run posts its outcome (status, size, duration) to the fleet view:

```yaml
defaults:
  control_plane:
    url: https://your-console.vercel.app
    token: ${DBRESTORE_CP_TOKEN}   # must match the console's INGEST_TOKEN
    server_id: prod-db-1           # optional; defaults to the hostname
```

Reporting is best-effort — if the console is unreachable the backup still
succeeds. The console (dashboard + ingestion API) lives in [web/](web/).

## Running 24/7 on a server

For unattended scheduled backups on a Linux server, see [deploy/](deploy/): the
Docker image driven by a systemd timer. It runs on a schedule and survives reboots.

## Example configuration

The default config filename is `dbrestore.yaml`. Use `--config` if you keep it somewhere else.

```yaml
version: 1
defaults:
  output_dir: ./backups
  log_dir: ./logs
  compression: gzip
  retention:
    keep_last: 7
  notifications:
    slack:
      webhook_url: ${SLACK_WEBHOOK}
      events:
        - backup.completed
        - backup.failed
        - verification.completed
        - verification.failed

profiles:
  local_pg:
    db_type: postgres
    host: localhost
    port: 5432
    username: app
    password: ${PGPASSWORD}
    database: app_db
    schedule:
      preset: hourly
      persistent: true
    verification:
      target_profile: local_pg_verify
      schedule_after_backup: true

  local_pg_verify:
    db_type: postgres
    host: localhost
    port: 5432
    username: app
    password: ${PGPASSWORD}
    database: app_db_verify

  local_sqlite:
    db_type: sqlite
    database: ./data/app.sqlite3
    compression: false
    retention:
      keep_last: 3
```

S3-backed storage example:

```yaml
version: 1
defaults:
  output_dir: ./.dbrestore-staging
  log_dir: ./logs

storage:
  type: s3
  bucket: my-dbrestore-backups
  prefix: prod
  region: eu-central-1
  access_key_id: ${AWS_ACCESS_KEY_ID}
  secret_access_key: ${AWS_SECRET_ACCESS_KEY}

profiles:
  local_pg:
    db_type: postgres
    host: localhost
    port: 5432
    username: app
    password: ${PGPASSWORD}
    database: app_db
```

## Commands

```bash
dbrestore validate-config
dbrestore preflight --profile local_pg
dbrestore status --profile local_pg
dbrestore test-connection --profile local_pg
dbrestore backup --profile local_pg
dbrestore backup --profile local_pg --passphrase "correct horse battery staple"
dbrestore sanitize --profile local_sqlite --output ./sanitized.sqlite
dbrestore run-scheduled --profile local_pg
dbrestore verify-latest --profile local_pg --target-profile local_pg_verify
dbrestore restore --profile local_pg --input ./backups/local_pg/20260315T120000_abcd1234
dbrestore restore --profile local_pg --input ./backups/local_pg/20260315T120000_abcd1234 --table public.items --table public.orders
dbrestore restore --profile local_mongo --input ./backups/local_mongo/20260315T120000_abcd1234 --collection users --collection audit
dbrestore gui
sudo dbrestore schedule install --profile local_pg
dbrestore schedule status --profile local_pg
dbrestore schedule show-env --profile local_pg
sudo dbrestore schedule remove --profile local_pg
```
