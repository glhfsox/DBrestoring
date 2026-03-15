IDEA : https://roadmap.sh/projects/database-backup-utility

# dbrestore

`dbrestore` is a Python CLI/GUI for full database backup and restore workflows across PostgreSQL, MySQL/MariaDB, MongoDB, and SQLite.

## Features

- YAML-based profile configuration
- Environment-variable-backed secrets
- Full backup and restore commands
- Minimal desktop GUI for profile editing, backup/restore actions, and history browsing
- Linux systemd schedule installation for unattended backups
- Local artifact storage with optional gzip compression
- Optional S3-compatible artifact storage
- Automatic retention cleanup for old backup runs
- Optional Slack webhook notifications for backup and verification outcomes
- JSONL run logging
- Preflight validation for config, output paths, and required native tools

## Under construction

-Incremental backup
-Differential backup
-Full Windows/macOS scheduling support 

## Installation

```bash
pip install .
```

Or for isolated CLI usage:

```bash
pipx install .
```

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
dbrestore test-connection --profile local_pg
dbrestore backup --profile local_pg
dbrestore verify-latest --profile local_pg --target-profile local_pg_verify
dbrestore restore --profile local_pg --input ./backups/local_pg/20260315T120000_abcd1234
dbrestore gui
sudo dbrestore schedule install --profile local_pg
dbrestore schedule status --profile local_pg
sudo dbrestore schedule remove --profile local_pg
```

## Notes

- PostgreSQL, MySQL/MariaDB, and MongoDB require their vendor dump/restore tools on `PATH`.
- S3-backed storage requires valid S3 credentials and uses `output_dir` as a local staging area during backup creation.
- SQLite backup and restore use Python's `sqlite3` API.
- `dbrestore.yaml` is the canonical default config filename. `dbrestore.yml` still loads as a legacy fallback.
- Slack notifications are best-effort only. Delivery failures are logged, but they do not turn a successful backup into a failed one.
- The GUI launches with `dbrestore gui` or `dbrestore-gui` and uses the same YAML config and backup logic as the CLI.
- `verify-latest` restores the newest backup from one profile into a separate target profile and then runs a connection test against that restored database.
- Each backup run writes a payload file and a colocated `manifest.json`.
- Run logs and manifests use the local machine timezone with second-level precision.
- Retention cleanup runs after each successful backup when a retention policy is configured.
- Schedule installation writes system-wide units to `/etc/systemd/system/` and per-profile env templates to `/etc/dbrestore/env/` by default.
- Scheduled profiles use `systemd` timer presets (`hourly`, `daily`, `weekly`) with `Persistent=true` support for catch-up runs after downtime.
- Run the install/remove commands with `sudo` so the CLI can write unit files and reload `systemd`.
