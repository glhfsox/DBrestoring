# Running dbrestore 24/7 on a server

This sets up automated backups on a Linux server you control (e.g. the VPS where
your database already runs). It uses the published Docker image — which bundles
`pg_dump`, `mysqldump`, `mongodump`, etc. — driven by a **systemd timer** on the
host. Nothing stays running between backups: the timer fires the container on a
schedule, it backs up, and exits.

## 1. Prerequisites

- Docker installed (`curl -fsSL https://get.docker.com | sh`)
- The server can reach your database (same box → use `--network host`, already
  set in the unit; remote DB → reference its address in `dbrestore.yaml`)

## 2. Lay down config and secrets

```bash
sudo mkdir -p /etc/dbrestore /var/lib/dbrestore/backups

# Your config (profiles, storage, retention). Reference secrets as ${VAR}.
sudo cp dbrestore.yaml /etc/dbrestore/dbrestore.yaml

# Secrets, kept out of the YAML and out of the process list.
sudo cp deploy/dbrestore.env.example /etc/dbrestore/dbrestore.env
sudo nano /etc/dbrestore/dbrestore.env        # fill in real values
sudo chmod 600 /etc/dbrestore/dbrestore.env
```

## 3. Install the systemd units

```bash
sudo cp deploy/dbrestore@.service deploy/dbrestore@.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

## 4. Enable a schedule per profile

The units are templated on the profile name (the part after `@`):

```bash
sudo systemctl enable --now dbrestore@local_pg.timer
# add more profiles the same way:
# sudo systemctl enable --now dbrestore@local_mongo.timer
```

## 5. Verify

```bash
# Run one backup right now to prove the whole path works:
sudo systemctl start dbrestore@local_pg.service
journalctl -u dbrestore@local_pg.service -n 50 --no-pager

# See when it fires next:
systemctl list-timers 'dbrestore@*'

# Confirm artifacts landed:
ls -la /var/lib/dbrestore/backups/
```

## Failure alerting (healthchecks.io)

A backup that silently stops running is the worst kind of failure. The units ping
[healthchecks.io](https://healthchecks.io) (free) so you get an email/Slack/etc.
alert if a scheduled backup ever fails *or simply doesn't run* (dead-man's switch).

1. Create a free account, open your project, and copy its **Ping key**.
2. Put it in `/etc/dbrestore/dbrestore.env`: `HC_PING_KEY=...`
3. That's it — the first run auto-creates a check named `dbrestore-<profile>`
   (and `dbrestore-offsite-<profile>` if you enable offsite sync). Set the
   period/grace to match your schedule in the healthchecks.io UI.

Leaving `HC_PING_KEY` unset disables pinging entirely — no errors.

## Offsite copies (disaster recovery)

A backup that lives only on the same box as the database dies with that box. Two
options:

- **Backups go straight offsite**: set `storage.type: s3` with an `endpoint_url`
  in `dbrestore.yaml` (free tiers: Cloudflare R2 / Backblaze B2). Simple, but you
  lose fast local restores. (Storage is a single global backend — it's local *or*
  S3, not both.)
- **Local + offsite mirror (recommended)**: keep backups local for fast restores
  and *also* sync them offsite with the included rclone units. Fill in the
  `OFFSITE_BUCKET` and `RCLONE_CONFIG_OFFSITE_*` values in the env file, then:

  ```bash
  sudo cp deploy/dbrestore-offsite@.service deploy/dbrestore-offsite@.timer /etc/systemd/system/
  sudo systemctl daemon-reload
  sudo systemctl enable --now dbrestore-offsite@local_pg.timer
  # test once:
  sudo systemctl start dbrestore-offsite@local_pg.service
  ```

  It runs at 03:00 by default — an hour after the 02:00 backup — and uploads only
  new run directories (`copy --immutable`).

## Scheduled sanitize (anonymized copies on a cadence)

To refresh an anonymized dev/staging copy automatically, set the destination in
the profile's `masking` block (`output:` and/or `target_profile:`), then install
the sanitize units:

```bash
sudo cp deploy/dbrestore-sanitize@.service deploy/dbrestore-sanitize@.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dbrestore-sanitize@prod.timer
# test once:
sudo systemctl start dbrestore-sanitize@prod.service
```

It runs nightly at 04:00 (after the 02:00 backup) and reads the masking rules +
destination from `dbrestore.yaml`, so no extra flags are needed.

## Notes

- **Change the schedule** by editing `OnCalendar=` in the timer unit, then
  `sudo systemctl daemon-reload`.
- **Retention** is handled by dbrestore itself (`retention.keep_last` in the
  config), so old local runs are pruned automatically.
- **No-Docker alternative**: `pip install dbrestore`, install the DB client
  tools on the host, then use the built-in scheduler:
  `sudo dbrestore schedule install --profile local_pg`.
