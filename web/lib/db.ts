import { createClient, type Client } from "@libsql/client";

// libSQL: a local file in dev (DATABASE_URL=file:local.db, the default) and
// Turso (libsql://...) in production via DATABASE_URL + DATABASE_AUTH_TOKEN.
let client: Client | null = null;
let ready: Promise<void> | null = null;

function getClient(): Client {
  if (!client) {
    client = createClient({
      url: process.env.DATABASE_URL ?? "file:local.db",
      authToken: process.env.DATABASE_AUTH_TOKEN,
    });
  }
  return client;
}

async function ensureSchema(): Promise<void> {
  const c = getClient();
  await c.execute(`
    CREATE TABLE IF NOT EXISTS servers (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      last_seen_at INTEGER NOT NULL,
      created_at INTEGER NOT NULL
    )
  `);
  await c.execute(`
    CREATE TABLE IF NOT EXISTS backup_runs (
      id TEXT PRIMARY KEY,
      server_id TEXT NOT NULL,
      profile TEXT NOT NULL,
      db_type TEXT NOT NULL,
      backup_type TEXT NOT NULL,
      status TEXT NOT NULL,
      size_bytes INTEGER,
      duration_ms INTEGER,
      started_at TEXT,
      finished_at TEXT,
      reported_at INTEGER NOT NULL,
      error TEXT
    )
  `);
  await c.execute(
    `CREATE INDEX IF NOT EXISTS idx_runs_server ON backup_runs (server_id, reported_at DESC)`,
  );
  await c.execute(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      at INTEGER NOT NULL,
      event TEXT NOT NULL,
      actor TEXT,
      ip TEXT,
      detail TEXT
    )
  `);
  await c.execute(`CREATE INDEX IF NOT EXISTS idx_audit_at ON audit_log (at DESC)`);
}

function init(): Promise<void> {
  if (!ready) ready = ensureSchema();
  return ready;
}

function num(value: unknown): number {
  return value == null ? 0 : Number(value);
}

function numOrNull(value: unknown): number | null {
  return value == null ? null : Number(value);
}

function str(value: unknown): string {
  return value == null ? "" : String(value);
}

function strOrNull(value: unknown): string | null {
  return value == null ? null : String(value);
}

export type RunInput = {
  server: { id: string; name: string };
  run: {
    id: string;
    profile: string;
    db_type: string;
    backup_type: string;
    status: "success" | "failed";
    size_bytes?: number | null;
    duration_ms?: number | null;
    started_at?: string | null;
    finished_at?: string | null;
    error?: string | null;
  };
};

export async function recordRun(input: RunInput): Promise<void> {
  await init();
  const c = getClient();
  const now = Date.now();
  const { server, run } = input;

  await c.execute({
    sql: `INSERT INTO servers (id, name, last_seen_at, created_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(id) DO UPDATE SET name = excluded.name, last_seen_at = excluded.last_seen_at`,
    args: [server.id, server.name, now, now],
  });

  await c.execute({
    sql: `INSERT INTO backup_runs
            (id, server_id, profile, db_type, backup_type, status,
             size_bytes, duration_ms, started_at, finished_at, reported_at, error)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(id) DO NOTHING`,
    args: [
      run.id,
      server.id,
      run.profile,
      run.db_type,
      run.backup_type,
      run.status,
      run.size_bytes ?? null,
      run.duration_ms ?? null,
      run.started_at ?? null,
      run.finished_at ?? null,
      now,
      run.error ?? null,
    ],
  });
}

export type FleetServer = {
  id: string;
  name: string;
  lastSeenAt: number;
  lastStatus: string | null;
  lastFinishedAt: string | null;
  runCount: number;
};

export async function fleetOverview(): Promise<FleetServer[]> {
  await init();
  const c = getClient();
  const res = await c.execute(`
    SELECT
      s.id, s.name, s.last_seen_at,
      (SELECT r.status FROM backup_runs r WHERE r.server_id = s.id ORDER BY r.reported_at DESC LIMIT 1) AS last_status,
      (SELECT r.finished_at FROM backup_runs r WHERE r.server_id = s.id ORDER BY r.reported_at DESC LIMIT 1) AS last_finished,
      (SELECT COUNT(*) FROM backup_runs r WHERE r.server_id = s.id) AS run_count
    FROM servers s
    ORDER BY s.last_seen_at DESC
  `);
  return res.rows.map((row) => ({
    id: str(row.id),
    name: str(row.name),
    lastSeenAt: num(row.last_seen_at),
    lastStatus: strOrNull(row.last_status),
    lastFinishedAt: strOrNull(row.last_finished),
    runCount: num(row.run_count),
  }));
}

export async function getServer(id: string): Promise<FleetServer | null> {
  const all = await fleetOverview();
  return all.find((s) => s.id === id) ?? null;
}

export type RunRecord = {
  id: string;
  profile: string;
  dbType: string;
  backupType: string;
  status: string;
  sizeBytes: number | null;
  durationMs: number | null;
  startedAt: string | null;
  finishedAt: string | null;
  reportedAt: number;
  error: string | null;
};

export async function listRuns(serverId: string, limit = 50): Promise<RunRecord[]> {
  await init();
  const c = getClient();
  const res = await c.execute({
    sql: `SELECT * FROM backup_runs WHERE server_id = ? ORDER BY reported_at DESC LIMIT ?`,
    args: [serverId, limit],
  });
  return res.rows.map((row) => ({
    id: str(row.id),
    profile: str(row.profile),
    dbType: str(row.db_type),
    backupType: str(row.backup_type),
    status: str(row.status),
    sizeBytes: numOrNull(row.size_bytes),
    durationMs: numOrNull(row.duration_ms),
    startedAt: strOrNull(row.started_at),
    finishedAt: strOrNull(row.finished_at),
    reportedAt: num(row.reported_at),
    error: strOrNull(row.error),
  }));
}

export type AuditEntry = {
  id: number;
  at: number;
  event: string;
  actor: string | null;
  ip: string | null;
  detail: string | null;
};

export async function recordAudit(entry: {
  event: string;
  actor?: string | null;
  ip?: string | null;
  detail?: string | null;
}): Promise<void> {
  await init();
  await getClient().execute({
    sql: `INSERT INTO audit_log (at, event, actor, ip, detail) VALUES (?, ?, ?, ?, ?)`,
    args: [Date.now(), entry.event, entry.actor ?? null, entry.ip ?? null, entry.detail ?? null],
  });
}

// Audit logging must never break the action it records.
export async function safeAudit(entry: {
  event: string;
  actor?: string | null;
  ip?: string | null;
  detail?: string | null;
}): Promise<void> {
  try {
    await recordAudit(entry);
  } catch (err) {
    console.error("[audit] failed to record:", err);
  }
}

export async function listAudit(limit = 100): Promise<AuditEntry[]> {
  await init();
  const res = await getClient().execute({
    sql: `SELECT * FROM audit_log ORDER BY at DESC LIMIT ?`,
    args: [limit],
  });
  return res.rows.map((row) => ({
    id: num(row.id),
    at: num(row.at),
    event: str(row.event),
    actor: strOrNull(row.actor),
    ip: strOrNull(row.ip),
    detail: strOrNull(row.detail),
  }));
}
