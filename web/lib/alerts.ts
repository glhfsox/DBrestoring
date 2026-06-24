import type { ServerHealth } from "./db";

export type AlertState = "ok" | "overdue" | "failing";

// "overdue" = no successful backup within the window (stopped or persistently
// failing); "failing" = recent success but the latest run failed.
export function evaluateState(
  h: { lastSuccessAt: number | null; lastStatus: string | null },
  maxAgeMs: number,
  now: number,
): AlertState {
  if (h.lastSuccessAt == null || now - h.lastSuccessAt > maxAgeMs) return "overdue";
  if (h.lastStatus === "failed") return "failing";
  return "ok";
}

export function describe(h: ServerHealth, state: AlertState, now: number): string {
  const who = `${h.name} (${h.id})`;
  if (state === "overdue") {
    const age =
      h.lastSuccessAt == null
        ? "never succeeded"
        : `last success ${Math.floor((now - h.lastSuccessAt) / 3_600_000)}h ago`;
    return `${who}: no recent successful backup — ${age}`;
  }
  if (state === "failing") return `${who}: latest backup FAILED`;
  return `${who}: recovered — backups healthy again`;
}
