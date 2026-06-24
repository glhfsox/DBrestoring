import { NextResponse } from "next/server";
import { getAlertStates, serverHealth, setAlertState, type ServerHealth } from "@/lib/db";
import { type AlertState, describe, evaluateState } from "@/lib/alerts";
import { sendEmail } from "@/lib/notify";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// Vercel Cron sends `Authorization: Bearer ${CRON_SECRET}` when CRON_SECRET is
// set. If it isn't set we allow the call (dev / manual trigger), but setting it
// is strongly recommended so the endpoint can't be triggered by anyone.
function authorized(req: Request): boolean {
  const secret = process.env.CRON_SECRET;
  if (!secret) return true;
  return req.headers.get("authorization") === `Bearer ${secret}`;
}

export async function GET(req: Request) {
  if (!authorized(req)) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const maxAgeHours = Number(process.env.ALERT_MAX_AGE_HOURS) || 26;
  const maxAgeMs = maxAgeHours * 3_600_000;
  const now = Date.now();

  let health: ServerHealth[];
  let states: Map<string, string>;
  try {
    health = await serverHealth();
    states = await getAlertStates();
  } catch (err) {
    console.error("[cron] check-backups failed:", err);
    return NextResponse.json({ error: "Check failed" }, { status: 500 });
  }

  const problems: string[] = [];
  const recovered: string[] = [];

  for (const h of health) {
    const state = evaluateState(h, maxAgeMs, now);
    const prev = (states.get(h.id) ?? "ok") as AlertState;
    if (state !== prev) await setAlertState(h.id, state, now);
    if (state !== "ok" && prev === "ok") problems.push(describe(h, state, now));
    if (state === "ok" && prev !== "ok") recovered.push(describe(h, "ok", now));
  }

  if (problems.length > 0 || recovered.length > 0) {
    const lines: string[] = [];
    if (problems.length > 0) lines.push("Backup problems:", ...problems);
    if (recovered.length > 0) lines.push(...(lines.length ? [""] : []), "Recovered:", ...recovered);
    try {
      await sendEmail({
        to: process.env.ALERT_TO_EMAIL ?? process.env.CONTACT_TO_EMAIL,
        subject: `dbrestore: ${problems.length} alert(s), ${recovered.length} recovered`,
        text: lines.join("\n"),
      });
    } catch (err) {
      console.error("[cron] failed to send alert email:", err);
    }
  }

  return NextResponse.json({
    checked: health.length,
    newProblems: problems.length,
    recovered: recovered.length,
    maxAgeHours,
  });
}
