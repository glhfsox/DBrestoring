import { NextResponse } from "next/server";
import { z } from "zod";
import { recordRun, safeAudit } from "@/lib/db";
import { safeEqual } from "@/lib/auth";
import { clientIp } from "@/lib/ip";

export const runtime = "nodejs";

const runSchema = z.object({
  server: z.object({
    id: z.string().min(1).max(200),
    name: z.string().min(1).max(200),
  }),
  run: z.object({
    id: z.string().min(1).max(200),
    profile: z.string().min(1).max(200),
    db_type: z.string().min(1).max(50),
    backup_type: z.string().min(1).max(50),
    status: z.enum(["success", "failed"]),
    size_bytes: z.number().int().nonnegative().nullable().optional(),
    duration_ms: z.number().int().nonnegative().nullable().optional(),
    started_at: z.string().max(100).nullable().optional(),
    finished_at: z.string().max(100).nullable().optional(),
    error: z.string().max(2000).nullable().optional(),
  }),
});

function bearer(req: Request): string {
  const header = req.headers.get("authorization") ?? "";
  return header.startsWith("Bearer ") ? header.slice(7) : "";
}

export async function POST(req: Request) {
  const expected = process.env.INGEST_TOKEN;
  if (!expected) {
    return NextResponse.json({ error: "Ingestion is not configured." }, { status: 503 });
  }
  const token = bearer(req);
  if (!token || !safeEqual(token, expected)) {
    await safeAudit({
      event: "ingest.unauthorized",
      actor: "agent",
      ip: clientIp(req),
      detail: token ? "invalid token" : "missing token",
    });
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await req.json().catch(() => null);
  const parsed = runSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json(
      { error: "Invalid payload", issues: parsed.error.flatten() },
      { status: 400 },
    );
  }

  try {
    await recordRun(parsed.data);
  } catch (err) {
    console.error("[ingest] failed to record run:", err);
    return NextResponse.json({ error: "Failed to record run." }, { status: 500 });
  }

  return NextResponse.json({ ok: true }, { status: 201 });
}
