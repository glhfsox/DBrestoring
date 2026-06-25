import { NextResponse } from "next/server";
import { z } from "zod";
import { getSession, sameOrigin } from "@/lib/auth";
import { revokeAgentToken, safeAudit } from "@/lib/db";
import { clientIp } from "@/lib/ip";

export const runtime = "nodejs";

const schema = z.object({ id: z.string().trim().min(1).max(100) });

export async function POST(req: Request) {
  if (!sameOrigin(req)) return NextResponse.json({ error: "Bad origin." }, { status: 403 });
  const session = getSession();
  if (!session) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  if (session.role !== "admin") return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  const parsed = schema.safeParse(await req.json().catch(() => null));
  if (!parsed.success) {
    return NextResponse.json({ error: "A token id is required." }, { status: 400 });
  }

  await revokeAgentToken(parsed.data.id);
  await safeAudit({
    event: "token.revoked",
    actor: "admin",
    ip: clientIp(req),
    detail: parsed.data.id,
  });
  return NextResponse.json({ ok: true });
}
