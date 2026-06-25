import { NextResponse } from "next/server";
import { z } from "zod";
import { getSession, sameOrigin } from "@/lib/auth";
import { deleteUser, safeAudit } from "@/lib/db";
import { clientIp } from "@/lib/ip";

export const runtime = "nodejs";

const schema = z.object({ username: z.string().trim().min(1).max(64) });

export async function POST(req: Request) {
  if (!sameOrigin(req)) return NextResponse.json({ error: "Bad origin." }, { status: 403 });
  const session = getSession();
  if (!session) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  if (session.role !== "admin") return NextResponse.json({ error: "Forbidden" }, { status: 403 });

  const parsed = schema.safeParse(await req.json().catch(() => null));
  if (!parsed.success) return NextResponse.json({ error: "username required" }, { status: 400 });

  // Don't let an admin delete their own account (avoids locking yourself out).
  if (parsed.data.username === session.sub) {
    return NextResponse.json({ error: "You can't delete your own account." }, { status: 400 });
  }

  await deleteUser(parsed.data.username);
  await safeAudit({
    event: "user.deleted",
    actor: session.sub,
    ip: clientIp(req),
    detail: parsed.data.username,
  });
  return NextResponse.json({ ok: true });
}
