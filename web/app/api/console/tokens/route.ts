import { NextResponse } from "next/server";
import { z } from "zod";
import { getSession, sameOrigin } from "@/lib/auth";
import { createAgentToken, safeAudit } from "@/lib/db";
import { clientIp } from "@/lib/ip";

export const runtime = "nodejs";

const schema = z.object({ name: z.string().trim().min(1).max(100) });

export async function POST(req: Request) {
  if (!sameOrigin(req)) return NextResponse.json({ error: "Bad origin." }, { status: 403 });
  const session = getSession();
  if (!session) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  if (session.role !== "admin") return NextResponse.json({ error: "Forbidden" }, { status: 403 });
  const parsed = schema.safeParse(await req.json().catch(() => null));
  if (!parsed.success) {
    return NextResponse.json({ error: "A token name is required." }, { status: 400 });
  }

  const { id, token } = await createAgentToken(parsed.data.name);
  await safeAudit({
    event: "token.created",
    actor: "admin",
    ip: clientIp(req),
    detail: `${parsed.data.name} (${id})`,
  });
  // The plaintext token is returned exactly once.
  return NextResponse.json({ id, token }, { status: 201 });
}
