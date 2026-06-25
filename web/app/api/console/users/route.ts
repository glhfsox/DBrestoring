import { NextResponse } from "next/server";
import { z } from "zod";
import { getSession, hashPassword, sameOrigin } from "@/lib/auth";
import { safeAudit, upsertUser } from "@/lib/db";
import { clientIp } from "@/lib/ip";

export const runtime = "nodejs";

const schema = z.object({
  username: z
    .string()
    .trim()
    .min(1)
    .max(64)
    .regex(/^[A-Za-z0-9_.-]+$/, "letters, digits, dot, dash, underscore only"),
  password: z.string().min(8).max(200),
  role: z.enum(["admin", "viewer"]),
});

export async function POST(req: Request) {
  if (!sameOrigin(req)) return NextResponse.json({ error: "Bad origin." }, { status: 403 });
  const session = getSession();
  if (!session) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  if (session.role !== "admin") return NextResponse.json({ error: "Forbidden" }, { status: 403 });

  const parsed = schema.safeParse(await req.json().catch(() => null));
  if (!parsed.success) {
    return NextResponse.json(
      { error: "Username, a password (8+ chars), and a role are required." },
      { status: 400 },
    );
  }
  const { username, password, role } = parsed.data;

  await upsertUser(username, hashPassword(password), role);
  await safeAudit({
    event: "user.upserted",
    actor: session.sub,
    ip: clientIp(req),
    detail: `${username} (${role})`,
  });
  return NextResponse.json({ ok: true }, { status: 201 });
}
