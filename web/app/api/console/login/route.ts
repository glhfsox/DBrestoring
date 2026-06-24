import { NextResponse } from "next/server";
import { createSessionToken, safeEqual, sessionCookieOptions, SESSION_COOKIE } from "@/lib/auth";
import { safeAudit } from "@/lib/db";
import { clientIp } from "@/lib/ip";

export const runtime = "nodejs";

export async function POST(req: Request) {
  const adminPassword = process.env.ADMIN_PASSWORD;
  if (!adminPassword || !process.env.AUTH_SECRET) {
    return NextResponse.json(
      { error: "Console auth is not configured (set ADMIN_PASSWORD and AUTH_SECRET)." },
      { status: 503 },
    );
  }

  const ip = clientIp(req);
  const body = (await req.json().catch(() => ({}))) as { password?: unknown };
  const password = typeof body.password === "string" ? body.password : "";
  if (!password || !safeEqual(password, adminPassword)) {
    await safeAudit({ event: "login.failure", actor: null, ip, detail: "invalid password" });
    return NextResponse.json({ error: "Invalid password." }, { status: 401 });
  }

  const res = NextResponse.json({ ok: true });
  res.cookies.set(SESSION_COOKIE, createSessionToken("admin"), sessionCookieOptions());
  await safeAudit({ event: "login.success", actor: "admin", ip });
  return res;
}
