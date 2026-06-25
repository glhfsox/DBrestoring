import { NextResponse } from "next/server";
import {
  createSessionToken,
  safeEqual,
  sameOrigin,
  sessionCookieOptions,
  SESSION_COOKIE,
  verifyPassword,
  type Role,
} from "@/lib/auth";
import { getUserCredentials, safeAudit } from "@/lib/db";
import { clientIp } from "@/lib/ip";
import { rateLimit } from "@/lib/rateLimit";

export const runtime = "nodejs";

export async function POST(req: Request) {
  if (!sameOrigin(req)) {
    return NextResponse.json({ error: "Bad origin." }, { status: 403 });
  }
  if (!process.env.AUTH_SECRET) {
    return NextResponse.json({ error: "Console auth is not configured." }, { status: 503 });
  }

  const ip = clientIp(req);
  // Throttle login attempts per IP to slow brute-force.
  if (!rateLimit(`login:${ip}`)) {
    return NextResponse.json({ error: "Too many attempts. Try again shortly." }, { status: 429 });
  }

  const body = (await req.json().catch(() => ({}))) as { username?: unknown; password?: unknown };
  const username = typeof body.username === "string" ? body.username.trim() : "";
  const password = typeof body.password === "string" ? body.password : "";

  let role: Role | null = null;
  if (username && password) {
    const creds = await getUserCredentials(username);
    if (creds && verifyPassword(password, creds.passwordHash)) {
      role = creds.role === "admin" ? "admin" : "viewer";
    } else if (
      username === "admin" &&
      process.env.ADMIN_PASSWORD &&
      safeEqual(password, process.env.ADMIN_PASSWORD)
    ) {
      // Bootstrap superuser from env, so you can always get in to create users.
      role = "admin";
    }
  }

  if (!role) {
    await safeAudit({ event: "login.failure", actor: username || null, ip, detail: "invalid credentials" });
    return NextResponse.json({ error: "Invalid username or password." }, { status: 401 });
  }

  const res = NextResponse.json({ ok: true });
  res.cookies.set(SESSION_COOKIE, createSessionToken(username, role), sessionCookieOptions());
  await safeAudit({ event: "login.success", actor: username, ip, detail: role });
  return res;
}
