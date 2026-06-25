import { NextResponse } from "next/server";
import { getSession, sameOrigin, SESSION_COOKIE } from "@/lib/auth";
import { safeAudit } from "@/lib/db";
import { clientIp } from "@/lib/ip";

export const runtime = "nodejs";

export async function POST(req: Request) {
  if (!sameOrigin(req)) return NextResponse.json({ error: "Bad origin." }, { status: 403 });
  await safeAudit({ event: "logout", actor: getSession()?.sub ?? null, ip: clientIp(req) });
  const res = NextResponse.redirect(new URL("/console/login", req.url));
  res.cookies.set(SESSION_COOKIE, "", { path: "/", maxAge: 0 });
  return res;
}
