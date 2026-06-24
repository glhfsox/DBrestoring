import { NextResponse } from "next/server";
import { SESSION_COOKIE } from "@/lib/auth";
import { safeAudit } from "@/lib/db";
import { clientIp } from "@/lib/ip";

export const runtime = "nodejs";

export async function POST(req: Request) {
  await safeAudit({ event: "logout", actor: "admin", ip: clientIp(req) });
  const res = NextResponse.redirect(new URL("/console/login", req.url));
  res.cookies.set(SESSION_COOKIE, "", { path: "/", maxAge: 0 });
  return res;
}
