import crypto from "node:crypto";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";

export const SESSION_COOKIE = "dbrestore_session";
const MAX_AGE_SECONDS = 60 * 60 * 24 * 7;

function secret(): string {
  const value = process.env.AUTH_SECRET;
  if (!value) throw new Error("AUTH_SECRET is not set");
  return value;
}

function sign(payload: string): string {
  return crypto.createHmac("sha256", secret()).update(payload).digest("base64url");
}

export function createSessionToken(subject: string): string {
  const payload = Buffer.from(JSON.stringify({ sub: subject, iat: Date.now() })).toString(
    "base64url",
  );
  return `${payload}.${sign(payload)}`;
}

export function verifySessionToken(token: string | undefined): { sub: string } | null {
  if (!token) return null;
  const [payload, sig] = token.split(".");
  if (!payload || !sig) return null;

  const expected = sign(payload);
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;

  try {
    const data = JSON.parse(Buffer.from(payload, "base64url").toString());
    return typeof data.sub === "string" ? { sub: data.sub } : null;
  } catch {
    return null;
  }
}

export function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  return ab.length === bb.length && crypto.timingSafeEqual(ab, bb);
}

export function sessionCookieOptions() {
  return {
    httpOnly: true,
    secure: true,
    sameSite: "lax" as const,
    path: "/",
    maxAge: MAX_AGE_SECONDS,
  };
}

export function getSession(): { sub: string } | null {
  try {
    return verifySessionToken(cookies().get(SESSION_COOKIE)?.value);
  } catch {
    return null;
  }
}

export function requireSession(): { sub: string } {
  const session = getSession();
  if (!session) redirect("/console/login");
  return session;
}
