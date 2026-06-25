import crypto from "node:crypto";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";

export const SESSION_COOKIE = "dbrestore_session";
const MAX_AGE_SECONDS = 60 * 60 * 24 * 7;
const MAX_AGE_MS = MAX_AGE_SECONDS * 1000;

export type Role = "admin" | "viewer";
export type Session = { sub: string; role: Role };

function secret(): string {
  const value = process.env.AUTH_SECRET;
  if (!value) throw new Error("AUTH_SECRET is not set");
  return value;
}

function sign(payload: string): string {
  return crypto.createHmac("sha256", secret()).update(payload).digest("base64url");
}

export function createSessionToken(subject: string, role: Role): string {
  const payload = Buffer.from(JSON.stringify({ sub: subject, role, iat: Date.now() })).toString(
    "base64url",
  );
  return `${payload}.${sign(payload)}`;
}

export function verifySessionToken(token: string | undefined): Session | null {
  if (!token) return null;
  const [payload, sig] = token.split(".");
  if (!payload || !sig) return null;

  const expected = sign(payload);
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;

  try {
    const data = JSON.parse(Buffer.from(payload, "base64url").toString());
    if (typeof data.sub !== "string") return null;
    if (typeof data.iat !== "number" || Date.now() - data.iat > MAX_AGE_MS) return null; // expired/replay
    const role: Role = data.role === "admin" ? "admin" : "viewer";
    return { sub: data.sub, role };
  } catch {
    return null;
  }
}

export function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  return ab.length === bb.length && crypto.timingSafeEqual(ab, bb);
}

// scrypt password hashing: stored as "scrypt$<salt-hex>$<hash-hex>".
export function hashPassword(password: string): string {
  const salt = crypto.randomBytes(16);
  const hash = crypto.scryptSync(password, salt, 64);
  return `scrypt$${salt.toString("hex")}$${hash.toString("hex")}`;
}

export function verifyPassword(password: string, stored: string): boolean {
  const [scheme, saltHex, hashHex] = stored.split("$");
  if (scheme !== "scrypt" || !saltHex || !hashHex) return false;
  const expected = Buffer.from(hashHex, "hex");
  const actual = crypto.scryptSync(password, Buffer.from(saltHex, "hex"), 64);
  return actual.length === expected.length && crypto.timingSafeEqual(actual, expected);
}

// Defense-in-depth CSRF check for state-changing requests (SameSite=Lax already
// blocks most). Allows requests with no Origin header (some legit non-browser
// clients); rejects a present, mismatched Origin.
export function sameOrigin(req: Request): boolean {
  const origin = req.headers.get("origin");
  if (!origin) return true;
  try {
    return new URL(origin).host === new URL(req.url).host;
  } catch {
    return false;
  }
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

export function getSession(): Session | null {
  try {
    return verifySessionToken(cookies().get(SESSION_COOKIE)?.value);
  } catch {
    return null;
  }
}

export function requireSession(): Session {
  const session = getSession();
  if (!session) redirect("/console/login");
  return session;
}

export function requireAdmin(): Session {
  const session = requireSession();
  if (session.role !== "admin") redirect("/console");
  return session;
}
