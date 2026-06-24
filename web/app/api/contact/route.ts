import { NextResponse } from "next/server";
import { contactSchema } from "@/lib/validation";
import { verifyTurnstile } from "@/lib/turnstile";
import { isEmailConfigured, sendLeadEmail } from "@/lib/email";
import { rateLimit } from "@/lib/rateLimit";

export const runtime = "nodejs";

// Health check: confirms whether email + spam protection are configured on this
// deployment. Returns booleans only — never secret values.
export function GET() {
  return NextResponse.json({
    emailConfigured: isEmailConfigured(),
    turnstileConfigured: Boolean(
      process.env.TURNSTILE_SECRET_KEY && process.env.NEXT_PUBLIC_TURNSTILE_SITE_KEY,
    ),
  });
}

export async function POST(req: Request) {
  const ip =
    req.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ||
    req.headers.get("x-real-ip") ||
    "unknown";

  if (!rateLimit(ip)) {
    return NextResponse.json(
      { error: "Too many requests. Please try again in a minute." },
      { status: 429 },
    );
  }

  let json: unknown;
  try {
    json = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid request body." }, { status: 400 });
  }

  const parsed = contactSchema.safeParse(json);
  if (!parsed.success) {
    return NextResponse.json(
      { error: "Please check the form and try again.", issues: parsed.error.flatten() },
      { status: 400 },
    );
  }
  const data = parsed.data;

  // honeypot tripped: drop silently
  if (data.website && data.website.length > 0) {
    return NextResponse.json({ ok: true });
  }

  const human = await verifyTurnstile(data.turnstileToken, ip);
  if (!human) {
    return NextResponse.json(
      { error: "Spam check failed. Please reload and try again." },
      { status: 400 },
    );
  }

  try {
    await sendLeadEmail(data);
  } catch (err) {
    console.error("[contact] failed to deliver lead:", err);
    return NextResponse.json(
      { error: "We couldn't send your message. Please email us directly." },
      { status: 502 },
    );
  }

  return NextResponse.json({ ok: true });
}
