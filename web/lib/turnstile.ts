// Returns true if no secret is configured, so the form works before Turnstile is set up.
export async function verifyTurnstile(
  token: string | undefined,
  ip: string,
): Promise<boolean> {
  const secret = process.env.TURNSTILE_SECRET_KEY;
  const siteKey = process.env.NEXT_PUBLIC_TURNSTILE_SITE_KEY;
  // Both keys are required: without the public site key the browser renders no
  // widget and produces no token, so enforcing here would reject every visitor.
  if (!secret || !siteKey) {
    console.warn("[turnstile] not fully configured (need both keys) — skipping spam check.");
    return true;
  }
  if (!token) return false;

  const form = new URLSearchParams();
  form.append("secret", secret);
  form.append("response", token);
  if (ip && ip !== "unknown") form.append("remoteip", ip);

  try {
    const res = await fetch(
      "https://challenges.cloudflare.com/turnstile/v0/siteverify",
      { method: "POST", body: form },
    );
    const data = (await res.json()) as { success?: boolean };
    return data.success === true;
  } catch {
    return false;
  }
}
