// Generic transactional email via Resend. No-op (logs) when unconfigured.
export async function sendEmail(opts: {
  to: string | undefined;
  subject: string;
  text: string;
  replyTo?: string;
}): Promise<void> {
  const apiKey = process.env.RESEND_API_KEY;
  const from = process.env.CONTACT_FROM_EMAIL ?? "onboarding@resend.dev";
  if (!apiKey || !opts.to) {
    console.warn(`[notify] email not configured — would have sent:\n${opts.subject}\n${opts.text}`);
    return;
  }

  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      from,
      to: [opts.to],
      subject: opts.subject,
      text: opts.text,
      reply_to: opts.replyTo,
    }),
  });
  if (!res.ok) {
    throw new Error(`Resend returned ${res.status}: ${await res.text()}`);
  }
}
