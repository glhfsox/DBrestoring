import type { ContactInput } from "./validation";

export function isEmailConfigured(): boolean {
  return Boolean(process.env.RESEND_API_KEY && process.env.CONTACT_TO_EMAIL);
}

// Sends the lead via Resend, or logs it if RESEND_API_KEY/CONTACT_TO_EMAIL are unset.
export async function sendLeadEmail(data: ContactInput): Promise<void> {
  const apiKey = process.env.RESEND_API_KEY;
  const to = process.env.CONTACT_TO_EMAIL;
  const from = process.env.CONTACT_FROM_EMAIL ?? "onboarding@resend.dev";

  const subject = `New ${data.plan} lead: ${data.name}${data.company ? ` (${data.company})` : ""}`;
  const text = [
    `Name:        ${data.name}`,
    `Email:       ${data.email}`,
    `Company:     ${data.company || "-"}`,
    `Team size:   ${data.teamSize || "-"}`,
    `Plan:        ${data.plan}`,
    "",
    "Message:",
    data.message,
  ].join("\n");

  if (!apiKey || !to) {
    console.warn(
      "[email] RESEND_API_KEY / CONTACT_TO_EMAIL not set — logging lead instead:\n" + text,
    );
    return;
  }

  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ from, to: [to], subject, text, reply_to: data.email }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Resend returned ${res.status}: ${body}`);
  }

  const result = (await res.json().catch(() => ({}))) as { id?: string };
  console.log(`[email] lead delivered via Resend (id=${result.id ?? "?"})`);
}
