"use client";

import { useState } from "react";
import Script from "next/script";

const SITE_KEY = process.env.NEXT_PUBLIC_TURNSTILE_SITE_KEY;

type Status = "idle" | "loading" | "ok" | "error";

export function ContactForm({ defaultPlan = "other" }: { defaultPlan?: string }) {
  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState("");

  async function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setStatus("loading");
    setError("");

    const form = e.currentTarget;
    const fd = new FormData(form);
    const payload = {
      name: String(fd.get("name") ?? ""),
      email: String(fd.get("email") ?? ""),
      company: String(fd.get("company") ?? ""),
      teamSize: String(fd.get("teamSize") ?? ""),
      plan: String(fd.get("plan") ?? "other"),
      message: String(fd.get("message") ?? ""),
      website: String(fd.get("website") ?? ""), // honeypot
      turnstileToken: String(fd.get("cf-turnstile-response") ?? ""),
    };

    try {
      const res = await fetch("/api/contact", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const data = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(data.error ?? "Something went wrong.");
      }
      setStatus("ok");
      form.reset();
    } catch (err) {
      setStatus("error");
      setError(err instanceof Error ? err.message : "Something went wrong.");
    }
  }

  if (status === "ok") {
    return (
      <div className="card text-center">
        <h3 className="text-xl font-semibold text-white">Thanks — we&apos;ll be in touch.</h3>
        <p className="mt-2 text-zinc-400">
          Your message is on its way to our team. We typically reply within one business day.
        </p>
      </div>
    );
  }

  const inputClass =
    "w-full rounded-lg border border-zinc-700 bg-zinc-900/60 px-3.5 py-2.5 text-sm text-zinc-100 placeholder-zinc-500 focus:border-brand-400 focus:outline-none focus:ring-1 focus:ring-brand-400";
  const labelClass = "mb-1.5 block text-sm font-medium text-zinc-300";

  return (
    <form onSubmit={onSubmit} className="card space-y-5">
      {SITE_KEY && (
        <Script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer />
      )}

      {/* honeypot */}
      <div className="hidden" aria-hidden="true">
        <label>
          Do not fill this in
          <input type="text" name="website" tabIndex={-1} autoComplete="off" />
        </label>
      </div>

      <div className="grid gap-5 sm:grid-cols-2">
        <div>
          <label htmlFor="name" className={labelClass}>Name</label>
          <input id="name" name="name" required maxLength={100} className={inputClass} placeholder="Jane Doe" />
        </div>
        <div>
          <label htmlFor="email" className={labelClass}>Work email</label>
          <input id="email" name="email" type="email" required maxLength={200} className={inputClass} placeholder="jane@company.com" />
        </div>
        <div>
          <label htmlFor="company" className={labelClass}>Company</label>
          <input id="company" name="company" maxLength={120} className={inputClass} placeholder="Acme Inc." />
        </div>
        <div>
          <label htmlFor="teamSize" className={labelClass}>Team size</label>
          <input id="teamSize" name="teamSize" maxLength={40} className={inputClass} placeholder="1–10, 50, 200+…" />
        </div>
      </div>

      <div>
        <label htmlFor="plan" className={labelClass}>Plan of interest</label>
        <select id="plan" name="plan" defaultValue={defaultPlan} className={inputClass}>
          <option value="team">Team</option>
          <option value="enterprise">Enterprise</option>
          <option value="other">Not sure yet</option>
        </select>
      </div>

      <div>
        <label htmlFor="message" className={labelClass}>How can we help?</label>
        <textarea id="message" name="message" required maxLength={4000} rows={5} className={inputClass} placeholder="Tell us about your databases, team, and what you need." />
      </div>

      {SITE_KEY && <div className="cf-turnstile" data-sitekey={SITE_KEY} />}

      {status === "error" && (
        <p className="text-sm text-red-400">{error}</p>
      )}

      <button type="submit" disabled={status === "loading"} className="btn-primary w-full disabled:opacity-60">
        {status === "loading" ? "Sending…" : "Talk to sales"}
      </button>
    </form>
  );
}
