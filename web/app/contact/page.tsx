import type { Metadata } from "next";
import { ContactForm } from "@/components/ContactForm";
import { Check } from "@/components/icons";

export const metadata: Metadata = {
  title: "Contact sales",
  description:
    "Talk to us about Team and Enterprise plans — centralized management, compliance, and support for dbrestore.",
};

const points = [
  "A walkthrough tailored to your databases and environment",
  "Help sizing Team vs. Enterprise",
  "Security, compliance, and deployment questions answered",
  "No pushy sales — engineers who use the tool",
];

function normalizePlan(value: string | string[] | undefined): string {
  const v = Array.isArray(value) ? value[0] : value;
  return v === "team" || v === "enterprise" ? v : "other";
}

export default function ContactPage({
  searchParams,
}: {
  searchParams: { plan?: string | string[] };
}) {
  const defaultPlan = normalizePlan(searchParams.plan);

  return (
    <div className="container-page grid gap-12 py-16 sm:py-20 lg:grid-cols-2">
      <div>
        <span className="eyebrow">Contact sales</span>
        <h1 className="mt-3 text-4xl font-bold tracking-tight text-white">
          Let&apos;s talk about your backups.
        </h1>
        <p className="mt-4 max-w-md text-zinc-400">
          Tell us about your setup and we&apos;ll help you pick the right plan. Prefer the
          free tier? It&apos;s always available — no conversation required.
        </p>

        <ul className="mt-8 space-y-3">
          {points.map((p) => (
            <li key={p} className="flex gap-3 text-sm text-zinc-300">
              <Check className="mt-0.5 h-4 w-4 shrink-0 text-brand-400" />
              <span>{p}</span>
            </li>
          ))}
        </ul>
      </div>

      <ContactForm defaultPlan={defaultPlan} />
    </div>
  );
}
