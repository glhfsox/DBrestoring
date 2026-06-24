import type { Metadata } from "next";
import Link from "next/link";
import { PricingTiers } from "@/components/PricingTiers";

export const metadata: Metadata = {
  title: "Pricing",
  description:
    "Free, open-source core for self-hosting. Paid Team and Enterprise tiers add centralized management, compliance, and support.",
};

const faqs = [
  {
    q: "Is the free tier really free?",
    a: "Yes. The core CLI and Docker image are MIT-licensed and free to use, including commercially. You self-host and own your data end to end.",
  },
  {
    q: "What do paid tiers add?",
    a: "Paid tiers are open-core: they layer centralized management, a web dashboard, hosted monitoring, SSO/RBAC, compliance features, and support on top of the free engine.",
  },
  {
    q: "How does billing work?",
    a: "Team and Enterprise are sold through our team so we can match the plan to your environment. Reach out via Contact sales and we'll get you set up.",
  },
  {
    q: "Can I move between tiers?",
    a: "Absolutely. Start free, upgrade when you need fleet-wide management or compliance, and downgrade any time — your backups are always yours.",
  },
];

export default function PricingPage() {
  return (
    <div className="container-page py-16 sm:py-20">
      <div className="mx-auto max-w-2xl text-center">
        <span className="eyebrow">Pricing</span>
        <h1 className="mt-3 text-4xl font-bold tracking-tight text-white">
          Simple pricing, open-core foundation.
        </h1>
        <p className="mt-4 text-zinc-400">
          Self-host the full backup engine for free. Pay only when you need to manage
          backups across many servers, meet compliance requirements, or get a support SLA.
        </p>
      </div>

      <div className="mt-14">
        <PricingTiers />
      </div>

      <p className="mt-6 text-center text-sm text-zinc-500">
        Prices shown for Team are introductory and billed per organization. Enterprise is
        tailored to your needs — <Link href="/contact" className="text-brand-400 hover:underline">talk to sales</Link>.
      </p>

      <section className="mx-auto mt-20 max-w-3xl">
        <h2 className="text-center text-2xl font-bold text-white">Frequently asked questions</h2>
        <dl className="mt-8 space-y-4">
          {faqs.map((f) => (
            <div key={f.q} className="card">
              <dt className="font-semibold text-zinc-100">{f.q}</dt>
              <dd className="mt-2 text-sm leading-relaxed text-zinc-400">{f.a}</dd>
            </div>
          ))}
        </dl>
      </section>
    </div>
  );
}
