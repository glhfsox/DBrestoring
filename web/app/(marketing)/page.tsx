import Link from "next/link";
import { Hero } from "@/components/Hero";
import { Features } from "@/components/Features";
import { HowItWorks } from "@/components/HowItWorks";
import { PricingTiers } from "@/components/PricingTiers";

export default function HomePage() {
  return (
    <>
      <Hero />
      <Features />
      <HowItWorks />

      <section className="container-page py-16 sm:py-20">
        <div className="mx-auto max-w-2xl text-center">
          <span className="eyebrow">Pricing</span>
          <h2 className="mt-3 text-3xl font-bold tracking-tight text-white">
            Free forever to self-host. Paid when you need scale and support.
          </h2>
          <p className="mt-4 text-zinc-400">
            The core is open source under the MIT license. Upgrade for centralized
            management, compliance, and a support SLA.
          </p>
        </div>
        <div className="mt-12">
          <PricingTiers />
        </div>
        <p className="mt-8 text-center text-sm text-zinc-500">
          Need details?{" "}
          <Link href="/pricing" className="text-brand-400 hover:underline">
            Compare plans →
          </Link>
        </p>
      </section>

      <section className="container-page pb-24">
        <div className="rounded-3xl border border-brand-500/30 bg-gradient-to-br from-brand-500/10 to-transparent p-10 text-center">
          <h2 className="text-3xl font-bold tracking-tight text-white">
            Stop hoping your backups work.
          </h2>
          <p className="mx-auto mt-3 max-w-xl text-zinc-400">
            Set up encrypted, verified, scheduled backups today — and talk to us when
            you&apos;re ready to manage them across a fleet.
          </p>
          <div className="mt-7 flex flex-wrap justify-center gap-3">
            <Link href="/pricing" className="btn-primary">See pricing</Link>
            <Link href="/contact" className="btn-ghost">Contact sales</Link>
          </div>
        </div>
      </section>
    </>
  );
}
