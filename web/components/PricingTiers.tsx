import Link from "next/link";
import { tiers } from "@/lib/tiers";
import { Check } from "./icons";

function isInternal(href: string) {
  return href.startsWith("/");
}

export function PricingTiers() {
  return (
    <div className="grid gap-6 lg:grid-cols-3">
      {tiers.map((tier) => (
        <div
          key={tier.id}
          className={
            "relative flex flex-col rounded-2xl border p-7 " +
            (tier.highlighted
              ? "border-brand-500/60 bg-brand-500/[0.06]"
              : "border-zinc-800/80 bg-zinc-900/40")
          }
        >
          {tier.highlighted && (
            <span className="absolute -top-3 left-7 rounded-full bg-brand-500 px-3 py-1 text-xs font-semibold text-zinc-950">
              Most popular
            </span>
          )}

          <h3 className="text-lg font-semibold text-zinc-100">{tier.name}</h3>
          <div className="mt-3 flex items-baseline gap-1">
            <span className="text-4xl font-bold tracking-tight text-white">{tier.price}</span>
            {tier.cadence && <span className="text-sm text-zinc-400">{tier.cadence}</span>}
          </div>
          <p className="mt-3 min-h-[3rem] text-sm text-zinc-400">{tier.tagline}</p>

          {isInternal(tier.cta.href) ? (
            <Link
              href={tier.cta.href}
              className={tier.highlighted ? "btn-primary mt-5 w-full" : "btn-ghost mt-5 w-full"}
            >
              {tier.cta.label}
            </Link>
          ) : (
            <a
              href={tier.cta.href}
              className={tier.highlighted ? "btn-primary mt-5 w-full" : "btn-ghost mt-5 w-full"}
            >
              {tier.cta.label}
            </a>
          )}

          <ul className="mt-7 space-y-3 text-sm">
            {tier.features.map((feature) => (
              <li key={feature} className="flex gap-3 text-zinc-300">
                <Check className="mt-0.5 h-4 w-4 shrink-0 text-brand-400" />
                <span>{feature}</span>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
}
