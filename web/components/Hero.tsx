import Link from "next/link";
import { site } from "@/lib/site";
import { Terminal } from "./Terminal";

export function Hero() {
  return (
    <section className="container-page grid items-center gap-12 py-16 lg:grid-cols-2 lg:py-24">
      <div>
        <span className="eyebrow">Open-source · self-hosted</span>
        <h1 className="mt-4 text-4xl font-bold tracking-tight text-white sm:text-5xl">
          Database backups you can{" "}
          <span className="text-brand-400">actually trust.</span>
        </h1>
        <p className="mt-5 max-w-xl text-lg text-zinc-400">
          {site.name} backs up and restores PostgreSQL, MySQL/MariaDB, MongoDB, and
          SQLite — with deduplicated incremental backups, AES-256-GCM encryption, and
          scheduling that survives reboots. Run it on your own server in minutes.
        </p>
        <div className="mt-8 flex flex-wrap gap-3">
          <a href={site.github} className="btn-primary">
            Get started — it&apos;s free
          </a>
          <Link href="/pricing" className="btn-ghost">
            See pricing
          </Link>
        </div>
        <p className="mt-4 text-sm text-zinc-500">
          No account required. MIT-licensed core · Docker image included.
        </p>
      </div>

      <div className="lg:pl-4">
        <Terminal />
      </div>
    </section>
  );
}
