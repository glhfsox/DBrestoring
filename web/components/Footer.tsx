import Link from "next/link";
import { site } from "@/lib/site";
import { Github } from "./icons";

export function Footer() {
  return (
    <footer className="border-t border-zinc-800/70">
      <div className="container-page flex flex-col gap-6 py-10 sm:flex-row sm:items-center sm:justify-between">
        <div className="max-w-sm">
          <div className="text-lg font-semibold text-zinc-100">{site.name}</div>
          <p className="mt-2 text-sm text-zinc-400">
            Open-core database backup &amp; restore. The core is free and MIT-licensed;
            paid tiers add centralized management, compliance, and support.
          </p>
        </div>

        <div className="flex flex-col gap-2 text-sm sm:items-end">
          <div className="flex flex-wrap gap-x-5 gap-y-2">
            <Link href="/pricing" className="text-zinc-300 hover:text-white">Pricing</Link>
            <a href={site.docs} className="text-zinc-300 hover:text-white">Docs</a>
            <Link href="/contact" className="text-zinc-300 hover:text-white">Contact sales</Link>
            <a href={site.github} className="inline-flex items-center gap-1.5 text-zinc-300 hover:text-white">
              <Github className="h-4 w-4" /> GitHub
            </a>
          </div>
          <p className="text-zinc-500">
            © {new Date().getFullYear()} {site.name}. MIT-licensed core.
          </p>
        </div>
      </div>
    </footer>
  );
}
