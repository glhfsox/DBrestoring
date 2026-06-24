import Link from "next/link";
import { site } from "@/lib/site";
import { Database, Github } from "./icons";

export function Nav() {
  return (
    <header className="sticky top-0 z-40 border-b border-zinc-800/70 bg-zinc-950/70 backdrop-blur">
      <nav className="container-page flex h-16 items-center justify-between">
        <Link href="/" className="flex items-center gap-2 font-semibold text-zinc-100">
          <Database className="h-6 w-6 text-brand-400" />
          <span className="text-lg tracking-tight">{site.name}</span>
        </Link>

        <div className="flex items-center gap-1 sm:gap-2">
          <Link href="/#features" className="hidden rounded-lg px-3 py-2 text-sm text-zinc-300 hover:text-white sm:inline-block">
            Features
          </Link>
          <Link href="/pricing" className="hidden rounded-lg px-3 py-2 text-sm text-zinc-300 hover:text-white sm:inline-block">
            Pricing
          </Link>
          <a href={site.docs} className="hidden rounded-lg px-3 py-2 text-sm text-zinc-300 hover:text-white sm:inline-block">
            Docs
          </a>
          <a
            href={site.github}
            className="inline-flex items-center gap-2 rounded-lg px-3 py-2 text-sm text-zinc-300 hover:text-white"
            aria-label="GitHub repository"
          >
            <Github className="h-5 w-5" />
            <span className="hidden lg:inline">GitHub</span>
          </a>
          <Link href="/contact" className="btn-primary ml-1">
            Contact sales
          </Link>
        </div>
      </nav>
    </header>
  );
}
