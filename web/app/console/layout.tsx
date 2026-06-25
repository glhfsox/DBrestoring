import Link from "next/link";
import { getSession } from "@/lib/auth";
import { Database } from "@/components/icons";

export default function ConsoleLayout({ children }: { children: React.ReactNode }) {
  const session = getSession();
  return (
    <div className="min-h-screen">
      <header className="border-b border-zinc-800/70">
        <div className="container-page flex h-14 items-center justify-between">
          <div className="flex items-center gap-6">
            <Link href="/console" className="flex items-center gap-2 font-semibold text-zinc-100">
              <Database className="h-5 w-5 text-brand-400" />
              <span>
                dbrestore <span className="text-zinc-500">console</span>
              </span>
            </Link>
            {session && (
              <nav className="flex items-center gap-4 text-sm">
                <Link href="/console" className="text-zinc-400 hover:text-white">
                  Fleet
                </Link>
                {session.role === "admin" && (
                  <>
                    <Link href="/console/tokens" className="text-zinc-400 hover:text-white">
                      Tokens
                    </Link>
                    <Link href="/console/users" className="text-zinc-400 hover:text-white">
                      Users
                    </Link>
                  </>
                )}
                <Link href="/console/audit" className="text-zinc-400 hover:text-white">
                  Audit
                </Link>
              </nav>
            )}
          </div>
          {session && (
            <div className="flex items-center gap-4">
              <span className="hidden text-sm text-zinc-500 sm:inline">
                {session.sub} · {session.role}
              </span>
              <form action="/api/console/logout" method="post">
                <button className="text-sm text-zinc-400 hover:text-white">Sign out</button>
              </form>
            </div>
          )}
        </div>
      </header>
      <div className="container-page py-10">{children}</div>
    </div>
  );
}
