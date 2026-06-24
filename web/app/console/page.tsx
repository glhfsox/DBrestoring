import Link from "next/link";
import { requireSession } from "@/lib/auth";
import { fleetOverview } from "@/lib/db";
import { timeAgo } from "@/lib/format";

export const dynamic = "force-dynamic";

function StatusBadge({ status }: { status: string | null }) {
  const map: Record<string, string> = {
    success: "bg-brand-500/15 text-brand-300",
    failed: "bg-red-500/15 text-red-300",
  };
  const cls = status ? (map[status] ?? "bg-zinc-700/30 text-zinc-300") : "bg-zinc-700/30 text-zinc-400";
  return (
    <span className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${cls}`}>
      {status ?? "no runs"}
    </span>
  );
}

export default async function ConsoleHome() {
  requireSession();
  const servers = await fleetOverview();

  return (
    <div>
      <div className="flex items-baseline justify-between">
        <h1 className="text-2xl font-bold text-white">Fleet</h1>
        <span className="text-sm text-zinc-500">{servers.length} server(s)</span>
      </div>

      {servers.length === 0 ? (
        <div className="card mt-6">
          <p className="text-zinc-300">No servers have reported yet.</p>
          <p className="mt-2 text-sm text-zinc-400">
            Add a <code className="text-brand-300">control_plane</code> block to a server&apos;s
            config and run a backup. Reported runs will appear here.
          </p>
        </div>
      ) : (
        <div className="mt-6 overflow-hidden rounded-2xl border border-zinc-800/80">
          <table className="w-full text-left text-sm">
            <thead className="bg-zinc-900/60 text-zinc-400">
              <tr>
                <th className="px-4 py-3 font-medium">Server</th>
                <th className="px-4 py-3 font-medium">Last backup</th>
                <th className="px-4 py-3 font-medium">Last seen</th>
                <th className="px-4 py-3 font-medium">Runs</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800/80">
              {servers.map((s) => (
                <tr key={s.id} className="hover:bg-zinc-900/40">
                  <td className="px-4 py-3">
                    <Link href={`/console/servers/${encodeURIComponent(s.id)}`} className="font-medium text-zinc-100 hover:text-brand-300">
                      {s.name}
                    </Link>
                    <div className="text-xs text-zinc-500">{s.id}</div>
                  </td>
                  <td className="px-4 py-3"><StatusBadge status={s.lastStatus} /></td>
                  <td className="px-4 py-3 text-zinc-400">{timeAgo(s.lastSeenAt)}</td>
                  <td className="px-4 py-3 text-zinc-400">{s.runCount}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
