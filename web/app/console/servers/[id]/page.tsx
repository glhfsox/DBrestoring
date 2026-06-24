import Link from "next/link";
import { notFound } from "next/navigation";
import { requireSession } from "@/lib/auth";
import { getServer, listRuns } from "@/lib/db";
import { formatBytes, formatDuration, timeAgo } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function ServerDetail({ params }: { params: { id: string } }) {
  requireSession();
  const id = decodeURIComponent(params.id);
  const server = await getServer(id);
  if (!server) notFound();

  const runs = await listRuns(id, 100);

  return (
    <div>
      <Link href="/console" className="text-sm text-zinc-400 hover:text-white">
        ← Fleet
      </Link>
      <h1 className="mt-3 text-2xl font-bold text-white">{server.name}</h1>
      <p className="text-sm text-zinc-500">
        {server.id} · last seen {timeAgo(server.lastSeenAt)} · {server.runCount} run(s)
      </p>

      {runs.length === 0 ? (
        <p className="mt-6 text-zinc-400">No runs recorded for this server.</p>
      ) : (
        <div className="mt-6 overflow-x-auto rounded-2xl border border-zinc-800/80">
          <table className="w-full min-w-[760px] text-left text-sm">
            <thead className="bg-zinc-900/60 text-zinc-400">
              <tr>
                <th className="px-4 py-3 font-medium">Profile</th>
                <th className="px-4 py-3 font-medium">Type</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">Size</th>
                <th className="px-4 py-3 font-medium">Duration</th>
                <th className="px-4 py-3 font-medium">Reported</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800/80">
              {runs.map((r) => (
                <tr key={r.id} className="align-top hover:bg-zinc-900/40">
                  <td className="px-4 py-3">
                    <div className="font-medium text-zinc-100">{r.profile}</div>
                    <div className="text-xs text-zinc-500">{r.dbType}</div>
                  </td>
                  <td className="px-4 py-3 text-zinc-300">{r.backupType}</td>
                  <td className="px-4 py-3">
                    <span
                      className={
                        "rounded-full px-2.5 py-0.5 text-xs font-medium " +
                        (r.status === "success"
                          ? "bg-brand-500/15 text-brand-300"
                          : "bg-red-500/15 text-red-300")
                      }
                    >
                      {r.status}
                    </span>
                    {r.error && <div className="mt-1 max-w-xs text-xs text-red-400/80">{r.error}</div>}
                  </td>
                  <td className="px-4 py-3 text-zinc-300">{formatBytes(r.sizeBytes)}</td>
                  <td className="px-4 py-3 text-zinc-300">{formatDuration(r.durationMs)}</td>
                  <td className="px-4 py-3 text-zinc-400">{timeAgo(r.reportedAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
