import { requireSession } from "@/lib/auth";
import { listAudit } from "@/lib/db";
import { timeAgo } from "@/lib/format";

export const dynamic = "force-dynamic";

const EVENT_STYLES: Record<string, string> = {
  "login.success": "bg-brand-500/15 text-brand-300",
  logout: "bg-zinc-700/30 text-zinc-300",
  "login.failure": "bg-red-500/15 text-red-300",
  "ingest.unauthorized": "bg-amber-500/15 text-amber-300",
};

function EventBadge({ event }: { event: string }) {
  const cls = EVENT_STYLES[event] ?? "bg-zinc-700/30 text-zinc-300";
  return <span className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${cls}`}>{event}</span>;
}

export default async function AuditPage() {
  requireSession();
  const entries = await listAudit(200);

  return (
    <div>
      <h1 className="text-2xl font-bold text-white">Audit log</h1>
      <p className="mt-1 text-sm text-zinc-500">
        Security-relevant events: console sign-ins and rejected agent requests.
      </p>

      {entries.length === 0 ? (
        <p className="mt-6 text-zinc-400">No events recorded yet.</p>
      ) : (
        <div className="mt-6 overflow-x-auto rounded-2xl border border-zinc-800/80">
          <table className="w-full min-w-[640px] text-left text-sm">
            <thead className="bg-zinc-900/60 text-zinc-400">
              <tr>
                <th className="px-4 py-3 font-medium">When</th>
                <th className="px-4 py-3 font-medium">Event</th>
                <th className="px-4 py-3 font-medium">Actor</th>
                <th className="px-4 py-3 font-medium">IP</th>
                <th className="px-4 py-3 font-medium">Detail</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-800/80">
              {entries.map((e) => (
                <tr key={e.id} className="hover:bg-zinc-900/40">
                  <td className="px-4 py-3 text-zinc-400">{timeAgo(e.at)}</td>
                  <td className="px-4 py-3"><EventBadge event={e.event} /></td>
                  <td className="px-4 py-3 text-zinc-300">{e.actor ?? "—"}</td>
                  <td className="px-4 py-3 font-mono text-xs text-zinc-400">{e.ip ?? "—"}</td>
                  <td className="px-4 py-3 text-zinc-400">{e.detail ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
