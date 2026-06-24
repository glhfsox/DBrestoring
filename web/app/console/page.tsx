import { requireSession } from "@/lib/auth";
import { fleetOverview } from "@/lib/db";
import { evaluateState } from "@/lib/alerts";
import { FleetTable, type FleetRow } from "@/components/console/FleetTable";

export const dynamic = "force-dynamic";

export default async function ConsoleHome() {
  requireSession();
  const servers = await fleetOverview();
  const maxAgeMs = (Number(process.env.ALERT_MAX_AGE_HOURS) || 26) * 3_600_000;
  const now = Date.now();

  const rows: FleetRow[] = servers.map((s) => ({
    id: s.id,
    name: s.name,
    state: s.runCount === 0 ? "none" : evaluateState(s, maxAgeMs, now),
    lastSuccessAt: s.lastSuccessAt,
    lastSeenAt: s.lastSeenAt,
    runCount: s.runCount,
  }));

  const unhealthy = rows.filter((r) => r.state === "overdue" || r.state === "failing").length;

  return (
    <div>
      <div className="flex items-baseline justify-between">
        <h1 className="text-2xl font-bold text-white">Fleet</h1>
        <span className="text-sm text-zinc-500">
          {rows.length} server(s){unhealthy > 0 ? ` · ${unhealthy} need attention` : ""}
        </span>
      </div>

      {rows.length === 0 ? (
        <div className="card mt-6">
          <p className="text-zinc-300">No servers have reported yet.</p>
          <p className="mt-2 text-sm text-zinc-400">
            Add a <code className="text-brand-300">control_plane</code> block to a server&apos;s
            config and run a backup. Reported runs will appear here.
          </p>
        </div>
      ) : (
        <div className="mt-6">
          <FleetTable rows={rows} />
        </div>
      )}
    </div>
  );
}
