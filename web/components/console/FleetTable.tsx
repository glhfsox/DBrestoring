"use client";

import Link from "next/link";
import { useState } from "react";
import { timeAgo } from "@/lib/format";

export type FleetRow = {
  id: string;
  name: string;
  state: "ok" | "overdue" | "failing" | "none";
  lastSuccessAt: number | null;
  lastSeenAt: number;
  runCount: number;
};

const STYLES: Record<FleetRow["state"], string> = {
  ok: "bg-brand-500/15 text-brand-300",
  failing: "bg-amber-500/15 text-amber-300",
  overdue: "bg-red-500/15 text-red-300",
  none: "bg-zinc-700/30 text-zinc-400",
};
const LABELS: Record<FleetRow["state"], string> = {
  ok: "healthy",
  failing: "failing",
  overdue: "overdue",
  none: "no runs",
};

export function FleetTable({ rows }: { rows: FleetRow[] }) {
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();
  const filtered = q
    ? rows.filter((r) => `${r.name} ${r.id}`.toLowerCase().includes(q))
    : rows;

  return (
    <div>
      <input
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Filter servers…"
        className="mb-4 w-full max-w-xs rounded-lg border border-zinc-700 bg-zinc-900/60 px-3.5 py-2 text-sm text-zinc-100 placeholder-zinc-500 focus:border-brand-400 focus:outline-none focus:ring-1 focus:ring-brand-400"
      />
      <div className="overflow-hidden rounded-2xl border border-zinc-800/80">
        <table className="w-full text-left text-sm">
          <thead className="bg-zinc-900/60 text-zinc-400">
            <tr>
              <th className="px-4 py-3 font-medium">Server</th>
              <th className="px-4 py-3 font-medium">Health</th>
              <th className="px-4 py-3 font-medium">Last success</th>
              <th className="px-4 py-3 font-medium">Last seen</th>
              <th className="px-4 py-3 font-medium">Runs</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-800/80">
            {filtered.map((r) => (
              <tr key={r.id} className="hover:bg-zinc-900/40">
                <td className="px-4 py-3">
                  <Link
                    href={`/console/servers/${encodeURIComponent(r.id)}`}
                    className="font-medium text-zinc-100 hover:text-brand-300"
                  >
                    {r.name}
                  </Link>
                  <div className="text-xs text-zinc-500">{r.id}</div>
                </td>
                <td className="px-4 py-3">
                  <span className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${STYLES[r.state]}`}>
                    {LABELS[r.state]}
                  </span>
                </td>
                <td className="px-4 py-3 text-zinc-400">{timeAgo(r.lastSuccessAt)}</td>
                <td className="px-4 py-3 text-zinc-400">{timeAgo(r.lastSeenAt)}</td>
                <td className="px-4 py-3 text-zinc-400">{r.runCount}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {filtered.length === 0 && (
        <p className="mt-4 text-sm text-zinc-500">No matching servers.</p>
      )}
    </div>
  );
}
