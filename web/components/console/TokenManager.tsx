"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { timeAgo } from "@/lib/format";

type Token = {
  id: string;
  name: string;
  createdAt: number;
  lastUsedAt: number | null;
  revokedAt: number | null;
};

export function TokenManager({ tokens }: { tokens: Token[] }) {
  const router = useRouter();
  const [name, setName] = useState("");
  const [created, setCreated] = useState<{ name: string; token: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function create(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setBusy(true);
    setError("");
    setCreated(null);
    try {
      const res = await fetch("/api/console/tokens", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      if (!res.ok) {
        const data = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(data.error ?? "Failed to create token.");
      }
      const data = (await res.json()) as { token: string };
      setCreated({ name, token: data.token });
      setName("");
      router.refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create token.");
    } finally {
      setBusy(false);
    }
  }

  async function revoke(id: string) {
    if (!window.confirm("Revoke this token? Any agent using it will stop reporting.")) return;
    await fetch("/api/console/tokens/revoke", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id }),
    });
    router.refresh();
  }

  const inputClass =
    "rounded-lg border border-zinc-700 bg-zinc-900/60 px-3.5 py-2 text-sm text-zinc-100 placeholder-zinc-500 focus:border-brand-400 focus:outline-none focus:ring-1 focus:ring-brand-400";

  return (
    <div>
      <form onSubmit={create} className="flex flex-wrap items-center gap-3">
        <input
          value={name}
          onChange={(e) => setName(e.target.value)}
          required
          maxLength={100}
          placeholder="Server name (e.g. prod-db-1)"
          className={`${inputClass} w-64`}
        />
        <button type="submit" disabled={busy} className="btn-primary disabled:opacity-60">
          {busy ? "Creating…" : "Create token"}
        </button>
      </form>
      {error && <p className="mt-2 text-sm text-red-400">{error}</p>}

      {created && (
        <div className="mt-4 rounded-xl border border-brand-500/40 bg-brand-500/[0.06] p-4">
          <p className="text-sm font-medium text-brand-300">
            Token for “{created.name}” — copy it now, it won’t be shown again:
          </p>
          <div className="mt-2 flex items-center gap-3">
            <code className="break-all rounded bg-zinc-900 px-3 py-2 font-mono text-xs text-zinc-100">
              {created.token}
            </code>
            <button
              onClick={() => navigator.clipboard?.writeText(created.token)}
              className="btn-ghost shrink-0 px-3 py-1.5 text-xs"
            >
              Copy
            </button>
          </div>
        </div>
      )}

      <div className="mt-6 overflow-hidden rounded-2xl border border-zinc-800/80">
        <table className="w-full text-left text-sm">
          <thead className="bg-zinc-900/60 text-zinc-400">
            <tr>
              <th className="px-4 py-3 font-medium">Name</th>
              <th className="px-4 py-3 font-medium">Created</th>
              <th className="px-4 py-3 font-medium">Last used</th>
              <th className="px-4 py-3 font-medium">Status</th>
              <th className="px-4 py-3" />
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-800/80">
            {tokens.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-4 py-6 text-zinc-500">
                  No agent tokens yet. Create one above and put it in a server’s{" "}
                  <code className="text-brand-300">control_plane.token</code>.
                </td>
              </tr>
            ) : (
              tokens.map((t) => (
                <tr key={t.id} className="hover:bg-zinc-900/40">
                  <td className="px-4 py-3">
                    <div className="font-medium text-zinc-100">{t.name}</div>
                    <div className="font-mono text-xs text-zinc-500">{t.id}</div>
                  </td>
                  <td className="px-4 py-3 text-zinc-400">{timeAgo(t.createdAt)}</td>
                  <td className="px-4 py-3 text-zinc-400">{timeAgo(t.lastUsedAt)}</td>
                  <td className="px-4 py-3">
                    {t.revokedAt ? (
                      <span className="rounded-full bg-red-500/15 px-2.5 py-0.5 text-xs font-medium text-red-300">
                        revoked
                      </span>
                    ) : (
                      <span className="rounded-full bg-brand-500/15 px-2.5 py-0.5 text-xs font-medium text-brand-300">
                        active
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-right">
                    {!t.revokedAt && (
                      <button
                        onClick={() => revoke(t.id)}
                        className="text-sm text-red-400 hover:text-red-300"
                      >
                        Revoke
                      </button>
                    )}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
