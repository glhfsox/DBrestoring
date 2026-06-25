"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { timeAgo } from "@/lib/format";

type User = { username: string; role: string; createdAt: number };

export function UserManager({ users, currentUser }: { users: User[]; currentUser: string }) {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState("viewer");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function create(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const res = await fetch("/api/console/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password, role }),
      });
      if (!res.ok) {
        const data = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(data.error ?? "Failed to save user.");
      }
      setUsername("");
      setPassword("");
      setRole("viewer");
      router.refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save user.");
    } finally {
      setBusy(false);
    }
  }

  async function remove(name: string) {
    if (!window.confirm(`Delete user "${name}"?`)) return;
    await fetch("/api/console/users/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: name }),
    });
    router.refresh();
  }

  const inputClass =
    "rounded-lg border border-zinc-700 bg-zinc-900/60 px-3.5 py-2 text-sm text-zinc-100 placeholder-zinc-500 focus:border-brand-400 focus:outline-none focus:ring-1 focus:ring-brand-400";

  return (
    <div>
      <form onSubmit={create} className="flex flex-wrap items-center gap-3">
        <input
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          required
          placeholder="username"
          className={`${inputClass} w-44`}
        />
        <input
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
          type="password"
          placeholder="password (8+ chars)"
          className={`${inputClass} w-56`}
        />
        <select value={role} onChange={(e) => setRole(e.target.value)} className={inputClass}>
          <option value="viewer">viewer</option>
          <option value="admin">admin</option>
        </select>
        <button type="submit" disabled={busy} className="btn-primary disabled:opacity-60">
          {busy ? "Saving…" : "Add / update user"}
        </button>
      </form>
      {error && <p className="mt-2 text-sm text-red-400">{error}</p>}
      <p className="mt-2 text-xs text-zinc-500">
        Re-using an existing username resets that user&apos;s password and role.
      </p>

      <div className="mt-6 overflow-hidden rounded-2xl border border-zinc-800/80">
        <table className="w-full text-left text-sm">
          <thead className="bg-zinc-900/60 text-zinc-400">
            <tr>
              <th className="px-4 py-3 font-medium">User</th>
              <th className="px-4 py-3 font-medium">Role</th>
              <th className="px-4 py-3 font-medium">Created</th>
              <th className="px-4 py-3" />
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-800/80">
            {users.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-4 py-6 text-zinc-500">
                  No console users yet. The bootstrap <code className="text-brand-300">admin</code>{" "}
                  (from <code className="text-brand-300">ADMIN_PASSWORD</code>) is always available.
                </td>
              </tr>
            ) : (
              users.map((u) => (
                <tr key={u.username} className="hover:bg-zinc-900/40">
                  <td className="px-4 py-3 font-medium text-zinc-100">{u.username}</td>
                  <td className="px-4 py-3">
                    <span
                      className={
                        "rounded-full px-2.5 py-0.5 text-xs font-medium " +
                        (u.role === "admin"
                          ? "bg-brand-500/15 text-brand-300"
                          : "bg-zinc-700/30 text-zinc-300")
                      }
                    >
                      {u.role}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-zinc-400">{timeAgo(u.createdAt)}</td>
                  <td className="px-4 py-3 text-right">
                    {u.username !== currentUser && (
                      <button
                        onClick={() => remove(u.username)}
                        className="text-sm text-red-400 hover:text-red-300"
                      >
                        Delete
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
