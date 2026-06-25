"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

export default function ConsoleLoginPage() {
  const router = useRouter();
  const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
  const [error, setError] = useState("");

  async function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setStatus("loading");
    setError("");
    const form = new FormData(e.currentTarget);
    const username = String(form.get("username") ?? "");
    const password = String(form.get("password") ?? "");
    try {
      const res = await fetch("/api/console/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });
      if (!res.ok) {
        const data = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(data.error ?? "Login failed.");
      }
      router.push("/console");
      router.refresh();
    } catch (err) {
      setStatus("error");
      setError(err instanceof Error ? err.message : "Login failed.");
    }
  }

  return (
    <div className="mx-auto max-w-sm pt-10">
      <h1 className="text-2xl font-bold text-white">Sign in</h1>
      <p className="mt-2 text-sm text-zinc-400">Sign in to view the fleet.</p>
      <form onSubmit={onSubmit} className="card mt-6 space-y-4">
        <div>
          <label htmlFor="username" className="mb-1.5 block text-sm font-medium text-zinc-300">
            Username
          </label>
          <input
            id="username"
            name="username"
            required
            autoFocus
            autoComplete="username"
            defaultValue="admin"
            className="w-full rounded-lg border border-zinc-700 bg-zinc-900/60 px-3.5 py-2.5 text-sm text-zinc-100 focus:border-brand-400 focus:outline-none focus:ring-1 focus:ring-brand-400"
          />
        </div>
        <div>
          <label htmlFor="password" className="mb-1.5 block text-sm font-medium text-zinc-300">
            Password
          </label>
          <input
            id="password"
            name="password"
            type="password"
            required
            autoComplete="current-password"
            className="w-full rounded-lg border border-zinc-700 bg-zinc-900/60 px-3.5 py-2.5 text-sm text-zinc-100 focus:border-brand-400 focus:outline-none focus:ring-1 focus:ring-brand-400"
          />
        </div>
        {status === "error" && <p className="text-sm text-red-400">{error}</p>}
        <button type="submit" disabled={status === "loading"} className="btn-primary w-full disabled:opacity-60">
          {status === "loading" ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
