export function Terminal() {
  return (
    <div className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900/80 shadow-2xl shadow-black/40">
      <div className="flex items-center gap-2 border-b border-zinc-800 bg-zinc-900 px-4 py-3">
        <span className="h-3 w-3 rounded-full bg-red-500/80" />
        <span className="h-3 w-3 rounded-full bg-yellow-500/80" />
        <span className="h-3 w-3 rounded-full bg-green-500/80" />
        <span className="ml-2 text-xs text-zinc-500">backup — prod</span>
      </div>
      <pre className="overflow-x-auto px-5 py-4 font-mono text-[13px] leading-relaxed text-zinc-300">
        <code>
          <span className="text-brand-400">$</span> docker run --rm \{"\n"}
          {"    "}-v $(pwd)/dbrestore.yaml:/work/dbrestore.yaml:ro \{"\n"}
          {"    "}-v $(pwd)/backups:/work/backups \{"\n"}
          {"    "}ghcr.io/you/dbrestore <span className="text-brand-300">backup --profile prod</span>{"\n"}
          {"\n"}
          <span className="text-zinc-500">Starting backup for profile &apos;prod&apos;</span>{"\n"}
          <span className="text-zinc-500">Compressing → encrypting (AES-256-GCM)</span>{"\n"}
          <span className="text-brand-400">✓</span> Backup completed{"  "}
          <span className="text-zinc-400">prod</span>{"  "}
          <span className="text-zinc-500">142 MB → 38 MB · 4.1s</span>{"\n"}
        </code>
      </pre>
    </div>
  );
}
