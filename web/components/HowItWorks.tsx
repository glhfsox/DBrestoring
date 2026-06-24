const steps = [
  {
    n: "01",
    title: "Describe your databases",
    body: "Point a small YAML file at each database. Secrets stay in environment variables, never in the config.",
  },
  {
    n: "02",
    title: "Schedule it once",
    body: "A systemd timer (or the bundled Docker units) runs encrypted backups on your cadence and ships them offsite.",
  },
  {
    n: "03",
    title: "Restore with confidence",
    body: "Restore any run with one command — and let automated verification prove your backups work before you need them.",
  },
];

export function HowItWorks() {
  return (
    <section className="border-y border-zinc-800/70 bg-zinc-900/20">
      <div className="container-page py-16 sm:py-20">
        <div className="max-w-2xl">
          <span className="eyebrow">How it works</span>
          <h2 className="mt-3 text-3xl font-bold tracking-tight text-white">
            From zero to automated backups in three steps.
          </h2>
        </div>

        <div className="mt-12 grid gap-6 md:grid-cols-3">
          {steps.map((s) => (
            <div key={s.n} className="card">
              <div className="font-mono text-sm text-brand-400">{s.n}</div>
              <h3 className="mt-3 text-lg font-semibold text-zinc-100">{s.title}</h3>
              <p className="mt-2 text-sm leading-relaxed text-zinc-400">{s.body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
