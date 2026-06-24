import { Database, Layers, Lock, Cloud, Clock, Shield, Bell } from "./icons";

const features = [
  {
    icon: Database,
    title: "Four engines, one tool",
    body: "PostgreSQL, MySQL/MariaDB, MongoDB, and SQLite behind a single, consistent CLI.",
  },
  {
    icon: Layers,
    title: "Incremental & differential",
    body: "Content-addressed deduplication stores only changed chunks, so repeat backups are small and fast.",
  },
  {
    icon: Lock,
    title: "AES-256-GCM encryption",
    body: "Artifacts are encrypted at rest with scrypt-derived keys. A wrong passphrase is rejected, not silently garbled.",
  },
  {
    icon: Cloud,
    title: "Local + S3-compatible",
    body: "Keep backups on disk for fast restores and mirror them offsite to any S3 API — including free R2 / B2.",
  },
  {
    icon: Clock,
    title: "Scheduling that survives reboots",
    body: "systemd and launchd integration runs unattended backups and catches up on missed runs after downtime.",
  },
  {
    icon: Shield,
    title: "Verified restores",
    body: "Automatically restore into a throwaway target to prove every backup is actually recoverable.",
  },
  {
    icon: Bell,
    title: "Failure alerting",
    body: "Slack notifications and healthchecks.io pings tell you the moment a backup fails — or simply stops running.",
  },
  {
    icon: Layers,
    title: "Automatic retention",
    body: "Keep-last policies prune old runs and garbage-collect unreferenced chunks for you.",
  },
];

export function Features() {
  return (
    <section id="features" className="container-page py-16 sm:py-20">
      <div className="max-w-2xl">
        <span className="eyebrow">Features</span>
        <h2 className="mt-3 text-3xl font-bold tracking-tight text-white">
          Everything a backup tool should do — and the parts most skip.
        </h2>
      </div>

      <div className="mt-12 grid gap-5 sm:grid-cols-2 lg:grid-cols-4">
        {features.map((f) => (
          <div key={f.title} className="card">
            <f.icon className="h-7 w-7 text-brand-400" />
            <h3 className="mt-4 font-semibold text-zinc-100">{f.title}</h3>
            <p className="mt-2 text-sm leading-relaxed text-zinc-400">{f.body}</p>
          </div>
        ))}
      </div>
    </section>
  );
}
