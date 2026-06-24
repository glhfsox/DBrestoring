# dbrestore website + control plane

Next.js (App Router) + Tailwind, built to deploy on Vercel. Two parts:

- **Marketing site** (`/`, `/pricing`, `/contact`) with a contact form (Zod
  validation, Cloudflare Turnstile, per-IP rate limiting, Resend email).
- **Control plane** — a fleet dashboard at `/console` plus an ingestion API at
  `POST /api/v1/runs`. Servers running `dbrestore` report each backup; the data
  is stored in libSQL (Turso) and shown in the dashboard.

## Develop

```bash
cd web
npm install
npm run dev   # http://localhost:3000
```

The form works with no configuration: leads are logged to the server console and
the spam check is skipped. Set the env vars below for production.

## Environment

Put these in `.env.local` for local dev and in the Vercel project settings for
production.

Marketing form (optional — leads log to the console without them):

| Variable | Purpose |
| --- | --- |
| `RESEND_API_KEY` | Resend API key for sending leads |
| `CONTACT_TO_EMAIL` | Inbox that receives leads |
| `CONTACT_FROM_EMAIL` | From address (defaults to `onboarding@resend.dev`) |
| `NEXT_PUBLIC_TURNSTILE_SITE_KEY` | Turnstile site key (public, in the browser) |
| `TURNSTILE_SECRET_KEY` | Turnstile secret key (server-side) |

Control plane (required for `/console` and ingestion):

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | libSQL URL. Defaults to `file:local.db` for dev; use a `libsql://…` Turso URL in production |
| `DATABASE_AUTH_TOKEN` | Turso auth token (omit for the local file) |
| `INGEST_TOKEN` | Bearer token agents present to `POST /api/v1/runs` |
| `AUTH_SECRET` | Secret used to sign admin session cookies |
| `ADMIN_PASSWORD` | Password for the `/console` login |

Missed-backup alerting (optional):

| Variable | Purpose |
| --- | --- |
| `ALERT_TO_EMAIL` | Where alert emails go (falls back to `CONTACT_TO_EMAIL`) |
| `ALERT_MAX_AGE_HOURS` | A server is "overdue" if its last successful backup is older than this (default 26) |
| `CRON_SECRET` | Secures `/api/cron/check-backups`; Vercel Cron sends it automatically |

Set up Turso (free) with `turso db create dbrestore` then `turso db show` /
`turso db tokens create` for the URL and token.

## Edit content

- Links and branding: `lib/site.ts`
- Pricing tiers: `lib/tiers.ts`

## Control plane

A server reports a run when its config has a `control_plane` block (see the root
README). The dashboard at `/console` is gated by the admin password; the
ingestion endpoint is gated by `INGEST_TOKEN`.

`/console/audit` shows a security audit log — console sign-ins (success and
failure) and rejected agent requests, each with timestamp and client IP.

**Missed-backup alerting:** a Vercel Cron job (`vercel.json`, daily) hits
`/api/cron/check-backups`, which emails you when a server hasn't reported a
successful backup within `ALERT_MAX_AGE_HOURS`, or when its latest run failed.
State is tracked per server so you get one alert per incident (and a recovery
note), not one per run. Trigger it manually to test:
`curl -H "authorization: Bearer $CRON_SECRET" https://YOUR-SITE/api/cron/check-backups`.
(The free Vercel plan runs crons once per day.)

This is the foundation; per-server tokens, RBAC, and SSO build on this schema next.

## Deploy

```bash
npm i -g vercel
cd web
vercel          # first run creates the project (set Root Directory to web)
vercel --prod
```

Add the env vars in the Vercel dashboard, or with `vercel env add <NAME> production`.

## Security

Headers (CSP, HSTS, X-Frame-Options, …) are set in `next.config.mjs`. The contact
API validates with Zod, drops honeypot submissions, verifies Turnstile, and
rate-limits per IP. The limiter is in-memory per instance — back it with Redis for
a strict global limit.
