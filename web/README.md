# dbrestore website

Marketing and pricing site for dbrestore. Next.js (App Router) + Tailwind, built
to deploy on Vercel. The contact form has server-side validation, Cloudflare
Turnstile, per-IP rate limiting, and email delivery via Resend.

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
production. All are optional.

| Variable | Purpose |
| --- | --- |
| `RESEND_API_KEY` | Resend API key for sending leads |
| `CONTACT_TO_EMAIL` | Inbox that receives leads |
| `CONTACT_FROM_EMAIL` | From address (defaults to `onboarding@resend.dev`) |
| `NEXT_PUBLIC_TURNSTILE_SITE_KEY` | Turnstile site key (public, in the browser) |
| `TURNSTILE_SECRET_KEY` | Turnstile secret key (server-side) |

## Edit content

- Links and branding: `lib/site.ts`
- Pricing tiers: `lib/tiers.ts`

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
