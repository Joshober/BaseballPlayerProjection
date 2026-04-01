# ScoutPro launch checklist

## Production configuration

- **Hosting:** Railway, Render, or Fly with this repo’s `docker-compose.yml` patterns (managed Postgres + Redis recommended).
- **Database:** Set `DATABASE_URL` to the managed Postgres URL. Run `cd backend && alembic upgrade head` on deploy (the backend container entrypoint runs this automatically).
- **Clerk:** Create an application at [clerk.com](https://clerk.com), set `SCOUTPRO_CLERK_PUBLISHABLE_KEY` and `SCOUTPRO_CLERK_SECRET_KEY`, configure allowed origins for your domain and `localhost:5173`. Set `SCOUTPRO_CLERK_ISSUER` to your Clerk Frontend API URL if using JWT verification in `backend/api/deps.py`.
- **Stripe:** Create products/prices in Stripe Dashboard; set `SCOUTPRO_STRIPE_SECRET_KEY`, `SCOUTPRO_STRIPE_WEBHOOK_SECRET`, and `STRIPE_PRICE_STARTER` / `STRIPE_PRICE_PRO` / `STRIPE_PRICE_AGENCY` as price IDs. Test with [Stripe test cards](https://stripe.com/docs/testing).
- **Sentry:** Create a project and set `SENTRY_DSN` in the backend environment (optional; `sentry-sdk` is wired in `backend/main.py`).
- **Domain / SSL:** Point DNS to your host; enable HTTPS; add the production origin to FastAPI `CORSMiddleware` in `backend/main.py` and to Clerk/Stripe redirect URLs.

## Legal and data

- **MLB Stats API:** Free tier is for development; contact `datarequest@mlb.com` for commercial licensing before charging customers.
- **Privacy / ToS:** Add site-wide links (e.g. Termly or Iubenda) before public billing.

## Smoke tests after deploy

1. `GET /health` and `GET /api/health/detail`
2. `GET /docs` — OpenAPI loads
3. Frontend loads on port 5173 (or your CDN); pricing → checkout in Stripe test mode
