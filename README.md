# BaseballPlayerProjection
Model that projects if a MiLB player will make it to the MLB, the time it will take for the player to make it, a comparison with a similar active MLB player, and a prediction of how much money they are going to make.

## Current APIs you need

- No paid external API is required for the current scraper.
- The project scrapes Baseball-Reference directly.
- For a hosted service, you only need:
  - a GitHub repo
  - a hosting provider account (Render/Railway/Fly/Heroku-style)

## Local run (web API)

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Start API:
   - `uvicorn api:app --host 0.0.0.0 --port 8000`
3. Open docs:
   - `http://localhost:8000/docs`
4. Open frontend dashboard:
   - `http://localhost:8000/`

## Endpoints

- `GET /health`
- `GET /db/health`
- `GET /scrape?url=<baseball_reference_player_url>`
  - optional: `delay`, `include_tables`, `table_limit`
- `GET /mlb/search?name=<player_name>`
  - free MLB StatsAPI player search
- `GET /mlb/player/<player_id>`
  - free MLB StatsAPI profile + career hitting/pitching summary
- `POST /api/features/build?feature_version=v1`
  - Phase 1 feature engineering + label build into `engineered_features`
- `POST /api/ingest/scrape?url=<register_player_url>`
  - Phase 1.5: scrape + upsert into `players`, `milb_batting`, `milb_pitching` (optional `delay`, `mlb_id`)

## Database setup

1. Run one command to create `.env`, set `DATABASE_URL`, and initialize schema:
   - `.\scripts\setup_db.ps1 -DatabaseUrl "postgresql://username:password@localhost:5432/baseball_project"`
2. Check DB status:
   - `GET /db/health`

## Phase 1 feature build

- CLI:
  - `python -m ml.build_features --feature-version v1`
- API:
  - `POST /api/features/build?feature_version=v1`

## Phase 1.5 ingest (scrape → DB)

- CLI:
  - `python -m ml.ingest --url "https://www.baseball-reference.com/register/player.fcgi?id=jones-000dru"`
- API:
  - `POST /api/ingest/scrape?url=https://www.baseball-reference.com/register/player.fcgi?id=jones-000dru`
- Then run Phase 1 feature build so `engineered_features` is populated from DB rows.

## Deploy quickly (Render example)

Use these settings:

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn api:app --host 0.0.0.0 --port $PORT`
- Runtime: Python 3.11+ recommended

After deploy, test:

- `GET https://<your-service>.onrender.com/health`
- `GET https://<your-service>.onrender.com/scrape?url=https://www.baseball-reference.com/register/player.fcgi?id=cabrer003jos`
