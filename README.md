# BaseballPlayerProjection / ScoutPro
Model that projects if a MiLB player will make it to the MLB, the time it will take for the player to make it, a comparison with a similar active MLB player, and a prediction of how much money they are going to make.

## Local development (no Docker)

Run the API and React app on your machine. If you were using Docker before, stop it: `docker compose down`.

### 1) Python

- Python 3.11+ recommended.
- From the repo root:
  - `python -m venv .venv`
  - **Windows:** `.\.venv\Scripts\Activate.ps1`
  - **macOS/Linux:** `source .venv/bin/activate`
  - `pip install -r requirements.txt`

### 2) PostgreSQL

- Install PostgreSQL locally and create a database (e.g. `baseball_project`).
- Copy `.env.example` to `.env` and set `DATABASE_URL`, for example:
  - `DATABASE_URL=postgresql://username:password@localhost:5432/baseball_project`
- Apply migrations (from repo root, with `PYTHONPATH` set to the repo root — the PowerShell script below does this):
  - **PowerShell:** `$env:PYTHONPATH = (Get-Location).Path; cd backend; alembic upgrade head; cd ..`
  - Or: `cd backend; $env:PYTHONPATH = ".."; alembic upgrade head` (if your shell resolves parent correctly — prefer the absolute repo path).

### 3) Environment

- `SCOUTPRO_DEV_AUTH=1` in `.env` skips JWT on protected API routes during local dev.
- Redis is optional: without `REDIS_URL`, `/api/health/detail` may show `cache: false`; the app still runs.

### 4) Start the backend

From the **repository root** (so `backend` and `ml` import correctly):

**PowerShell (Windows):**

```powershell
.\scripts\run-local.ps1
```

**Manual:**

```powershell
$env:PYTHONPATH = "$PWD"   # repo root
uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

- API docs: **http://127.0.0.1:8000/docs**
- Shims: `uvicorn api:app --reload --host 127.0.0.1 --port 8000` also works.

### 5) Start the frontend

Second terminal:

```bash
cd frontend
npm install
npm run dev
```

- App: **http://localhost:5173** (Vite proxies `/api` to the backend when configured; the app uses `VITE_API_BASE_URL` defaulting to `http://localhost:8000` for `/mlb/*` and other routes.)

### Optional: Docker

If you prefer containers: copy `.env`, then `docker compose up --build`. API on **8000**, Postgres on host **5433** (mapped from the container so it does not fight a local Postgres on **5432**), app on **5173**. Migrations run when the backend container starts.

## Current APIs you need

- No paid external API is required for the current scraper.
- The project scrapes Baseball-Reference directly.
- MLB Stats API is used for search/player helpers and pipeline ingestion (see `backend/pipeline/`).
- For a hosted service, you only need:
  - a GitHub repo
  - a hosting provider account (Render/Railway/Fly/Heroku-style)

## Web UI (React)

- **http://localhost:5173** — main app
- **http://localhost:5173/tools** — MLB search, MLB profile, MiLB scrape/ingest (legacy static dashboard)

## Endpoints

- `GET /health`
- `GET /db/health`
- `GET /scrape?url=<baseball_reference_player_url>` (legacy alias)
  - same as `GET /api/scrape/preview?url=...`
  - optional: `delay`, `include_tables`, `table_limit`
- `GET /api/scrape/preview?url=<register_player_url>`
  - BBRef register scrape JSON (uses `milb_scraper.MiLBScraper` via `ml.scrape_pipeline`)
- `POST /api/scrape/ingest?url=<register_player_url>`
  - scrape + upsert into `players`, `milb_batting`, `milb_pitching`; optional `build_features=true` to run `POST /api/features/build` for all players
- `GET /mlb/search?name=<player_name>`
  - free MLB StatsAPI player search
- `GET /mlb/player/<player_id>`
  - free MLB StatsAPI profile + career hitting/pitching summary
- `POST /api/features/build?feature_version=v1`
  - Phase 1 feature engineering + label build into `engineered_features`
- `POST /api/ingest/scrape?url=<register_player_url>`
  - same pipeline as `POST /api/scrape/ingest` (optional `delay`, `mlb_id`, `build_features`, `feature_version`)

## Database setup

1. Create `.env` with `DATABASE_URL`, or run:
   - `.\scripts\setup_db.ps1 -DatabaseUrl "postgresql://username:password@localhost:5432/baseball_project"`
   - (That script uses `db.init_db` with `schema.sql`; you can use Alembic instead: `cd backend` + `alembic upgrade head` with `PYTHONPATH` at repo root.)
2. Check DB: `GET http://127.0.0.1:8000/db/health`

## Phase 1 feature build

- CLI:
  - `python -m ml.build_features --feature-version v1`
- API:
  - `POST /api/features/build?feature_version=v1`

## ML training data (labels + features)

Training uses `engineered_features` (`ml/train_all.py`) and needs **many players** with MiLB stats and labels. Typical flow:

1. **Ingest** Baseball-Reference register data (Tools page `/tools` or `POST /api/scrape/ingest`) so `milb_batting` / `milb_pitching` fill and `players.mlb_id` is set when you pick an MLB player. For volume, use **Automated dataset build** on `/tools` (or `POST /api/scrape/batch-start`) which loops MLB search → BBRef register URL → ingest using `data/batch_search_queries.txt` until your target ingest count or MiLB row count is reached. CLI: `python -m ml.batch_ingest_discovery --target-ingests 100`.
2. **Backfill labels** from the MLB Stats API (`reached_mlb`, `years_to_mlb`, `is_active` for rows with `mlb_id`):
   - `python -m ml.backfill_player_labels`
3. **Build features** into `engineered_features`:
   - `python -m ml.build_features --feature-version v1`
4. **Check counts**:
   - `python -m ml.data_status`
5. **Optional one-shot pipeline** (PowerShell): `.\scripts\prepare_ml_data.ps1` (add `--train` to run `ml.train_all` after features). `train_all` expects dozens of rows before metrics are meaningful (`insufficient rows or features` is normal until the dataset grows).

**MiLB→MLB arrival models** (`train_arrival_by_role` in `ml/arrival_training.py`): per role (`bat` vs combined `sp`+`rp`), training needs at least **30** eligible rows after filtering and **5** positives and **5** negatives for a stratified split. Expand `data/batch_search_queries.txt` with pitcher-heavy names so `milb_pitching` fills; pure MLB name search skews toward big-leaguers, so long-term you may need **BBRef-only minor careers** (or an explicit negative URL list) to improve negative-class balance.

Offline labels from Lahman CSVs: `python -m ml.build_labels_csv` (writes `data/processed/labels.csv` when `data/raw/lahman/People.csv` is present).

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
