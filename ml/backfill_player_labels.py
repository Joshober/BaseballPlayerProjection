"""Backfill players.reached_mlb, years_to_mlb, is_active, eligibility, draft using MLB Stats API + MiLB min season."""
from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime

import psycopg

from db.config import load_project_env
from free_apis import career_has_mlb_games, get_mlb_player
from ml.lahman_optional import load_mlb_id_flags_from_csv

load_project_env()

_USA = frozenset({"USA", "US", "UNITED STATES", "U.S.A.", "U.S."})


def _min_milb_season(cur, player_id: int) -> int | None:
    cur.execute("SELECT MIN(season) FROM milb_batting WHERE player_id = %s", (player_id,))
    b = cur.fetchone()[0]
    cur.execute("SELECT MIN(season) FROM milb_pitching WHERE player_id = %s", (player_id,))
    p = cur.fetchone()[0]
    vals = [x for x in (b, p) if x is not None]
    return int(min(vals)) if vals else None


def _years_milb_to_mlb_fractional(min_season: int | None, mlb_debut: str | None) -> float | None:
    if min_season is None or not mlb_debut:
        return None
    try:
        debut = datetime.strptime(mlb_debut[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
    approx_start = date(min_season, 4, 1)
    if debut < approx_start:
        return 0.0
    return round((debut - approx_start).days / 365.25, 2)


def _years_int_debut_minus_first_milb(min_season: int | None, mlb_debut: str | None) -> int | None:
    """Primary label: MLB debut calendar year minus first MiLB season year in our data."""
    if min_season is None or not mlb_debut:
        return None
    try:
        debut_y = int(mlb_debut[:4])
    except (ValueError, TypeError):
        return None
    return max(0, debut_y - int(min_season))


def _parse_debut_date(s: str | None) -> date | None:
    if not s or not str(s).strip():
        return None
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _infer_international(prof: dict) -> bool | None:
    bc = (prof.get("birth_country") or "").strip().upper()
    dr = prof.get("draft_round")
    dy = prof.get("draft_year")
    if dr is not None or dy is not None:
        return False
    if not bc:
        return None
    return bc not in _USA


def _eligible_for_training(reached: bool, active: bool, age: int | None) -> bool:
    """Exclude active players under 28 with no MLB time — career not resolved."""
    if reached:
        return True
    if not active:
        return True
    if age is None:
        return True
    return age >= 28


def _draft_round_int(val: object) -> int | None:
    if val is None:
        return None
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if not s or not s[0].isdigit():
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def backfill_all(*, delay_s: float = 0.35, limit: int | None = None) -> dict[str, int]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    lahman_flags = load_mlb_id_flags_from_csv()
    updated = 0
    errors = 0

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            if limit is not None:
                cur.execute(
                    """
                    SELECT id, mlb_id FROM players
                    WHERE mlb_id IS NOT NULL
                    ORDER BY id
                    LIMIT %s
                    """,
                    (limit,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, mlb_id FROM players
                    WHERE mlb_id IS NOT NULL
                    ORDER BY id
                    """
                )
            rows = cur.fetchall()

        for pid, mlb_id in rows:
            try:
                data = get_mlb_player(int(mlb_id))
                prof = data.get("profile") or {}
                debut = prof.get("mlb_debut_date")
                reached_debut = debut is not None and str(debut).strip() != ""
                reached_stats = career_has_mlb_games(data)
                reached_lahman = lahman_flags.get(int(mlb_id)) if lahman_flags else None
                reached = reached_debut or reached_stats or (reached_lahman is True)
                active = bool(prof.get("active"))

                with conn.cursor() as cur:
                    min_season = _min_milb_season(cur, int(pid))
                y_frac = _years_milb_to_mlb_fractional(min_season, debut if reached_debut else None)
                y_int = _years_int_debut_minus_first_milb(min_season, debut if reached_debut else None)

                age = prof.get("current_age")
                if age is None and prof.get("birth_date"):
                    try:
                        bd = datetime.strptime(str(prof["birth_date"])[:10], "%Y-%m-%d").date()
                        age = int((date.today() - bd).days // 365.25)
                    except (ValueError, TypeError):
                        age = None

                eligible = _eligible_for_training(reached, active, age)
                debut_dt = _parse_debut_date(debut if isinstance(debut, str) else str(debut) if debut else None)
                intl = _infer_international(prof)
                dr_int = _draft_round_int(prof.get("draft_round"))
                dy = prof.get("draft_year")
                try:
                    dy_int = int(dy) if dy is not None else None
                except (TypeError, ValueError):
                    dy_int = None

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE players
                        SET reached_mlb = %s,
                            years_to_mlb = %s,
                            years_to_mlb_fractional = %s,
                            is_active = %s,
                            mlb_debut_date = COALESCE(%s, players.mlb_debut_date),
                            birth_date = COALESCE(players.birth_date, %s),
                            draft_round = COALESCE(%s, players.draft_round),
                            draft_year = COALESCE(%s, players.draft_year),
                            is_international = COALESCE(%s, players.is_international),
                            eligible_for_training = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            reached,
                            float(y_int) if y_int is not None else None,
                            y_frac,
                            active,
                            debut_dt,
                            _parse_debut_date(prof.get("birth_date")) if prof.get("birth_date") else None,
                            dr_int,
                            dy_int,
                            intl,
                            eligible,
                            int(pid),
                        ),
                    )
                conn.commit()
                updated += 1
            except Exception:
                conn.rollback()
                errors += 1
            time.sleep(delay_s)

    return {"updated": updated, "errors": errors, "candidates": len(rows)}


def main() -> None:
    p = argparse.ArgumentParser(description="Backfill MLB outcome labels for players with mlb_id")
    p.add_argument("--delay", type=float, default=0.35, help="Seconds between MLB API calls")
    p.add_argument("--limit", type=int, default=None, help="Max players to process (debug)")
    args = p.parse_args()

    stats = backfill_all(delay_s=args.delay, limit=args.limit)
    print(stats)


if __name__ == "__main__":
    main()
