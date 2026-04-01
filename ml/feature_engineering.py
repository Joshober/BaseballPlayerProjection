from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import psycopg

from db.config import load_project_env
from ml.cutoff_policy import format_policy

load_project_env()


@dataclass
class FeatureBuildResult:
    feature_version: str
    built_rows: int
    upserted_rows: int


def _fetch_df(conn: psycopg.Connection, query: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def _slope_by_season(frame: pd.DataFrame, value_col: str) -> float | None:
    s = frame[["season", value_col]].dropna()
    if len(s) < 2:
        return None
    x = s["season"].astype(float).to_numpy()
    y = s[value_col].astype(float).to_numpy()
    return float(np.polyfit(x, y, 1)[0])


def _prepare_payload_row(row: pd.Series) -> dict[str, Any]:
    payload = row.where(pd.notna(row), None).to_dict()
    return payload


def _peak_milb_tier(level_order: int) -> int:
    """Map DB level_order (1=Rk … 6=AAA) to Rk=1 … AAA=5 style score."""
    if level_order <= 1:
        return 1
    if level_order <= 3:
        return 2
    if level_order == 4:
        return 3
    if level_order == 5:
        return 4
    return 5


def _cohort_mean_age(
    batting: pd.DataFrame,
    pitching: pd.DataFrame,
    player_ids: set[int] | None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for df in (batting, pitching):
        if df.empty:
            continue
        sub = df
        if player_ids is not None:
            sub = sub[sub["player_id"].isin(player_ids)]
        for _, r in sub.iterrows():
            if pd.isna(r.get("age")) or pd.isna(r.get("season")) or pd.isna(r.get("level_order")):
                continue
            rows.append(
                {
                    "season": int(r["season"]),
                    "level_order": int(r["level_order"]),
                    "age": float(r["age"]),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["season", "level_order", "mean_age"])
    c = pd.DataFrame(rows)
    return c.groupby(["season", "level_order"], as_index=False)["age"].mean().rename(columns={"age": "mean_age"})


def _weighted_age_vs_cohort(
    frame: pd.DataFrame,
    cohort: pd.DataFrame,
    weight_col: str,
) -> float | None:
    if frame.empty or cohort.empty or weight_col not in frame.columns:
        return None
    m = frame.merge(cohort, on=["season", "level_order"], how="left")
    m["mean_age"] = m["mean_age"].fillna(m["age"])
    m["diff"] = m["age"] - m["mean_age"]
    w = m[weight_col].fillna(0).astype(float)
    if w.sum() <= 0:
        return None
    return float((m["diff"] * w).sum() / w.sum())


def _ever_repeated_level(lvl: pd.DataFrame) -> bool:
    if lvl.empty:
        return False
    for lo in lvl["level_order"].dropna().unique():
        sub = lvl[lvl["level_order"] == lo]
        if sub["season"].nunique() > 1:
            return True
    return False


def _ops_yoy_delta_and_improve(b: pd.DataFrame) -> tuple[float | None, bool]:
    if b.empty or b["season"].nunique() < 2:
        return None, False
    seasons = sorted(b["season"].dropna().unique())
    ops_by_season: list[tuple[int, float]] = []
    for sea in seasons:
        grp = b[b["season"] == sea]
        pa = float(grp["pa"].fillna(0).sum())
        if pa <= 0:
            continue
        wops = float((grp["ops"].fillna(0) * grp["pa"].fillna(0)).sum() / pa)
        ops_by_season.append((int(sea), wops))
    if len(ops_by_season) < 2:
        return None, False
    last, prev = ops_by_season[-1][1], ops_by_season[-2][1]
    delta = last - prev
    return delta, delta > 0


def _kbb_yoy_delta_and_improve(pit: pd.DataFrame) -> tuple[float | None, bool]:
    if pit.empty or pit["season"].nunique() < 2 or "k_minus_bb" not in pit.columns:
        return None, False
    seasons = sorted(pit["season"].dropna().unique())
    vals: list[tuple[int, float]] = []
    for sea in seasons:
        grp = pit[pit["season"] == sea]
        bf = grp["ip"].fillna(0).sum()  # proxy weight if BF missing
        if bf <= 0:
            continue
        w = float((grp["k_minus_bb"].fillna(0) * grp["ip"].fillna(0)).sum() / bf)
        vals.append((int(sea), w))
    if len(vals) < 2:
        return None, False
    last, prev = vals[-1][1], vals[-2][1]
    delta = last - prev
    return delta, delta > 0


def _low_sample_flag(b: pd.DataFrame, pit: pd.DataFrame, position_group: str) -> bool:
    if position_group in ("sp", "rp"):
        if pit.empty:
            return True
        mx = float(pit.groupby("season")["ip"].sum().max())
        return mx < 60.0
    if b.empty:
        return True
    mx = float(b.groupby("season")["pa"].sum().max())
    return mx < 200.0


def _annotate_ops_age_percentiles(batting: pd.DataFrame) -> pd.DataFrame:
    """Within (season, level_order), OPS/age percentile among all MiLB batting rows."""
    if batting.empty:
        return batting
    out = batting.copy()
    if "ops" in out.columns:
        out["ops_pctile_row"] = out.groupby(["season", "level_order"])["ops"].rank(pct=True, ascending=True)
    else:
        out["ops_pctile_row"] = np.nan
    if "age" in out.columns:
        out["age_pctile_row"] = out.groupby(["season", "level_order"])["age"].rank(pct=True, ascending=True)
    else:
        out["age_pctile_row"] = np.nan
    return out


def _annotate_pitching_percentiles(pitching: pd.DataFrame) -> pd.DataFrame:
    """Within (season, level_order), ERA percentile (higher = better) and age percentile."""
    if pitching.empty:
        return pitching
    out = pitching.copy()
    if "era" in out.columns:
        out["era_pctile_row"] = 1.0 - out.groupby(["season", "level_order"])["era"].rank(pct=True, ascending=True)
    else:
        out["era_pctile_row"] = np.nan
    if "age" in out.columns:
        out["age_pctile_row"] = out.groupby(["season", "level_order"])["age"].rank(pct=True, ascending=True)
    else:
        out["age_pctile_row"] = np.nan
    return out


def _apply_first_k_milb_seasons(
    b: pd.DataFrame, pit: pd.DataFrame, k: int
) -> tuple[pd.DataFrame, pd.DataFrame, int | None, str]:
    """Keep only rows from the first K distinct MiLB seasons (by calendar year)."""
    policy = format_policy(k)
    seasons: set[int] = set()
    if not b.empty:
        seasons |= set(b["season"].dropna().astype(int).unique())
    if not pit.empty:
        seasons |= set(pit["season"].dropna().astype(int).unique())
    if not seasons:
        return b, pit, None, policy
    ordered = sorted(seasons)
    allowed = set(ordered[: min(k, len(ordered))])
    cutoff = max(allowed) if allowed else None
    bb = b[b["season"].isin(allowed)].copy() if not b.empty else b
    pp = pit[pit["season"].isin(allowed)].copy() if not pit.empty else pit
    return bb, pp, cutoff, policy


def _weighted_pctile_mean(frame: pd.DataFrame, pct_col: str, weight_col: str) -> float | None:
    if frame.empty or pct_col not in frame.columns:
        return None
    w = frame[weight_col].fillna(0).astype(float)
    p = frame[pct_col].astype(float)
    if w.sum() <= 0 or p.notna().sum() == 0:
        return None
    m = p.notna()
    if not m.any():
        return None
    return float((p[m] * w[m]).sum() / w[m].sum())


def build_features_dataframe(
    conn: psycopg.Connection,
    feature_version: str = "v1",
    cohort_player_ids: set[int] | None = None,
    first_k_milb_seasons: int | None = None,
) -> pd.DataFrame:
    try:
        players = _fetch_df(
            conn,
            """
            SELECT id AS player_id, position, reached_mlb, years_to_mlb, is_active,
                   draft_round, draft_year, is_international, signing_bonus_usd, eligible_for_training
            FROM players
            """,
        )
    except Exception:
        players = _fetch_df(
            conn,
            """
            SELECT id AS player_id, position, reached_mlb, years_to_mlb, is_active
            FROM players
            """,
        )
        for c in ("draft_round", "draft_year", "is_international", "signing_bonus_usd", "eligible_for_training"):
            players[c] = None
    if players.empty:
        return pd.DataFrame()

    batting = _fetch_df(
        conn,
        """
        SELECT
            player_id, season, level, level_order, age, pa, bb, so, ops, ba, slg, level_adj_ops, age_adj_ops
        FROM milb_batting
        """,
    )
    pitching = _fetch_df(
        conn,
        """
        SELECT
            player_id, season, level, level_order, age, gs, ip, era, so9, bb9, k_minus_bb, whip, level_adj_era
        FROM milb_pitching
        """,
    )
    mlb_bat = _fetch_df(conn, "SELECT player_id, war FROM mlb_batting")
    mlb_pit = _fetch_df(conn, "SELECT player_id, war FROM mlb_pitching")
    salary = _fetch_df(conn, "SELECT player_id, salary_usd FROM salary_history")

    cohort = _cohort_mean_age(batting, pitching, cohort_player_ids)

    use_cutoff = first_k_milb_seasons is not None and first_k_milb_seasons > 0
    if use_cutoff:
        batting = _annotate_ops_age_percentiles(batting)
        pitching = _annotate_pitching_percentiles(pitching)

    out_rows: list[dict[str, Any]] = []

    batting_group = batting.groupby("player_id") if not batting.empty else None
    pitching_group = pitching.groupby("player_id") if not pitching.empty else None

    for _, p in players.iterrows():
        player_id = int(p["player_id"])
        b = (
            batting_group.get_group(player_id)
            if batting_group is not None and player_id in batting_group.groups
            else pd.DataFrame()
        )
        pit = (
            pitching_group.get_group(player_id)
            if pitching_group is not None and player_id in pitching_group.groups
            else pd.DataFrame()
        )

        prediction_cutoff_season = None
        cutoff_policy_val = None
        ops_pctile_w = None
        era_pctile_w = None
        age_pctile_w = None
        if use_cutoff and first_k_milb_seasons is not None:
            b, pit, prediction_cutoff_season, cutoff_policy_val = _apply_first_k_milb_seasons(
                b, pit, first_k_milb_seasons
            )
            ops_pctile_w = _weighted_pctile_mean(b, "ops_pctile_row", "pa")
            era_pctile_w = _weighted_pctile_mean(pit, "era_pctile_row", "ip")
            if not b.empty:
                age_pctile_w = _weighted_pctile_mean(b, "age_pctile_row", "pa")
            if age_pctile_w is None and not pit.empty:
                age_pctile_w = _weighted_pctile_mean(pit, "age_pctile_row", "ip")

        all_levels = []
        if not b.empty:
            all_levels.append(b[["level", "level_order", "age", "season"]].copy())
        if not pit.empty:
            all_levels.append(pit[["level", "level_order", "age", "season"]].copy())
        lvl = pd.concat(all_levels, ignore_index=True) if all_levels else pd.DataFrame()

        peak_level_order = int(lvl["level_order"].max()) if not lvl.empty and lvl["level_order"].notna().any() else None
        peak_level = None
        age_at_peak_level = None
        seasons_in_minors = None
        age_at_pro_debut = None

        if not lvl.empty:
            seasons_in_minors = int(lvl["season"].dropna().nunique()) if lvl["season"].notna().any() else None
            age_at_pro_debut = float(lvl["age"].dropna().min()) if lvl["age"].notna().any() else None
            if peak_level_order is not None:
                peak_rows = lvl[lvl["level_order"] == peak_level_order]
                peak_level = str(peak_rows["level"].dropna().iloc[0]) if not peak_rows["level"].dropna().empty else None
                age_at_peak_level = float(peak_rows["age"].dropna().mean()) if peak_rows["age"].notna().any() else None

        # Batting aggregates
        career_milb_pa = int(b["pa"].fillna(0).sum()) if not b.empty else None
        best_season_ops = float(b["ops"].dropna().max()) if not b.empty and b["ops"].notna().any() else None
        best_season_ops_level_adj = (
            float(b["level_adj_ops"].dropna().max()) if not b.empty and b["level_adj_ops"].notna().any() else None
        )
        career_milb_iso = float((b["slg"] - b["ba"]).dropna().mean()) if not b.empty else None
        career_milb_bb_pct = (
            float((b["bb"].fillna(0).sum() / b["pa"].fillna(0).sum())) if not b.empty and b["pa"].fillna(0).sum() > 0 else None
        )
        career_milb_k_pct = (
            float((b["so"].fillna(0).sum() / b["pa"].fillna(0).sum())) if not b.empty and b["pa"].fillna(0).sum() > 0 else None
        )
        ops_trajectory = _slope_by_season(b, "ops") if not b.empty else None
        ops_aaa = float(b.loc[b["level_order"] == 6, "ops"].dropna().mean()) if not b.empty else None
        pa_at_aa_plus = int(b.loc[b["level_order"] >= 5, "pa"].fillna(0).sum()) if not b.empty else None
        age_adj_ops_peak = (
            float(b.loc[b["level_order"] == peak_level_order, "age_adj_ops"].dropna().mean())
            if not b.empty and peak_level_order is not None
            else None
        )

        # Pitching aggregates
        career_milb_ip = float(pit["ip"].fillna(0).sum()) if not pit.empty else None
        best_season_era = float(pit["era"].dropna().min()) if not pit.empty and pit["era"].notna().any() else None
        best_season_era_level_adj = (
            float(pit["level_adj_era"].dropna().min()) if not pit.empty and pit["level_adj_era"].notna().any() else None
        )
        career_milb_k9 = float(pit["so9"].dropna().mean()) if not pit.empty and pit["so9"].notna().any() else None
        career_milb_bb9 = float(pit["bb9"].dropna().mean()) if not pit.empty and pit["bb9"].notna().any() else None
        career_milb_k_minus_bb = (
            float(pit["k_minus_bb"].dropna().mean()) if not pit.empty and pit["k_minus_bb"].notna().any() else None
        )
        career_milb_whip = float(pit["whip"].dropna().mean()) if not pit.empty and pit["whip"].notna().any() else None
        era_trajectory = _slope_by_season(pit, "era") if not pit.empty else None
        era_aaa = float(pit.loc[pit["level_order"] == 6, "era"].dropna().mean()) if not pit.empty else None
        ip_at_aa_plus = float(pit.loc[pit["level_order"] >= 5, "ip"].fillna(0).sum()) if not pit.empty else None

        # Labels
        player_mlb_war = float(
            pd.concat(
                [
                    mlb_bat.loc[mlb_bat["player_id"] == player_id, "war"],
                    mlb_pit.loc[mlb_pit["player_id"] == player_id, "war"],
                ],
                ignore_index=True,
            )
            .dropna()
            .sum()
        )
        if player_mlb_war == 0.0:
            player_mlb_war = None

        salary_slice = salary.loc[salary["player_id"] == player_id, "salary_usd"].dropna()
        label_peak_salary_usd = int(salary_slice.max()) if not salary_slice.empty else None
        label_career_earnings_usd = int(salary_slice.sum()) if not salary_slice.empty else None

        position_group = "bat"
        pos = str(p["position"]).upper() if p["position"] is not None else ""
        if pos == "P":
            if not pit.empty and pit["gs"].fillna(0).sum() >= 0.35 * max(len(pit), 1):
                position_group = "sp"
            else:
                position_group = "rp"

        first_milb = int(lvl["season"].min()) if not lvl.empty and lvl["season"].notna().any() else None
        v2_on = feature_version != "v1"
        career_age_vs = None
        ev_rep = None
        promo_speed = None
        ops_yoy = None
        kbb_yoy = None
        improving = None
        low_samp = None
        dr_f = None
        intl_f = None
        bonus_f = None

        def _row_val(key: str):
            if key not in p.index:
                return None
            v = p[key]
            if pd.isna(v):
                return None
            return v

        elig_raw = _row_val("eligible_for_training")
        elig_bool = bool(elig_raw) if elig_raw is not None else None

        if v2_on:
            av_b = _weighted_age_vs_cohort(b, cohort, "pa") if not b.empty else None
            av_p = _weighted_age_vs_cohort(pit, cohort, "ip") if not pit.empty else None
            if position_group == "bat" and av_b is not None:
                career_age_vs = av_b
            elif position_group in ("sp", "rp") and av_p is not None:
                career_age_vs = av_p
            elif av_b is not None:
                career_age_vs = av_b
            elif av_p is not None:
                career_age_vs = av_p
            ev_rep = _ever_repeated_level(lvl)
            n_sea = int(lvl["season"].nunique()) if not lvl.empty else 1
            pk_ord = peak_level_order or 1
            promo_speed = float(_peak_milb_tier(pk_ord) / max(n_sea, 1))
            ops_yoy, imp_o = _ops_yoy_delta_and_improve(b)
            kbb_yoy, imp_k = _kbb_yoy_delta_and_improve(pit)
            improving = bool(imp_o or imp_k)
            low_samp = _low_sample_flag(b, pit, position_group)
            dr_raw = _row_val("draft_round")
            try:
                dr_f = int(dr_raw) if dr_raw is not None else None
            except (TypeError, ValueError):
                dr_f = None
            intl_raw = _row_val("is_international")
            intl_f = bool(intl_raw) if intl_raw is not None else None
            bonus_raw = _row_val("signing_bonus_usd")
            try:
                bonus_f = int(bonus_raw) if bonus_raw is not None else None
            except (TypeError, ValueError):
                bonus_f = None

        row_base: dict[str, Any] = {
            "player_id": player_id,
            "feature_version": feature_version,
            "position_group": position_group,
            "peak_level": peak_level,
            "peak_level_order": peak_level_order,
            "seasons_in_minors": seasons_in_minors,
            "age_at_pro_debut": age_at_pro_debut,
            "age_at_peak_level": age_at_peak_level,
            "career_milb_pa": career_milb_pa,
            "best_season_ops": best_season_ops,
            "best_season_ops_level_adj": best_season_ops_level_adj,
            "career_milb_iso": career_milb_iso,
            "career_milb_bb_pct": career_milb_bb_pct,
            "career_milb_k_pct": career_milb_k_pct,
            "ops_trajectory": ops_trajectory,
            "ops_aaa": ops_aaa,
            "pa_at_aa_plus": pa_at_aa_plus,
            "age_adj_ops_peak": age_adj_ops_peak,
            "career_milb_ip": career_milb_ip,
            "best_season_era": best_season_era,
            "best_season_era_level_adj": best_season_era_level_adj,
            "career_milb_k9": career_milb_k9,
            "career_milb_bb9": career_milb_bb9,
            "career_milb_k_minus_bb": career_milb_k_minus_bb,
            "career_milb_whip": career_milb_whip,
            "era_trajectory": era_trajectory,
            "era_aaa": era_aaa,
            "ip_at_aa_plus": ip_at_aa_plus,
            "label_reached_mlb": bool(p["reached_mlb"]) if p["reached_mlb"] is not None else None,
            "label_years_to_mlb": float(p["years_to_mlb"]) if p["years_to_mlb"] is not None else None,
            "label_censored": (not bool(p["reached_mlb"])) and bool(p["is_active"]),
            "label_career_war": player_mlb_war,
            "label_peak_salary_usd": label_peak_salary_usd,
            "label_career_earnings_usd": label_career_earnings_usd,
            "first_milb_season": first_milb if v2_on else None,
            "career_age_vs_level_avg": career_age_vs,
            "ever_repeated_level": ev_rep,
            "promotion_speed_score": promo_speed,
            "ops_yoy_delta": ops_yoy,
            "k_minus_bb_yoy_delta": kbb_yoy,
            "is_improving": improving,
            "low_sample_season_flag": low_samp,
            "draft_round_feat": dr_f,
            "is_international_feat": intl_f,
            "signing_bonus_usd_feat": bonus_f,
            "label_eligible_for_training": elig_bool,
            "prediction_cutoff_season": prediction_cutoff_season,
            "cutoff_policy": cutoff_policy_val,
            "ops_pctile_milb_weighted": ops_pctile_w,
            "era_pctile_milb_weighted": era_pctile_w,
            "age_pctile_milb_weighted": age_pctile_w,
        }
        out_rows.append(row_base)

    return pd.DataFrame(out_rows)


def upsert_engineered_features(conn: psycopg.Connection, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0

    cols = [
        "player_id",
        "feature_version",
        "position_group",
        "peak_level",
        "peak_level_order",
        "seasons_in_minors",
        "age_at_pro_debut",
        "age_at_peak_level",
        "career_milb_pa",
        "best_season_ops",
        "best_season_ops_level_adj",
        "career_milb_iso",
        "career_milb_bb_pct",
        "career_milb_k_pct",
        "ops_trajectory",
        "ops_aaa",
        "pa_at_aa_plus",
        "age_adj_ops_peak",
        "career_milb_ip",
        "best_season_era",
        "best_season_era_level_adj",
        "career_milb_k9",
        "career_milb_bb9",
        "career_milb_k_minus_bb",
        "career_milb_whip",
        "era_trajectory",
        "era_aaa",
        "ip_at_aa_plus",
        "label_reached_mlb",
        "label_years_to_mlb",
        "label_censored",
        "label_career_war",
        "label_peak_salary_usd",
        "label_career_earnings_usd",
        "first_milb_season",
        "career_age_vs_level_avg",
        "ever_repeated_level",
        "promotion_speed_score",
        "ops_yoy_delta",
        "k_minus_bb_yoy_delta",
        "is_improving",
        "low_sample_season_flag",
        "draft_round_feat",
        "is_international_feat",
        "signing_bonus_usd_feat",
        "label_eligible_for_training",
        "prediction_cutoff_season",
        "cutoff_policy",
        "ops_pctile_milb_weighted",
        "era_pctile_milb_weighted",
        "age_pctile_milb_weighted",
    ]
    set_cols = [c for c in cols if c not in ("player_id", "feature_version")]
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in set_cols] + ["computed_at=NOW()"])

    sql = f"""
    INSERT INTO engineered_features ({", ".join(cols)})
    VALUES ({", ".join([f"%({c})s" for c in cols])})
    ON CONFLICT (player_id, feature_version)
    DO UPDATE SET {updates}
    """

    payload = [_prepare_payload_row(frame[cols].iloc[i]) for i in range(len(frame))]
    with conn.cursor() as cur:
        cur.executemany(sql, payload)
    conn.commit()
    return len(payload)


def build_and_upsert_features(
    database_url: str,
    feature_version: str = "v1",
    cohort_player_ids: set[int] | None = None,
    first_k_milb_seasons: int | None = None,
) -> FeatureBuildResult:
    with psycopg.connect(database_url) as conn:
        frame = build_features_dataframe(
            conn,
            feature_version=feature_version,
            cohort_player_ids=cohort_player_ids,
            first_k_milb_seasons=first_k_milb_seasons,
        )
        upserted = upsert_engineered_features(conn, frame)
    return FeatureBuildResult(feature_version=feature_version, built_rows=len(frame), upserted_rows=upserted)
