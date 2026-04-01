"""Prediction-time cutoff rules for leakage-safe MiLB→MLB models.

Features must use only MiLB information that would have been known at the
simulated decision point. The default v3 policy uses the first *K* distinct
MiLB seasons (calendar years) of activity, inclusive.

See model card / Data & Models UI for the active `cutoff_policy` string stored
on `engineered_features.cutoff_policy`.
"""
from __future__ import annotations

DEFAULT_FIRST_K_MILB_SEASONS = 2

# Stored on engineered_features.cutoff_policy, e.g. first_k_milb_seasons:2


def format_policy(first_k_milb_seasons: int) -> str:
    return f"first_k_milb_seasons:{first_k_milb_seasons}"
