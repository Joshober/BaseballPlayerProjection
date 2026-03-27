from typing import Any, Dict, List, Optional

import requests

MLB_STATS_API_BASE = "https://statsapi.mlb.com/api/v1"


def _get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 20) -> Dict[str, Any]:
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def search_mlb_people(name: str) -> List[Dict[str, Any]]:
    """Free MLB StatsAPI player search by name."""
    data = _get_json(f"{MLB_STATS_API_BASE}/people/search", params={"names": name})
    people = data.get("people", [])
    out: List[Dict[str, Any]] = []
    for p in people:
        out.append(
            {
                "id": p.get("id"),
                "full_name": p.get("fullName"),
                "primary_position": (p.get("primaryPosition") or {}).get("abbreviation"),
                "mlb_debut_date": p.get("mlbDebutDate"),
                "bat_side": (p.get("batSide") or {}).get("code"),
                "pitch_hand": (p.get("pitchHand") or {}).get("code"),
                "active": p.get("active"),
            }
        )
    return out


def get_mlb_player(player_id: int) -> Dict[str, Any]:
    """Free MLB StatsAPI player profile + career hitting/pitching stats."""
    profile_data = _get_json(f"{MLB_STATS_API_BASE}/people/{player_id}")
    people = profile_data.get("people", [])
    if not people:
        raise ValueError(f"No player found for id={player_id}")
    profile = people[0]

    stats_data = _get_json(
        f"{MLB_STATS_API_BASE}/people/{player_id}/stats",
        params={"stats": "career", "group": "hitting,pitching"},
    )
    split_stats = stats_data.get("stats", [])
    hitting: List[Dict[str, Any]] = []
    pitching: List[Dict[str, Any]] = []
    for group in split_stats:
        gname = ((group.get("group") or {}).get("displayName") or "").lower()
        splits = group.get("splits", [])
        if gname == "hitting":
            hitting = [s.get("stat", {}) for s in splits]
        elif gname == "pitching":
            pitching = [s.get("stat", {}) for s in splits]

    return {
        "profile": {
            "id": profile.get("id"),
            "full_name": profile.get("fullName"),
            "first_name": profile.get("firstName"),
            "last_name": profile.get("lastName"),
            "birth_date": profile.get("birthDate"),
            "current_age": profile.get("currentAge"),
            "birth_city": profile.get("birthCity"),
            "birth_state_province": profile.get("birthStateProvince"),
            "birth_country": profile.get("birthCountry"),
            "height": profile.get("height"),
            "weight": profile.get("weight"),
            "primary_position": (profile.get("primaryPosition") or {}).get("abbreviation"),
            "bat_side": (profile.get("batSide") or {}).get("code"),
            "pitch_hand": (profile.get("pitchHand") or {}).get("code"),
            "active": profile.get("active"),
            "mlb_debut_date": profile.get("mlbDebutDate"),
        },
        "career_hitting": hitting,
        "career_pitching": pitching,
    }
