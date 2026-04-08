"""
V1 data bridge
==============

Loads the three V1 data exports in `data/v1/` and turns them into the
small in-memory tables that update.py + score.py use to overlay V1
onto the V2 live pipeline without making any new network calls.

All three functions are cached on first call so repeated imports during
a single launchd pulse don't re-parse the 5 MB CSV.
"""

from __future__ import annotations

import csv
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

REPO_ROOT = Path(__file__).resolve().parent.parent
V1_DIR    = REPO_ROOT / "data" / "v1"

MOVIES_FILE  = V1_DIR / "movies.json"
PEOPLE_FILE  = V1_DIR / "people.json"
HISTORY_FILE = V1_DIR / "movie-history.csv"


# ---------------------------------------------------------------------------
# Title normalization — both V1 and V2 (TMDb) use slightly different
# conventions (colons, "The" placement, punctuation). We normalize both
# sides to a common key for intersection + lookup.
# ---------------------------------------------------------------------------

_PUNCT_RX = re.compile(r"[^\w\s]")
_SPACE_RX = re.compile(r"\s+")

def normalize_title(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    # Drop leading articles
    for art in ("the ", "a ", "an "):
        if s.startswith(art):
            s = s[len(art):]
            break
    # Strip punctuation, collapse spaces
    s = _PUNCT_RX.sub(" ", s)
    s = _SPACE_RX.sub(" ", s).strip()
    return s


def _pick_best_release_date(release_dates: Optional[List[Dict[str, str]]]) -> Optional[str]:
    """
    V1 movies.json has per-country release dates. We prefer the US date
    when present, otherwise the earliest date across all listed
    countries (to match TMDb's "primary" release date behaviour).
    """
    if not release_dates:
        return None
    # Prefer US
    for rd in release_dates:
        if rd.get("country") == "US" and rd.get("date"):
            return rd["date"]
    # Else earliest
    parsed = sorted(rd.get("date") or "" for rd in release_dates if rd.get("date"))
    return parsed[0] if parsed else None


# ---------------------------------------------------------------------------
# movies.json — tracked film universe
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def load_v1_movies() -> List[Dict[str, Any]]:
    """
    Returns [{id, name, norm, release_date}, ...] with ~253 entries.
    norm is the normalized title key used for V1↔V2 joins.
    """
    if not MOVIES_FILE.exists():
        return []
    raw = json.loads(MOVIES_FILE.read_text())
    items = raw.get("items") if isinstance(raw, dict) else raw
    out = []
    for m in items or []:
        name = (m.get("name") or "").strip()
        if not name:
            continue
        out.append({
            "id":           m.get("id"),
            "name":         name,
            "norm":         normalize_title(name),
            "release_date": _pick_best_release_date(m.get("releaseDates")),
        })
    return out


@lru_cache(maxsize=1)
def v1_title_index() -> Dict[str, Dict[str, Any]]:
    """normalized title → V1 movie entry (for O(1) lookups)."""
    return {m["norm"]: m for m in load_v1_movies() if m["norm"]}


# ---------------------------------------------------------------------------
# people.json — tracked actor/director catalog
# ---------------------------------------------------------------------------

_PEOPLE_PUNCT = re.compile(r"[^\w\s]")

def normalize_person(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    s = _PEOPLE_PUNCT.sub(" ", s)
    s = _SPACE_RX.sub(" ", s).strip()
    return s


@lru_cache(maxsize=1)
def load_v1_people() -> List[Dict[str, Any]]:
    """Returns [{id, name, norm}, ...] with ~1398 entries."""
    if not PEOPLE_FILE.exists():
        return []
    raw = json.loads(PEOPLE_FILE.read_text())
    items = raw.get("items") if isinstance(raw, dict) else raw
    out = []
    for p in items or []:
        name = (p.get("name") or "").strip()
        if not name:
            continue
        out.append({
            "id":   p.get("id"),
            "name": name,
            "norm": normalize_person(name),
        })
    return out


@lru_cache(maxsize=1)
def v1_person_name_set() -> Set[str]:
    """Set of normalized person names — cheap membership test."""
    return {p["norm"] for p in load_v1_people() if p["norm"]}


# ---------------------------------------------------------------------------
# movie-history.csv — per-movie hype_score history, momentum_v2 only
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def load_v1_history() -> Dict[str, Dict[str, Any]]:
    """
    Returns a dict keyed by normalized title:
      {
        "project hail mary": {
          "id": "...",
          "name": "Project Hail Mary",
          "release_date": "2026-03-18",
          "v1_score": 891,            # latest hype_score × 1000, int
          "raw_score": 3.698,         # latest raw_score (can be negative)
          "snapshot_date": "2026-04-07",
          "history_1d": [              # 1d window snapshots in date order
            {"date": "2026-02-23", "score": 170},
            ...
          ],
        },
        ...
      }
    Only momentum_v2 + 1d window rows are indexed (the richest daily
    series). We take the latest snapshot's hype_score as the headline
    "V1 score" for the comparison column.
    """
    if not HISTORY_FILE.exists():
        return {}

    # First pass — collect 1d rows per title
    per_title: Dict[str, List[Dict[str, Any]]] = {}
    with HISTORY_FILE.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("scoring_method") != "momentum_v2":
                continue
            if row.get("window") != "1d":
                continue
            name = (row.get("title") or "").strip()
            if not name:
                continue
            key = normalize_title(name)
            try:
                hype = float(row.get("hype_score") or 0)
            except ValueError:
                continue
            per_title.setdefault(key, []).append({
                "id":           row.get("movie_id"),
                "name":         name,
                "release_date": (row.get("release_date_utc") or "")[:10],
                "snapshot":     row.get("snapshot_date_utc"),
                "hype":         hype,
                "raw":          float(row.get("raw_score") or 0),
            })

    # Second pass — sort each title's snapshots and compute headline
    out: Dict[str, Dict[str, Any]] = {}
    for key, snaps in per_title.items():
        snaps.sort(key=lambda r: r["snapshot"] or "")
        latest = snaps[-1]
        history_1d = [
            {"date": s["snapshot"], "score": int(round(s["hype"] * 1000))}
            for s in snaps
        ]
        out[key] = {
            "id":            latest["id"],
            "name":          latest["name"],
            "release_date":  latest["release_date"],
            "v1_score":      int(round(latest["hype"] * 1000)),
            "raw_score":     round(latest["raw"], 3),
            "snapshot_date": latest["snapshot"],
            "history_1d":    history_1d,
        }
    return out


# ---------------------------------------------------------------------------
# Momentum v2 scoring weights (per the V1 scoring method)
# ---------------------------------------------------------------------------
#
# V1 uses:
#   raw_score = 0.8 × youtube_source_score + 0.2 × x_source_score
#   hype_score = raw_score normalized to [0, 1]
#
# V2 doesn't have X (Twitter) data, so we use Reddit volume as the
# closest social-signal proxy. update.py's score.py applies these
# weights when the new scoring mode is active.

MOMENTUM_V2_WEIGHTS = {
    "youtube": 0.8,
    "social":  0.2,   # X in V1, Reddit-as-proxy in V2
}
