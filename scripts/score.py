"""
MoviePass Hype Index V2 — AMSI scoring
======================================

Implements the Animoca Movie Sentiment Index from
HypeIndex_V2_Spec.md §3:

    AMSI Score = (
        YouTube Views Score    × 0.30 +
        YouTube Engagement     × 0.15 +
        Reddit Volume Score    × 0.20 +
        Google Trends Score    × 0.20 +
        News Impact Score      × 0.15
    ) × 1000

Sub-scores normalize against the top performer in the current batch
(top = 1.0) so the leaderboard is always relative to today's slate.

Input  → list[dict] of raw movie data from scripts/fetch_data.py
Output → same list with `scores`, `score`, `sentiment_pct` fields populated
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List

LOG = logging.getLogger("score")

WEIGHTS = {
    "youtube_views":      0.30,
    "youtube_engagement": 0.15,
    "reddit_volume":      0.20,
    "google_trends":      0.20,
    "news_impact":        0.15,
}


# ---------------------------------------------------------------------------
# Sub-score helpers
# ---------------------------------------------------------------------------

def _normalize(values: List[float]) -> List[float]:
    """Scale a list to 0..1 against its max. All-zero stays all-zero."""
    if not values:
        return []
    peak = max(values)
    if peak <= 0:
        return [0.0] * len(values)
    return [v / peak for v in values]


def _youtube_views(movies: List[Dict[str, Any]]) -> List[float]:
    raw = [float((m.get("youtube") or {}).get("views", 0)) for m in movies]
    return _normalize(raw)


def _youtube_engagement(movies: List[Dict[str, Any]]) -> List[float]:
    """(likes + comments) / views, then normalized."""
    raw: List[float] = []
    for m in movies:
        yt = m.get("youtube") or {}
        views = float(yt.get("views", 0))
        if views <= 0:
            raw.append(0.0)
        else:
            raw.append((float(yt.get("likes", 0)) + float(yt.get("comments", 0))) / views)
    return _normalize(raw)


def _reddit_volume(movies: List[Dict[str, Any]]) -> List[float]:
    """posts + comments, normalized."""
    raw = [
        float((m.get("reddit") or {}).get("posts", 0)) +
        float((m.get("reddit") or {}).get("comments", 0))
        for m in movies
    ]
    return _normalize(raw)


def _google_trends(movies: List[Dict[str, Any]]) -> List[float]:
    """pytrends already returns 0-100 → divide by 100."""
    return [float(m.get("trends", 0)) / 100.0 for m in movies]


def _news_impact(movies: List[Dict[str, Any]],
                 outlet_weights: Dict[str, float]) -> List[float]:
    """
    Sum of (count × tier weight) per outlet for headlines mentioning the title
    in the last ~48h. Normalized against the top performer.
    """
    raw: List[float] = []
    for m in movies:
        score = 0.0
        for mention in m.get("news_mentions", []) or []:
            outlet = mention.get("source", "")
            score += outlet_weights.get(outlet, 0.3)  # blogs default 0.3
        raw.append(score)
    return _normalize(raw)


def _sentiment_pct(movie: Dict[str, Any]) -> int:
    """
    Lightweight stand-in for full LLM sentiment (Claude integration is Phase 2).
    Uses YouTube engagement as a directional positive-sentiment proxy and
    boosts when news + reddit volume agree. Output is clamped to 30..95
    so the bar in the UI stays meaningful even before sentiment.py lands.
    """
    yt = movie.get("youtube") or {}
    views = float(yt.get("views", 0))
    likes = float(yt.get("likes", 0))
    if views > 0:
        like_ratio = likes / views          # typical 0.005 .. 0.04
        base = 50 + (like_ratio * 1500)     # → ~50 .. ~110
    else:
        base = 50
    # News + reddit boost (caps at +10)
    activity = (
        len(movie.get("news_mentions", []) or []) +
        ((movie.get("reddit") or {}).get("posts", 0) / 10)
    )
    base += min(activity, 10)
    return max(30, min(95, int(round(base))))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def score_movies(movies: List[Dict[str, Any]],
                 outlet_weights: Dict[str, float] | None = None) -> List[Dict[str, Any]]:
    """
    Mutates and returns `movies` with new fields:
        scores: {"1d": int, "7d": int, "30d": int}   ← see note
        score:  int (alias of scores["7d"])
        sentiment_pct: int 0..100
        sub_scores: {...} for debugging / detail page

    NOTE on time windows: this scorer only has a single live snapshot of raw
    data, so all three windows are seeded with the same value on first run.
    update.py back-fills the 1d / 7d / 30d windows from historical snapshots
    once they exist on disk.
    """
    if not movies:
        return movies

    # Defensive date filter — last 90 days through upcoming. fetch_data.py
    # also filters at fetch time, but we re-apply here so no caller can
    # accidentally inject back-catalog films into the rankings.
    try:
        from fetch_data import filter_by_release_window  # local import to avoid hard dep
        before = len(movies)
        movies = filter_by_release_window(movies, window_days=90)
        if len(movies) != before:
            LOG.info("score.py defensive filter: %d → %d movies", before, len(movies))
    except Exception as exc:  # noqa: BLE001
        LOG.debug("Skipping defensive date filter: %s", exc)

    if not movies:
        return movies

    weights = outlet_weights or {}

    yt_views = _youtube_views(movies)
    yt_eng   = _youtube_engagement(movies)
    rd_vol   = _reddit_volume(movies)
    gt       = _google_trends(movies)
    nis      = _news_impact(movies, weights)

    # Pass 1: compute raw weighted aggregates in [0..1]
    raw_aggregates: List[float] = []
    for i in range(len(movies)):
        raw = (
            yt_views[i] * WEIGHTS["youtube_views"]      +
            yt_eng[i]   * WEIGHTS["youtube_engagement"] +
            rd_vol[i]   * WEIGHTS["reddit_volume"]      +
            gt[i]       * WEIGHTS["google_trends"]      +
            nis[i]      * WEIGHTS["news_impact"]
        )
        raw_aggregates.append(raw)

    # Pass 2: rescale into V1's familiar 800-999 range. Top performer = 999,
    # bottom = 800. Floor of 800 gives the dashboard the "live exchange" feel
    # of an active leaderboard rather than a sparse 0-1000 scale where most
    # rows clump near zero.
    LO, HI = 800, 999
    raw_min = min(raw_aggregates)
    raw_max = max(raw_aggregates)
    raw_span = raw_max - raw_min
    def _rescale(raw: float) -> int:
        if raw_span <= 0:
            return HI  # all tied → everyone gets the top
        return int(round(LO + ((raw - raw_min) / raw_span) * (HI - LO)))

    for i, m in enumerate(movies):
        sub = {
            "youtube_views":      round(yt_views[i], 4),
            "youtube_engagement": round(yt_eng[i],   4),
            "reddit_volume":      round(rd_vol[i],   4),
            "google_trends":      round(gt[i],       4),
            "news_impact":        round(nis[i],      4),
            "raw_amsi":           round(raw_aggregates[i], 4),
        }
        amsi_int = _rescale(raw_aggregates[i])

        m["sub_scores"]    = sub
        m["score"]         = amsi_int
        m["scores"]        = {"1d": amsi_int, "7d": amsi_int, "30d": amsi_int}
        m["sentiment_pct"] = _sentiment_pct(m)

    return movies


# ---------------------------------------------------------------------------
# CLI — score a previously cached raw fetch
# ---------------------------------------------------------------------------

def main() -> int:
    import argparse, json, sys
    from pathlib import Path

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--in",  dest="inp", type=Path,
                        default=Path(__file__).resolve().parent.parent / "data" / "cache" / "raw.json")
    parser.add_argument("--config", type=Path,
                        default=Path(__file__).resolve().parent.parent / "config.json")
    args = parser.parse_args()

    raw = json.loads(args.inp.read_text())
    cfg = json.loads(args.config.read_text())
    scored = score_movies(raw["movies"], cfg.get("outlet_tier_weights", {}))

    LOG.info("Top 10 by AMSI:")
    for m in sorted(scored, key=lambda x: x["score"], reverse=True)[:10]:
        LOG.info("  %4d  %s", m["score"], m.get("title"))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
