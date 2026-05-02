"""
MoviePass Hype Index V2 — Theatrical Attention Model
=====================================================

Hype Score = 0.50 × Altitude + 0.30 × Velocity + 0.20 × Consensus

    HYPE_SCORE = (
        0.4 × normalize(short) +
        0.2 × normalize(acceleration) +
        0.4 × normalize(baseline)
    ) × log(1 + baseline) × 1000

Where baseline is the weighted sum of source scores normalized
against the top performer in the current batch:

    baseline = (
        youtube_velocity × 0.35 +
        x_mentions       × 0.25 +
        reddit_volume    × 0.20 +
        google_trends    × 0.15 +
        news_impact      × 0.05
    )

Definitions:
    baseline:      weighted sum of all source scores (0..1 each)
    short:         average of last 3 data points (baseline values)
    acceleration:  rate of change between short and previous short window

Input  → list[dict] of raw movie data from scripts/fetch_data.py
Output → same list with `scores`, `score`, `sentiment_pct` fields populated
"""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Any, Dict, List

LOG = logging.getLogger("score")

# Hype Score = 0.50 × Altitude + 0.30 × Velocity + 0.20 × Consensus
# Altitude weights: YT 0.35, X 0.30, Trends 0.25, News 0.10
# Velocity weights: ΔX 0.40, ΔTrends 0.30, ΔYT 0.20, ΔNews 0.10
WEIGHTS = {
    "youtube_views":  0.35,
    "x_mentions":     0.30,
    "google_trends":  0.25,
    "news_impact":    0.10,
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
    """
    Three-component YouTube view scoring:
      40% rolling 7d average velocity (trailer)
      20% 24h delta × spike multiplier (trailer)
      40% event YouTube search views (CinemaCon etc.)
    Event component is zero when no event data exists.
    """
    raw_7d: List[float] = []
    raw_24h: List[float] = []
    raw_event: List[float] = []
    multipliers: List[float] = []
    for m in movies:
        vel = m.get("youtube_velocity") or {}
        raw_7d.append(float(vel.get("views_7d_avg", 0)))
        raw_24h.append(float(vel.get("views_24h", 0)))
        ev = float(m.get("event_youtube_views", 0))
        raw_event.append(ev)
        # Spike multiplier: use velocity spike, or event spike if >50K
        spike = float(vel.get("spike_multiplier", 1.0))
        if ev > 50000:
            spike = max(spike, 2.0)
        multipliers.append(spike)

    norm_7d = _normalize(raw_7d)
    norm_24h = _normalize(raw_24h)
    norm_event = _normalize(raw_event)

    has_event = any(v > 0 for v in raw_event)

    if has_event:
        return [
            0.4 * norm_7d[i] + 0.2 * norm_24h[i] * multipliers[i] + 0.4 * norm_event[i]
            for i in range(len(movies))
        ]
    return [
        0.6 * norm_7d[i] + 0.4 * norm_24h[i] * multipliers[i]
        for i in range(len(movies))
    ]


def _youtube_engagement(movies: List[Dict[str, Any]]) -> List[float]:
    """
    Engagement velocity: two-component model matching views.
      60% rolling 7d avg engagement rate
      40% 24h engagement rate × spike multiplier
    """
    raw_7d: List[float] = []
    raw_24h: List[float] = []
    multipliers: List[float] = []
    for m in movies:
        vel = m.get("youtube_velocity") or {}
        avg_v = float(vel.get("views_7d_avg", 0))
        avg_l = float(vel.get("likes_7d_avg", 0))
        avg_c = float(vel.get("comments_7d_avg", 0))
        d_v = float(vel.get("views_24h", 0))
        d_l = float(vel.get("likes_24h", 0))
        d_c = float(vel.get("comments_24h", 0))

        raw_7d.append((avg_l + avg_c) / avg_v if avg_v > 0 else 0.0)
        raw_24h.append((d_l + d_c) / d_v if d_v > 0 else 0.0)
        multipliers.append(float(vel.get("spike_multiplier", 1.0)))

    norm_7d = _normalize(raw_7d)
    norm_24h = _normalize(raw_24h)

    return [
        0.6 * norm_7d[i] + 0.4 * norm_24h[i] * multipliers[i]
        for i in range(len(movies))
    ]


def _google_trends(movies: List[Dict[str, Any]]) -> List[float]:
    """pytrends already returns 0-100 → divide by 100."""
    return [float(m.get("trends", 0)) / 100.0 for m in movies]


def _x_mentions(movies: List[Dict[str, Any]]) -> List[float]:
    """X (Twitter) mention count per movie, normalized."""
    raw = [float(m.get("x_mentions", 0)) for m in movies]
    return _normalize(raw)


def _news_impact(movies: List[Dict[str, Any]],
                 outlet_weights: Dict[str, float]) -> List[float]:
    """
    Sum of (count × tier weight) per outlet for headlines mentioning the title
    in the last ~48h. Event news (is_event=True) gets a 1.5x multiplier.
    Normalized against the top performer.
    """
    raw: List[float] = []
    for m in movies:
        score = 0.0
        for mention in m.get("news_mentions", []) or []:
            outlet = mention.get("source", "")
            base = outlet_weights.get(outlet, 0.3)
            if mention.get("is_event"):
                base *= 1.5
            score += base
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

    # -----------------------------------------------------------------------
    # ALTITUDE — size/capacity of attention (log-scaled, normalized)
    # -----------------------------------------------------------------------
    # Raw signal extraction (log-scaled)
    raw_yt = [math.log(1 + float((m.get("youtube_velocity") or {}).get("views_24h", 0)
                                  or (m.get("youtube") or {}).get("views", 0)))
              for m in movies]
    raw_x  = [math.log(1 + float(m.get("x_mentions", 0))) for m in movies]
    raw_gt = [math.log(1 + float(m.get("trends", 0))) for m in movies]
    raw_ni = [math.log(1 + sum(weights.get(n.get("source", ""), 0.3)
              for n in (m.get("news_mentions") or [])))
              for m in movies]

    # Normalize each signal across the universe (0..1)
    norm_yt = _normalize(raw_yt)
    norm_x  = _normalize(raw_x)
    norm_gt = _normalize(raw_gt)
    norm_ni = _normalize(raw_ni)

    # Altitude = weighted sum
    ALTITUDE_W = {"yt": 0.35, "x": 0.30, "gt": 0.25, "ni": 0.10}
    altitudes: List[float] = []
    for i in range(len(movies)):
        alt = (
            norm_yt[i] * ALTITUDE_W["yt"] +
            norm_x[i]  * ALTITUDE_W["x"]  +
            norm_gt[i] * ALTITUDE_W["gt"] +
            norm_ni[i] * ALTITUDE_W["ni"]
        )
        altitudes.append(alt)

    # -----------------------------------------------------------------------
    # VELOCITY — movement / momentum (deltas normalized by sqrt of Altitude)
    # -----------------------------------------------------------------------
    # Delta computation: compare current signal to previous pulse's value
    # stored in _momentum_history on each movie dict
    VELOCITY_W = {"dx": 0.40, "dgt": 0.30, "dyt": 0.20, "dni": 0.10}
    velocities: List[float] = []

    for i, m in enumerate(movies):
        history = list(m.get("_momentum_history") or [])
        prev = history[-1] if history else {}

        # Compute deltas (current - previous, clamped to 0 minimum)
        dx  = max(0, raw_x[i]  - prev.get("x", 0))
        dgt = max(0, raw_gt[i] - prev.get("gt", 0))
        dyt = max(0, raw_yt[i] - prev.get("yt", 0))
        dni = max(0, raw_ni[i] - prev.get("ni", 0))

        raw_vel = (
            dx  * VELOCITY_W["dx"]  +
            dgt * VELOCITY_W["dgt"] +
            dyt * VELOCITY_W["dyt"] +
            dni * VELOCITY_W["dni"]
        )
        # Normalize by sqrt(altitude) — high-altitude titles need bigger deltas
        vel = raw_vel / math.sqrt(altitudes[i] + 0.01)
        velocities.append(vel)

        # Save current signals for next pulse's delta computation
        m["_momentum_history"] = [{"x": raw_x[i], "gt": raw_gt[i],
                                   "yt": raw_yt[i], "ni": raw_ni[i]}]

    norm_vel = _normalize(velocities)

    # -----------------------------------------------------------------------
    # CONSENSUS — signal breadth (how many sources confirm attention)
    # -----------------------------------------------------------------------
    medians = {
        "yt": sorted(raw_yt)[len(raw_yt) // 2] if raw_yt else 0,
        "x":  sorted(raw_x)[len(raw_x) // 2] if raw_x else 0,
        "gt": sorted(raw_gt)[len(raw_gt) // 2] if raw_gt else 0,
        "ni": sorted(raw_ni)[len(raw_ni) // 2] if raw_ni else 0,
    }

    consensus_vals: List[float] = []
    for i in range(len(movies)):
        active = sum([
            1 if raw_yt[i] > medians["yt"] else 0,
            1 if raw_x[i]  > medians["x"]  else 0,
            1 if raw_gt[i] > medians["gt"] else 0,
            1 if raw_ni[i] > medians["ni"] else 0,
        ])
        consensus_vals.append(active / 4.0)

    # -----------------------------------------------------------------------
    # HYPE SCORE = 0.50 × Altitude + 0.30 × Velocity + 0.20 × Consensus
    # -----------------------------------------------------------------------
    norm_alt = _normalize(altitudes)
    raw_scores: List[float] = []
    for i in range(len(movies)):
        hype = (
            0.50 * norm_alt[i] +
            0.30 * norm_vel[i] +
            0.20 * consensus_vals[i]
        ) * 1000
        raw_scores.append(hype)

    # Pass 4: calibrated Rating (100-1500 absolute scale)
    # Curve: raw^0.50 × 74, capped at 1500. Anchored so:
    #   raw ~295 → ~1270 (today's top), raw ~150 → ~900, raw ~100 → ~740
    def _calibrate(raw: float) -> int:
        if raw <= 0:
            return 0
        return min(1500, int(round(raw ** 0.50 * 74)))

    RATING_BANDS = [
        (1350, "GENERATIONAL"),
        (1150, "ELITE"),
        (900,  "STRONG"),
        (650,  "DECENT"),
        (400,  "MODEST"),
        (250,  "LOW"),
        (100,  "MINIMAL"),
    ]

    def _band(rating: int) -> str:
        for threshold, name in RATING_BANDS:
            if rating >= threshold:
                return name
        return "UNRATED"

    # Load previous smoothed ratings for exponential smoothing
    _smooth_path = Path(__file__).resolve().parent.parent / "data" / "cache" / "rating_smoothed.json"
    prev_smooth: Dict[str, int] = {}
    if _smooth_path.exists():
        try:
            prev_smooth = json.loads(_smooth_path.read_text())
        except Exception:
            pass

    new_smooth: Dict[str, int] = {}

    for i, m in enumerate(movies):
        sub = {
            "altitude":           round(norm_alt[i], 4),
            "velocity":           round(norm_vel[i], 4),
            "consensus":          round(consensus_vals[i], 4),
            "altitude_raw":       round(altitudes[i], 4),
            "velocity_raw":       round(velocities[i], 4),
            "youtube_views":      round(norm_yt[i], 4),
            "x_mentions":         round(norm_x[i], 4),
            "google_trends":      round(norm_gt[i], 4),
            "news_impact":        round(norm_ni[i], 4),
            "raw_hype":           round(raw_scores[i], 4),
        }
        raw_rating = _calibrate(raw_scores[i])

        # Exponential smoothing: displayed = 0.7 × new + 0.3 × previous
        tid_str = str(m.get("tmdb_id", ""))
        prev = prev_smooth.get(tid_str)
        if prev is not None and prev > 0:
            smoothed = int(round(0.7 * raw_rating + 0.3 * prev))
        else:
            smoothed = raw_rating

        # Theatrical lifecycle — boost opening titles, decay post-theatrical
        rd = m.get("release_date") or ""
        try:
            from datetime import datetime as _dt, timezone as _tz
            _rel = _dt.strptime(rd, "%Y-%m-%d").replace(tzinfo=_tz.utc)
            _days_out = (_dt.now(_tz.utc) - _rel).days  # positive = released
            _days_until = -_days_out  # positive = upcoming
        except (ValueError, TypeError):
            _days_out = 0
            _days_until = 0

        # Upcoming boost
        if _days_until > 0 and _days_until <= 30:
            smoothed = int(smoothed * 1.15)  # pre-release peak boost
        elif _days_out > 0 and _days_out <= 14:
            smoothed = int(smoothed * 1.10)  # opening window boost

        # Post-theatrical decay
        if _days_out > 120:
            smoothed = int(smoothed * 0.1)
        elif _days_out > 90:
            smoothed = int(smoothed * 0.3)
        elif _days_out > 60:
            smoothed = int(smoothed * 0.6)

        new_smooth[tid_str] = smoothed

        m["sub_scores"]    = sub
        m["rating"]        = smoothed
        m["rating_raw"]    = raw_rating
        m["rating_band"]   = _band(smoothed)
        m["score"]         = smoothed  # backward compat
        m["days_since_release"] = _days_out if _days_out > 0 else 0
        m["scores"]        = {"1d": smoothed, "7d": smoothed, "30d": smoothed}
        m["sentiment_pct"] = _sentiment_pct(m)
        # Clean up internal field (keep history for next pulse's velocity)
        if "_momentum_history" in m:
            del m["_momentum_history"]

    # Persist smoothed ratings for next pulse
    try:
        _smooth_path.parent.mkdir(parents=True, exist_ok=True)
        _smooth_path.write_text(json.dumps(new_smooth))
    except Exception:
        pass

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

    LOG.info("Top 10 by HypeScore:")
    for m in sorted(scored, key=lambda x: x["score"], reverse=True)[:10]:
        LOG.info("  %4d  %s", m["score"], m.get("title"))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
