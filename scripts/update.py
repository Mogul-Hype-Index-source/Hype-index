"""
MoviePass Hype Index V2 — master orchestrator
=============================================

Run this every hour. It does the following:

  1. Loads config.json
  2. Calls fetch_data.fetch_all()  → raw signal from all 5 sources
  3. Calls score.score_movies()    → HypeScore 0-1000 per movie
  4. Loads yesterday's snapshot from data/historical/ to compute:
        • rank movement   (current rank vs 24h ago)
        • is_new flag     (not present in yesterday's index)
        • hot flag        (rising 3 days in a row)
  5. Back-fills the 1d / 7d / 30d windows from historical snapshots
     so the SNAPSHOT toggle in the UI shows real momentum
  6. Assembles data/index.json (the file the frontend reads)
  7. Writes data/historical/YYYY-MM-DD.json snapshot
  8. Writes a tiny run log to data/cache/last_run.json

Run modes:
    python scripts/update.py                # one-shot, current hour
    python scripts/update.py --loop         # run forever, every 60 min
    python scripts/update.py --limit 20     # quick smoke test
    python scripts/update.py --skip-fetch   # re-score from cached raw.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow `python scripts/update.py` to import siblings
sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_data  # noqa: E402
import score       # noqa: E402

LOG = logging.getLogger("update")

REPO_ROOT  = Path(__file__).resolve().parent.parent
DATA_DIR   = REPO_ROOT / "data"
HIST_DIR   = DATA_DIR / "historical"
CACHE_DIR  = DATA_DIR / "cache"
# NOTE: written to data/v2.json (not data/index.json) because the legacy V1
# hourly cron still pushes to this repo and overwrites data/index.json with
# its own schema. data/v2.json is exclusively V2 territory.
INDEX_PATH = DATA_DIR / "v2.json"
RAW_CACHE  = CACHE_DIR / "raw.json"
VIEWS_HIST = HIST_DIR / "views"


# ---------------------------------------------------------------------------
# YouTube velocity helpers
# ---------------------------------------------------------------------------

def _save_view_snapshot(movies: List[Dict[str, Any]], today_iso: str) -> None:
    """Persist current YouTube stats keyed by tmdb_id for velocity calculation."""
    VIEWS_HIST.mkdir(parents=True, exist_ok=True)
    snap: Dict[str, Dict[str, int]] = {}
    for m in movies:
        tid = str(m.get("tmdb_id", ""))
        yt = m.get("youtube") or {}
        snap[tid] = {
            "views": int(yt.get("views", 0)),
            "likes": int(yt.get("likes", 0)),
            "comments": int(yt.get("comments", 0)),
        }
    path = VIEWS_HIST / f"{today_iso}.json"
    path.write_text(json.dumps(snap, indent=2))
    LOG.info("View snapshot → %s (%d entries)", path, len(snap))


def _load_view_snapshot(date_iso: str) -> Dict[str, Dict[str, int]]:
    """Load a historical views snapshot. Returns {} if not found."""
    p = VIEWS_HIST / f"{date_iso}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _enrich_youtube_velocity(movies: List[Dict[str, Any]], today: datetime) -> None:
    """
    Compute YouTube velocity (daily delta) for each movie.
    Mutates movies in place, adding:
        youtube_velocity: {views_24h, likes_24h, comments_24h}
        views_today: int (alias for views_24h)
        views_trend: "accelerating" | "decelerating" | "flat"
    Falls back to 7d average, then discounted total views.
    """
    yesterday = (today - timedelta(days=1)).date().isoformat()
    week_ago  = (today - timedelta(days=7)).date().isoformat()
    two_days  = (today - timedelta(days=2)).date().isoformat()

    snap_1d = _load_view_snapshot(yesterday)
    snap_7d = _load_view_snapshot(week_ago)
    snap_2d = _load_view_snapshot(two_days)

    for m in movies:
        tid = str(m.get("tmdb_id", ""))
        yt = m.get("youtube") or {}
        curr_views = int(yt.get("views", 0))
        curr_likes = int(yt.get("likes", 0))
        curr_comments = int(yt.get("comments", 0))

        prev_1d = snap_1d.get(tid)
        prev_7d = snap_7d.get(tid)
        prev_2d = snap_2d.get(tid)

        if prev_1d:
            # 24h delta
            views_24h = max(0, curr_views - prev_1d.get("views", 0))
            likes_24h = max(0, curr_likes - prev_1d.get("likes", 0))
            comments_24h = max(0, curr_comments - prev_1d.get("comments", 0))
        elif prev_7d:
            # 7d average as fallback
            views_24h = max(0, (curr_views - prev_7d.get("views", 0)) // 7)
            likes_24h = max(0, (curr_likes - prev_7d.get("likes", 0)) // 7)
            comments_24h = max(0, (curr_comments - prev_7d.get("comments", 0)) // 7)
        else:
            # No history: estimate daily velocity from total views and release date
            rd = m.get("release_date") or ""
            try:
                release = datetime.strptime(rd, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                days_out = max(1, (today - release).days)
            except (ValueError, TypeError):
                days_out = 30  # unknown release → assume ~1 month
            # Heavy discount: divide by days since release, cap at 90 days
            days_out = min(days_out, 90)
            views_24h = curr_views // days_out
            likes_24h = curr_likes // days_out
            comments_24h = curr_comments // days_out

        m["youtube_velocity"] = {
            "views_24h": views_24h,
            "likes_24h": likes_24h,
            "comments_24h": comments_24h,
        }
        m["views_today"] = views_24h

        # Trend: compare today's delta to yesterday's delta
        if prev_1d and prev_2d:
            yesterday_delta = max(0, prev_1d.get("views", 0) - prev_2d.get("views", 0))
            if views_24h > yesterday_delta + 1000:
                m["views_trend"] = "accelerating"
            elif views_24h < yesterday_delta - 1000:
                m["views_trend"] = "decelerating"
            else:
                m["views_trend"] = "flat"
        elif views_24h < 1000:
            m["views_trend"] = "flat"
        else:
            m["views_trend"] = "accelerating"  # new entry with signal


# ---------------------------------------------------------------------------
# Historical snapshot helpers
# ---------------------------------------------------------------------------

def _load_snapshot(date_iso: str) -> Optional[Dict[str, Any]]:
    p = HIST_DIR / f"{date_iso}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Could not parse historical %s: %s", p, exc)
        return None


def _load_previous_snapshot(today_iso: str) -> Optional[Dict[str, Any]]:
    """
    Return the live data/v2.json from BEFORE this run, if it exists.
    Used to compute rank movement on a per-pulse basis (so movers populate
    starting from the second pulse, not 24h after first run).
    """
    if not INDEX_PATH.exists():
        return None
    try:
        return json.loads(INDEX_PATH.read_text())
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Could not parse previous %s: %s", INDEX_PATH, exc)
        return None


def _rank_index(snapshot: Optional[Dict[str, Any]]) -> Dict[int, int]:
    """tmdb_id → rank from a historical snapshot."""
    if not snapshot:
        return {}
    return {
        m["tmdb_id"]: m["rank"]
        for m in (snapshot.get("movies") or [])
        if "tmdb_id" in m and "rank" in m
    }


def _score_index(snapshot: Optional[Dict[str, Any]]) -> Dict[int, int]:
    if not snapshot:
        return {}
    return {
        m["tmdb_id"]: m.get("score", 0)
        for m in (snapshot.get("movies") or [])
        if "tmdb_id" in m
    }


def _enrich_with_history(movies: List[Dict[str, Any]],
                         today: datetime) -> Dict[str, Any]:
    """
    Compute move (rank delta vs the previous pulse), is_new, hot, and
    1d/7d/30d windows. Returns a summary count dict.

    Move is computed against the PREVIOUS PULSE (i.e. the live v2.json from
    before this run), not strictly "yesterday's snapshot". This way movers
    populate after the second pulse instead of 24h after first run.
    """
    seven_ago  = (today - timedelta(days=7)).date().isoformat()
    thirty_ago = (today - timedelta(days=30)).date().isoformat()

    snap_prev  = _load_previous_snapshot(today.date().isoformat())
    snap_7d    = _load_snapshot(seven_ago)
    snap_30d   = _load_snapshot(thirty_ago)

    yrank   = _rank_index(snap_prev)
    s7  = _score_index(snap_7d)
    s30 = _score_index(snap_30d)

    # 3-day-rising "hot" detection — look back 3 days
    streak_snaps = [
        _load_snapshot((today - timedelta(days=i)).date().isoformat())
        for i in range(1, 4)
    ]
    streak_scores = [_score_index(s) for s in streak_snaps]

    rising = falling = flat = 0
    for m in movies:
        rank   = m["rank"]
        tid    = m["tmdb_id"]
        prev   = yrank.get(tid)

        if prev is None:
            m["move"]   = 0
            m["is_new"] = True
        else:
            m["move"]   = prev - rank   # positive = climbed
            m["is_new"] = False

        if m["move"] > 0:   rising  += 1
        elif m["move"] < 0: falling += 1
        else:               flat    += 1

        # Time-window scores: backfill from history if available
        today_score = m["score"]
        m["scores"] = {
            "1d":  today_score,
            "7d":  s7.get(tid,  today_score),
            "30d": s30.get(tid, today_score),
        }

        # "Hot" = score went up across each of the last 3 days
        hot = True
        prev_s = today_score
        for sm in streak_scores:
            ps = sm.get(tid)
            if ps is None or ps >= prev_s:
                hot = False
                break
            prev_s = ps
        m["hot"] = hot

    return {"rising": rising, "falling": falling, "flat": flat}


def _enrich_people_with_history(people: List[Dict[str, Any]], key: str) -> None:
    """
    Compute rank movement (move) and is_new flags for actors / directors by
    comparing against the previous v2.json. Mutates `people` in place.
    `key` is "actors" or "directors".
    """
    prev = _load_previous_snapshot("")
    prev_ranks: Dict[int, int] = {}
    if prev:
        for p in (prev.get(key) or []):
            pid = p.get("tmdb_id")
            if pid is not None and "rank" in p:
                prev_ranks[pid] = p["rank"]
    for p in people:
        pid = p.get("tmdb_id")
        prev_rank = prev_ranks.get(pid)
        if prev_rank is None:
            p["move"]   = 0
            p["is_new"] = True
        else:
            p["move"]   = prev_rank - p["rank"]
            p["is_new"] = False


# ---------------------------------------------------------------------------
# Assemble the public index.json the frontend reads
# ---------------------------------------------------------------------------

def build_index_payload(scored: List[Dict[str, Any]],
                        news_items: List[Dict[str, Any]],
                        generated_at: datetime,
                        poster_base: str = "https://image.tmdb.org/t/p/w185",
                        x_counts: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    # Rank by HypeScore
    scored.sort(key=lambda m: m.get("score", 0), reverse=True)
    for i, m in enumerate(scored, 1):
        m["rank"] = i

    # Move / windows / hot / is_new (in place + summary counts)
    summary_counts = _enrich_with_history(scored, generated_at)

    # Highlight #1 in the UI
    if scored:
        scored[0]["highlight"] = True

    # Roll up cast + crew across all movies into the actors / directors tabs.
    # This must run BEFORE we trim cast_full / directors_full off the public
    # movie objects below.
    people = fetch_data.derive_people(
        scored, news_items, poster_base=poster_base,
        top_actors=50, top_directors=25,
        x_counts=x_counts,
    )
    # Per-person rank movement against the previous v2.json
    _enrich_people_with_history(people["actors"],    "actors")
    _enrich_people_with_history(people["directors"], "directors")

    # Movers strip — top 5 up, top 5 down by absolute move
    movers_up = sorted(
        (m for m in scored if (m.get("move") or 0) > 0),
        key=lambda m: m["move"],
        reverse=True,
    )[:5]
    movers_down = sorted(
        (m for m in scored if (m.get("move") or 0) < 0),
        key=lambda m: m["move"],
    )[:5]

    def _trim_mover(m: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "tmdb_id":    m["tmdb_id"],
            "title":      m["title"],
            "poster_url": m.get("poster_url"),
            "move":       m.get("move", 0),
            "score":      m.get("score", 0),
        }

    # Categorized news for the ticker. Build up to 5 items per category
    # (the frontend cycles through them as labeled groups). We pull from a
    # large pool — up to 60 items — so each of the 8 categories has a fair
    # chance of being represented even when the most-recent slice is
    # dominated by one outlet.
    CATEGORY_ORDER = [
        "production", "finance", "international", "creative",
        "pr-marketing", "ai-tech", "festivals", "box-office",
    ]
    PER_CAT = 5
    bucket: Dict[str, List[Dict[str, Any]]] = {c: [] for c in CATEGORY_ORDER}
    for n in news_items[:120]:
        if not n.get("headline"):
            continue
        c = n.get("category") or "production"
        if c not in bucket:
            bucket[c] = []
        if len(bucket[c]) < PER_CAT:
            bucket[c].append({
                "headline": n["headline"],
                "source":   n["source"],
                "url":      n["url"],
                "category": c,
            })

    # Flatten in display order, dropping empty categories so the ticker
    # never shows a label with zero headlines.
    ticker_news: List[Dict[str, Any]] = []
    for c in CATEGORY_ORDER:
        if bucket.get(c):
            ticker_news.extend(bucket[c])

    total_mentions_24h = sum(
        ((m.get("reddit") or {}).get("posts", 0) +
         (m.get("reddit") or {}).get("comments", 0) +
         len(m.get("news_mentions") or []) +
         int(m.get("x_mentions") or 0))
        for m in scored
    )
    avg_score = (
        round(sum(m.get("score", 0) for m in scored) / len(scored)) if scored else 0
    )

    # Trim each movie to the public payload. Detail page (movie.html) needs
    # the cast/director full lists with headshots, so they stay.
    public_movies = []
    for m in scored:
        yt = m.get("youtube") or {}
        rd = m.get("reddit")  or {}
        public_movies.append({
            "rank":           m["rank"],
            "tmdb_id":        m["tmdb_id"],
            "title":          m["title"],
            "director":       m.get("director", ""),
            "cast":           m.get("cast", ""),
            "release_date":   m.get("release_date", ""),
            "poster_url":     m.get("poster_url"),
            "youtube_views":  int(yt.get("views", 0)),
            "youtube_likes":  int(yt.get("likes", 0)),
            "youtube_comments": int(yt.get("comments", 0)),
            "youtube_video_id": m.get("youtube_video_id"),
            "views_today":    int(m.get("views_today") or 0),
            "views_trend":    m.get("views_trend", "flat"),
            "reddit_posts":   int(rd.get("posts", 0)),
            "reddit_comments": int(rd.get("comments", 0)),
            "x_mentions":     int(m.get("x_mentions") or 0),
            "mentions":       int(rd.get("posts", 0) + rd.get("comments", 0) + len(m.get("news_mentions") or []) + int(m.get("x_mentions") or 0)),
            "sentiment_pct":  int(m.get("sentiment_pct", 50)),
            "scores":         m.get("scores", {}),
            "score":          m.get("score", 0),
            "move":           m.get("move", 0),
            "is_new":         bool(m.get("is_new", False)),
            "hot":            bool(m.get("hot", False)),
            "highlight":      bool(m.get("highlight", False)),
            "sub_scores":     m.get("sub_scores", {}),
            "cast_full":      m.get("cast_full") or [],
            "directors_full": m.get("directors_full") or [],
            "news_mentions":  m.get("news_mentions") or [],
            "trends":         int(m.get("trends", 0)),
        })

    return {
        "generated_at":   generated_at.isoformat(),
        "next_update_at": (generated_at + timedelta(hours=1)).isoformat(),
        "snapshot_date":  generated_at.date().isoformat(),
        "summary": {
            "total":              len(public_movies),
            "rising":             summary_counts["rising"],
            "falling":            summary_counts["falling"],
            "flat":               summary_counts["flat"],
            "avg_score":          avg_score,
            "total_mentions_24h": total_mentions_24h,
        },
        "news":        ticker_news,
        "movers_up":   [_trim_mover(m) for m in movers_up],
        "movers_down": [_trim_mover(m) for m in movers_down],
        "movies":      public_movies,
        "actors":      people["actors"],
        "directors":   people["directors"],
    }


# ---------------------------------------------------------------------------
# Quality circuit breaker
# ---------------------------------------------------------------------------

def _quality_ok(new_payload: Dict[str, Any]) -> bool:
    """
    Compare a freshly-built payload against the existing live data/index.json
    and return False if the new fetch is dramatically worse — specifically:

      • The live index had >= 25% of movies with non-zero YouTube views, AND
      • The new payload has < 50% as many movies with non-zero YouTube views.

    This catches API quota exhaustion (the most common failure mode) without
    blocking legitimate updates where the slate has shifted.
    """
    if not INDEX_PATH.exists():
        return True  # nothing to compare against
    try:
        live = json.loads(INDEX_PATH.read_text())
    except Exception:  # noqa: BLE001
        return True

    def yt_coverage(payload: Dict[str, Any]) -> float:
        movies = payload.get("movies") or []
        if not movies:
            return 0.0
        with_views = sum(1 for m in movies if (m.get("youtube_views") or 0) > 0)
        return with_views / len(movies)

    live_cov = yt_coverage(live)
    new_cov  = yt_coverage(new_payload)

    if live_cov >= 0.25 and new_cov < live_cov * 0.5:
        LOG.warning(
            "YouTube coverage dropped: live=%.0f%% → new=%.0f%%",
            live_cov * 100, new_cov * 100,
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Run one cycle
# ---------------------------------------------------------------------------

def run_once(limit: Optional[int] = None, *, skip_fetch: bool = False,
             force: bool = False) -> Path:
    cfg = fetch_data.load_config()
    DATA_DIR.mkdir(exist_ok=True)
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Fetch (or reuse cached raw)
    if skip_fetch and RAW_CACHE.exists():
        LOG.info("--skip-fetch: reusing %s", RAW_CACHE)
        raw = json.loads(RAW_CACHE.read_text())
    else:
        raw = fetch_data.fetch_all(cfg, limit=limit)
        RAW_CACHE.write_text(json.dumps(raw, indent=2))
        LOG.info("Cached raw fetch → %s", RAW_CACHE)

    # 1a. YouTube velocity — compute daily deltas before scoring
    generated_at = datetime.now(timezone.utc).replace(microsecond=0)
    _enrich_youtube_velocity(raw["movies"], generated_at)

    # 2. Score
    scored = score.score_movies(
        raw["movies"],
        outlet_weights=cfg.get("outlet_tier_weights", {}),
    )

    # 2a. Save view snapshot for tomorrow's velocity calculation
    _save_view_snapshot(raw["movies"], generated_at.date().isoformat())
    payload = build_index_payload(
        scored, raw["news"], generated_at,
        poster_base=cfg.get("tmdb_image_base", "https://image.tmdb.org/t/p/w185"),
        x_counts=raw.get("x_counts"),
    )

    # 3a. Circuit breaker — refuse to overwrite the live index.json with
    # obviously degraded data. Specifically, if the previous index had
    # meaningful YouTube view coverage and the new fetch lost it for the
    # majority of movies (e.g. quota exhaustion), keep the old data live
    # and let the next scheduled run try again. This protects the public
    # site from intermittent API failures without needing manual rollback.
    # Pass --force to bypass when shipping a structural change (filter
    # rules, schema, etc.) that the breaker would otherwise reject.
    if force:
        LOG.info("--force: bypassing circuit breaker")
    if not force and INDEX_PATH.exists() and not _quality_ok(payload):
        LOG.warning(
            "Circuit breaker tripped — new fetch is degraded vs the live index.json. "
            "Preserving the existing live data and skipping write."
        )
        # Still cache the raw + write a degraded snapshot for debugging
        debug_path = CACHE_DIR / "degraded_payload.json"
        debug_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        LOG.warning("Degraded payload saved → %s", debug_path)
        return INDEX_PATH

    # 4. Write public index.json (atomic)
    tmp = INDEX_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    tmp.replace(INDEX_PATH)
    LOG.info("Wrote %s (%d movies)", INDEX_PATH, len(payload["movies"]))

    # 5. Snapshot today
    snap_path = HIST_DIR / f"{generated_at.date().isoformat()}.json"
    snap_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    LOG.info("Snapshot → %s", snap_path)

    # 6. Tiny run log
    (CACHE_DIR / "last_run.json").write_text(json.dumps({
        "ran_at":      generated_at.isoformat(),
        "movies":      len(payload["movies"]),
        "news":        len(payload["news"]),
        "rising":      payload["summary"]["rising"],
        "falling":     payload["summary"]["falling"],
        "flat":        payload["summary"]["flat"],
        "top1":        payload["movies"][0]["title"] if payload["movies"] else None,
    }, indent=2))

    return INDEX_PATH


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="MoviePass Hype Index V2 — hourly orchestrator")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap movies fetched (smoke testing)")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Re-score from data/cache/raw.json without re-fetching")
    parser.add_argument("--loop", action="store_true",
                        help="Run forever, sleeping 60 minutes between cycles")
    parser.add_argument("--force", action="store_true",
                        help="Bypass the quality circuit breaker (use when shipping a structural change)")
    args = parser.parse_args()

    while True:
        try:
            run_once(limit=args.limit, skip_fetch=args.skip_fetch, force=args.force)
        except Exception as exc:  # noqa: BLE001
            LOG.exception("Run failed: %s", exc)
            if not args.loop:
                return 1
        if not args.loop:
            return 0
        LOG.info("Sleeping 60 minutes…")
        time.sleep(60 * 60)


if __name__ == "__main__":
    sys.exit(main())
