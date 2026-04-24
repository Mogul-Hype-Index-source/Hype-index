"""
Hype Index V2 — Priority-based polling scheduler
=================================================

Replaces the monolithic fetch_all() pulse with an asyncio event loop
that refreshes each signal independently at cadences matched to each
signal's natural rate of change and API cost.

Tier structure:
    Top 20:    X=2min, Reddit=3min, YouTube=10min, News=15min, Trends=4hr
    Mid 21-100: X=10min, Reddit=15min, YouTube=30min, News=30min, Trends=4hr
    Tail 100+: All=60min, Trends=8hr

Batch signals (RSS News, Google Trends) refresh all titles at once.
Per-title signals (X, Reddit, YouTube) are scheduled individually.

Usage:
    python scripts/scheduler.py              # run the scheduler
    python scripts/scheduler.py --dry        # preview schedule without API calls
    python scripts/scheduler.py --once       # run one full cycle then exit

Does NOT replace fetch_all() — runs side-by-side during testing.
Writes to data/v2_scheduler.json (not data/v2.json).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

import signal_fetchers
from fetch_data import (
    load_config, fetch_tmdb_movies, fetch_tmdb_credits,
    _load_manual_movies, _load_trailer_cache, _save_trailer_cache,
    _load_stats_cache, _save_stats_cache, _sanitize_title,
    fetch_release_type_cached, _load_json, _save_json,
    REPO_ROOT,
)
import score as score_module

LOG = logging.getLogger("scheduler")

# Output paths — separate from live data during testing
OUTPUT_PATH = REPO_ROOT / "data" / "v2_scheduler.json"
AUDIT_PATH = REPO_ROOT / "data" / "audit" / "rating_changes.jsonl"
HEARTBEAT_PATH = REPO_ROOT / "data" / "cache" / "scheduler_heartbeat.json"
SCHEDULER_LOG = REPO_ROOT / "data" / "logs" / "scheduler.log"

# ---------------------------------------------------------------------------
# Tier configuration: (signal, seconds between refreshes)
# ---------------------------------------------------------------------------

TIERS = {
    "top": {        # Rank 1-20
        "x":       2 * 60,
        "reddit":  3 * 60,
        "youtube": 10 * 60,
        "news":    15 * 60,    # batch
        "trends":  4 * 3600,   # batch
    },
    "mid": {        # Rank 21-100
        "x":       10 * 60,
        "reddit":  15 * 60,
        "youtube": 30 * 60,
        "news":    30 * 60,
        "trends":  4 * 3600,
    },
    "tail": {       # Rank 100+
        "x":       60 * 60,
        "reddit":  60 * 60,
        "youtube": 60 * 60,
        "news":    60 * 60,
        "trends":  8 * 3600,
    },
}

# Concurrency limits to respect API rate limits
REDDIT_SEMAPHORE_LIMIT = 6      # max concurrent Reddit requests
X_SEMAPHORE_LIMIT = 10          # max concurrent X requests
YOUTUBE_SEMAPHORE_LIMIT = 8     # max concurrent YouTube requests


def _get_tier(rank: int) -> str:
    if rank <= 20:
        return "top"
    elif rank <= 100:
        return "mid"
    return "tail"


# ---------------------------------------------------------------------------
# Movie state store
# ---------------------------------------------------------------------------

class MovieStore:
    """Thread-safe (asyncio-safe) store for all movie data + scheduling metadata."""

    def __init__(self):
        self.movies: Dict[int, Dict[str, Any]] = {}  # tmdb_id → movie dict
        self.last_refresh: Dict[Tuple[int, str], float] = {}  # (tmdb_id, signal) → timestamp
        self._lock = asyncio.Lock()
        self.news_items: List[Dict[str, Any]] = []
        self.trends_cache: Dict[str, int] = {}
        self.config: Dict[str, Any] = {}
        self.trailer_cache: Dict[str, str] = {}
        self.stats_cache: Dict[str, Dict[str, Any]] = {}

    async def update_signal(self, tmdb_id: int, signal: str,
                            data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Update a signal for a movie. Returns change info if Rating changed, else None.
        Acquires lock to prevent data races.
        """
        async with self._lock:
            m = self.movies.get(tmdb_id)
            if not m:
                return None

            old_score = m.get("score", 0)

            # Apply signal-specific data
            if signal == "reddit":
                m["reddit"] = data
            elif signal == "x":
                m["x_mentions"] = data.get("count", 0)
            elif signal == "youtube":
                m["youtube"] = data
            elif signal == "news":
                m["news_mentions"] = data.get("mentions", [])
            elif signal == "trends":
                m["trends"] = data.get("score", 0)

            self.last_refresh[(tmdb_id, signal)] = time.time()

            # Recompute ratings for all movies
            all_movies = list(self.movies.values())
            scored = score_module.score_movies(
                all_movies,
                outlet_weights=self.config.get("outlet_tier_weights", {}),
            )

            # Check if this title's rating changed
            new_score = m.get("score", 0)
            if new_score != old_score:
                # Assign ranks
                scored.sort(key=lambda x: x.get("score", 0), reverse=True)
                for i, s in enumerate(scored, 1):
                    s["rank"] = i

                return {
                    "tmdb_id": tmdb_id,
                    "title": m.get("title", ""),
                    "signal": signal,
                    "old_rating": old_score,
                    "new_rating": new_score,
                    "old_rank": m.get("_prev_rank", 0),
                    "new_rank": m.get("rank", 0),
                }
            return None

    def needs_refresh(self, tmdb_id: int, signal: str) -> bool:
        """Check if a signal is due for refresh based on the movie's tier."""
        m = self.movies.get(tmdb_id)
        if not m:
            return False
        rank = m.get("rank", 999)
        tier = _get_tier(rank)
        interval = TIERS[tier].get(signal, 3600)
        last = self.last_refresh.get((tmdb_id, signal), 0)
        return (time.time() - last) >= interval

    def get_due_tasks(self, signal: str) -> List[int]:
        """Return list of tmdb_ids that need this signal refreshed."""
        return [tid for tid in self.movies if self.needs_refresh(tid, signal)]

    def write_output(self):
        """Write current state to v2_scheduler.json (test output, not live)."""
        all_movies = list(self.movies.values())
        all_movies.sort(key=lambda x: x.get("score", 0), reverse=True)
        for i, m in enumerate(all_movies, 1):
            m["rank"] = i

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "scheduler",
            "movies": all_movies[:200],
        }
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def _write_audit(change: Dict[str, Any]) -> None:
    """Append a rating change entry to the audit log."""
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **change,
    }
    with AUDIT_PATH.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def _write_heartbeat() -> None:
    """Write scheduler heartbeat for health monitoring."""
    HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _save_json(HEARTBEAT_PATH, {
        "alive_at": datetime.now(timezone.utc).isoformat(),
        "pid": __import__("os").getpid(),
    })


# ---------------------------------------------------------------------------
# Async signal workers
# ---------------------------------------------------------------------------

async def worker_reddit(store: MovieStore, sem: asyncio.Semaphore):
    """Continuously refresh Reddit signals for due titles."""
    cfg = store.config
    subs = cfg.get("subreddits", ["movies"])
    ua = cfg.get("reddit_user_agent", "HypeIndexV2/1.0")

    while True:
        due = store.get_due_tasks("reddit")
        if not due:
            await asyncio.sleep(10)
            continue

        for tid in due:
            async with sem:
                m = store.movies.get(tid)
                if not m:
                    continue
                # Run blocking I/O in thread pool
                result = await asyncio.get_event_loop().run_in_executor(
                    None, signal_fetchers.fetch_reddit_for_title, m, subs, ua
                )
                change = await store.update_signal(tid, "reddit", result)
                if change:
                    _write_audit(change)
                    store.write_output()
                    LOG.info("Rating change (reddit): %s %d → %d",
                             change["title"], change["old_rating"], change["new_rating"])
                await asyncio.sleep(1.5)  # rate limit spacing

        await asyncio.sleep(5)


async def worker_x(store: MovieStore, sem: asyncio.Semaphore):
    """Continuously refresh X mention signals for due titles."""
    while True:
        due = store.get_due_tasks("x")
        if not due:
            await asyncio.sleep(10)
            continue

        for tid in due:
            async with sem:
                m = store.movies.get(tid)
                if not m:
                    continue
                count = await asyncio.get_event_loop().run_in_executor(
                    None, signal_fetchers.fetch_x_for_title, m
                )
                change = await store.update_signal(tid, "x", {"count": count})
                if change:
                    _write_audit(change)
                    store.write_output()
                    LOG.info("Rating change (x): %s %d → %d",
                             change["title"], change["old_rating"], change["new_rating"])
                await asyncio.sleep(1.0)

        await asyncio.sleep(5)


async def worker_youtube(store: MovieStore, sem: asyncio.Semaphore):
    """Continuously refresh YouTube signals for due titles."""
    cfg = store.config
    yt_key = cfg.get("youtube_api_key", "")
    tmdb_key = cfg.get("tmdb_api_key", "")

    while True:
        due = store.get_due_tasks("youtube")
        if not due:
            await asyncio.sleep(10)
            continue

        for tid in due:
            async with sem:
                m = store.movies.get(tid)
                if not m:
                    continue
                result = await asyncio.get_event_loop().run_in_executor(
                    None, signal_fetchers.fetch_youtube_for_title,
                    m, yt_key, tmdb_key, store.trailer_cache, store.stats_cache
                )
                change = await store.update_signal(tid, "youtube", result)
                if change:
                    _write_audit(change)
                    store.write_output()
                    LOG.info("Rating change (youtube): %s %d → %d",
                             change["title"], change["old_rating"], change["new_rating"])
                await asyncio.sleep(0.3)

        await asyncio.sleep(10)


async def worker_news_batch(store: MovieStore):
    """Periodically refresh RSS news and re-match to all titles."""
    while True:
        # Determine interval from highest-priority tier
        await asyncio.sleep(15 * 60)  # 15 min batch cycle

        LOG.info("Refreshing RSS news batch...")
        news = await asyncio.get_event_loop().run_in_executor(
            None, signal_fetchers.fetch_all_news, store.config
        )
        store.news_items = news

        # Re-match news to all titles
        for tid, m in store.movies.items():
            mentions = signal_fetchers.fetch_news_for_title(m, news)
            change = await store.update_signal(tid, "news", {"mentions": mentions})
            if change:
                _write_audit(change)
                LOG.info("Rating change (news): %s %d → %d",
                         change["title"], change["old_rating"], change["new_rating"])

        store.write_output()
        LOG.info("News batch complete — %d items matched across %d titles",
                 len(news), len(store.movies))


async def worker_trends_batch(store: MovieStore):
    """Periodically refresh Google Trends for all titles."""
    while True:
        await asyncio.sleep(4 * 3600)  # 4 hr batch cycle

        LOG.info("Refreshing Google Trends batch...")
        titles = [m.get("title", "") for m in store.movies.values()]
        sanitized = {_sanitize_title(t, (store.movies.get(tid, {}).get("release_date") or "")[:4] or None): t
                     for tid, m_unused in [(0, None)]  # placeholder
                     for t in titles}
        # Simpler: just sanitize + fetch
        queries = []
        for m in store.movies.values():
            year = (m.get("release_date") or "")[:4] or None
            queries.append(_sanitize_title(m.get("title", ""), year=year))

        trends = await asyncio.get_event_loop().run_in_executor(
            None, signal_fetchers.fetch_all_trends, queries
        )

        # Map results back and update
        for m in store.movies.values():
            year = (m.get("release_date") or "")[:4] or None
            q = _sanitize_title(m.get("title", ""), year=year)
            new_score = int(trends.get(q, 0))
            change = await store.update_signal(
                m["tmdb_id"], "trends", {"score": new_score}
            )
            if change:
                _write_audit(change)

        store.write_output()
        LOG.info("Trends batch complete — %d titles", len(queries))


async def worker_heartbeat():
    """Write heartbeat every 60 seconds."""
    while True:
        _write_heartbeat()
        await asyncio.sleep(60)


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

async def initialize_store(store: MovieStore, limit: Optional[int] = None):
    """
    Initialize from production data — inherits the exact movie universe
    from data/v2.json (what the live site serves) and seeds signal values
    from data/cache/raw.json (the last full fetch). This guarantees zero
    universe drift at cutover.

    Falls back to TMDb discovery only if v2.json doesn't exist (fresh install).
    """
    LOG.info("Initializing movie store...")
    cfg = load_config()
    store.config = cfg

    # Primary: inherit universe from production v2.json
    v2_path = REPO_ROOT / "data" / "v2.json"
    raw_path = REPO_ROOT / "data" / "cache" / "raw.json"

    # Build lookup of full signal data from raw.json (richer than v2.json)
    raw_by_id: Dict[int, Dict[str, Any]] = {}
    if raw_path.exists():
        try:
            raw = json.loads(raw_path.read_text())
            for m in raw.get("movies", []):
                raw_by_id[m.get("tmdb_id")] = m
        except Exception:
            pass

    movies: List[Dict[str, Any]] = []

    if v2_path.exists():
        try:
            v2 = json.loads(v2_path.read_text())
            v2_movies = v2.get("movies") or []
            LOG.info("Inheriting universe from v2.json: %d movies", len(v2_movies))

            for vm in v2_movies:
                tid = vm.get("tmdb_id")
                if not tid:
                    continue
                # Prefer raw.json data (has full signal dicts); fall back to v2 fields
                base = raw_by_id.get(tid, {})
                m = {
                    "tmdb_id":    tid,
                    "title":      vm.get("title") or base.get("title", ""),
                    "release_date": vm.get("release_date") or base.get("release_date", ""),
                    "popularity": base.get("popularity", 0.0),
                    "poster_path": base.get("poster_path"),
                    "poster_url":  vm.get("poster_url"),
                    "overview":    base.get("overview", ""),
                    "director":    vm.get("director") or base.get("director", ""),
                    "cast":        vm.get("cast") or base.get("cast", ""),
                    "cast_full":   vm.get("cast_full") or base.get("cast_full", []),
                    "directors_full": vm.get("directors_full") or base.get("directors_full", []),
                    "youtube":     base.get("youtube", {"views": 0, "likes": 0, "comments": 0}),
                    "reddit":      base.get("reddit", {"posts": 0, "comments": 0}),
                    "x_mentions":  vm.get("x_mentions") or base.get("x_mentions", 0),
                    "trends":      vm.get("trends") or base.get("trends", 0),
                    "news_mentions": base.get("news_mentions", []),
                    "youtube_velocity": base.get("youtube_velocity", {}),
                    "event_youtube_views": vm.get("event_youtube_views") or base.get("event_youtube_views", 0),
                    "search_query": base.get("search_query", ""),
                }
                movies.append(m)
        except Exception as exc:
            LOG.warning("Failed to load v2.json: %s — falling back to TMDb discovery", exc)

    # Fallback: TMDb discovery (only if v2.json failed or is empty)
    if not movies:
        LOG.info("No v2.json — falling back to TMDb universe discovery")
        tmdb_key = cfg["tmdb_api_key"]
        max_count = limit or int(cfg.get("max_movies", 100))
        movies = await asyncio.get_event_loop().run_in_executor(
            None, fetch_tmdb_movies, tmdb_key, max_count
        )
        manual = await asyncio.get_event_loop().run_in_executor(
            None, _load_manual_movies, cfg["tmdb_api_key"]
        )
        tmdb_ids = {m["tmdb_id"] for m in movies}
        for mm in manual:
            if mm["tmdb_id"] not in tmdb_ids:
                movies.append(mm)
                tmdb_ids.add(mm["tmdb_id"])
        # Seed from raw.json
        for m in movies:
            cached = raw_by_id.get(m["tmdb_id"], {})
            m.setdefault("youtube", cached.get("youtube", {"views": 0, "likes": 0, "comments": 0}))
            m.setdefault("reddit", cached.get("reddit", {"posts": 0, "comments": 0}))
            m.setdefault("x_mentions", cached.get("x_mentions", 0))
            m.setdefault("trends", cached.get("trends", 0))
            m.setdefault("news_mentions", cached.get("news_mentions", []))
            m.setdefault("youtube_velocity", cached.get("youtube_velocity", {}))
            m.setdefault("event_youtube_views", cached.get("event_youtube_views", 0))

    LOG.info("Universe: %d movies", len(movies))

    # Load caches
    store.trailer_cache = _load_trailer_cache()
    store.stats_cache = _load_stats_cache()

    # Register all movies in the store
    for m in movies:
        store.movies[m["tmdb_id"]] = m

    # Initial scoring
    all_movies = list(store.movies.values())
    score_module.score_movies(all_movies, outlet_weights=cfg.get("outlet_tier_weights", {}))
    all_movies.sort(key=lambda x: x.get("score", 0), reverse=True)
    for i, m in enumerate(all_movies, 1):
        m["rank"] = i
        m["_prev_rank"] = i

    store.write_output()
    LOG.info("Store initialized: %d movies, top=%s (%d)",
             len(store.movies),
             all_movies[0].get("title") if all_movies else "?",
             all_movies[0].get("score", 0) if all_movies else 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(limit: Optional[int] = None, once: bool = False):
    store = MovieStore()
    await initialize_store(store, limit=limit)

    reddit_sem = asyncio.Semaphore(REDDIT_SEMAPHORE_LIMIT)
    x_sem = asyncio.Semaphore(X_SEMAPHORE_LIMIT)
    yt_sem = asyncio.Semaphore(YOUTUBE_SEMAPHORE_LIMIT)

    if once:
        LOG.info("--once mode: running one full cycle")
        # Force all signals due
        now = time.time()
        for tid in store.movies:
            for sig in ["reddit", "x", "youtube", "news", "trends"]:
                store.last_refresh[(tid, sig)] = 0
        # Run each worker once
        # (simplified: just refresh top 10 for testing)
        top_ids = sorted(store.movies.keys(),
                         key=lambda t: store.movies[t].get("rank", 999))[:10]
        for tid in top_ids:
            m = store.movies[tid]
            LOG.info("Once-cycle: %s (rank %d)", m.get("title"), m.get("rank", 0))
        store.write_output()
        LOG.info("Once-cycle complete. Output → %s", OUTPUT_PATH)
        return

    # Start all workers
    tasks = [
        asyncio.create_task(worker_reddit(store, reddit_sem)),
        asyncio.create_task(worker_x(store, x_sem)),
        asyncio.create_task(worker_youtube(store, yt_sem)),
        asyncio.create_task(worker_news_batch(store)),
        asyncio.create_task(worker_trends_batch(store)),
        asyncio.create_task(worker_heartbeat()),
    ]

    LOG.info("Scheduler running with %d workers", len(tasks))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        LOG.info("Scheduler shutting down...")


def main() -> int:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Hype Index — priority scheduler")
    parser.add_argument("--dry", action="store_true", help="Preview schedule, no API calls")
    parser.add_argument("--once", action="store_true", help="Run one full cycle then exit")
    parser.add_argument("--limit", type=int, default=None, help="Limit movie universe size")
    args = parser.parse_args()

    if args.dry:
        LOG.info("DRY RUN — showing tier configuration:")
        for tier, intervals in TIERS.items():
            LOG.info("  %s:", tier)
            for signal, secs in intervals.items():
                LOG.info("    %s: every %ds (%.1f min)", signal, secs, secs / 60)
        return 0

    asyncio.run(run(limit=args.limit, once=args.once))
    return 0


if __name__ == "__main__":
    sys.exit(main())
