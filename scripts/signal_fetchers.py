"""
Signal fetchers — per-title API calls decomposed from fetch_data.py
===================================================================

Each function fetches a single signal for a single title (or batch where
appropriate). Designed for use by the asyncio scheduler.

These are thin wrappers around existing fetch_data.py functions. They
handle their own error catching and return a dict of signal values or
an empty dict on failure.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

LOG = logging.getLogger("signal_fetchers")

REPO_ROOT = Path(__file__).resolve().parent.parent

# Import shared utilities from fetch_data
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_data import (
    _http_get, _sanitize_title, load_config,
    fetch_youtube_video_stats, fetch_youtube_for_movie,
    fetch_google_trends,
    fetch_news_feeds, _load_entity_tags,
    _load_trailer_cache, _save_trailer_cache,
    _load_stats_cache, _save_stats_cache,
    _news_mentions_for,
    REQUEST_TIMEOUT, TMDB_BASE, YOUTUBE_BASE, X_API_BASE,
)


# ---------------------------------------------------------------------------
# Per-title signal fetchers
# ---------------------------------------------------------------------------

# fetch_reddit_for_title removed — Reddit signal discontinued per commercial ToS


def fetch_youtube_for_title(movie: Dict[str, Any],
                            yt_key: str, tmdb_key: str,
                            trailer_cache: Dict[str, str],
                            stats_cache: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    """Fetch YouTube stats for a single movie. Returns {views, likes, comments}."""
    title = movie.get("title", "")
    tmdb_id = movie.get("tmdb_id")
    if not tmdb_id:
        return {"views": 0, "likes": 0, "comments": 0}
    try:
        year = (movie.get("release_date") or "")[:4] or None
        return fetch_youtube_for_movie(
            yt_key, tmdb_key,
            tmdb_id=tmdb_id, title=title, year=year,
            trailer_cache=trailer_cache,
            stats_cache=stats_cache,
        )
    except Exception as exc:
        LOG.warning("YouTube failed for %s: %s", title, exc)
        return {"views": 0, "likes": 0, "comments": 0}


def _x_count_single(query: str, bearer: str) -> int:
    """Single X API call. Returns count or 0."""
    try:
        r = requests.get(
            f"{X_API_BASE}/tweets/counts/recent",
            params={"query": query},
            headers={"Authorization": f"Bearer {bearer}"},
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code in (402, 403, 429):
            return 0
        if r.status_code != 200:
            return 0
        return int(r.json().get("meta", {}).get("total_tweet_count", 0))
    except Exception:
        return 0


def fetch_x_for_title(movie: Dict[str, Any]) -> int:
    """Fetch X mention count. Uses bare title — X volume is high enough
    that 'title + movie' over-filters (52x reduction for Spider-Man).
    Sanity cap at 50K handles common-word noise."""
    bearer = os.environ.get("X_BEARER_TOKEN")
    if not bearer:
        return 0
    title = movie.get("title", "")
    clean = _sanitize_title(title)
    count = _x_count_single(f'"{clean}"', bearer)
    return count


def fetch_news_for_title(movie: Dict[str, Any],
                         news_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Match news headlines to a single movie. Zero API cost."""
    title = movie.get("title", "")
    return _news_mentions_for(title, news_items)


# ---------------------------------------------------------------------------
# Batch signal fetchers (shared across all titles)
# ---------------------------------------------------------------------------

def fetch_all_news(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Fetch all RSS feeds. Returns list of news items."""
    feeds = config.get("rss_feeds", [])
    entity_tags = _load_entity_tags()
    return fetch_news_feeds(feeds, entity_tags=entity_tags)


def fetch_all_trends(titles: List[str]) -> Dict[str, int]:
    """Fetch Google Trends for all titles. Returns {title: score}."""
    return fetch_google_trends(titles)
