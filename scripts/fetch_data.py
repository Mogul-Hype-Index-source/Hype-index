"""
MoviePass Hype Index V2 — data collection
=========================================

Pulls raw signal from free APIs for the V2 dashboard:

  • TMDb            — universe of movies (now playing + upcoming + popular)
  • YouTube Data v3 — official trailer view / like / comment counts
  • Reddit          — public JSON search across configured subreddits
  • Google Trends   — pytrends interest score (0-100)
  • RSS feeds       — Deadline / Variety / Hollywood Reporter / IndieWire

Each source is wrapped in try/except so a single failure does not abort the run.
The output is a Python dict ready for scripts/score.py to consume.

Run as a module from the repo root:
    python scripts/fetch_data.py [--limit 20]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

LOG = logging.getLogger("fetch_data")

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config.json"

TMDB_BASE = "https://api.themoviedb.org/3"
YOUTUBE_BASE = "https://www.googleapis.com/youtube/v3"
REDDIT_BASE = "https://www.reddit.com"

REQUEST_TIMEOUT = 15  # seconds

# Hard-coded title blacklist — re-releases / remakes of old films that TMDb
# tags with a recent release_date but are not part of the live theatrical
# slate. Anything matched here is dropped before scoring.
TITLE_BLACKLIST = {
    "faces of death",          # 1978 mondo film, periodic re-releases
}

# News headlines containing any of these tokens are dropped from the ticker.
# Variety / THR / IndieWire publish a fair amount of sports business and TV
# news; the Hype Index ticker should stay film-focused.
NEWS_REJECT_KEYWORDS = [
    "nfl", "espn", "nba", "mlb", "nhl", "college football", "ncaa",
    "wnba", "ufc", "boxing", "wwe", "premier league", "world cup",
    "fifa", "f1 ", "formula 1", "nascar", "olympics", "olympic",
    "tennis", "golf", "pga", "lpga", "super bowl", "playoffs",
]


# ---------------------------------------------------------------------------
# Config + HTTP helpers
# ---------------------------------------------------------------------------

def load_config(path: Path = CONFIG_PATH) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Copy config.example.json to config.json and add your keys."
        )
    with path.open() as f:
        return json.load(f)


def _http_get(url: str, *, params: Optional[Dict[str, Any]] = None,
              headers: Optional[Dict[str, str]] = None,
              retries: int = 0, backoff: float = 2.0) -> Optional[Dict[str, Any]]:
    """
    GET → JSON with logging on failure. Returns None on any error.
    Set `retries` > 0 to retry on HTTP 429 / 5xx with exponential backoff.
    """
    attempt = 0
    while True:
        try:
            r = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                wait = backoff * (2 ** attempt)
                LOG.info("HTTP %s for %s — retry %d/%d in %.1fs",
                         r.status_code, url, attempt + 1, retries, wait)
                time.sleep(wait)
                attempt += 1
                continue
            LOG.warning("HTTP %s for %s", r.status_code, url)
            return None
        except Exception as exc:  # noqa: BLE001
            if attempt < retries:
                wait = backoff * (2 ** attempt)
                LOG.info("Request error %s — retry %d/%d in %.1fs", exc, attempt + 1, retries, wait)
                time.sleep(wait)
                attempt += 1
                continue
            LOG.warning("Request failed: %s — %s", url, exc)
            return None


# ---------------------------------------------------------------------------
# TMDb — discover the universe of movies
# ---------------------------------------------------------------------------

def filter_by_release_window(movies: Dict[int, Dict[str, Any]] | List[Dict[str, Any]],
                             window_days: int = 90) -> Any:
    """
    Keep only movies whose theatrical release is in the live window:
        today - window_days  ≤  release_date  (no upper bound — upcoming OK)

    Anything older than `window_days` (default 90) is dropped. Movies with
    a missing or unparseable release_date are dropped — they are almost
    always bad records on TMDb.

    Accepts either a {tmdb_id: movie} dict (returns dict) or a list (returns
    list), so it can be used both inside fetch_tmdb_movies and as a
    post-fetch defensive filter from score.py / update.py.
    """
    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=window_days)

    def _keep(m: Dict[str, Any]) -> bool:
        # Title blacklist (re-releases of old films, etc.)
        if (m.get("title") or "").strip().lower() in TITLE_BLACKLIST:
            return False
        rd = m.get("release_date") or ""
        try:
            rd_date = datetime.strptime(rd, "%Y-%m-%d").date()
        except ValueError:
            return False
        return rd_date >= cutoff

    if isinstance(movies, dict):
        return {tid: m for tid, m in movies.items() if _keep(m)}
    return [m for m in movies if _keep(m)]


def fetch_tmdb_movies(api_key: str, max_count: int = 100) -> List[Dict[str, Any]]:
    """
    Pull the universe of movies to track:
      now_playing  → currently in theaters
      upcoming     → next ~6 weeks (multiple pages for more headroom)
      popular      → broader signal for forward-facing slate

    Filters out anything released more than 2 years ago — V2 only tracks
    current theatrical and the upcoming pipeline. Dedupes by tmdb_id, sorts
    by popularity, takes the top `max_count`.
    """
    universe: Dict[int, Dict[str, Any]] = {}

    # Pull more pages now that we filter aggressively by release date.
    endpoints = [
        ("now_playing", 3),
        ("upcoming",    5),
        ("popular",     5),
    ]
    for ep, pages in endpoints:
        for page in range(1, pages + 1):
            data = _http_get(
                f"{TMDB_BASE}/movie/{ep}",
                params={"api_key": api_key, "language": "en-US", "page": page},
            )
            if not data:
                continue
            for r in data.get("results", []):
                tid = r.get("id")
                if tid is None:
                    continue
                if tid not in universe:
                    universe[tid] = {
                        "tmdb_id": tid,
                        "title": r.get("title") or r.get("original_title") or "",
                        "release_date": r.get("release_date") or "",
                        "popularity": r.get("popularity") or 0.0,
                        "poster_path": r.get("poster_path"),
                        "overview": r.get("overview") or "",
                        "vote_average": r.get("vote_average") or 0.0,
                        "vote_count": r.get("vote_count") or 0,
                    }
                else:
                    universe[tid]["popularity"] = max(
                        universe[tid]["popularity"], r.get("popularity") or 0.0
                    )
            time.sleep(0.15)

    # Date filter: keep only movies whose theatrical release is within the
    # last 90 days OR is still upcoming. Anything older than 90 days is
    # dropped — V2 tracks the live theatrical window and the forward slate,
    # not back-catalog. Movies with an unparseable release_date are dropped
    # (TMDb publishes most legitimate titles with dates).
    filtered = filter_by_release_window(universe, window_days=90)
    LOG.info("Date filter: %d → %d movies (last 90d + upcoming)",
             len(universe), len(filtered))

    movies = sorted(filtered.values(), key=lambda m: m["popularity"], reverse=True)
    LOG.info("TMDb universe: %d eligible movies, taking top %d", len(movies), max_count)
    return movies[:max_count]


def fetch_tmdb_credits(api_key: str, tmdb_id: int) -> Dict[str, Any]:
    """
    Returns:
      director:        comma-separated display string of director name(s)
      cast:            comma-separated display string of top 2 billed cast
      directors_full:  [{id, name, profile_path}, ...]
      cast_full:       [{id, name, character, profile_path}, ...] (top 8 billed)

    The full lists are used downstream to build the actors + directors
    rollups (top 50 / top 25) without having to hit TMDb again.
    """
    data = _http_get(
        f"{TMDB_BASE}/movie/{tmdb_id}/credits",
        params={"api_key": api_key},
    )
    empty: Dict[str, Any] = {
        "director": "", "cast": "",
        "directors_full": [], "cast_full": [],
    }
    if not data:
        return empty

    crew = data.get("crew") or []
    cast = data.get("cast") or []

    directors_full = [
        {
            "id":           c.get("id"),
            "name":         c.get("name"),
            "profile_path": c.get("profile_path"),
        }
        for c in crew
        if c.get("job") == "Director" and c.get("name")
    ]
    cast_full = [
        {
            "id":           c.get("id"),
            "name":         c.get("name"),
            "character":    c.get("character"),
            "profile_path": c.get("profile_path"),
        }
        for c in cast[:8]
        if c.get("name")
    ]

    return {
        "director":       ", ".join(d["name"] for d in directors_full[:2]),
        "cast":           ", ".join(c["name"] for c in cast_full[:2]),
        "directors_full": directors_full,
        "cast_full":      cast_full,
    }


# ---------------------------------------------------------------------------
# YouTube — trailer stats (TMDb supplies the video ID, we only fetch stats)
# ---------------------------------------------------------------------------
#
# Architecture note: YouTube's /search endpoint costs 100 quota units per call
# (10K daily quota = ~100 searches/day). Calling it once per movie per hour
# blows the budget after the first run. Instead we ask TMDb's free
# /movie/{id}/videos endpoint for the official trailer's YouTube video ID
# (TMDb has no realistic quota for free use), and then call YouTube /videos
# for stats — which costs only 1 quota unit per call. 87 movies × 1 unit ×
# 24 runs/day = ~2K units/day, comfortably under the 10K cap.
#
# Trailer IDs are also cached on disk (data/cache/youtube_trailers.json),
# keyed by tmdb_id, so we only re-query TMDb when a movie is new to the slate.

# Per-source cache TTLs. Each source's fetch function checks its own
# timestamp and skips the network round-trip if cached data is still fresh.
# This lets a single 15-minute pulse cycle keep the news ticker live while
# spending YouTube quota only once per day.
YOUTUBE_STATS_TTL_HOURS = 24       # daily — quota-conservative
GOOGLE_TRENDS_TTL_HOURS = 2        # bi-hourly
NEWS_TTL_MINUTES        = 15       # ticker freshness target
TMDB_UNIVERSE_TTL_HOURS = 6        # the slate doesn't shift fast


def _trailer_cache_path() -> Path:
    return REPO_ROOT / "data" / "cache" / "youtube_trailers.json"


def _stats_cache_path() -> Path:
    return REPO_ROOT / "data" / "cache" / "youtube_stats.json"


def _load_json(p: Path) -> Dict[str, Any]:
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:  # noqa: BLE001
        return {}


def _save_json(p: Path, data: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2))


def _load_trailer_cache() -> Dict[str, str]:
    return _load_json(_trailer_cache_path())


def _save_trailer_cache(cache: Dict[str, str]) -> None:
    _save_json(_trailer_cache_path(), cache)


def _load_stats_cache() -> Dict[str, Dict[str, Any]]:
    return _load_json(_stats_cache_path())


def _save_stats_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    _save_json(_stats_cache_path(), cache)


def _stats_cache_fresh(entry: Dict[str, Any], ttl_hours: int = YOUTUBE_STATS_TTL_HOURS) -> bool:
    """True if the cache entry is younger than ttl_hours."""
    iso = entry.get("fetched_at")
    if not iso:
        return False
    try:
        ts = datetime.fromisoformat(iso)
    except ValueError:
        return False
    age = datetime.now(timezone.utc) - ts
    return age < timedelta(hours=ttl_hours)


def _cache_age(blob: Dict[str, Any]) -> Optional[timedelta]:
    """How old is a cache file's payload? Returns None if no fetched_at."""
    iso = blob.get("fetched_at")
    if not iso:
        return None
    try:
        ts = datetime.fromisoformat(iso)
    except ValueError:
        return None
    return datetime.now(timezone.utc) - ts


def _news_cache_path()    -> Path: return REPO_ROOT / "data" / "cache" / "news.json"
def _trends_cache_path()  -> Path: return REPO_ROOT / "data" / "cache" / "trends.json"


def fetch_tmdb_trailer_video_id(api_key: str, tmdb_id: int) -> Optional[str]:
    """Pick the best YouTube trailer ID for a movie from TMDb /videos."""
    data = _http_get(
        f"{TMDB_BASE}/movie/{tmdb_id}/videos",
        params={"api_key": api_key, "language": "en-US"},
    )
    if not data:
        return None
    videos = data.get("results") or []
    # Preference order: official Trailer → any Trailer → Teaser → any YouTube clip
    def _pick(predicate):
        for v in videos:
            if v.get("site") == "YouTube" and predicate(v):
                return v.get("key")
        return None

    return (
        _pick(lambda v: v.get("type") == "Trailer" and v.get("official"))
        or _pick(lambda v: v.get("type") == "Trailer")
        or _pick(lambda v: v.get("type") == "Teaser")
        or _pick(lambda v: True)
    )


def fetch_youtube_video_stats(api_key: str, video_id: str,
                              diag: bool = False) -> Dict[str, int]:
    """
    Fetch view/like/comment counts for a known YouTube video ID.
    Costs 1 YouTube Data API quota unit per call.
    Returns {"views":0,"likes":0,"comments":0} on any failure (quota,
    deleted video, network error, etc).
    """
    empty = {"views": 0, "likes": 0, "comments": 0}
    if not video_id:
        return empty
    stats = _http_get(
        f"{YOUTUBE_BASE}/videos",
        params={"part": "statistics", "id": video_id, "key": api_key},
    )
    if diag:
        LOG.info("DIAG youtube /videos id=%s → %s",
                 video_id, json.dumps(stats)[:300] if stats else "None")
    if not stats or not stats.get("items"):
        return empty
    s = stats["items"][0].get("statistics", {})
    return {
        "views":    int(s.get("viewCount", 0) or 0),
        "likes":    int(s.get("likeCount", 0) or 0),
        "comments": int(s.get("commentCount", 0) or 0),
    }


def fetch_youtube_search(api_key: str, query: str,
                         diag: bool = False) -> Optional[str]:
    """
    YouTube /search → top result video ID. Costs 100 quota units per call.
    Used as the FALLBACK when TMDb has no trailer for a movie.
    """
    if not query:
        return None
    search = _http_get(
        f"{YOUTUBE_BASE}/search",
        params={
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": 1,
            "key": api_key,
        },
    )
    if diag:
        LOG.info("DIAG youtube /search q=%r → %s",
                 query, json.dumps(search)[:300] if search else "None")
    if not search:
        return None
    items = search.get("items") or []
    if not items:
        return None
    return items[0].get("id", {}).get("videoId")


def fetch_youtube_for_movie(yt_key: str, tmdb_key: str,
                            tmdb_id: int, title: str, year: Optional[str],
                            trailer_cache: Dict[str, str],
                            stats_cache: Dict[str, Dict[str, Any]],
                            diag: bool = False) -> Dict[str, int]:
    """
    Full chain for a single movie:

      1. Stats cache hit (within 24h TTL)         → return cached stats
      2. Trailer cache hit (tmdb_id → video_id)   → fetch fresh stats
      3. Ask TMDb /movie/{id}/videos              → cache + fetch fresh
      4. YouTube /search "{title} official trailer {year}"  → cache + fetch
      5. YouTube /search "{title} trailer"        → cache + fetch
      6. Stale stats cache (any age)              → return stale
      7. Empty {0,0,0}

    The cache + fallback chain means quota exhaustion does NOT zero the
    leaderboard — we keep showing the most recent good values until quota
    resets at midnight Pacific.
    """
    tmdb_id_str = str(tmdb_id)
    cached = stats_cache.get(tmdb_id_str)

    # 1. Fresh cache hit — zero API cost
    if cached and _stats_cache_fresh(cached):
        if diag:
            LOG.info("DIAG cache HIT (fresh) tmdb=%d %s — views=%d",
                     tmdb_id, title, cached.get("views", 0))
        return {
            "views":    int(cached.get("views", 0) or 0),
            "likes":    int(cached.get("likes", 0) or 0),
            "comments": int(cached.get("comments", 0) or 0),
        }

    # 2. We have a cached trailer ID for this movie
    video_id = trailer_cache.get(tmdb_id_str)

    # 3. Otherwise ask TMDb for the trailer (free)
    if not video_id:
        video_id = fetch_tmdb_trailer_video_id(tmdb_key, tmdb_id)
        if video_id:
            trailer_cache[tmdb_id_str] = video_id

    # 4. Try YouTube /search with year qualifier
    if not video_id and year:
        video_id = fetch_youtube_search(yt_key, f"{title} official trailer {year}", diag=diag)
        if video_id:
            trailer_cache[tmdb_id_str] = video_id

    # 5. Try YouTube /search without year
    if not video_id:
        video_id = fetch_youtube_search(yt_key, f"{title} trailer", diag=diag)
        if video_id:
            trailer_cache[tmdb_id_str] = video_id

    # If we have a video_id, try to fetch fresh stats
    if video_id:
        fresh = fetch_youtube_video_stats(yt_key, video_id, diag=diag)
        # Treat zero-view result as a soft failure — re-search without year
        if fresh["views"] == 0 and year:
            alt_video_id = fetch_youtube_search(yt_key, f"{title} trailer", diag=diag)
            if alt_video_id and alt_video_id != video_id:
                alt_fresh = fetch_youtube_video_stats(yt_key, alt_video_id, diag=diag)
                if alt_fresh["views"] > 0:
                    video_id = alt_video_id
                    fresh = alt_fresh
                    trailer_cache[tmdb_id_str] = video_id

        if fresh["views"] > 0:
            stats_cache[tmdb_id_str] = {
                **fresh,
                "video_id":    video_id,
                "title":       title,
                "fetched_at":  datetime.now(timezone.utc).isoformat(),
            }
            if diag:
                LOG.info("DIAG cache MISS → fetched fresh tmdb=%d %s views=%d",
                         tmdb_id, title, fresh["views"])
            return fresh

    # 6. Last-resort: stale cache (older than TTL but better than nothing)
    if cached:
        if diag:
            LOG.info("DIAG cache STALE fallback tmdb=%d %s views=%d (age=%s)",
                     tmdb_id, title, cached.get("views", 0), cached.get("fetched_at"))
        return {
            "views":    int(cached.get("views", 0) or 0),
            "likes":    int(cached.get("likes", 0) or 0),
            "comments": int(cached.get("comments", 0) or 0),
        }

    # 7. Nothing
    if diag:
        LOG.info("DIAG no data tmdb=%d %s", tmdb_id, title)
    return {"views": 0, "likes": 0, "comments": 0}


def fetch_youtube_trailer(api_key: str, title: str) -> Dict[str, int]:
    """
    Legacy entry point — kept for ad-hoc use. Costs 100+1 quota units per call.
    Prefer fetch_youtube_for_movie() inside fetch_all().
    """
    empty = {"views": 0, "likes": 0, "comments": 0}
    if not title:
        return empty
    video_id = fetch_youtube_search(api_key, f"{title} official trailer")
    return fetch_youtube_video_stats(api_key, video_id) if video_id else empty


# ---------------------------------------------------------------------------
# Reddit — public JSON search across subs
# ---------------------------------------------------------------------------

def fetch_reddit_mentions(title: str, subreddits: Iterable[str], user_agent: str) -> Dict[str, int]:
    """
    Count posts + total comments for a title across configured subreddits in the last 7 days.
    Uses Reddit's public unauthenticated JSON endpoint.
    """
    posts = 0
    comments = 0
    if not title:
        return {"posts": 0, "comments": 0}

    headers = {"User-Agent": user_agent}
    q = f'"{title}"'
    for sub in subreddits:
        url = f"{REDDIT_BASE}/r/{sub}/search.json"
        data = _http_get(
            url,
            params={"q": q, "restrict_sr": 1, "sort": "new", "limit": 100, "t": "week"},
            headers=headers,
            retries=2,
            backoff=4.0,  # 4s, then 8s
        )
        if not data:
            time.sleep(2.0)  # extra cushion if we just hit a wall
            continue
        children = (data.get("data") or {}).get("children") or []
        posts += len(children)
        for c in children:
            comments += int((c.get("data") or {}).get("num_comments") or 0)
        time.sleep(1.5)  # be polite to reddit's unauthenticated endpoint
    return {"posts": posts, "comments": comments}


# ---------------------------------------------------------------------------
# Google Trends — pytrends batches of 5
# ---------------------------------------------------------------------------

def fetch_google_trends(titles: List[str], force: bool = False) -> Dict[str, int]:
    """
    Returns {title: 0..100 interest score over the last 7 days}.
    pytrends caps comparisons at 5 keywords per request, so we batch.

    Cached for GOOGLE_TRENDS_TTL_HOURS (default 2h). pytrends rate-limits
    aggressively, so this caching is essential — without it the launchd
    pulse hammers Google every 15 minutes and gets 429s on every batch.

    On any error we fall back to the previously cached scores so a single
    batch failure doesn't zero out trends for everyone.
    """
    if not titles:
        return {}

    cache = _load_json(_trends_cache_path())
    age = _cache_age(cache)
    cached_scores: Dict[str, int] = cache.get("scores") or {}

    if not force and age is not None and age < timedelta(hours=GOOGLE_TRENDS_TTL_HOURS):
        LOG.info("Trends cache HIT — %d titles, age %s", len(cached_scores), age)
        # Return cached scores with 0 for any new titles. The next eligible
        # refresh (after TTL) will fill them in.
        return {t: int(cached_scores.get(t, 0)) for t in titles}

    try:
        from pytrends.request import TrendReq  # type: ignore
    except ImportError:
        LOG.warning("pytrends not installed — skipping Google Trends. `pip install pytrends`")
        return cached_scores or {t: 0 for t in titles}

    try:
        py = TrendReq(hl="en-US", tz=0)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("pytrends init failed: %s — using stale cache", exc)
        return cached_scores or {t: 0 for t in titles}

    out: Dict[str, int] = {}
    any_success = False
    batch_size = 5
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i + batch_size]
        try:
            py.build_payload(batch, timeframe="now 7-d", geo="")
            df = py.interest_over_time()
            if df is None or df.empty:
                for t in batch:
                    out[t] = int(cached_scores.get(t, 0))
                continue
            for t in batch:
                if t in df.columns:
                    out[t] = int(df[t].mean() or 0)
                    any_success = True
                else:
                    out[t] = int(cached_scores.get(t, 0))
        except Exception as exc:  # noqa: BLE001
            LOG.warning("pytrends batch failed (%s): %s", batch, exc)
            # Fall back to whatever was cached for these titles.
            for t in batch:
                out.setdefault(t, int(cached_scores.get(t, 0)))
        time.sleep(2.0)  # rate limit cushion

    if any_success:
        _save_json(_trends_cache_path(), {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "scores": out,
        })
        LOG.info("Trends cache updated — %d titles", len(out))
    else:
        LOG.warning("Trends fetch produced no fresh data — keeping previous cache")
    return out


# ---------------------------------------------------------------------------
# RSS news feeds
# ---------------------------------------------------------------------------

def fetch_news_feeds(feeds: List[Dict[str, str]],
                     force: bool = False) -> List[Dict[str, Any]]:
    """
    Returns a flat, time-sorted list of news items from all configured feeds.

    Cached for NEWS_TTL_MINUTES. The 15-minute TTL means the ticker stays
    fresh on every pulse cycle but we still avoid hammering RSS endpoints
    if a pulse fires more often (e.g. operator manual run).
    """
    cache = _load_json(_news_cache_path())
    age = _cache_age(cache)
    if not force and age is not None and age < timedelta(minutes=NEWS_TTL_MINUTES):
        items = cache.get("items") or []
        LOG.info("News cache HIT — %d items, age %s", len(items), age)
        return items

    try:
        import feedparser  # type: ignore
    except ImportError:
        LOG.warning("feedparser not installed — skipping news. `pip install feedparser`")
        return cache.get("items") or []

    items: List[Dict[str, Any]] = []
    rejected = 0
    for feed in feeds:
        src = feed.get("source", "")
        url = feed.get("url", "")
        if not url:
            continue
        try:
            parsed = feedparser.parse(url)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("RSS parse failed for %s: %s", url, exc)
            continue
        for entry in (parsed.entries or [])[:30]:
            headline = (entry.get("title") or "").strip()
            if not headline:
                continue
            # Reject sports / non-film content. The four trade pubs all
            # cover sports business too, which polluted the ticker.
            lower = headline.lower()
            if any(kw in lower for kw in NEWS_REJECT_KEYWORDS):
                rejected += 1
                continue

            published_iso = ""
            if getattr(entry, "published_parsed", None):
                try:
                    published_iso = datetime(
                        *entry.published_parsed[:6], tzinfo=timezone.utc
                    ).isoformat()
                except Exception:  # noqa: BLE001
                    pass
            items.append({
                "headline":  headline,
                "source":    src,
                "url":       entry.get("link") or "",
                "published": published_iso,
                "summary":   re.sub(r"<[^>]+>", "", entry.get("summary", ""))[:240],
            })
    items.sort(key=lambda x: x.get("published") or "", reverse=True)
    LOG.info("RSS: pulled %d items across %d feeds (dropped %d sports/non-film)",
             len(items), len(feeds), rejected)

    # Persist for the next pulse so we don't re-hit RSS within the TTL.
    _save_json(_news_cache_path(), {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    })
    return items


# ---------------------------------------------------------------------------
# Master orchestrator (this module's "fetch everything")
# ---------------------------------------------------------------------------

def fetch_all(config: Dict[str, Any], limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Returns a dict ready for scripts/score.py:
      {
        "generated_at": iso8601,
        "movies":  [ {tmdb_id, title, director, cast, poster_url, release_date,
                      youtube: {views,likes,comments},
                      reddit:  {posts,comments},
                      trends:  int 0-100,
                      news:    [headlines that mention this title in last 24h]} ],
        "news":    [...all RSS items, time-sorted...],
      }
    """
    yt_key   = config["youtube_api_key"]
    tmdb_key = config["tmdb_api_key"]
    ua       = config.get("reddit_user_agent", "HypeIndexV2/1.0")
    subs     = config.get("subreddits", ["movies"])
    feeds    = config.get("rss_feeds", [])
    poster_base = config.get("tmdb_image_base", "https://image.tmdb.org/t/p/w185")
    max_count   = limit or int(config.get("max_movies", 100))

    LOG.info("=== Fetch start (limit=%d) ===", max_count)

    # 1. Movie universe
    movies = fetch_tmdb_movies(tmdb_key, max_count=max_count)

    # 2. News feeds (single shot, used for both ticker + per-movie NIS)
    news_items = fetch_news_feeds(feeds)

    # Trailer-ID cache (tmdb_id → youtube video_id) so we never re-search
    trailer_cache: Dict[str, str] = _load_trailer_cache()
    stats_cache: Dict[str, Dict[str, Any]] = _load_stats_cache()
    trailer_before = dict(trailer_cache)
    stats_before   = dict(stats_cache)

    # 3. Per-movie enrichment
    for idx, m in enumerate(movies, 1):
        title = m["title"]
        LOG.info("[%d/%d] %s", idx, len(movies), title)

        creds = fetch_tmdb_credits(tmdb_key, m["tmdb_id"])
        m.update(creds)
        m["poster_url"] = (
            f"{poster_base}{m['poster_path']}" if m.get("poster_path") else None
        )

        # YouTube — fully cached, with TMDb→search fallback chain.
        # Diagnostic logging is enabled for the first 3 movies of every run
        # so we can see exactly what the API is returning vs the cache.
        diag = (idx <= 3)
        try:
            year = (m.get("release_date") or "")[:4] or None
            m["youtube"] = fetch_youtube_for_movie(
                yt_key, tmdb_key,
                tmdb_id=m["tmdb_id"], title=title, year=year,
                trailer_cache=trailer_cache,
                stats_cache=stats_cache,
                diag=diag,
            )
            m["youtube_video_id"] = trailer_cache.get(str(m["tmdb_id"]))
        except Exception as exc:  # noqa: BLE001
            LOG.warning("YouTube failed for %s: %s", title, exc)
            m["youtube"] = {"views": 0, "likes": 0, "comments": 0}

        try:
            m["reddit"] = fetch_reddit_mentions(title, subs, ua)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Reddit failed for %s: %s", title, exc)
            m["reddit"] = {"posts": 0, "comments": 0}

        # Per-movie news mentions = items whose headline contains the title
        # (case-insensitive, whole-word). We'll keep the matched outlets for NIS scoring.
        m["news_mentions"] = _news_mentions_for(title, news_items)

        time.sleep(0.2)  # gentle pacing

    # Persist trailer + stats cache for next run
    if trailer_cache != trailer_before:
        _save_trailer_cache(trailer_cache)
        LOG.info("Trailer cache: %d entries persisted", len(trailer_cache))
    if stats_cache != stats_before:
        _save_stats_cache(stats_cache)
        LOG.info("Stats cache: %d entries persisted", len(stats_cache))

    # Diagnostic summary: how many movies got real YouTube data this run?
    yt_hits = sum(1 for m in movies if (m.get("youtube") or {}).get("views", 0) > 0)
    LOG.info("YouTube coverage: %d/%d (%d%%)", yt_hits, len(movies),
             (yt_hits * 100 // len(movies)) if movies else 0)

    # 4. Google Trends — batch all titles at once (cheap, single block)
    titles = [m["title"] for m in movies]
    trends = fetch_google_trends(titles)
    for m in movies:
        m["trends"] = int(trends.get(m["title"], 0))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "movies": movies,
        "news":   news_items,
    }


def _news_mentions_for(title: str, news_items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Return news items whose headline mentions the movie title (case-insensitive)."""
    if not title:
        return []
    needle = re.escape(title.lower())
    pattern = re.compile(rf"\b{needle}\b", re.IGNORECASE)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    out = []
    for item in news_items:
        if not pattern.search(item.get("headline", "")):
            continue
        # Optional time filter — only count "recent" news for NIS
        try:
            if item.get("published"):
                pub = datetime.fromisoformat(item["published"])
                if pub < cutoff:
                    continue
        except Exception:  # noqa: BLE001
            pass
        out.append({"source": item.get("source", ""), "headline": item.get("headline", "")})
    return out


# ---------------------------------------------------------------------------
# Actors + Directors rollup
# ---------------------------------------------------------------------------

def _name_news_mentions(name: str, news_items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """News items whose headline contains the person's name (case-insensitive)."""
    if not name:
        return []
    pattern = re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)
    out = []
    for item in news_items:
        if pattern.search(item.get("headline", "")):
            out.append({"source": item.get("source", ""), "headline": item.get("headline", "")})
    return out


def derive_people(movies: List[Dict[str, Any]],
                  news_items: List[Dict[str, Any]],
                  poster_base: str,
                  top_actors: int = 50,
                  top_directors: int = 25) -> Dict[str, List[Dict[str, Any]]]:
    """
    Roll the cast/crew across all enriched movies into per-person entries.
    Each person gets a film_count, total popularity, news mentions, average
    sentiment, and a list of films they appear in. The lists are sorted
    by film count then aggregate popularity, then truncated.
    """
    actors:    Dict[int, Dict[str, Any]] = {}
    directors: Dict[int, Dict[str, Any]] = {}
    image_base = poster_base.replace("/w185", "/w300")  # higher res for headshots

    def _bucket(target: Dict[int, Dict[str, Any]], person: Dict[str, Any],
                movie: Dict[str, Any], role_kind: str) -> None:
        pid = person.get("id")
        name = person.get("name")
        if not pid or not name:
            return
        slot = target.get(pid)
        if slot is None:
            slot = {
                "tmdb_id":      pid,
                "name":         name,
                "profile_url":  f"{image_base}{person['profile_path']}" if person.get("profile_path") else None,
                "film_count":   0,
                "films":        [],
                "popularity":   0.0,
                "sentiment_acc": 0,
                "sentiment_n":   0,
                "score_acc":    0,
            }
            target[pid] = slot
        slot["film_count"] += 1
        slot["popularity"] = max(slot["popularity"], movie.get("popularity") or 0.0)
        slot["sentiment_acc"] += int(movie.get("sentiment_pct", 50))
        slot["sentiment_n"]   += 1
        slot["score_acc"]    += int(movie.get("score", 0))
        slot["films"].append({
            "tmdb_id":      movie.get("tmdb_id"),
            "title":        movie.get("title"),
            "release_date": movie.get("release_date"),
            "poster_url":   movie.get("poster_url"),
            "score":        movie.get("score", 0),
            "character":    person.get("character"),
        })

    for m in movies:
        for c in (m.get("cast_full") or [])[:6]:    # top 6 billed per film
            _bucket(actors, c, m, "actor")
        for d in (m.get("directors_full") or []):
            _bucket(directors, d, m, "director")

    def _finalize(slot: Dict[str, Any]) -> Dict[str, Any]:
        n = slot["sentiment_n"] or 1
        slot["sentiment_pct"] = int(round(slot["sentiment_acc"] / n))
        slot["avg_film_score"] = int(round(slot["score_acc"] / n))
        slot["news_mentions"] = _name_news_mentions(slot["name"], news_items)
        slot["mentions"] = len(slot["news_mentions"]) + slot["film_count"] * 5
        del slot["sentiment_acc"]
        del slot["sentiment_n"]
        del slot["score_acc"]
        return slot

    actor_list    = [_finalize(s) for s in actors.values()]
    director_list = [_finalize(s) for s in directors.values()]

    # Score each person: weighted blend of film_count, popularity, news,
    # avg film score. Then min-max rescale into V1's 800-999 band so the
    # leaderboard reads like the movies tab.
    def _score_people(people: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not people:
            return people
        # Raw score components, normalized to 0..1 against max in batch
        max_films = max(p["film_count"] for p in people) or 1
        max_pop   = max(p["popularity"] for p in people) or 1
        max_news  = max(len(p["news_mentions"]) for p in people) or 1
        max_avgf  = max(p["avg_film_score"] for p in people) or 1
        for p in people:
            raw = (
                (p["film_count"]          / max_films) * 0.35 +
                (p["popularity"]          / max_pop)   * 0.25 +
                (len(p["news_mentions"])  / max_news)  * 0.20 +
                (p["avg_film_score"]      / max_avgf)  * 0.20
            )
            p["_raw"] = raw
        raws = [p["_raw"] for p in people]
        lo, hi = min(raws), max(raws)
        span = (hi - lo) or 1
        for p in people:
            p["score"]  = int(round(800 + ((p["_raw"] - lo) / span) * 199))
            p["scores"] = {"1d": p["score"], "7d": p["score"], "30d": p["score"]}
            del p["_raw"]
        people.sort(key=lambda x: x["score"], reverse=True)
        for i, p in enumerate(people, 1):
            p["rank"] = i
        return people

    actor_list    = _score_people(actor_list)[:top_actors]
    director_list = _score_people(director_list)[:top_directors]

    LOG.info("People rollup: %d actors, %d directors", len(actor_list), len(director_list))
    return {"actors": actor_list, "directors": director_list}


# ---------------------------------------------------------------------------
# CLI entry point — for ad-hoc fetches / debugging
# ---------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap the universe (useful for fast smoke tests)")
    parser.add_argument("--out", type=Path,
                        default=REPO_ROOT / "data" / "cache" / "raw.json",
                        help="Where to dump the raw fetch result")
    args = parser.parse_args()

    cfg = load_config()
    raw = fetch_all(cfg, limit=args.limit)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as f:
        json.dump(raw, f, indent=2)
    LOG.info("Wrote raw fetch → %s (%d movies, %d news)",
             args.out, len(raw["movies"]), len(raw["news"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
