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
import os
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
X_API_BASE = "https://api.twitter.com/2"

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

# Global keyword whitelist — applied to EVERY source. Headlines must
# contain at least one of these tokens (case-insensitive substring match)
# or they're dropped from the ticker. This is the user-specified filter.
#
# Note on AI: " ai " (with surrounding whitespace) catches the standalone
# token without matching "rain", "main", "stadium" etc. "ai film" / "ai
# movie" etc are caught explicitly so an unrelated AI story like "Uber's
# new AI chips" is dropped.
NEWS_GLOBAL_KEYWORDS = [
    "film", "movie", "cinema", "box office", "theatrical", "director",
    "actor", "actress", "studio", "screenplay", "hollywood",
    "cannes", "sundance", "tiff", "venice", "berlin", "berlinale",
    "tribeca", "sxsw",
    "sequel", "franchise", "premiere", "trailer", "casting",
    # Studios + theatre chains + streamers
    "disney", "paramount", "universal pictures", "warner bros",
    "warner discovery", "wbd", "comcast", "cmcsa", "sony pictures",
    "nbcuniversal", "lionsgate", "a24", "neon", "mubi",
    "netflix", "hbo max", "max ", "hulu", "apple tv", "amazon studios",
    "amc theat", "cinemark", "imax",
    # Distribution / business
    "co-production", "film fund", "film financing", "acquisition rights",
    "rights deal", "uae", "saudi film",
    "box office tracking", "opening weekend", "weekend gross",
    "domestic gross", "worldwide gross",
    # AI-in-film specifically (NOT generic AI)
    "ai film", "ai movie", "ai screenplay", "ai-generated film",
    "deepfake", "virtual production", "led volume", "visual effects",
    "vfx", "previs", "previz", "previsualization",
    # Prediction markets
    "polymarket", "kalshi",
    # Festivals + awards
    "film festival", "festival lineup", "festival premiere",
    "academy award", "oscar nomination",
]

# Category classification — ordered list of (category, [keywords]).
# First match wins. If nothing matches, the source's default_category
# is used. Each category drives both grouping and color in the ticker.
NEWS_CATEGORY_RULES = [
    ("box-office", [
        "box office", "opening weekend", "weekend gross", "domestic gross",
        "worldwide gross", "per screen", "per-screen",
        "second weekend", "tops the box", "box office tracking",
        "box office debut", "opens to", "grossing", "ticket sales",
    ]),
    ("festivals", [
        "cannes", "sundance", "tiff", "toronto international", "venice film",
        "venezia", "berlin film", "berlinale", "tribeca", "sxsw", "afi fest",
        "festival lineup", "film festival", "festival premiere",
    ]),
    ("ai-tech", [
        "artificial intelligence", " ai ", "a.i.", "machine learning",
        "deepfake", "virtual production", "led volume", "led wall",
        "previs", "previz", "previsualization", "vfx", "visual effects",
        "imax", "laser projection", "streaming tech", "post-production tech",
        "render", "unreal engine", "generative",
    ]),
    ("finance", [
        "disney", "paramount", "warner bros", "warner discovery", "wbd",
        "comcast", "universal pictures", "sony pictures", "netflix earnings",
        "amc theatres", "amc entertainment", "cinemark", "stock", "shares",
        "earnings", "merger", "acquisition", "co-production", "film fund",
        "venture", "polymarket", "kalshi", "investment", "raises", "ipo",
        "valuation", "equity", "debt", "wall street", "analyst",
    ]),
    ("international", [
        "bollywood", "k-drama", "korean cinema", "international box office",
        "global release", "foreign language", "saudi arabia", "uae",
        "international co-production", "european film", "japanese film",
        "chinese film", "indian cinema", "latin america", "mena",
    ]),
    ("pr-marketing", [
        "trailer drops", "trailer release", "trailer:", "trailer reveal",
        "first trailer", "new trailer", "premiere", "press tour", "viral",
        "tiktok", "social media campaign", "first look", "teaser",
        "marketing campaign", "reveals", "unveiled", "red carpet",
    ]),
    ("creative", [
        "screenplay", "cinematographer", "score", "soundtrack", "composer",
        "director profile", "interview", "writer-director", "auteur",
        "production design", "editor", "costume design",
    ]),
    ("production", [
        "greenlight", "casting", "to direct", "attached to", "joins",
        "lands lead", "in talks", "production starts", "wraps",
        "screenplay deal", "starring", "to star", "boards",
    ]),
]


def _normalize_for_match(s: str) -> str:
    """
    Lowercase, replace non-word characters with spaces, collapse runs of
    whitespace, and pad with single spaces on each side. The result allows
    space-bounded substring matching that approximates word boundaries
    without dragging in regex compilation.

    Critical: this avoids the "factor" → "actor" false positive that the
    naive substring filter let through.
    """
    if not s:
        return " "
    cleaned = re.sub(r"[^\w]", " ", s.lower())
    return " " + re.sub(r"\s+", " ", cleaned).strip() + " "


def _kw_matches(needle: str, hay: str) -> bool:
    """Space-bounded substring match — both needle and hay must be normalized."""
    return needle in hay


def _classify_headline(headline: str, default: str = "production") -> str:
    """Pick the most specific category for a headline."""
    if not headline:
        return default
    norm = _normalize_for_match(headline)
    for cat, keywords in NEWS_CATEGORY_RULES:
        for kw in keywords:
            if _kw_matches(_normalize_for_match(kw), norm):
                return cat
    return default


def _passes_global_filter(headline: str) -> bool:
    """True if the headline contains at least one whitelist keyword."""
    if not headline:
        return False
    norm = _normalize_for_match(headline)
    for kw in NEWS_GLOBAL_KEYWORDS:
        if _kw_matches(_normalize_for_match(kw), norm):
            return True
    return False


def _load_entity_tags() -> List[str]:
    """
    Load all tags from data/manual_movies.json entries.
    Returns a flat list of unique lowercase tag strings.
    """
    p = REPO_ROOT / "data" / "manual_movies.json"
    if not p.exists():
        return []
    try:
        entries = json.loads(p.read_text())
    except Exception:  # noqa: BLE001
        return []
    tags: set = set()
    for entry in entries:
        for tag in (entry.get("tags") or []):
            tags.add(tag.lower())
        # Also add movie titles as keywords so headlines mentioning
        # tracked films pass through
        title = (entry.get("title") or "").strip()
        if title:
            tags.add(title.lower())
    return list(tags)


def _passes_tag_filter(headline: str, entity_tags: List[str]) -> bool:
    """True if the headline contains at least one entity tag."""
    if not headline or not entity_tags:
        return False
    norm = _normalize_for_match(headline)
    for tag in entity_tags:
        if _kw_matches(_normalize_for_match(tag), norm):
            return True
    return False


# ---------------------------------------------------------------------------
# Config + HTTP helpers
# ---------------------------------------------------------------------------

def _sanitize_title(title: str, year: Optional[str] = None) -> str:
    """Strip embedded quote characters from TMDb titles and optionally append year."""
    cleaned = re.sub(r'["\u201c\u201d\u2018\u2019\u0027]', '', title).strip()
    if year and len(year) == 4:
        cleaned = f"{cleaned} {year}"
    return cleaned


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


MANUAL_MOVIES_PATH = REPO_ROOT / "data" / "manual_movies.json"
MANUAL_TMDB_CACHE_PATH = REPO_ROOT / "data" / "cache" / "manual_tmdb_ids.json"


def fetch_tmdb_search(api_key: str, title: str, year: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Search TMDb by title (and optional year) to find a movie's ID and metadata.
    Returns the top result as a movie dict, or None.
    """
    params: Dict[str, Any] = {"api_key": api_key, "query": title, "language": "en-US"}
    if year:
        params["year"] = year
    data = _http_get(f"{TMDB_BASE}/search/movie", params=params)
    if not data:
        return None
    results = data.get("results") or []
    if not results:
        # Retry without year if no results
        if year:
            params.pop("year")
            data = _http_get(f"{TMDB_BASE}/search/movie", params=params)
            results = (data or {}).get("results") or []
        if not results:
            return None
    r = results[0]
    tid = r.get("id")
    if tid is None:
        return None
    return {
        "tmdb_id": tid,
        "title": r.get("title") or r.get("original_title") or title,
        "release_date": r.get("release_date") or "",
        "popularity": r.get("popularity") or 0.0,
        "poster_path": r.get("poster_path"),
        "overview": r.get("overview") or "",
        "vote_average": r.get("vote_average") or 0.0,
        "vote_count": r.get("vote_count") or 0,
    }


def _load_manual_movies(api_key: str) -> List[Dict[str, Any]]:
    """
    Load data/manual_movies.json and resolve each title to TMDb metadata.
    Caches TMDb IDs in data/cache/manual_tmdb_ids.json so we only search
    once per title across runs.
    """
    if not MANUAL_MOVIES_PATH.exists():
        return []
    try:
        entries = json.loads(MANUAL_MOVIES_PATH.read_text())
    except Exception:  # noqa: BLE001
        return []
    if not entries:
        return []

    # Load cached title→tmdb_id mappings
    id_cache = _load_json(MANUAL_TMDB_CACHE_PATH)
    id_cache_before = dict(id_cache)
    movies: List[Dict[str, Any]] = []

    for entry in entries:
        title = entry.get("title", "").strip()
        if not title:
            continue
        rd = entry.get("release_date") or ""
        year = rd[:4] if rd and len(rd) >= 4 else None

        # Check cache first
        cached = id_cache.get(title)
        if cached and isinstance(cached, dict) and cached.get("tmdb_id"):
            movies.append(cached)
            continue

        # Search TMDb — use search_query override if provided
        search_title = entry.get("search_query") or title
        result = fetch_tmdb_search(api_key, search_title, year=year)
        if result:
            id_cache[title] = result
            movies.append(result)
            LOG.info("Manual movie resolved: %s → tmdb_id=%d", title, result["tmdb_id"])
        else:
            LOG.warning("Manual movie not found on TMDb: %s", title)
        time.sleep(0.15)

    if id_cache != id_cache_before:
        _save_json(MANUAL_TMDB_CACHE_PATH, id_cache)
        LOG.info("Manual TMDb ID cache: %d entries persisted", len(id_cache))

    return movies


RELEASE_TYPE_CACHE_PATH = REPO_ROOT / "data" / "cache" / "release_types.json"

# TMDb release_dates type codes
# 1=Premiere, 2=Theatrical (limited), 3=Theatrical, 4=Digital, 5=Physical, 6=TV
THEATRICAL_TYPES = {1, 2, 3}
STREAMING_TYPES = {4, 6}


def fetch_tmdb_release_type(api_key: str, tmdb_id: int) -> str:
    """
    Classify a film as "theatrical", "streaming", "both", or "unknown"
    by checking TMDb /movie/{id}/release_dates for US releases.
    Falls back to all countries if no US data.
    """
    data = _http_get(
        f"{TMDB_BASE}/movie/{tmdb_id}/release_dates",
        params={"api_key": api_key},
    )
    if not data:
        return "unknown"

    results = data.get("results") or []

    # Prefer US release info, fall back to all countries
    us_types: set = set()
    all_types: set = set()
    for country in results:
        for rd in (country.get("release_dates") or []):
            rtype = rd.get("type")
            if rtype:
                all_types.add(rtype)
                if country.get("iso_3166_1") == "US":
                    us_types.add(rtype)

    types = us_types or all_types
    if not types:
        return "unknown"

    has_theatrical = bool(types & THEATRICAL_TYPES)
    has_streaming = bool(types & STREAMING_TYPES)

    if has_theatrical and has_streaming:
        return "both"
    elif has_theatrical:
        return "theatrical"
    elif has_streaming:
        return "streaming"
    return "unknown"


def _load_release_type_cache() -> Dict[str, str]:
    return _load_json(RELEASE_TYPE_CACHE_PATH) or {}


def _save_release_type_cache(cache: Dict[str, str]) -> None:
    _save_json(RELEASE_TYPE_CACHE_PATH, cache)


def fetch_release_type_cached(api_key: str, tmdb_id: int,
                              cache: Dict[str, str]) -> str:
    """Fetch release type with persistent cache (never re-queries known films)."""
    tid_str = str(tmdb_id)
    if tid_str in cache:
        return cache[tid_str]
    result = fetch_tmdb_release_type(api_key, tmdb_id)
    cache[tid_str] = result
    return result


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
X_MENTIONS_TTL_HOURS    = 1        # hourly — matches pulse cadence


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
# X (Twitter) — mention counts via /2/tweets/counts/recent
# ---------------------------------------------------------------------------

def _x_cache_path() -> Path:
    return REPO_ROOT / "data" / "cache" / "x_mentions.json"


def _get_x_bearer_token() -> Optional[str]:
    return os.environ.get("X_BEARER_TOKEN") or None


def fetch_x_mention_count(query: str, bearer_token: str) -> int:
    """
    GET /2/tweets/search/recent for `query` (free tier).
    Uses max_results=10 and reads result_count from the meta field.
    Returns 0 on any failure. Raises RuntimeError on 402/403 so the
    batch caller can short-circuit instead of retrying every query.
    """
    try:
        r = requests.get(
            f"{X_API_BASE}/tweets/search/recent",
            params={"query": query, "max_results": 10},
            headers={"Authorization": f"Bearer {bearer_token}"},
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code in (402, 403):
            raise RuntimeError(f"X API rejected with {r.status_code} — tier limit")
        if r.status_code == 429:
            raise RuntimeError("X API rate limited (429)")
        if r.status_code != 200:
            LOG.warning("X API HTTP %s for query: %s", r.status_code, query[:60])
            return 0
        data = r.json()
    except RuntimeError:
        raise
    except Exception as exc:  # noqa: BLE001
        LOG.warning("X API request failed: %s", exc)
        return 0
    return int(data.get("meta", {}).get("result_count", 0))


def fetch_x_mentions_batch(queries: Dict[str, str],
                           force: bool = False) -> Dict[str, int]:
    """
    Fetch X mention counts for a {key: search_query} dict.
    Cached for X_MENTIONS_TTL_HOURS. Returns {key: count}.
    """
    bearer = _get_x_bearer_token()
    if not bearer:
        LOG.info("X_BEARER_TOKEN not set — skipping X mentions")
        return {k: 0 for k in queries}

    cache = _load_json(_x_cache_path())
    age = _cache_age(cache)
    cached_counts: Dict[str, int] = cache.get("counts") or {}

    if not force and age is not None and age < timedelta(hours=X_MENTIONS_TTL_HOURS):
        LOG.info("X mentions cache HIT — %d entries, age %s", len(cached_counts), age)
        return {k: int(cached_counts.get(k, 0)) for k in queries}

    out: Dict[str, int] = {}
    any_success = False
    for key, q in queries.items():
        try:
            count = fetch_x_mention_count(q, bearer)
            out[key] = count
            if count > 0:
                any_success = True
        except RuntimeError as exc:
            # 402/403/429 — API tier or rate limit; stop hammering
            LOG.warning("X API unavailable (%s) — skipping remaining %d queries",
                        exc, len(queries) - len(out))
            for remaining_key in queries:
                if remaining_key not in out:
                    out[remaining_key] = int(cached_counts.get(remaining_key, 0))
            break
        except Exception as exc:  # noqa: BLE001
            LOG.warning("X mentions failed for %s: %s", key, exc)
            out[key] = int(cached_counts.get(key, 0))
        time.sleep(1.0)  # respect rate limits (300 requests / 15 min)

    if any_success:
        _save_json(_x_cache_path(), {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "counts": out,
        })
        LOG.info("X mentions cache updated — %d entries", len(out))
    else:
        LOG.warning("X mentions fetch produced no fresh data — keeping previous cache")
        out = {k: int(cached_counts.get(k, 0)) for k in queries}

    return out


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
                     force: bool = False,
                     entity_tags: Optional[List[str]] = None) -> List[Dict[str, Any]]:
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
    rejected_sports = 0
    rejected_offtopic = 0
    by_source: Dict[str, int] = {}
    for feed in feeds:
        src              = feed.get("source", "")
        url              = feed.get("url", "")
        default_category = feed.get("default_category", "production")
        filter_strict    = bool(feed.get("filter_strict", False))
        use_tag_filter   = bool(feed.get("filter_by_tags", False))
        if not url:
            continue
        try:
            parsed = feedparser.parse(url)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("RSS parse failed for %s: %s", url, exc)
            continue
        kept_for_source = 0
        for entry in (parsed.entries or [])[:50]:
            headline = (entry.get("title") or "").strip()
            if not headline:
                continue
            lower = headline.lower()

            # Always reject sports / non-film content first.
            if any(kw in lower for kw in NEWS_REJECT_KEYWORDS):
                rejected_sports += 1
                continue

            # Geopolitical feeds use entity-tag filter instead of
            # the entertainment keyword whitelist.
            if use_tag_filter:
                if not (entity_tags and _passes_tag_filter(headline, entity_tags)):
                    rejected_offtopic += 1
                    continue
            elif not _passes_global_filter(headline):
                rejected_offtopic += 1
                continue

            published_iso = ""
            if getattr(entry, "published_parsed", None):
                try:
                    published_iso = datetime(
                        *entry.published_parsed[:6], tzinfo=timezone.utc
                    ).isoformat()
                except Exception:  # noqa: BLE001
                    pass

            category = _classify_headline(headline, default=default_category)

            items.append({
                "headline":  headline,
                "source":    src,
                "url":       entry.get("link") or "",
                "published": published_iso,
                "summary":   re.sub(r"<[^>]+>", "", entry.get("summary", ""))[:240],
                "category":  category,
            })
            kept_for_source += 1
        if kept_for_source:
            by_source[src] = kept_for_source
    items.sort(key=lambda x: x.get("published") or "", reverse=True)
    LOG.info("RSS: %d items from %d feeds (dropped %d sports, %d off-topic)",
             len(items), len(feeds), rejected_sports, rejected_offtopic)
    if by_source:
        LOG.info("RSS per-source: %s",
                 ", ".join(f"{s}={n}" for s, n in by_source.items()))
    # Category breakdown for sanity-checking the classifier
    cat_counts: Dict[str, int] = {}
    for it in items:
        cat_counts[it["category"]] = cat_counts.get(it["category"], 0) + 1
    LOG.info("RSS by category: %s",
             ", ".join(f"{c}={n}" for c, n in sorted(cat_counts.items(), key=lambda x: -x[1])))

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

    # 1. Movie universe — TMDb discovery + V1 manual list merge
    movies = fetch_tmdb_movies(tmdb_key, max_count=max_count)
    tmdb_ids = {m["tmdb_id"] for m in movies}

    manual = _load_manual_movies(tmdb_key)
    added = 0
    for mm in manual:
        if mm["tmdb_id"] not in tmdb_ids:
            movies.append(mm)
            tmdb_ids.add(mm["tmdb_id"])
            added += 1
    LOG.info("Manual movie merge: %d V1 titles, %d new (total %d)",
             len(manual), added, len(movies))

    # 2. News feeds (single shot, used for both ticker + per-movie NIS)
    entity_tags = _load_entity_tags()
    news_items = fetch_news_feeds(feeds, entity_tags=entity_tags)

    # Trailer-ID cache (tmdb_id → youtube video_id) so we never re-search
    trailer_cache: Dict[str, str] = _load_trailer_cache()
    stats_cache: Dict[str, Dict[str, Any]] = _load_stats_cache()
    trailer_before = dict(trailer_cache)
    stats_before   = dict(stats_cache)
    release_cache: Dict[str, str] = _load_release_type_cache()
    release_before = dict(release_cache)

    # 3. Per-movie enrichment
    for idx, m in enumerate(movies, 1):
        title = m["title"]
        LOG.info("[%d/%d] %s", idx, len(movies), title)

        creds = fetch_tmdb_credits(tmdb_key, m["tmdb_id"])
        m.update(creds)
        m["poster_url"] = (
            f"{poster_base}{m['poster_path']}" if m.get("poster_path") else None
        )
        m["release_type"] = fetch_release_type_cached(tmdb_key, m["tmdb_id"], release_cache)

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
            reddit_year = (m.get("release_date") or "")[:4] or None
            reddit_query = _sanitize_title(title, year=reddit_year) + " movie"
            m["reddit"] = fetch_reddit_mentions(reddit_query, subs, ua)
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
    if release_cache != release_before:
        _save_release_type_cache(release_cache)
        LOG.info("Release type cache: %d entries persisted", len(release_cache))

    # Diagnostic summary: how many movies got real YouTube data this run?
    yt_hits = sum(1 for m in movies if (m.get("youtube") or {}).get("views", 0) > 0)
    LOG.info("YouTube coverage: %d/%d (%d%%)", yt_hits, len(movies),
             (yt_hits * 100 // len(movies)) if movies else 0)

    # 4. Google Trends — sanitized queries with year for disambiguation
    trends_queries: Dict[str, str] = {}  # sanitized_query → raw_title
    for m in movies:
        year = (m.get("release_date") or "")[:4] or None
        sanitized = _sanitize_title(m["title"], year=year)
        trends_queries[sanitized] = m["title"]
    trends = fetch_google_trends(list(trends_queries.keys()))
    # Map results back to movies by raw title
    reverse_map = {v: k for k, v in trends_queries.items()}
    for m in movies:
        q = reverse_map.get(m["title"], m["title"])
        m["trends"] = int(trends.get(q, 0))

    # 5. X (Twitter) mention counts — one query per movie title
    x_queries: Dict[str, str] = {}
    for m in movies:
        key = f"movie:{m['tmdb_id']}"
        x_queries[key] = f'"{m["title"]}" movie'
    # Also query actors and directors (by name) — these will be attached
    # to person entries downstream in derive_people().
    seen_people: set = set()
    for m in movies:
        for c in (m.get("cast_full") or [])[:6]:
            pid = c.get("id")
            name = c.get("name")
            if pid and name and pid not in seen_people:
                x_queries[f"actor:{pid}"] = f'"{name}"'
                seen_people.add(pid)
        for d in (m.get("directors_full") or []):
            pid = d.get("id")
            name = d.get("name")
            if pid and name and pid not in seen_people:
                x_queries[f"director:{pid}"] = f'"{name}"'
                seen_people.add(pid)

    x_counts = fetch_x_mentions_batch(x_queries)
    for m in movies:
        m["x_mentions"] = x_counts.get(f"movie:{m['tmdb_id']}", 0)
    x_hits = sum(1 for m in movies if m.get("x_mentions", 0) > 0)
    LOG.info("X mentions coverage: %d/%d (%d%%)", x_hits, len(movies),
             (x_hits * 100 // len(movies)) if movies else 0)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "movies":   movies,
        "news":     news_items,
        "x_counts": x_counts,
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
                  top_directors: int = 25,
                  x_counts: Optional[Dict[str, int]] = None) -> Dict[str, List[Dict[str, Any]]]:
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

    xc = x_counts or {}

    def _finalize(slot: Dict[str, Any], kind: str) -> Dict[str, Any]:
        n = slot["sentiment_n"] or 1
        slot["sentiment_pct"] = int(round(slot["sentiment_acc"] / n))
        slot["avg_film_score"] = int(round(slot["score_acc"] / n))
        slot["news_mentions"] = _name_news_mentions(slot["name"], news_items)
        slot["x_mentions"] = xc.get(f"{kind}:{slot['tmdb_id']}", 0)
        slot["mentions"] = len(slot["news_mentions"]) + slot["film_count"] * 5 + slot["x_mentions"]
        del slot["sentiment_acc"]
        del slot["sentiment_n"]
        del slot["score_acc"]
        return slot

    actor_list    = [_finalize(s, "actor") for s in actors.values()]
    director_list = [_finalize(s, "director") for s in directors.values()]

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
