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


def fetch_tmdb_credits(api_key: str, tmdb_id: int) -> Dict[str, str]:
    """Director (first one) + top 2 billed cast as comma strings."""
    data = _http_get(
        f"{TMDB_BASE}/movie/{tmdb_id}/credits",
        params={"api_key": api_key},
    )
    if not data:
        return {"director": "", "cast": ""}
    crew = data.get("crew") or []
    cast = data.get("cast") or []
    directors = [c.get("name") for c in crew if c.get("job") == "Director"]
    top_cast = [c.get("name") for c in cast[:2] if c.get("name")]
    return {
        "director": ", ".join(d for d in directors[:2] if d),
        "cast": ", ".join(top_cast),
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

def _trailer_cache_path() -> Path:
    return REPO_ROOT / "data" / "cache" / "youtube_trailers.json"


def _load_trailer_cache() -> Dict[str, str]:
    p = _trailer_cache_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:  # noqa: BLE001
        return {}


def _save_trailer_cache(cache: Dict[str, str]) -> None:
    p = _trailer_cache_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, indent=2))


def fetch_tmdb_trailer_video_id(api_key: str, tmdb_id: int) -> Optional[str]:
    """Pick the best YouTube trailer ID for a movie from TMDb /videos."""
    data = _http_get(
        f"{TMDB_BASE}/movie/{tmdb_id}/videos",
        params={"api_key": api_key, "language": "en-US"},
    )
    if not data:
        return None
    videos = data.get("results") or []
    # Preference order: official Trailer → any Trailer → any YouTube clip
    def _pick(predicate):
        for v in videos:
            if v.get("site") == "YouTube" and predicate(v):
                return v.get("key")
        return None

    return (
        _pick(lambda v: v.get("type") == "Trailer" and v.get("official"))
        or _pick(lambda v: v.get("type") == "Trailer")
        or _pick(lambda v: True)
    )


def fetch_youtube_video_stats(api_key: str, video_id: str) -> Dict[str, int]:
    """Fetch view/like/comment counts for a known YouTube video ID. Costs 1 quota unit."""
    empty = {"views": 0, "likes": 0, "comments": 0}
    if not video_id:
        return empty
    stats = _http_get(
        f"{YOUTUBE_BASE}/videos",
        params={"part": "statistics", "id": video_id, "key": api_key},
    )
    if not stats or not stats.get("items"):
        return empty
    s = stats["items"][0].get("statistics", {})
    return {
        "views":    int(s.get("viewCount", 0) or 0),
        "likes":    int(s.get("likeCount", 0) or 0),
        "comments": int(s.get("commentCount", 0) or 0),
    }


def fetch_youtube_trailer(api_key: str, title: str) -> Dict[str, int]:
    """
    Legacy entry point — kept for ad-hoc use. Costs 100+1 quota units per call
    because it does a full /search. Prefer fetch_tmdb_trailer_video_id +
    fetch_youtube_video_stats inside fetch_all().
    """
    empty = {"views": 0, "likes": 0, "comments": 0}
    if not title:
        return empty
    search = _http_get(
        f"{YOUTUBE_BASE}/search",
        params={
            "part": "snippet",
            "q": f"{title} official trailer",
            "type": "video",
            "maxResults": 1,
            "key": api_key,
        },
    )
    if not search:
        return empty
    items = search.get("items") or []
    if not items:
        return empty
    video_id = items[0].get("id", {}).get("videoId")
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

def fetch_google_trends(titles: List[str]) -> Dict[str, int]:
    """
    Returns {title: 0..100 interest score over the last 7 days}.
    pytrends caps comparisons at 5 keywords per request, so we batch.
    Falls back to {} on any error so the rest of the pipeline keeps running.
    """
    if not titles:
        return {}
    try:
        from pytrends.request import TrendReq  # type: ignore
    except ImportError:
        LOG.warning("pytrends not installed — skipping Google Trends. `pip install pytrends`")
        return {}

    out: Dict[str, int] = {}
    try:
        py = TrendReq(hl="en-US", tz=0)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("pytrends init failed: %s", exc)
        return {}

    batch_size = 5
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i + batch_size]
        try:
            py.build_payload(batch, timeframe="now 7-d", geo="")
            df = py.interest_over_time()
            if df is None or df.empty:
                for t in batch:
                    out[t] = 0
                continue
            for t in batch:
                if t in df.columns:
                    out[t] = int(df[t].mean() or 0)
                else:
                    out[t] = 0
        except Exception as exc:  # noqa: BLE001
            LOG.warning("pytrends batch failed (%s): %s", batch, exc)
            for t in batch:
                out.setdefault(t, 0)
        time.sleep(2.0)  # rate limit cushion
    return out


# ---------------------------------------------------------------------------
# RSS news feeds
# ---------------------------------------------------------------------------

def fetch_news_feeds(feeds: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Returns a flat, time-sorted list of news items from all configured feeds."""
    try:
        import feedparser  # type: ignore
    except ImportError:
        LOG.warning("feedparser not installed — skipping news. `pip install feedparser`")
        return []

    items: List[Dict[str, Any]] = []
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
            published_iso = ""
            if getattr(entry, "published_parsed", None):
                try:
                    published_iso = datetime(
                        *entry.published_parsed[:6], tzinfo=timezone.utc
                    ).isoformat()
                except Exception:  # noqa: BLE001
                    pass
            items.append({
                "headline":  (entry.get("title") or "").strip(),
                "source":    src,
                "url":       entry.get("link") or "",
                "published": published_iso,
                "summary":   re.sub(r"<[^>]+>", "", entry.get("summary", ""))[:240],
            })
    items.sort(key=lambda x: x.get("published") or "", reverse=True)
    LOG.info("RSS: pulled %d items across %d feeds", len(items), len(feeds))
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
    cache_dirty = False

    # 3. Per-movie enrichment
    for idx, m in enumerate(movies, 1):
        title = m["title"]
        LOG.info("[%d/%d] %s", idx, len(movies), title)

        creds = fetch_tmdb_credits(tmdb_key, m["tmdb_id"])
        m.update(creds)
        m["poster_url"] = (
            f"{poster_base}{m['poster_path']}" if m.get("poster_path") else None
        )

        # YouTube: TMDb supplies the trailer video ID (free), then YouTube
        # /videos returns stats (1 quota unit). Cache the video ID per
        # tmdb_id so we never spend a quota unit on /search again.
        try:
            cache_key = str(m["tmdb_id"])
            video_id = trailer_cache.get(cache_key)
            if not video_id:
                video_id = fetch_tmdb_trailer_video_id(tmdb_key, m["tmdb_id"])
                if video_id:
                    trailer_cache[cache_key] = video_id
                    cache_dirty = True
            m["youtube_video_id"] = video_id
            m["youtube"] = fetch_youtube_video_stats(yt_key, video_id)
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

    # Persist trailer cache for next run
    if cache_dirty:
        _save_trailer_cache(trailer_cache)
        LOG.info("Trailer cache: %d entries persisted", len(trailer_cache))

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
