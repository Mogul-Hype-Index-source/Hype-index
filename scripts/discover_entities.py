"""
Hype Index V2 — automatic entity discovery from news feeds
==========================================================

Scans RSS headlines for film/actor/director announcements not yet
tracked in data/v2.json or data/manual_movies.json. Resolves
new finds via TMDb search and either auto-adds them to
manual_movies.json or queues them for manual review.

Run:
    python scripts/discover_entities.py          # one-shot
    python scripts/discover_entities.py --dry    # preview only, no writes
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

sys.path.insert(0, str(Path(__file__).resolve().parent))

import requests

LOG = logging.getLogger("discover")

REPO_ROOT = Path(__file__).resolve().parent.parent
MANUAL_PATH = REPO_ROOT / "data" / "manual_movies.json"
V2_PATH = REPO_ROOT / "data" / "v2.json"
QUEUE_PATH = REPO_ROOT / "data" / "discovery_queue.json"
LOG_DIR = REPO_ROOT / "data" / "logs"
LOG_PATH = LOG_DIR / "discovery.log"
CONFIG_PATH = REPO_ROOT / "config.json"

TMDB_BASE = "https://api.themoviedb.org/3"
REQUEST_TIMEOUT = 15

# RSS feeds to scan — existing config feeds plus CinemaCon-specific sources
DISCOVERY_FEEDS = [
    "https://deadline.com/feed/",
    "https://variety.com/feed/",
    "https://www.hollywoodreporter.com/feed/",
    "https://www.indiewire.com/feed/",
    "https://bleedingcool.com/feed/",
]

# Keywords that signal a film/entity announcement headline
ANNOUNCEMENT_KEYWORDS = [
    "cinemacon", "cinema con",
    "announced", "announces", "unveils", "reveals",
    "first look", "first trailer", "new trailer",
    "greenlit", "greenlight", "green-lit",
    "confirmed", "in development",
    "set to star", "joins cast", "cast in", "to star in",
    "to direct", "will direct", "directing",
    "new film", "new movie", "upcoming film", "upcoming movie",
    "sequel", "reboot", "remake", "adaptation",
    "release date", "dated for", "opening",
    "sony pictures", "universal", "warner bros", "disney",
    "paramount", "lionsgate", "a24", "netflix", "amazon",
]

# Patterns to extract potential film titles from headlines
# Matches: quoted titles, title-case phrases near keywords
TITLE_PATTERNS = [
    # Quoted titles: 'Film Title' or "Film Title"
    re.compile(r"['\u2018\u2019\u201c\u201d\"]+([A-Z][^'\u2018\u2019\u201c\u201d\"]{2,60})['\u2018\u2019\u201c\u201d\"]+"),
    # Title after colon: "Studio Announces: Film Title"
    re.compile(r":\s+([A-Z][A-Za-z0-9\s:'\-&]{2,50})(?:\s+(?:Film|Movie|Sequel|Set|Gets|Will|Is|Stars)|\s*$)"),
]

# Patterns to extract person names
PERSON_PATTERNS = [
    # "Actor Name Joins/Stars In/Cast In"
    re.compile(r"([A-Z][a-z]+ [A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:joins|stars|cast|set to|boards|attached|to star|to direct|will direct|directing)"),
    # "Starring Actor Name"
    re.compile(r"(?:starring|directed by|from director|with)\s+([A-Z][a-z]+ [A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)"),
]


def _load_config() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        return json.loads(CONFIG_PATH.read_text())
    return {}


def _load_json(p: Path) -> Any:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _save_json(p: Path, data: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def _existing_titles() -> Set[str]:
    """Collect all known movie titles from v2.json and manual_movies.json."""
    titles: Set[str] = set()

    v2 = _load_json(V2_PATH)
    if v2:
        for m in v2.get("movies", []):
            titles.add(m.get("title", "").strip().lower())

    manual = _load_json(MANUAL_PATH) or []
    for m in manual:
        titles.add(m.get("title", "").strip().lower())

    return titles


def _fetch_headlines() -> List[Dict[str, str]]:
    """Fetch headlines from discovery feeds."""
    try:
        import feedparser
    except ImportError:
        LOG.warning("feedparser not installed — cannot discover entities")
        return []

    headlines: List[Dict[str, str]] = []
    for url in DISCOVERY_FEEDS:
        try:
            parsed = feedparser.parse(url)
            for entry in (parsed.entries or [])[:50]:
                title = (entry.get("title") or "").strip()
                if not title:
                    continue
                headlines.append({
                    "headline": title,
                    "url": entry.get("link") or "",
                    "source": url.split("/")[2] if "/" in url else url,
                })
        except Exception as exc:
            LOG.warning("Feed fetch failed %s: %s", url, exc)
        time.sleep(0.5)

    LOG.info("Fetched %d headlines from %d feeds", len(headlines), len(DISCOVERY_FEEDS))
    return headlines


def _is_announcement(headline: str) -> bool:
    """Check if a headline contains announcement keywords."""
    lower = headline.lower()
    return any(kw in lower for kw in ANNOUNCEMENT_KEYWORDS)


def _extract_titles(headline: str) -> List[str]:
    """Extract potential film titles from a headline."""
    titles: List[str] = []
    for pat in TITLE_PATTERNS:
        for match in pat.finditer(headline):
            candidate = match.group(1).strip()
            # Filter out too-short or clearly not titles
            if len(candidate) < 3 or len(candidate) > 60:
                continue
            # Must have at least one uppercase letter
            if not any(c.isupper() for c in candidate):
                continue
            titles.append(candidate)
    return titles


def _extract_people(headline: str) -> List[str]:
    """Extract potential person names from a headline."""
    names: List[str] = []
    for pat in PERSON_PATTERNS:
        for match in pat.finditer(headline):
            name = match.group(1).strip()
            if len(name.split()) >= 2:
                names.append(name)
    return names


def _infer_tags(headline: str) -> List[str]:
    """Infer topic tags from headline keywords."""
    tags: List[str] = []
    lower = headline.lower()
    tag_keywords = {
        "Horror": ["horror", "terrif", "scary"],
        "Action": ["action", "thriller", "explosive"],
        "Comedy": ["comedy", "funny", "comedic"],
        "Drama": ["drama", "dramatic"],
        "Animation": ["animated", "animation", "anime"],
        "Documentary": ["documentary", "doc", "docuseries"],
        "Sequel": ["sequel", "part 2", "part 3", "chapter"],
        "Reboot": ["reboot", "remake", "reimagining"],
        "Video Game": ["video game", "game adaptation", "playstation", "nintendo", "xbox"],
        "Superhero": ["superhero", "marvel", "dc ", "comic book"],
        "CinemaCon": ["cinemacon", "cinema con"],
    }
    for tag, keywords in tag_keywords.items():
        if any(kw in lower for kw in keywords):
            tags.append(tag)

    # Studio tags
    studio_map = {
        "Sony": ["sony"], "Disney": ["disney"], "Universal": ["universal"],
        "Warner Bros": ["warner"], "Paramount": ["paramount"],
        "Netflix": ["netflix"], "A24": ["a24"], "Lionsgate": ["lionsgate"],
    }
    for tag, keywords in studio_map.items():
        if any(kw in lower for kw in keywords):
            tags.append(tag)

    return tags or ["Untagged"]


def _tmdb_search(api_key: str, title: str) -> Optional[Dict[str, Any]]:
    """Search TMDb for a film. Returns result with confidence score."""
    try:
        r = requests.get(
            f"{TMDB_BASE}/search/movie",
            params={"api_key": api_key, "query": title, "language": "en-US"},
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            return None
        results = r.json().get("results") or []
        if not results:
            return None
        top = results[0]
        # Confidence: how well does the TMDb title match our query?
        tmdb_title = (top.get("title") or "").lower()
        query_lower = title.lower()
        if tmdb_title == query_lower:
            confidence = 1.0
        elif query_lower in tmdb_title or tmdb_title in query_lower:
            confidence = 0.85
        else:
            # Jaccard similarity on words
            q_words = set(query_lower.split())
            t_words = set(tmdb_title.split())
            intersection = q_words & t_words
            union = q_words | t_words
            confidence = len(intersection) / len(union) if union else 0.0

        return {
            "tmdb_id": top["id"],
            "title": top.get("title") or title,
            "release_date": top.get("release_date") or "",
            "popularity": top.get("popularity") or 0.0,
            "confidence": round(confidence, 2),
        }
    except Exception as exc:
        LOG.warning("TMDb search failed for %s: %s", title, exc)
        return None


def run(dry: bool = False) -> Dict[str, Any]:
    cfg = _load_config()
    tmdb_key = cfg.get("tmdb_api_key", "")

    existing = _existing_titles()
    LOG.info("Existing tracked titles: %d", len(existing))

    # 1. Fetch headlines
    headlines = _fetch_headlines()

    # 2. Filter for announcements and extract entities
    candidates: Dict[str, Dict[str, Any]] = {}  # title → info
    people_candidates: List[str] = []

    for h in headlines:
        text = h["headline"]
        if not _is_announcement(text):
            continue

        for title in _extract_titles(text):
            key = title.strip().lower()
            if key in existing or key in candidates:
                continue
            candidates[key] = {
                "title": title,
                "headline": text,
                "source": h["source"],
                "tags": _infer_tags(text),
            }

        for name in _extract_people(text):
            if name.lower() not in [p.lower() for p in people_candidates]:
                people_candidates.append(name)

    LOG.info("Candidates found: %d films, %d people", len(candidates), len(people_candidates))

    if dry:
        LOG.info("DRY RUN — not writing any files")
        for key, info in candidates.items():
            LOG.info("  CANDIDATE: %s (from: %s)", info["title"], info["headline"][:80])
        return {"headlines": len(headlines), "candidates": len(candidates), "added": 0, "queued": 0}

    # 3. Resolve via TMDb and categorize
    auto_added: List[Dict[str, Any]] = []
    queued: List[Dict[str, Any]] = []
    today = datetime.now(timezone.utc).date().isoformat()

    manual = _load_json(MANUAL_PATH) or []
    manual_titles = {m.get("title", "").strip().lower() for m in manual}

    queue = _load_json(QUEUE_PATH) or []
    queue_titles = {q.get("title", "").strip().lower() for q in queue}

    for key, info in candidates.items():
        if key in manual_titles or key in queue_titles:
            continue

        result = _tmdb_search(tmdb_key, info["title"]) if tmdb_key else None
        time.sleep(0.2)

        if result and result["confidence"] >= 0.8:
            entry = {
                "title": result["title"],
                "search_query": info["title"],
                "tags": info["tags"],
                "source": "auto-discovered",
                "discovered_date": today,
            }
            manual.append(entry)
            manual_titles.add(result["title"].lower())
            auto_added.append({**entry, "tmdb_id": result["tmdb_id"],
                               "confidence": result["confidence"]})
            LOG.info("AUTO-ADDED: %s (tmdb=%d, confidence=%.2f)",
                     result["title"], result["tmdb_id"], result["confidence"])
        else:
            q_entry = {
                "title": info["title"],
                "headline": info["headline"],
                "source": info["source"],
                "tags": info["tags"],
                "discovered_date": today,
                "tmdb_match": result["title"] if result else None,
                "tmdb_confidence": result["confidence"] if result else 0.0,
            }
            queue.append(q_entry)
            queue_titles.add(info["title"].lower())
            queued.append(q_entry)
            LOG.info("QUEUED: %s (confidence=%.2f)",
                     info["title"], result["confidence"] if result else 0.0)

    # 4. Write results
    if auto_added:
        manual.sort(key=lambda x: x.get("title", ""))
        _save_json(MANUAL_PATH, manual)
        LOG.info("Updated manual_movies.json: %d total entries", len(manual))

    if queued:
        _save_json(QUEUE_PATH, queue)
        LOG.info("Updated discovery_queue.json: %d total entries", len(queue))

    # 5. Write discovery log
    summary = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "headlines_scanned": len(headlines),
        "announcement_candidates": len(candidates),
        "auto_added": len(auto_added),
        "queued_for_review": len(queued),
        "added_titles": [a["title"] for a in auto_added],
        "queued_titles": [q["title"] for q in queued],
    }

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a") as f:
        f.write(json.dumps(summary) + "\n")

    LOG.info("Discovery complete: %d headlines, %d added, %d queued",
             len(headlines), len(auto_added), len(queued))

    return summary


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Hype Index — entity discovery")
    parser.add_argument("--dry", action="store_true", help="Preview only, no writes")
    args = parser.parse_args()

    run(dry=args.dry)
    return 0


if __name__ == "__main__":
    sys.exit(main())
