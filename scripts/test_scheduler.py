"""
Test harness — compare scheduler output vs fetch_all() output
=============================================================

Runs both the new scheduler (--once mode) and the existing fetch_all()
against the same title universe, then compares Rating outputs.

Usage:
    python scripts/test_scheduler.py              # compare top 20
    python scripts/test_scheduler.py --limit 50   # compare top 50
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import score as score_module
from fetch_data import load_config, REPO_ROOT

LOG = logging.getLogger("test_scheduler")

OUTPUT_SCHEDULER = REPO_ROOT / "data" / "v2_scheduler.json"
OUTPUT_LEGACY = REPO_ROOT / "data" / "v2.json"


def load_movies(path: Path) -> dict:
    """Load movies keyed by tmdb_id."""
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    return {m["tmdb_id"]: m for m in data.get("movies", [])}


def compare(limit: int = 20):
    legacy = load_movies(OUTPUT_LEGACY)
    scheduler = load_movies(OUTPUT_SCHEDULER)

    if not legacy:
        print("ERROR: v2.json not found or empty")
        return
    if not scheduler:
        print("ERROR: v2_scheduler.json not found or empty")
        print("       Run: python scripts/scheduler.py --once")
        return

    # Sort both by score descending
    leg_sorted = sorted(legacy.values(), key=lambda m: m.get("score", 0), reverse=True)[:limit]
    sch_sorted = sorted(scheduler.values(), key=lambda m: m.get("score", 0), reverse=True)[:limit]

    print(f"\n{'='*75}")
    print(f"  COMPARISON: Legacy fetch_all() vs Scheduler (top {limit})")
    print(f"  Legacy movies: {len(legacy)}, Scheduler movies: {len(scheduler)}")
    print(f"{'='*75}\n")

    print(f"{'TITLE':<35} {'LEG SCORE':>10} {'SCH SCORE':>10} {'DELTA':>7} {'MATCH':>6}")
    print("-" * 75)

    matches = 0
    total = 0
    deltas = []

    for lm in leg_sorted:
        tid = lm["tmdb_id"]
        sm = scheduler.get(tid)
        l_score = lm.get("score", 0)

        if sm:
            s_score = sm.get("score", 0)
            delta = s_score - l_score
            match = "OK" if abs(delta) <= 5 else "DIFF"
            if abs(delta) <= 5:
                matches += 1
            deltas.append(abs(delta))
        else:
            s_score = "N/A"
            delta = "N/A"
            match = "MISS"

        total += 1
        print(f"  {lm.get('title', '?')[:33]:<35} {l_score:>10} {str(s_score):>10} {str(delta):>7} {match:>6}")

    print("-" * 75)
    if deltas:
        print(f"  Match rate: {matches}/{total} ({matches*100//total}%)")
        print(f"  Avg delta: {sum(deltas)/len(deltas):.1f}")
        print(f"  Max delta: {max(deltas)}")
    print()

    # Check for titles in scheduler but not legacy
    sch_only = set(scheduler.keys()) - set(legacy.keys())
    if sch_only:
        print(f"  Scheduler-only titles ({len(sch_only)}):")
        for tid in list(sch_only)[:5]:
            m = scheduler[tid]
            print(f"    {m.get('title', '?')} (score={m.get('score', 0)})")

    leg_only = set(legacy.keys()) - set(scheduler.keys())
    if leg_only:
        print(f"  Legacy-only titles ({len(leg_only)}):")
        for tid in list(leg_only)[:5]:
            m = legacy[tid]
            print(f"    {m.get('title', '?')} (score={m.get('score', 0)})")


def main() -> int:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20, help="Number of titles to compare")
    args = parser.parse_args()

    compare(limit=args.limit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
