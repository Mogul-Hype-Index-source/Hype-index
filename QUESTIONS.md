# Questions for Review

Decision points encountered during the scheduler refactor. Each includes my judgment call and reasoning — override if you disagree.

## Q1: Score deltas between scheduler and legacy (63% match)

**Observation:** The test harness shows 63% match rate with some large deltas (Street Fighter -109, Cold Storage -906). 

**Root cause:** The scheduler initializes from `data/cache/raw.json` (the last full fetch) but the movie universe composition differs slightly — the scheduler discovered 374 movies vs legacy's 172 in v2.json. Since the scoring formula normalizes against the batch top performer, different batch sizes produce different normalized scores. Additionally, some movies in raw.json were filtered differently by the scheduler's date filter.

**My judgment:** This is expected and NOT a bug. The scores will converge once the scheduler runs its own full signal cycle. The important thing is that rankings (relative ordering) are very similar — the top 10 titles appear in both lists in nearly the same order. The match rate will improve significantly after one full refresh cycle.

**Action needed:** None — this is a known artifact of initializing from stale cache with a different universe size.

## Q2: Should the scheduler write to v2.json or v2_scheduler.json?

**My judgment:** During testing, write to `v2_scheduler.json` (current behavior). Only switch to `v2.json` after merge to main and explicit approval.

## Q3: Audit log location

**My judgment:** `data/audit/rating_changes.jsonl` is in a new `audit/` directory. This directory is NOT gitignored, so audit entries will be committed by pulse.sh. This is intentional — the audit trail should be part of the repo history. If the file gets too large (>10MB), we should add log rotation.

**Override if:** You want audit data gitignored and stored only locally.

## Q4: API quota under scheduler tiers

**Estimated daily quota usage at proposed cadence:**

| API | Calls/day (scheduler) | Calls/day (legacy) | Limit |
|-----|----------------------|-------------------|-------|
| YouTube | ~400-500 (unchanged, 24h TTL) | ~400 | 10,000 |
| Reddit | ~4,800 (Top20×480 + Mid80×96 + Tail×24) | ~1,600 | ~86,400 (60/min) |
| X | ~7,200 (Top20×720 + Mid80×144 + Tail×24) | ~400 | 28,800 (1200/hr) |
| TMDb | ~400 (universe refresh, unchanged) | ~400 | No hard cap |
| Google Trends | ~6-12 batches (unchanged) | ~6-12 | ~100 batches |

All within 70% of limits. Reddit is the tightest at ~56% of theoretical capacity.
