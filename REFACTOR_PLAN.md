# Scheduler Refactor Plan

## Architecture: Priority-Based Polling Scheduler

Replace the monolithic `fetch_all()` pulse with an asyncio event loop that refreshes each signal independently, at cadences matched to each signal's natural rate of change and API cost.

### Tier Structure

| Signal | Top 20 | Mid (21-100) | Tail (100+) |
|--------|--------|-------------|------------|
| X mentions | 2 min | 10 min | 60 min |
| Reddit | 3 min | 15 min | 60 min |
| YouTube velocity | 10 min | 30 min | 60 min |
| News (RSS) | 15 min (batch) | 15 min (batch) | 60 min (batch) |
| Google Trends | 4 hr (batch) | 4 hr (batch) | 8 hr (batch) |

Batch signals (RSS, Trends) refresh all titles at once. Per-title signals (X, Reddit, YouTube) are scheduled individually.

### Four Questions Answered

#### 1. Estimate: How long to build and stabilize?

**Build: 1 night (this session).** The scheduler is a single new file (`scripts/scheduler.py`) with ~400 lines of asyncio code. It decomposes existing fetch functions into per-title callables and wraps them in a priority queue.

**Stabilize: 2-3 days of parallel running.** The scheduler runs alongside the existing `fetch_all()` pipeline. Both produce Ratings for the same titles. We compare outputs via the test harness until they converge, then swap.

**Risk period: 1 week.** After merging, the first week of production will surface edge cases (API quota spikes, Reddit rate limit patterns under sustained polling, asyncio error handling gaps). Plan to monitor daily for the first week.

#### 2. Risks

**API quota exhaustion.** The scheduler makes more frequent calls than the current 15-minute monolithic pulse. Mitigations:
- Reddit semaphore (max 10 concurrent, 1.5s spacing) keeps us at ~40 req/min, well under the ~60 req/min ceiling
- YouTube stays at 1 unit per stats call, total ~400-500/day (unchanged from current)
- X free tier at 1,200 req/hr; Top 20 × every 2 min = 600 req/hr = 50% of limit
- Google Trends stays batch with 4hr TTL (unchanged)

**Process crash.** If the asyncio loop dies, no signals refresh. Mitigation: keep the existing launchd + fetch_all() as a fallback. The scheduler writes to a separate `data/v2_scheduler.json` during testing. Only after merge does it take over `data/v2.json`.

**Rating jitter.** Per-signal updates mean a title's Rating can change when just one signal moves, even if others are stale. Mitigation: exponential smoothing (α=0.7) on displayed Rating, and the audit log captures every change for post-hoc analysis.

**Data race.** Multiple async tasks writing to the same movie dict simultaneously. Mitigation: all signal updates go through a single `update_signal()` function that acquires an asyncio Lock before modifying state.

#### 3. Monitoring

**Audit log** (`data/audit/rating_changes.jsonl`): append-only JSONL with every Rating change. Fields: timestamp, tmdb_id, title, signal_that_triggered, old_rating, new_rating, old_rank, new_rank. This is the primary monitoring artifact.

**Quota dashboard** (logged to `data/logs/scheduler.log`): each API call logs source, response time, status code. Aggregated hourly into a quota summary.

**Health check**: the scheduler writes a heartbeat to `data/cache/scheduler_heartbeat.json` every 60 seconds. If the heartbeat is stale (>5 min), the launchd fallback pulse takes over.

#### 4. When to Ship the Rating Migration

**After the scheduler is stable (3-5 days of parallel running).** The Rating migration (replacing min-max 800-999 with calibrated 100-1500) is a display-layer change that doesn't depend on the scheduler. However, shipping them together gives us:
- A clean switchover moment (old system → new system)
- The audit log captures the Rating migration as a visible step change
- Users see both improvements at once

**Recommended sequence:**
1. Merge scheduler to main, run in production for 3 days
2. Verify audit log shows stable, non-jittery Ratings
3. Apply Rating calibration curve (raw^0.50 × 74)
4. Migrate historical data
5. Update frontend labels
6. Push to production

### Implementation Files

| File | Purpose |
|------|---------|
| `scripts/scheduler.py` | Asyncio priority scheduler — the main new code |
| `scripts/signal_fetchers.py` | Per-title signal fetchers decomposed from fetch_data.py |
| `scripts/test_scheduler.py` | Test harness comparing old vs new outputs |
| `data/audit/rating_changes.jsonl` | Append-only audit log |
| `data/cache/scheduler_heartbeat.json` | Health check heartbeat |
| `QUESTIONS.md` | Decision points flagged during implementation |
| `REFACTOR_PLAN.md` | This document |
