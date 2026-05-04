# MoviePass Hype Index V2

A real-time cultural attention tracker for theatrical films. The Hype Index
measures **how much people are talking about a movie right now** — not whether
it's good, not whether it will make money. Pure attention signal.

Live: https://mogul-hype-index-source.github.io/Hype-index/

## Two views of the world

### Hype Index (cumulative)

The main ranking. Scores every tracked film on a 100–1500 calibrated scale
using all available signal history. Updated hourly. This is the "all-time
momentum" view — a film's overall cultural footprint.

### 1-Day view (last 24 hours)

What dominated attention **today**. Uses only activity from the last 24 hours:
true tweet volumes, YouTube view deltas, Trends movement, and new press
coverage. A film can be #80 on the cumulative index but #3 on the 1-Day chart
if it's having a breakout moment.

## Signals

| Signal | Source | Weight | What it measures |
|--------|--------|--------|-----------------|
| **YouTube** | YouTube Data API v3 | 0.35 | Consumption — trailer views, clips, reactions |
| **X (Twitter)** | /2/tweets/counts/recent | 0.30 | Conversation — hourly tweet volume per title |
| **Google Trends** | pytrends | 0.25 | Intent — search interest relative to baseline |
| **News** | RSS (Deadline, Variety, THR, IndieWire) | 0.10 | Amplification — press coverage |

For the 1-Day view, each signal is the **true last-24-hour value**:

- **X**: sum of hourly tweet counts from the API (not a rolling-window delta)
- **YouTube**: current view count minus the closest snapshot from ~24h ago
- **Google**: Trends score delta vs ~24h ago
- **News**: unique articles published in the last 24 hours

## Scoring model

```
Altitude_24h = 0.35 × YouTube_norm + 0.30 × X_norm + 0.25 × Google_norm + 0.10 × News_norm
Consensus_24h = (signals above median) / 4
Hype_1D = 0.75 × Altitude_24h + 0.25 × Consensus_24h
```

**Altitude** captures the magnitude of attention. **Consensus** captures the
breadth — a film trending on 3 of 4 platforms scores higher than one
dominating a single channel.

## Theatrical-only constraint

The 1-Day view filters to the **active theatrical window**:

- Films releasing within **365 days** (upcoming through pre-release)
- Films up to **90 days** after theatrical release (opening through late-run)

This excludes streaming-only titles, video game adaptations years from release,
and post-theatrical catalog titles. The cumulative index tracks a broader
universe.

## Architecture

```
scripts/
  fetch_data.py       ← Signal collection (TMDb, YouTube, X, Trends, RSS)
  score.py            ← Altitude/Consensus scoring model
  update.py           ← Master orchestrator — run hourly via launchd
  scheduler.py        ← Async priority scheduler (alternating X pulses)
data/
  v2.json             ← Public payload (frontend reads this)
  historical/         ← Daily snapshots for delta computation
  cache/              ← API response caches (gitignored)
  manual_movies.json  ← Curated title list with query overrides
index.html            ← Single-page dashboard
movie.html            ← Per-title detail page
```

## Running

```bash
# One-shot
python scripts/update.py

# Re-score without fetching (uses cached data)
python scripts/update.py --skip-fetch

# Quick test with limited titles
python scripts/update.py --limit 20
```

Production runs every 15 minutes via launchd (`com.moviepass.hypeindex.pulse.plist`).

## Evolving model

This is v2 of an evolving system. The scoring weights, signal sources, and
filtering rules are subject to change as we validate against real-world
theatrical performance. The goal is a reliable leading indicator of cultural
momentum — not a static formula.

## Security

- `config.json` and `.env` hold API keys and are gitignored
- X bearer tokens are never logged (first 6 + `[REDACTED]` + last 4 only)
- No credentials in the public payload or git history
