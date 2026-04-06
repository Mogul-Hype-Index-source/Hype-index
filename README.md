# MoviePass Hype Index V2

A real-time Hollywood sentiment dashboard — Bloomberg terminal meets Billboard
Hot 100 meets Hyperliquid exchange. 100 movies ranked every hour by the
**AMSI score** (Animoca Movie Sentiment Index, 0–1000) using free public APIs.

Live: https://mogul-hype-index-source.github.io/Hype-index/

## What's in here

```
index.html              ← Single-page dashboard (vanilla JS, reads data/index.json)
config.json             ← API keys (gitignored — copy from config.example.json)
data/
  index.json            ← Public payload the frontend reads (rewritten hourly)
  historical/           ← Per-day snapshots, used to compute rank movement & windows
  cache/                ← Raw fetch cache + last_run.json (gitignored)
scripts/
  fetch_data.py         ← Pulls TMDb / YouTube / Reddit / Trends / RSS
  score.py              ← AMSI scoring formula (see HypeIndex_V2_Spec.md §3)
  update.py             ← Master orchestrator — run hourly
  requirements.txt
```

## First-time setup

```bash
cp config.example.json config.json
# edit config.json and paste your YouTube + TMDb API keys
python3 -m venv .venv
.venv/bin/pip install -r scripts/requirements.txt
```

## Running

One-shot (good for testing):
```bash
.venv/bin/python scripts/update.py            # full 100-movie cycle
.venv/bin/python scripts/update.py --limit 10 # quick smoke test
.venv/bin/python scripts/update.py --skip-fetch  # re-score from cached raw.json
```

Continuous (production):
```bash
.venv/bin/python scripts/update.py --loop     # runs forever, sleeps 60 min between cycles
```

Or schedule via cron — every hour at :00:
```cron
0 * * * * cd /path/to/Hype-index && .venv/bin/python scripts/update.py >> data/cache/cron.log 2>&1
```

## How the AMSI score works

```
AMSI = (
    youtube_views_score    × 0.30 +
    youtube_engagement     × 0.15 +
    reddit_volume_score    × 0.20 +
    google_trends_score    × 0.20 +
    news_impact_score      × 0.15
) × 1000
```

Each sub-score is normalized against the top performer in the current batch
(top = 1.0), so the leaderboard is always relative to today's slate.
The 1D / 7D / 30D snapshot toggle in the UI is back-filled from
`data/historical/` snapshots — once you've been running daily for a week,
the time-window momentum becomes the killer feature vs V1.

## Deploy

The repo is deployed to GitHub Pages from `main`. After running `update.py`
locally, commit the regenerated `data/index.json` and push:

```bash
git add data/index.json data/historical/
git commit -m "Hourly pulse update"
git push origin main
```

GitHub Pages will pick up the new data within 30s.

## Security

`config.json` holds API keys and is gitignored. Never commit it. If you ever
suspect a key has leaked, rotate it immediately:

- TMDb: https://www.themoviedb.org/settings/api
- YouTube: https://console.cloud.google.com/apis/credentials
