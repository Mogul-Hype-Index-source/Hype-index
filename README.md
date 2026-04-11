# MoviePass Hype Index V2

A real-time Hollywood sentiment dashboard — Bloomberg terminal meets Billboard
Hot 100 meets Hyperliquid exchange. 100 movies ranked every hour by the
**HypeScore** (proprietary momentum-based scoring, 0–1000) using free public APIs.

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
  score.py              ← HypeScore momentum formula
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

## How the HypeScore works

```
HYPE_SCORE = (
    0.4 × normalize(short) +
    0.2 × normalize(acceleration) +
    0.4 × normalize(baseline)
) × log(1 + baseline) × 1000
```

Where baseline is calculated from sources:
```
baseline = (
    youtube_views  × 0.35 +
    x_mentions     × 0.25 +
    reddit_volume  × 0.20 +
    google_trends  × 0.15 +
    news_impact    × 0.05
)
```

- **baseline**: weighted sum of all source scores normalized against top performer
- **short**: average of last 3 data points
- **acceleration**: rate of change between short and previous short window

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
