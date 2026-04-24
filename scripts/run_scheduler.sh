#!/bin/bash
# Wrapper for launchd to run the scheduler with proper environment
REPO="/Users/stacyspikes/Hype-index"
PYTHON="$REPO/.venv/bin/python"
LOG="$REPO/data/logs/scheduler_wrapper.log"

mkdir -p "$REPO/data/logs"

echo "$(date -u) wrapper starting" >> "$LOG"

cd "$REPO" || { echo "$(date -u) FATAL: cd failed" >> "$LOG"; exit 1; }

# Load .env for X API credentials
if [[ -f "$REPO/.env" ]]; then
  set -a
  source "$REPO/.env"
  set +a
fi

echo "$(date -u) launching python" >> "$LOG"
exec "$PYTHON" -u scripts/scheduler.py 2>&1
