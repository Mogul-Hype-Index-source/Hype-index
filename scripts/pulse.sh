#!/bin/bash
#
# MoviePass Hype Index V2 — hourly pulse
# =======================================
# 1. Run scripts/update.py (TMDb / YouTube / Reddit / Trends / X / RSS → HypeScore)
# 2. Commit any changes under data/
# 3. Push to GitHub so Pages picks up the new pulse
#
# Designed to be invoked by launchd every 60 minutes (or by hand).
# Logs everything to data/cache/pulse.log.
#
# Auth: reads a GitHub Personal Access Token from
#       $HOME/.config/moviepass-hypeindex/pat   (chmod 600, NOT in the repo)
# After rotating the PAT, just overwrite that file with the new value.

set -uo pipefail

# Absolute paths so launchd can run this with a minimal environment.
# NOTE: must NOT live under ~/Desktop — macOS TCC blocks background
# processes (launchd) from accessing Desktop. Symlink in ~/Desktop is fine.
REPO="/Users/stacyspikes/Hype-index"
PYTHON="$REPO/.venv/bin/python"
GIT="/usr/bin/git"
PAT_FILE="$HOME/.config/moviepass-hypeindex/pat"
LOG="$REPO/data/cache/pulse.log"
REMOTE_URL_BASE="github.com/Mogul-Hype-Index-source/Hype-index.git"

# ---- load .env so launchd gets the X API credentials ----
if [[ -f "$REPO/.env" ]]; then
  set -a
  source "$REPO/.env"
  set +a
fi

mkdir -p "$REPO/data/cache"

# ---- logging helper ----
log() {
  printf '%s  %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$LOG"
}

log "==== pulse start ===="
cd "$REPO" || { log "FATAL: cannot cd into $REPO"; exit 1; }

# ---- 0. SAFETY: never run if the operator has unstaged source edits ----
# A pulse that touches index.html / scripts/* via rebase can clobber an
# in-progress edit. If anything outside data/ is dirty, abort cleanly and
# wait for the next interval. Data files (v2.json, historical/) are ours.
DIRTY=$("$GIT" status --porcelain | awk '{print $2}' | grep -v -E '^(data/|$)' || true)
if [[ -n "$DIRTY" ]]; then
  log "ABORT: operator has unstaged changes outside data/ — skipping this pulse:"
  echo "$DIRTY" | sed 's/^/  - /' >> "$LOG"
  log "==== pulse end (safety abort) ===="
  exit 0
fi

# ---- 1. fetch + score + write data/v2.json ----
if ! "$PYTHON" scripts/update.py >> "$LOG" 2>&1; then
  log "FATAL: update.py failed"
  exit 1
fi

# ---- 2. stage data changes ----
# data/v2.json is the V2 payload (data/index.json is owned by the V1 cron).
"$GIT" add data/v2.json data/historical/ 2>> "$LOG"

if "$GIT" diff --cached --quiet; then
  log "no data changes, skipping commit"
  log "==== pulse end (no-op) ===="
  exit 0
fi

# ---- 3. commit (env-var identity, never touches git config) ----
TIMESTAMP=$(date -u '+%Y-%m-%d %H:%M UTC')
COMMIT_MSG="Hourly pulse — $TIMESTAMP"

GIT_AUTHOR_NAME="Hype Index Pulse" \
GIT_AUTHOR_EMAIL="stacy.spikes@gmail.com" \
GIT_COMMITTER_NAME="Hype Index Pulse" \
GIT_COMMITTER_EMAIL="stacy.spikes@gmail.com" \
"$GIT" commit -m "$COMMIT_MSG" >> "$LOG" 2>&1 || {
  log "FATAL: commit failed"
  exit 1
}

# ---- 4. push using the PAT inline (never written to git config) ----
if [[ ! -r "$PAT_FILE" ]]; then
  log "FATAL: PAT file not found at $PAT_FILE — commit succeeded but cannot push"
  exit 1
fi
PAT=$(tr -d '[:space:]' < "$PAT_FILE")
if [[ -z "$PAT" ]]; then
  log "FATAL: PAT file is empty"
  exit 1
fi

PUSH_URL="https://x-access-token:${PAT}@${REMOTE_URL_BASE}"

# Try once, if rejected because remote moved, rebase and retry once.
push_attempt() {
  "$GIT" push "$PUSH_URL" main 2>&1 | sed "s|${PAT}|***REDACTED***|g" >> "$LOG"
  return ${PIPESTATUS[0]}
}

if ! push_attempt; then
  log "push rejected, attempting rebase + retry"
  # Stash any unrelated working-tree changes (e.g. operator editing
  # index.html or scripts) so the rebase doesn't refuse to run. We
  # restore them after the rebase succeeds.
  STASH_CREATED=0
  if ! "$GIT" diff --quiet || ! "$GIT" diff --cached --quiet; then
    if "$GIT" stash push --include-untracked --keep-index --message "pulse-rebase-tmp" >> "$LOG" 2>&1; then
      STASH_CREATED=1
      log "stashed working-tree changes for rebase"
    fi
  fi
  GIT_AUTHOR_NAME="Hype Index Pulse" \
  GIT_AUTHOR_EMAIL="stacy.spikes@gmail.com" \
  GIT_COMMITTER_NAME="Hype Index Pulse" \
  GIT_COMMITTER_EMAIL="stacy.spikes@gmail.com" \
  "$GIT" pull --rebase "$PUSH_URL" main >> "$LOG" 2>&1
  REBASE_RC=$?
  if [[ $STASH_CREATED -eq 1 ]]; then
    "$GIT" stash pop >> "$LOG" 2>&1 || log "WARN: stash pop failed (manual cleanup needed)"
  fi
  if [[ $REBASE_RC -ne 0 ]]; then
    log "FATAL: rebase failed — manual intervention required"
    exit 1
  fi
  if ! push_attempt; then
    log "FATAL: push failed after rebase"
    exit 1
  fi
fi

log "pushed: $COMMIT_MSG"
log "==== pulse end ===="
exit 0
