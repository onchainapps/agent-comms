#!/bin/bash
# deploy/bootstrap.sh — one-time admin token mint (RFC-001 §5: bootstrap is
# LOCAL-ONLY; there is no HTTP bootstrap path). Run as the service user
# BEFORE the first server start. It creates the DB with the right owner and
# modes, and holds the §9 single-writer latch, so it refuses to run while a
# server holds it.
#
# usage: deploy/bootstrap.sh <comms-home> [agent-id-for-admin]   (default: admin)
set -euo pipefail
HOME_DIR="${1:?usage: bootstrap.sh <comms-home> [admin-agent]}"
ADMIN="${2:-admin}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
BUN="${BUN:-$(command -v bun || echo /usr/local/bin/bun)}"
[ -x "$BUN" ] || { echo "bootstrap: bun not found (set BUN=/abs/path/bun)" >&2; exit 1; }
umask 077
mkdir -p "$HOME_DIR/.comms"
chmod 700 "$HOME_DIR" "$HOME_DIR/.comms"
exec 9>"$HOME_DIR/.server.lock"
flock -n 9 || { echo "bootstrap: a server holds $HOME_DIR/.server.lock — stop it first (§9 single writer)" >&2; exit 1; }
OUT=$(COMMS_HOME="$HOME_DIR" "$BUN" "$REPO/bin/comms.ts" token create --agent "$ADMIN" --admin 2>&1) || { echo "$OUT" >&2; exit 1; }
TOK=$(printf '%s\n' "$OUT" | grep -oE 'ac_[A-Za-z0-9_-]{43}') || { echo "bootstrap: $OUT" >&2; exit 1; }
echo "admin token (shown ONCE — store in your secret manager): $TOK"
echo "next: start the server, then mint per-agent tokens over HTTP:"
echo "  COMMS_URL=https://… COMMS_TOKEN=*** bun bin/comms.ts token create --agent <id>"
