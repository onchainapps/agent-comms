#!/bin/bash
# deploy/bootstrap.sh — one-time admin token mint (RFC-001 §5: bootstrap is
# LOCAL-ONLY; there is no HTTP bootstrap path). Run as the service user on the
# host, while the server is STOPPED or right after first start (the guard is
# anti-duplicate, not a security boundary — a second admin token needs force).
#
# usage: deploy/bootstrap.sh <home> [agent-id-for-admin]   (default: admin)
set -euo pipefail
HOME_DIR="${1:?usage: bootstrap.sh <comms-home> [admin-agent]}"
ADMIN="${2:-admin}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
[ -d "$HOME_DIR/.comms" ] || mkdir -p "$HOME_DIR/.comms"
chmod 700 "$HOME_DIR/.comms"
umask 077
TOK=$(COMMS_HOME="$HOME_DIR" bun "$REPO/bin/comms.ts" token create --agent "$ADMIN" --admin | grep -oE 'ac_[A-Za-z0-9_-]{43}')
echo "admin token (shown ONCE — store in your secret manager): $TOK"
echo "next: mint per-agent tokens over HTTP:"
echo "  comms.ts (COMMS_URL=https://… COMMS_TOKEN=*** token create --agent <id>"
