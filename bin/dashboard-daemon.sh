#!/usr/bin/env bash
# Resilient supervisor for the agent-comms dashboard.
# Launched detached (setsid) so it survives session teardown; while-true so it
# survives crashes. The sleep lives INSIDE this loop (never in the launching
# shell command, where a foreground sleep is blocked/SIGTERMed).
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || exit 1
unset COMMS_HOME
LOG=${COMMS_DASH_LOG:-/tmp/comms-dash.log}
echo "[$(date -u +%FT%TZ)] dashboard supervisor start (pid $$) root=$ROOT" >>"$LOG"
while true; do
  bun "$ROOT/bin/dashboard.ts" --port "${COMMS_DASH_PORT:-8787}" >>"$LOG" 2>&1
  echo "[$(date -u +%FT%TZ)] dashboard exited rc=$?, restart in 2s" >>"$LOG"
  sleep 2
done
