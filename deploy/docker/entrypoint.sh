#!/bin/sh
# deploy/docker/entrypoint.sh — the §9 discipline the systemd unit provides on
# a bare host, reproduced for containers (there is no systemd inside):
#   1. first run on an empty volume ⇒ local bootstrap mints the admin token
#      (shown ONCE — it lands in `docker compose logs`, store it, then silence
#      it with COMMS_BOOTSTRAP_QUIET=1);
#   2. the server runs under the SAME flock latch file bootstrap.sh uses, so
#      `docker run` twice against one volume ⇒ the second container exits 75
#      LOUD (not silent double-writer corruption).
set -eu

HOME_DIR="${COMMS_HOME:-/data}"
LOCK="$HOME_DIR/.server.lock"
BUN="${BUN:-$(command -v bun)}"

if [ ! -f "$HOME_DIR/.comms/comms.db" ]; then
  echo "entrypoint: empty volume — bootstrapping admin token (shown ONCE)" >&2
  deploy/bootstrap.sh "$HOME_DIR" "${COMMS_BOOTSTRAP_AGENT:-admin}" >&2
fi

# -E 75: exit code when the lock is held by another writer (§9 refusal).
exec flock -n -E 75 "$LOCK" "$BUN" bin/server.ts
