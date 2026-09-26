#!/bin/bash
# deploy/restore.sh — restore a backup AND mechanically rotate meta.epoch
# (RFC-001 §9: "never a manual step, never on plain server start"). Without
# rotation, client cursors carrying the OLD epoch would be treated as foreign
# (that part is safe — resync), but any client that committed at the SAME
# epoch name could skip; and SSE ids embed the epoch. Rotation makes every
# pre-restore cursor resync deterministically.
#
# usage: deploy/restore.sh <backup.db> <comms-home>
# Order: STOP the server → restore → rotate → START. (restore runs LOCAL mode,
# which is the only mode allowed to write meta from outside the server.)
set -euo pipefail
BK="${1:?usage: restore.sh <backup.db> <comms-home>}"
HOME_DIR="${2:?usage: restore.sh <backup.db> <comms-home>}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
[ -f "$BK" ] || { echo "no backup at $BK" >&2; exit 1; }
sqlite3 "$BK" "PRAGMA integrity_check" | head -1 | grep -qx ok || { echo "backup fails integrity_check — refusing" >&2; exit 1; }
umask 077
mkdir -p "$HOME_DIR/.comms"
# atomic-ish replace: backup first (the thing we are about to lose), then copy
# db + drop -wal/-shm (a restored db must start clean; WAL files from the old
# db are NOT valid against the restored image).
if [ -f "$HOME_DIR/.comms/comms.db" ]; then
  cp "$HOME_DIR/.comms/comms.db" "$HOME_DIR/.comms/comms.db.prerestore-$(date -u +%Y%m%dT%H%M%SZ)"
fi
cp "$BK" "$HOME_DIR/.comms/comms.db"
rm -f "$HOME_DIR/.comms/comms.db-wal" "$HOME_DIR/.comms/comms.db-shm"
chmod 600 "$HOME_DIR/.comms/comms.db"*
# mechanical epoch rotation THROUGH THE CORE (same code path as the server's
# rotateEpoch: new random 16-byte epoch + gc_floor=0 — never hand-rolled SQL).
# NB: openBus takes the BUS HOME (it appends .comms/comms.db itself) — pass
# $HOME_DIR, not $HOME_DIR/.comms, or the path double-nests.
NEW=$(cd "$REPO" && bun -e '
import { openBus } from "./src/bus.ts";
const b = openBus({ home: process.argv[1], mode: "local" });
console.log(b.rotateEpoch());
b.close();
' "$HOME_DIR")
echo "restored $BK → $HOME_DIR; epoch rotated to $NEW"
echo "all pre-restore cursors now resync (§6) — clients re-baseline via history."
echo "start the server."
