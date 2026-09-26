#!/bin/bash
# deploy/restore.sh — restore a backup as an EPOCH EVENT (RFC-001 §9).
#
# Invariants (claude M6 review):
#  I1 never runs against a live writer (systemd unit active OR any process
#     holding comms.db*) — a restore under a live server is silently undone.
#  I2 atomic: the live home only ever holds (old image) or (restored image
#     WITH rotated epoch). Rotation + migration run on a STAGED copy; the
#     swap is a rename. A failure anywhere leaves the old image untouched —
#     the "restored image with the OLD epoch" state (silent-skip for
#     client-held cursors) is unreachable.
#  I3 the pre-restore safety copy is a real SQLite backup (includes committed
#     WAL rows), never cp.
#  I4 revocations newer than the backup are carried forward (a restore must
#     not resurrect a token revoked because it leaked).
#
# usage: deploy/restore.sh <backup.db> <comms-home>     (env: UNIT=agent-comms, BUN=/abs/bun)
set -euo pipefail
BK="${1:?usage: restore.sh <backup.db> <comms-home>}"
HOME_DIR="${2:?usage: restore.sh <backup.db> <comms-home>}"
UNIT="${UNIT:-agent-comms}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
BUN="${BUN:-$(command -v bun || echo /usr/local/bin/bun)}"
DB="$HOME_DIR/.comms/comms.db"
die() { echo "restore: $*" >&2; exit 1; }

# ---- preflight: everything that can fail, BEFORE touching the live home ----
[ -x "$BUN" ] || die "bun not found (set BUN=/abs/path/bun)"
command -v sqlite3 >/dev/null || die "sqlite3 CLI not found"
[ -f "$BK" ] || die "no backup at $BK"
sqlite3 "$BK" "PRAGMA integrity_check" | head -1 | grep -qx ok || die "backup fails integrity_check — refusing"
# I1: live-writer guard. First the §9 latch (the same lock the unit's flock
# holds), then systemd for both managers, then any open handle on the db.
# The fuser check catches a dashboard or local CLI, which do not take the latch.
mkdir -p "$HOME_DIR/.comms"
exec 9>"$HOME_DIR/.server.lock"
flock -n 9 || die "a server holds $HOME_DIR/.server.lock — stop it first (§9 single writer)"
for scope in "" "--user"; do
  if systemctl $scope is-active --quiet "$UNIT" 2>/dev/null; then die "$UNIT is active ($scope) — stop it first"; fi
done
if command -v fuser >/dev/null && fuser -s "$DB" "$DB-wal" "$DB-shm" 2>/dev/null; then
  die "a process still holds $DB (server, dashboard, local CLI) — stop it first"
fi

umask 077
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
STAGE="$HOME_DIR/.restore-stage-$STAMP"
mkdir -p "$STAGE/.comms"
trap 'rm -rf "$STAGE"' EXIT
cp "$BK" "$STAGE/.comms/comms.db"          # the backup is a closed, WAL-less file: cp is correct HERE

# I3: safety copy of the CURRENT image, WAL included
if [ -f "$DB" ]; then
  PRE="$HOME_DIR/.comms/comms.db.prerestore-$STAMP"
  sqlite3 "$DB" ".backup '$PRE'"
  sqlite3 "$PRE" "PRAGMA journal_mode=DELETE" >/dev/null   # single self-contained file (no -wal/-shm siblings)
  chmod 600 "$PRE"
else PRE=""; fi

# I2 + I4: migrate + carry revocations + rotate — THROUGH THE CORE, on the stage
NEW=$(cd "$REPO" && "$BUN" -e '
import { openBus, localCtx } from "./src/bus.ts";
import { Database } from "bun:sqlite";
const [stage, pre] = [process.argv[1], process.argv[2]];
const b = openBus({ home: stage, mode: "local" });   // runs schema migration on the staged image
let carried = 0;
if (pre) {
  const old = new Database(pre, { readonly: true });
  const revoked = new Set((old.query("SELECT prefix FROM tokens WHERE revoked_at IS NOT NULL").all() as any[]).map((r) => r.prefix));
  old.close();
  const ctx = localCtx("restore");
  for (const t of b.tokenList(ctx).value!.tokens)
    if (!t.revoked_at && revoked.has(t.prefix)) { b.tokenRevoke(ctx, { id: t.id }); carried++; }
}
const e = b.rotateEpoch();
b.close();                                            // checkpoint: stage is a single clean file
console.log(e + " " + carried);
' "$STAGE" "$PRE")
set -- $NEW; NEW_EPOCH=$1; CARRIED=$2
[ -f "$STAGE/.comms/comms.db-wal" ] && die "stage did not checkpoint cleanly — refusing swap"
sqlite3 "$STAGE/.comms/comms.db" "PRAGMA integrity_check" | head -1 | grep -qx ok || die "staged image fails integrity_check"

# swap: rename is atomic on the same filesystem; stale -wal/-shm belong to the OLD image
rm -f "$DB-wal" "$DB-shm"
mv "$STAGE/.comms/comms.db" "$DB"
chmod 600 "$DB"
echo "restored $BK → $HOME_DIR"
echo "  epoch rotated to $NEW_EPOCH; revocations carried forward: $CARRIED; safety copy: ${PRE:-none}"
echo "  every pre-restore cursor now resyncs (§6). Messages posted after the backup are GONE;"
echo "  consumers re-receive retained history and MUST dedupe by message id."
echo "  start the server."
