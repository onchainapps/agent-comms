#!/bin/bash
# deploy/backup.sh — out-of-process SQLite backup (RFC-001 §9).
#  - NEVER cp: the db is in WAL mode.
#  - NEVER run on the server's own connection.
#  - Run as the service user, umask 077: the backup contains token digests.
# sqlite3 .backup is hot-safe.
# The messages/ mirror is DERIVED (the db is authoritative), so it is not
# copied. KEEP_DAYS (default 14) prunes old artifacts. Ship DEST off-host: a
# backup on the same disk as the db does not survive disk loss.
#
# usage: deploy/backup.sh <comms-home> <dest-dir>
set -euo pipefail
HOME_DIR="${1:?usage: backup.sh <comms-home> <dest-dir>}"
DEST="${2:?usage: backup.sh <comms-home> <dest-dir>}"
KEEP_DAYS="${KEEP_DAYS:-14}"
DB="$HOME_DIR/.comms/comms.db"
[ -f "$DB" ] || { echo "no db at $DB" >&2; exit 1; }
umask 077
cd / # sudo -u comms keeps the caller's cwd; find -delete restores it at exit
     # and dies (false "no backup") when that dir is 0750 someone-else.
mkdir -p "$DEST"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="$DEST/comms-$STAMP.db"
TMP="$OUT.part"
trap 'rm -f "$TMP"' EXIT
printf ".timeout 5000\n.backup '%s'\n" "$TMP" | sqlite3 "file:$DB?mode=ro"
# Integrity-check the ARTIFACT, not the live db (a corrupt backup is worse
# than no backup), and do it BEFORE the rename, so a failed artifact never
# carries a final name that restore.sh or retention would trust.
sqlite3 "$TMP" "PRAGMA integrity_check" | head -1 | grep -qx ok || { echo "BACKUP FAILED integrity_check" >&2; exit 1; }
chmod 600 "$TMP"
mv "$TMP" "$OUT"
find "$DEST" -maxdepth 1 -name 'comms-*.db' -mtime +"$KEEP_DAYS" -delete
echo "backup ok: $OUT ($(stat -c %s "$OUT") bytes, epoch $(sqlite3 "$OUT" "SELECT value FROM meta WHERE key='epoch'"))"
