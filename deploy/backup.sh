#!/bin/bash
# deploy/backup.sh — out-of-process SQLite backup (RFC-001 §9: NEVER cp — WAL;
# NEVER on the server's own connection; run as the service user; umask 077 —
# the backup contains token digests). sqlite3 .backup is hot-safe.
#
# usage: deploy/backup.sh <comms-home> <dest-dir>
set -euo pipefail
HOME_DIR="${1:?usage: backup.sh <comms-home> <dest-dir>}"
DEST="${2:?usage: backup.sh <comms-home> <dest-dir>}"
DB="$HOME_DIR/.comms/comms.db"
[ -f "$DB" ] || { echo "no db at $DB" >&2; exit 1; }
umask 077
mkdir -p "$DEST"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="$DEST/comms-$STAMP.db"
TMP="$OUT.part"
sqlite3 "file:$DB?mode=ro" ".backup '$TMP'"
mv "$TMP" "$OUT"
# integrity-check the ARTIFACT, not the live db (a corrupt backup is worse
# than no backup):
sqlite3 "$OUT" "PRAGMA integrity_check" | head -1 | grep -qx ok || { echo "BACKUP FAILED integrity_check" >&2; exit 1; }
chmod 600 "$OUT"
echo "backup ok: $OUT ($(stat -c %s "$OUT") bytes, epoch $(sqlite3 "$OUT" "SELECT value FROM meta WHERE key='epoch'"))"
