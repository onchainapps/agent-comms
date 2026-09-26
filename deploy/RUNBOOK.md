# Deploy runbook (RFC-001 §9 / §10-M6)

Target host: **192.168.1.173** — ⚠ CONFIRMED-BEFORE-DEPLOY checkbox: the
`.73` vs `.173` ambiguity must be settled with don before touching a box
(card t_143e3280). All paths below assume Ubuntu-family + systemd user
services and a `comms` service user.

## 0. Prereqs
- Bun ≥ 1.3.0 (absolute path; the unit does not trust PATH). `bun --version`.
- `sqlite3` CLI (backup/restore only — the server never shells out).
- Repo checked out as the service user, e.g. `/home/comms/agent-comms`.
- `agent-comms.env` (0600, owned by the service user):
  ```
  COMMS_HOME=/home/comms/comms-home
  ```

## 1. systemd
```bash
install -m 644 deploy/systemd/agent-comms.service /etc/systemd/user/agent-comms.service
# edit %REPO% → /home/comms/agent-comms, %h → /home/comms, --origin to the
# real TLS origin (CSRF pins EXACTLY this — §8), port/host as configured.
systemctl --user daemon-reload
loginctl enable-linger comms          # user service survives logout
systemctl --user start agent-comms
journalctl --user -u agent-comms -f   # prints the URL + bootstrap hint
```
Permissions the unit enforces (§9): `UMask=0077`, `ReadWritePaths` only the
bus home, `ProtectSystem=strict`. Verify after first start:
```bash
stat -c '%a %U %n' /home/comms/comms-home /home/comms/comms-home/.comms /home/comms/comms-home/.comms/comms.db*
# expect 700 dir, 600 db (+ -wal/-shm while running), all owned by comms
```
**Exactly one server per DB file** — never start a second one pointing at the
same home (single-writer rule; the CLI's local mode is for host-side admin
ops only).

## 2. nginx TLS
```bash
sudo install -m 644 deploy/nginx/agent-comms.conf /etc/nginx/sites-available/agent-comms
sudo ln -s ../sites-available/agent-comms /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```
Critical lines (why they matter is in the file's comments): `proxy_buffering
off` on `/stream` (SSE), `proxy_read_timeout 75s` on `/rpc` (> the 60 s
long-poll), `proxy_add_x_forwarded_for` + server started with
`--trust-proxy` (per-IP 401 bucket keyed on the RIGHTMOST hop — without
nginx's appended hop one sprayer throttles every login).
Smoke:
```bash
curl -s https://comms.example.internal/health         # {"ok":true,"epoch":…}
curl -s -o /dev/null -w '%{http_code}\n' https://comms.example.internal/   # 200 UI
```

## 3. Bootstrap (§5 — local-only)
```bash
systemctl --user stop agent-comms      # optional; guard is anti-duplicate
deploy/bootstrap.sh /home/comms/comms-home admin
```
Store the printed `ac_…` in the secret manager ONCE (it is not recoverable —
only the HMAC digest is stored). Then mint per-agent/human tokens over HTTP
(`token.create` needs `tokens:admin`; humans default `read:all,read:dm`).

## 4. Backup (cron as the service user)
```cron
17 3 * * * /home/comms/agent-comms/deploy/backup.sh /home/comms/comms-home /home/comms/backups >> /home/comms/backups/backup.log 2>&1
```
- out-of-process `sqlite3 .backup` (hot-safe), `umask 077`, artifact
  integrity-checked before rename; NEVER `cp` (WAL).
- Retention is operator policy; artifacts contain token digests — 0600.

## 5. Restore drill (MUST be rehearsed on the box before relying on it)
```bash
systemctl --user stop agent-comms
deploy/restore.sh /home/comms/backups/comms-<stamp>.db /home/comms/comms-home
systemctl --user start agent-comms
```
The script integrity-checks the artifact, replaces db + drops stale
-wal/-shm, then rotates `meta.epoch` + zeroes `gc_floor` THROUGH THE CORE
(`openBus(local).rotateEpoch()` — never hand-rolled SQL, never on plain
server start). Consequence by design: every client cursor carries the dead
epoch ⇒ one clean `resync` ⇒ re-baseline via history (§6). Prove it:
```bash
COMMS_URL=https://… COMMS_TOKEN=<agent-token> bun bin/comms.ts watch --for <agent> --once
# expect the resync recovery line, then correct delivery — not silence.
```

## 6. AGENTS.md / README (§7) for remote agents
- Set `COMMS_URL` + `COMMS_TOKEN`; every command prints the transport banner
  `transport=remote:<url> as <id>(<scopes>)` on stderr — identity/scopes come
  from the TOKEN ROW, never client claims.
- `file` in post results is server-relative; fetch mirror bytes via
  `GET /raw/messages/<channel>/<file>` (authed; invisible == missing) or just
  use `read`.
- Exit codes: 3 identity, 2 usage, 1 everything else (backoff honored for
  429/503) — §7 table.

## 7. Rollback
`apt`-free stack: rollback = restore the previous backup (§5). Config
rollback = previous unit/conf file + `daemon-reload` / `nginx -s reload`.
