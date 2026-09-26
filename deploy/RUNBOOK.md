# Deploy runbook (RFC-001 §9 / §10-M6)

Target host: **192.168.1.173**. ⚠ CONFIRM BEFORE DEPLOY: settle the `.73` vs
`.173` ambiguity with don before touching a box (card t_143e3280).
Layout assumed below: Ubuntu-family with a **system** systemd unit and a
`comms` system user.

| what | where | owner / mode |
|---|---|---|
| code (immutable releases) | `/opt/agent-comms/releases/<git-sha>/`, `current` → symlink | root, 755 (the service CANNOT modify its own code) |
| bus home (`COMMS_HOME`) | `/var/lib/agent-comms/` (systemd `StateDirectory`) | comms, 700; `.comms/comms.db*` 600 |
| env file | `/etc/agent-comms/agent-comms.env` | root:comms, 640 |
| backups | `/var/backups/agent-comms/` **and off-host** | comms, 700; artifacts 600 |

Why a system unit and not `systemctl --user`: in a user manager,
`ProtectSystem` / `ProtectHome` / `PrivateTmp` / `ReadWritePaths` need an
unprivileged user namespace. Ubuntu ≥ 23.10 denies that namespace via
AppArmor, and systemd then **silently skips the whole mount sandbox** (the
unit still starts). Check on the box with
`sysctl kernel.apparmor_restrict_unprivileged_userns`.

## 0. Prereqs
- Bun ≥ 1.3.0 at `/usr/local/bin/bun`. The unit and scripts never trust PATH;
  set `BUN=/usr/local/bin/bun` when running scripts via `sudo -u comms`.
- `sqlite3` CLI (backup/restore), `flock` + `fuser` (util-linux / psmisc).
- nginx ≥ 1.25.1 (`http2 on;`); see the conf header for 1.24.
- `useradd --system --home-dir /var/lib/agent-comms --shell /usr/sbin/nologin comms`
- Env file (0640 root:comms):
  ```
  COMMS_HOME=/var/lib/agent-comms
  COMMS_PORT=8700
  COMMS_ORIGIN=https://comms.example.internal   # CSRF pins EXACTLY this (§8)
  ```
  The unit pins `--host 127.0.0.1 --trust-proxy`; they are deliberately NOT
  in the env file (they form the trust boundary).
- Firewall: only 443 (plus 80 for the redirect) inbound, e.g.
  `ufw allow 443/tcp`. 8700 is loopback-bound; still add
  `ufw deny 8700` as a second fence.
- journald: logs are small (startup lines + gc warnings), but pin a cap:
  `SystemMaxUse=500M` in `/etc/systemd/journald.conf`. Nothing writes log
  files, so there is nothing for logrotate to do.

## 1. Install a release
```bash
SHA=$(git -C ~/src/agent-comms rev-parse --short HEAD)
sudo git -C ~/src/agent-comms worktree add /opt/agent-comms/releases/$SHA $SHA   # or rsync an export
sudo ln -sfn /opt/agent-comms/releases/$SHA /opt/agent-comms/current
```

## 2. Bootstrap (§5 — local-only, BEFORE first start)
```bash
sudo -u comms BUN=/usr/local/bin/bun /opt/agent-comms/current/deploy/bootstrap.sh /var/lib/agent-comms admin
```
This creates the DB with the right owner and modes (700/600). It takes the
§9 latch, so it refuses while a server runs. Store the printed `ac_…` in the
secret manager ONCE: it is not recoverable (only the HMAC digest is stored).

## 3. systemd
```bash
sudo install -m 644 /opt/agent-comms/current/deploy/systemd/agent-comms.service /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/agent-comms.service
sudo systemctl daemon-reload && sudo systemctl enable --now agent-comms
journalctl -u agent-comms -f
```
Verify the unit and the sandbox actually applied:
```bash
systemd-analyze security agent-comms | tail -1          # expect ~1.9 "OK"
stat -c '%a %U %n' /var/lib/agent-comms /var/lib/agent-comms/.comms /var/lib/agent-comms/.comms/comms.db* /var/lib/agent-comms/messages
# expect 700 dirs (messages/ is server-created under UMask=0077 — if you
# pre-created it, chown comms + chmod 700 yourself), 600 db (+ -wal/-shm while
# running), all owned by comms
sudo -u comms flock -n /var/lib/agent-comms/.server.lock true; echo $?   # expect 1 (latch held)
```
**Exactly one server per DB** (§9). The `flock -F -n` latch in ExecStart
makes a second server on the same home exit 1. restore.sh and bootstrap.sh
take the same latch. Local-mode CLI and the direct-DB dashboard do NOT take
it, so never point them at `/var/lib/agent-comms` while the server runs
(use the server-mode dashboard: `--url`).

## 4. nginx TLS
```bash
sudo install -m 644 /opt/agent-comms/current/deploy/nginx/agent-comms.conf /etc/nginx/sites-available/agent-comms
sudo ln -s ../sites-available/agent-comms /etc/nginx/sites-enabled/
# nginx.conf main context: worker_shutdown_timeout 30s;  (SSE vs reload)
sudo nginx -t && sudo systemctl reload nginx
```
Critical lines (the file's comments say why):
- `proxy_buffering off` on `/stream` (SSE);
- `proxy_read_timeout 75s` on `/rpc` (> the 60 s long-poll);
- `proxy_add_x_forwarded_for`, paired with the unit's `--trust-proxy`. This
  is single-hop only; see the conf before adding an LB;
- typed JSON 413 at the edge.

`.internal` names need an internal CA (Let's Encrypt cannot issue for them).

## 5. Smoke
```bash
curl -s https://comms.example.internal/health                         # {"ok":true,"epoch":…}
curl -s -o /dev/null -w '%{http_code}\n' https://comms.example.internal/   # 200 UI
COMMS_URL=https://comms.example.internal COMMS_TOKEN=<admin> bun bin/comms.ts who   # banner shows as admin(...)
```
Then mint per-agent and human tokens over HTTP (`token create --agent <id>`).
Agents get `COMMS_URL` + `COMMS_TOKEN` via their secret store, never via a
checked-in file.

## 6. Backup (cron as the service user)
```cron
17 3 * * * BUN=/usr/local/bin/bun KEEP_DAYS=14 /opt/agent-comms/current/deploy/backup.sh /var/lib/agent-comms /var/backups/agent-comms >> /var/backups/agent-comms/backup.log 2>&1
```
- Out-of-process `sqlite3 .backup` (hot-safe), `umask 077`. The artifact is
  integrity-checked BEFORE it gets its final name. NEVER `cp` (WAL).
- Ship `/var/backups/agent-comms` off-host (rsync/restic). Artifacts contain
  token digests, so keep them 0600 and encrypted at rest off-host.
- The `messages/` mirror is derived and not backed up (the db is
  authoritative).

## 7. Restore (an EPOCH EVENT — rehearse on the box before relying on it)
```bash
sudo systemctl stop agent-comms
sudo -u comms BUN=/usr/local/bin/bun /opt/agent-comms/current/deploy/restore.sh /var/backups/agent-comms/comms-<stamp>.db /var/lib/agent-comms
sudo systemctl start agent-comms
```
The script refuses unless it can take the latch (and no unit is active and
no process holds the db). Then it:
1. makes a WAL-inclusive safety copy of the current image;
2. stages the backup and runs migration, the revocation carry-forward
   (tokens revoked after the backup STAY revoked) and
   `rotateEpoch()` THROUGH THE CORE, all on the staged copy;
3. swaps it in with a rename.

Any failure leaves the old image untouched.

What clients experience (by design, §3/§6):
- every pre-restore cursor carries the dead epoch, so it gets one `resync`;
- the recovery commit goes to `<epoch>.0`;
- consumers re-receive ALL retained history and **must dedupe by message
  id**;
- messages posted after the backup are gone (their ids are not reused).

Tell agent owners before you restore. Prove it:
```bash
# 1) the epoch actually changed (rotation ran — the only mechanical proof):
curl -s https://comms.example.internal/health | grep -o '"epoch":"[^"]*"'   # compare vs pre-restore
# 2) a consumer WITH a pre-restore cursor resyncs then receives (fresh token
#    proves nothing — no old cursor exists, so no resync line can appear):
COMMS_URL=https://… COMMS_TOKEN=<agent-token> bun bin/comms.ts watch --for <agent> --once
# expect "watch: resync — recovery commit to …", then delivery — not silence.
```
Tokens minted after the backup no longer exist. Re-mint them.

## 8. Upgrade / rollback of the CODE
```bash
# upgrade: backup FIRST (a new SCHEMA_VERSION / acl_generation migrates on open)
sudo -u comms BUN=/usr/local/bin/bun /opt/agent-comms/current/deploy/backup.sh /var/lib/agent-comms /var/backups/agent-comms
sudo ln -sfn /opt/agent-comms/releases/<new-sha> /opt/agent-comms/current
sudo systemctl restart agent-comms && curl -s https://comms.example.internal/health
```
Rollback: re-point `current` at the previous sha and restart, **only if that
release opens the migrated DB**. If a release refuses a DB (for example
`acl_generation` greater than compiled, §G5) or predates a schema bump:
restore the pre-upgrade backup (§7; an epoch event) under the old release.
Never run a pre-G binary against a DB that has dm-shaped channels (§G5).

## 9. Config rollback
Previous unit/conf file, then `systemctl daemon-reload` / `nginx -t && nginx -s reload`.

## 10. AGENTS.md / README (§7) for remote agents
- Set `COMMS_URL` + `COMMS_TOKEN`. Every command prints the transport banner
  `transport=remote:<url> as <id>(<scopes>)` on stderr. Identity and scopes
  come from the TOKEN ROW, never from client claims.
- `file` in post results is server-relative. Fetch mirror bytes via
  `GET /raw/messages/<channel>/<file>` (authed; invisible == missing), or
  just use `read`.
- Exit codes: 3 identity, 2 usage, 1 everything else (backoff is honored for
  429/503). See the §7 table.
