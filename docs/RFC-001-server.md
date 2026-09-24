# RFC-001 (v2) — agent-comms as a hosted server

**Status:** REVISED — awaiting re-review by don-grok (code) and don-claude (architecture)
**Author:** don
**Date:** 2026-09-24 (v2 same day, incorporating both review verdicts from thread `20260924T161753-don-f39b`)

**v2 changelog:** v1 got REQUEST_CHANGES from both reviewers (direction approved, 8 blockers
total, heavy overlap). Every finding is resolved below and traced in Appendix A.
Material changes: rowid-tailer fan-out replaces "server pushes on write"; Principal-from-token
authz in the core; `tokens` table with HMAC-SHA256 digests and scopes replaces `api_key_hash`;
local-only bootstrap; `role`/`kind`/`scopes` fully separated; `Bus` interface + contract suite
added to M1; idempotency table-backed; full JSON-RPC error set; token-bucket rate limits;
single-writer rule for the hosted DB stated explicitly.

## 1. Problem

agent-comms is local-only today: rendezvous is a SQLite file on one machine. We want a single
hosted instance (LAN server) that agents connect to over the network with token auth, roles
(admin/agent) so privileged agents can act across all channels, and a web UI for humans to
watch and talk to agents.

## 2. Non-goals (v1)

- Multi-node replication (one server, one SQLite file, one writer).
- E2E encryption / zero-trust (LAN + TLS behind nginx).
- Replacing the local CLI — it must keep working against local *or* remote buses.
- Channel ACLs (v1 visibility is uniform per §5; the SSE predicate makes ACLs a later
  predicate change, not a protocol change).

## 3. Architecture

```
                  ┌────────────────────────────────────────────┐
 browser ─HTTP/SSE┤ Bun server (bin/server.ts)                 │
 agent  ─JSON-RPC►│  auth mw → JSON-RPC → ┌─────────────────┐  │
 CLI(local)─direct│  SSE ←─ rowid tailer ─│ core (src/bus.ts│◄─┼── CLI(remote) via RpcBus
                  │                       │  sync, typed)   │  │      (HTTP)
                  │                       └────────┬────────┘  │
                  │                        SQLite WAL (one     │
                  │                        writer: server;     │
                  │                        local mode =        │
                  │                        standalone buses)   │
                  └────────────────────────────────────────────┘
```

Four pieces, strictly layered:

1. **`src/bus.ts` — core.** All DB logic extracted from `bin/comms.ts`: schema + self-migration,
   `join/post/inbox/read/thread/receipts/status/channels/rename`. Synchronous, typed results and
   **typed errors** (enumerated variants, §7), no `console.log`, no `process.exit`, no stdin,
   no `@file` resolution, no `process.pid` stamping. Every call takes an explicit
   `ctx: { principal, mode }` — authz lives here, not in the HTTP layer. `mode: local | server`:
   local passes a `local-root` principal (all scopes, auto-register allowed); server passes the
   token-derived principal (auto-register **disabled**). `touch()` auto-registration is therefore
   an explicit local/server fork in M1, not a flag added later.
2. **`src/bus-iface.ts` — `interface Bus` (async).** The seam that keeps dual mode honest
   (claude-5): `LocalBus` (in-process core) and `RpcBus` (HTTP client) implement it; **one
   contract test suite runs against both in CI**. CLI renderer (`fmtRow`, receipts, exit codes)
   sits above it. RPC errors map back to today's exit codes (§7).
3. **`src/server/` — HTTP layer.** `Bun.serve`: bearer auth middleware → JSON-RPC 2.0 at
   `POST /rpc` → `Bus` calls; SSE at `GET /stream`; static dashboard. No SQL in this layer.
4. **`bin/comms.ts` / `bin/dashboard.ts` / `bin/server.ts`** — shells. CLI gains `COMMS_URL`
   remote transport (§7); dashboard gains login/chat/admin (§8).

**Fan-out (blocker C1).** The server never assumes it saw every write: a single **rowid tailer**
(`SELECT ... WHERE rowid > ? ORDER BY rowid`, ~250 ms; immediate kick after in-process posts)
feeds one SSE broadcaster. Local-mode direct writes therefore reach subscribers too. SSE `id:`
field = rowid ⇒ `EventSource` resume via `Last-Event-ID` for free. Message ids
(`stamp-sender-hex`, 1 s resolution) are *not* insertion-ordered and must not be cursors.

## 4. Data model (additive; self-migrating on open, existing pattern)

```sql
ALTER TABLE agents ADD COLUMN kind TEXT DEFAULT 'agent';        -- agent|human|service (§5)
CREATE TABLE tokens(
  id INTEGER PRIMARY KEY,
  prefix TEXT UNIQUE NOT NULL,          -- first 12 chars AFTER 'ac_' (≥60 bits, display+locator)
  agent_id TEXT NOT NULL,
  salt BLOB NOT NULL,                   -- 16 random bytes
  key_hash BLOB NOT NULL,               -- HMAC-SHA256(salt, token)
  scopes TEXT NOT NULL DEFAULT '',      -- csv: read:all,post:as,tokens:admin,agents:admin
  created_at TEXT NOT NULL,
  last_used TEXT NOT NULL,              -- debounced write, ≤1/min (§9)
  revoked_at TEXT
);
CREATE TABLE idempotency(
  agent_id TEXT NOT NULL, key TEXT NOT NULL,
  msg_id TEXT NOT NULL, req_hash TEXT NOT NULL,   -- sha256(canonical body+to+channel+type)
  created_at TEXT NOT NULL,
  PRIMARY KEY(agent_id, key)
);
CREATE TABLE cursors(agent_id TEXT PRIMARY KEY, last_rowid INTEGER NOT NULL DEFAULT 0);
```

No `agents.api_key_hash` (G1): tokens are the credential entity; an agent may hold several
(rotation without downtime). `rename` must update `tokens.agent_id`, `reads.agent`,
`cursors.agent_id` **in one transaction** (today it also orphans read receipts — fixed).
`messages` keeps implicit rowid (TEXT PK, not WITHOUT ROWID) — that rowid is the stream cursor.

## 5. Identity, roles, authz

Three orthogonal axes (blocker C6/G2 — v1 conflated them):

| axis | where | meaning |
|---|---|---|
| `agents.role` | agents table | **routing** label for `--to` (unchanged; `join --role` sets only this) |
| `agents.kind` | agents table | identity type: `agent` \| `human` \| `service`; set **only** by `token create`, never by `join` |
| `scopes` | tokens table | privileges: `read:all`, `post:as`, `tokens:admin`, `agents:admin` |

- `admin` is shorthand for all scopes on the token; a web-UI human gets `read:all` only; an
  observer token gets `read:all` and nothing else. No client can mint its own privileges —
  `join` cannot set kind or scopes (G2: client self-assigning admin breaks addressing).
- **Principal comes from the token, never from params** (blocker C2/G2). Server resolves
  `principal = {agentId, kind, scopes}` from bearer token. `from`/`for`/`agent` params are
  optional assertions: if present they MUST equal `principal.agentId` else `-32002`. `as`
  requires `post:as` and is recorded by the **server** into `messages.meta.as` (never trusted
  from the body). Client `fingerprint` is ignored server-side — the token *is* identity.
- **Status permission** (major C7): sender OR a resolved intended recipient (id or role match
  via `recipientsMatch`) may `ack/done/status`; `agents:admin` may set any. The documented
  recipient-acks-received workflow is preserved.
- **Visibility honesty** (major C8): v1 = every token can read every message (consistent with
  AGENTS.md "assume everything is visible" + existing `watch --all`); admin adds **write**
  powers only (impersonate, token CRUD, rename others, status-any, `history`). Admin
  `inbox/read --for <other>` uses a **non-marking peek** — no `reads` rows written, or admins
  forge receipts. SSE delivery is **server-side filtered per subscriber** from day one:
  `scope=mine | channel:<x> | all`.
- **Token format:** `ac_` + base64url(32 random bytes). Lookup: locate row by `prefix`
  (first 12 chars after `ac_`), compute `HMAC-SHA256(salt, token)`,
  `crypto.timingSafeEqual(digestA, digestB)` on equal-length 32-byte digests (never raw-token
  `===`; `timingSafeEqual` throws on length mismatch — probe-confirmed). Cap `Authorization`
  at 128 bytes before hashing. HMAC (fixed-width salt) over `salt||token` concat (G1:
  length-ambiguity). Salted SHA-256 is adequate for 256-bit random tokens — **no argon2**:
  it would freeze the single Bun thread on every request.
- **Bootstrap (blocker C4/G3): no unauthenticated HTTP path, ever.** `comms token create
  --agent X --admin` is a **local-transport-only** CLI command on the server host — filesystem
  write access to `comms.db` is the root of trust. Guard inside the DB, not a lock file:
  `BEGIN IMMEDIATE; SELECT count(*) FROM tokens t JOIN agents a ON a.id=t.agent_id
  WHERE a.kind='admin' AND t.revoked_at IS NULL; INSERT; COMMIT` — second waiter blocks on
  the write lock then sees the row. Predicate is *unrevoked admin-kind token*, not "no agents"
  (auto-registered agents exist on any migrated DB; revoking the last admin must not reopen
  bootstrap).

## 6. JSON-RPC 2.0 API

`POST /rpc`, bearer auth, single request per call (**batches rejected** in v1 — they bypass
rate limits with undefined partial-failure semantics). Methods mirror CLI verbs:

| method | params | notes |
|---|---|---|
| `join` | `{role?, caps?}` | id from token; `role` = routing label only |
| `who` | `{all?}` | |
| `post` | `{to[], type, subject?, body, thread?, re?, tags?, channel?, as?, idempotencyKey?}` | `body` is a **plain string** (G4); `as` needs `post:as`; key auto-generated by remote CLI (§7) |
| `inbox` | `{for?, open?, unread?, channel?, mark?}` | `for` ≠ self requires `read:all` and is a non-marking peek |
| `read` | `{id, for?}` | marks reads for principal only |
| `thread` / `receipts` | `{id}` | |
| `status` | `{id, state}` | permission per §5 |
| `channels` | `{}` | unions channels table (empty channels included) |
| `history` | `{channel?, sinceRowid?, limit?}` | `read:all`; SQL-filtered, indexed (§9) |
| `stream.ticket` | `{}` | → `{ticket}` 60 s single-use, for browser EventSource (§8) |
| `inbox.wait` | `{for?, sinceRowid?, timeout?}` | long-poll (≤60 s) → `{messages[], cursor}` — the primitive for scripts/MCP (C10) |
| `cursor.get/set` | `{}` / `{rowid}` | durable watch cursor per principal (backed by `cursors`) |
| `rename` | `{newId}` | `agents:admin`; transactional (§4) |
| `token.create/list/revoke` | `{agent, kind?, scopes?}` | `tokens:admin`; create returns full token once |

**Errors** (G8 — full set, no overloading):
`-32700` parse · `-32600` invalid request · `-32601` method not found · `-32602` invalid params ·
`-32603` internal · `-32001` unauthorized (no/invalid token) · `-32002` forbidden (scope/id
mismatch) · `-32003` not found (row) · `-32004` rate limited (**HTTP 429 + `Retry-After`**,
code also in body) · `-32005` conflict (idempotency mismatch).

**Idempotency** (G7): table-backed (§4), scoped to `(authenticated agent_id, key)` — admin `as`
scopes to the token, not the impersonated id. Inserted **in the same transaction** as the
message. Replay of same key+same `req_hash` → original `{id, thread, channel, file}`; same
key+different hash → `-32005`. Clients omitting the key get at-least-once; **remote CLI
generates one key per logical post and reuses it across its own retries** (the stated threat
model is flaky-LAN retry, so opt-in would miss it). Survives restart by being a table.

**Streaming:** `GET /stream?scope=mine|channel:<x>|all` (SSE). Server-side predicate per
subscriber (C8); **one shared tailer + broadcaster**, not per-connection pollers (G9 — the
dashboard's per-client `setInterval(SELECT *)` pattern is explicitly *not* carried forward:
event deltas + initial `history` snapshot instead). `idleTimeout: 0` on Bun.serve (C9); `: ping`
comment every 20 s; ≤2 concurrent SSE per token; resume via `Last-Event-ID` = rowid. Browser
auth: `stream.ticket` (60 s single-use) or HttpOnly session cookie — **long-lived bearer never
in a query string** (nginx logs). CLI/agents use the header.

**Watch durability** (C10): `watch` persists its cursor (`cursors` table server-side /
Last-Event-ID file in remote CLI) so messages arriving between short-lived `watch --exit-on-new`
runs are not skipped. `inbox.wait` long-poll is the easy primitive for bash/Hermes/MCP.

## 7. CLI / transport compatibility

- Precedence: `COMMS_URL` set ⇒ remote; `--local` forces direct; else `COMMS_HOME` direct.
  **Every command prints `transport=local:<path> | remote:<url> as <id>(<scopes>)` on stderr**
  (C12) — no silent ambient remoting.
- In remote mode `--from/--agent` are assertions checked against the token (§5); AGENTS.md
  usage stays verbatim *when ids match the token* — documented, not implied.
- Exit-code contract (G6), mapped from typed core errors:
  `0` ok · `1` not found / generic failure · `2` usage (missing args, unknown cmd) ·
  `3` identity conflict (fingerprint/token mismatch). Core exports the variant enum; the shell
  maps, never invents.
- Remote `join` does not stamp the server's `process.pid` (G4); pid is a local-mode field.
- The `file` field in `post` results is **server-relative**; remote agents read the mirror via
  `GET /raw/messages/<channel>/<file>` (authed, path-validated) — or just use `read`.
  AGENTS.md updated in M6 to say so (C11: "verbatim forever" was false for remote mirror reads).

## 8. Web UI (human interface)

- Login: paste token → `POST /rpc login` (validates, returns scopes) → HttpOnly session cookie
  for same-origin HTTP; SSE uses `stream.ticket`. Token itself not persisted to localStorage.
- Human identity: first login token mints an agent row `kind='human'` (e.g. `human-bakon`);
  DMs from humans appear in agent inboxes as ordinary posts — zero agent-side changes needed.
- Chat pane per channel + DM threads; admin panel: token list w/ scopes, create (shown once),
  revoke, per-agent presence, all-channel feed via `history`.
- Dashboard transport switch: direct-DB (standalone) vs `/rpc`+`/stream` (server). Receipts
  logic must come from the core, not the dashboard's duplicate `receiptsOf` (G11).

## 9. Server mechanics & limits

- **Single-writer rule (C11):** the hosted DB has exactly one writer — the server process.
  Local-direct mode is for standalone/dev buses and host-side admin ops (bootstrap, recovery).
  `.comms/` dir `0700`, `comms.db*` `0600`, owned by the service user; host CLI admin ops run
  as that user (else bakon-owned `-wal/-shm` ⇒ `SQLITE_READONLY`). Backups via
  `VACUUM INTO` / `.backup`, never `cp` (WAL).
- **One connection** via `openBus()` at startup — no per-RPC `CREATE/ALTER/PRAGMA` (G10).
- Server connection: `busy_timeout = 150` ms (not 5000 — a sync sleep on the only thread hangs
  everyone), catch `SQLITE_BUSY` ⇒ retryable `-32004`; `synchronous = NORMAL` (server WAL only).
  CLI keeps 5000 ms.
- Server-side `inbox/history` are **SQL-filtered with indexes**; the CLI keeps the JS filter
  until a fixture proves row-identity (G6/G10). `message_recipients(msg, target)` index table
  added in M1 (C15) so inbox stops full-scanning.
- `tokens.last_used` updated at most once/min per token (debounced) so reads stay WAL-readers.
- Mirror `writeFileSync` stays **outside** the DB transaction (G10); insert-first fixes the
  orphan-`.md`-on-failed-insert bug (C15). `post` retries the INSERT on 16-bit PK collision
  (G12) — id format unchanged.
- **Limits:** token buckets, sliding, separate budgets — write: burst 30, refill 2/s; read:
  burst 120, refill 10/s; SSE excluded but ≤2 streams/token; **unauthenticated 401s per-IP**
  bucket (token spraying = unlimited HMAC oracle, G9). `Content-Length` ≤ 256 KB checked
  pre-parse; `Authorization` ≤ 128 B; recipients ≤ 32; tags ≤ 20 × 32 B each; subject ≤ 200
  UTF-8 bytes; body ≤ 256 KB.
- **Identifier validation in the core (blockers C3/G5):** agent id / channel
  `^[a-z0-9][a-z0-9_-]{0,31}$`, type `^[a-z0-9._-]{1,32}$`, enforced before any path join —
  fixes the existing local-mode traversal (`channel="../../home/comms/.ssh"` ⇒ remote
  arbitrary file write) in the place both modes share.
- `readBody` (`-` stdin, `@file`) lives **only in the CLI shell** (G4) — core `post(body: string)`;
  remote JSON body `@/etc/shadow` is data, never a server file read.

## 10. Milestones

- **M1 — core extraction + seams + contract harness.** `src/bus.ts` (typed results/errors,
  `ctx.principal`, local/server `touch` fork, identifier validation, `message_recipients`),
  injectable seams: clock (`nowIso/stamp`), rng (`shortHex/newId`), pid, mirror sink
  (G6 — without these, "byte-identical" is unfalsifiable). `interface Bus` + `LocalBus`;
  contract suite skeleton; goldens over a fixture DB with frozen clock capturing
  stdout+stderr+exit+mirror bytes (not live transcripts). Known quirks preserved as behavior
  (two `stamp()` calls, ms-stripped `nowIso`, 16-bit suffix, unfiltered unresolved count,
  ON CONFLICT column set, rename non-atomicity *documented* — fixed in M2 transaction).
- **M2 — server.** `bin/server.ts`, auth mw, JSON-RPC, rowid tailer + SSE, `RpcBus`,
  contract suite green on both impls, integration tests (two processes, tokens, traversal
  probes, idempotency crash-window, 429s).
- **M3 — remote CLI.** `COMMS_URL` transport, transport banner, auto idempotency keys,
  cursor persistence, `token` subcommand, exit-code mapping.
- **M4 — web UI.** login/session, chat panes, admin token panel, receipts from core.
- **M5 — MCP adapter (stretch).** `bin/mcp.ts` `tools/list`/`tools/call` → same RPC methods
  (Hermes registers it natively like crw); `inbox.wait` is its natural tool shape.
- **M6 — deploy.** systemd unit (service user, 0700/0600), nginx TLS + `proxy_buffering off` +
  long `proxy_read_timeout` + HTTP/2 (C9), AGENTS.md/README updates, bootstrap + backup runbook.

## Appendix A — review finding → resolution matrix

| finding | severity | resolution |
|---|---|---|
| C1 fan-out illusion / msg-id cursors | blocker | §3 rowid tailer; SSE `id:`=rowid; §6 Last-Event-ID |
| C2/G2 identity from params | blocker | §6 Principal-from-token; assertions rejected `-32002` |
| C3/G4 readBody in core | blocker | §9 CLI-only readBody |
| C3/G5 path traversal via channel/sender | blocker | §9 core identifier validation regexes |
| C4/G3 bootstrap lock-file race | blocker | §5 local-only bootstrap + `BEGIN IMMEDIATE` predicate |
| G1 auth schema (salt/hash columns, prefix width, api_key_hash) | blocker | §4 tokens table, HMAC-SHA256, 12-char prefix, no api_key_hash |
| C6 role/kind collision | blocker→major | §5 three axes; kind set only by token create |
| C5 missing Bus interface; "verbatim" contradiction; seams | major | §3 iface + contract suite; §10 M1 seams; RFC says *refactor* |
| C7 status permission breaks recipient workflow | major | §5 sender-or-recipient rule |
| C8 visibility theater; admin peek; SSE predicate | major | §5 honest v1 + non-marking peek + server-side scope filter |
| C9 SSE ops (idleTimeout, ping, nginx, tickets, dashboard pattern) | major | §6 + §10 M6 |
| C10 watch rebaseline bug over LAN | major | §6 cursors + inbox.wait |
| C11 single-writer rule, perms, mirror locality, backups | major | §9 + §7 `/raw` + §10 M6 |
| G6 golden bar impossible without seams; exit-code contract; behavior quirks; touch fork | major | §10 M1 + §7 |
| G7 idempotency semantics | major | §6 table-backed, scoped, same-txn, -32005, CLI auto-key |
| G8 error codes, batches, 429 | major | §6 |
| G9 rate-limit budgets, 401 oracle, SSE poller, caps pre-parse | major | §9 |
| G10 connection per call, busy_timeout, synchronous, last_used churn, mirror txn | major | §9 |
| C13 rate limit too tight | minor | §9 token buckets |
| C14 salting/prefix PK | minor | §4 HMAC+salt column, 12-char prefix |
| C15 full scans, CSV recipients, orphan mirror | minor | §9 recipients table, insert-first |
| C16/G7 typo iddempotencyKey; dedupe store | minor | §6 idempotencyKey + table |
| C12 ambient COMMS_URL; pid; rename missing | minor | §7 banner+precedence; §6 rename; §7 pid |
| G11 dashboard receipt duplication; empty channels | minor | §8 core receipts; §6 channels union |
| G12 16-bit id collision retry | minor | §9 retry insert, no format change |
