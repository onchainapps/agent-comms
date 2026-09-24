# RFC-001 (v2.1) — agent-comms as a hosted server

**Status:** REVISED — awaiting diff re-review by don-grok (code) and don-claude (architecture)
**Author:** don
**Date:** 2026-09-24

**v2.1 changelog:** v2 converged hard — claude-1: REQUEST_CHANGES, **0 blockers** ("I'll
re-review a v2.1 diff only"); grok-1: REQUEST_CHANGES, **1 blocker**, both "closed/do not
reopen" lists honored. The shared blocker: v2's bootstrap predicate `WHERE a.kind='admin'`
contradicts v2's own three-axis model (admin is a *scope*, not a kind → predicate never fires).
Majors are second-order, several probe-verified (SQLite 3.53.2 + bun:sqlite). Changes in this
revision: **events table + DB triggers** as the single fan-out source (covers UPDATEs, survives
stale writers, durable AUTOINCREMENT seq + epoch); per-parameter assertion rules replacing the
blanket rule; scope-based bootstrap with honest non-security framing; cursor durability
(`(agent_id, consumer)`, at-least-once commit); cookie-based browser SSE + CSRF guards;
canonical full-params `req_hash`; `-32006`/503 split from rate-limit; ops hardening.
Full trace: Appendix B (this doc) on top of Appendix A (v1 → v2, all verified closed).

## 1. Problem

agent-comms is local-only today: rendezvous is a SQLite file on one machine. We want a single
hosted instance (LAN server) that agents connect to over the network with token auth, roles
(admin/agent) so privileged agents can act across all channels, and a web UI for humans to
watch and talk to agents.

## 2. Non-goals (v1)

- Multi-node replication (one server, one SQLite file, one writer).
- E2E encryption / zero-trust (LAN + TLS behind nginx).
- Replacing the local CLI — it must keep working against local *or* remote buses.
- Channel ACLs (v1 visibility is uniform per §5; SSE already uses a per-subscriber predicate,
  so ACLs later are a predicate change, not a protocol change).

## 3. Architecture

```
                  ┌────────────────────────────────────────────┐
 browser ─HTTP/SSE┤ Bun server (bin/server.ts)                 │
 agent  ─JSON-RPC►│  auth mw → JSON-RPC → ┌─────────────────┐  │
 CLI(local)─direct│  SSE ←─ events tailer─│ core (src/bus.ts│◄─┼── CLI(remote) via RpcBus
                  │                       │  sync, typed)   │  │      (HTTP)
                  │                       └────────┬────────┘  │
                  │             SQLite WAL: triggers write     │
                  │             events(seq) on every change —  │
                  │             the DB itself is the change    │
                  │             log, so NO writer can bypass   │
                  │             fan-out (local CLI included)   │
                  └────────────────────────────────────────────┘
```

Four pieces, strictly layered:

1. **`src/bus.ts` — core.** All DB logic extracted from `bin/comms.ts`: schema + self-migration
   (+ triggers, §4), `join/post/inbox/read/thread/receipts/status/channels/rename`.
   Synchronous, typed results and **typed errors** (enumerated variants, §7), no `console.log`,
   no `process.exit`, no stdin, no `@file` resolution, no `process.pid` stamping. Every call
   takes `ctx: { principal, actor }`; **`mode: local | server` is bound at `openBus()`**, not
   per-call — the server build is *type-level unable* to construct a local-root principal, so
   no stray code path can re-enable auto-register or param identity.
   - `local` openBus: `principal = {agentId: <--from value>, kind: 'agent', scopes: ALL,
     localRoot: true}`; auto-register (`touch`) enabled; `actor` = `--from`/`--for`.
   - `server` openBus: principal from token (§5); auto-register **disabled**; `actor` =
     token's agentId unless `as` with `post:as`.
   - **Assertion rule lives in the core, per-parameter (§5), never as a blanket check.**
2. **`src/bus-iface.ts` — `interface Bus` (async).** The seam that keeps dual mode honest:
   `LocalBus` (in-process core) and `RpcBus` (HTTP client) implement it; **one contract test
   suite runs against both in CI**. CLI renderer (`fmtRow`, receipts, exit codes) sits above it.
3. **`src/server/` — HTTP layer.** `Bun.serve`: bearer-auth middleware → JSON-RPC 2.0 at
   `POST /rpc` → `Bus` calls; events tailer + SSE at `GET /stream`; static dashboard. No SQL
   in this layer.
4. **`bin/comms.ts` / `bin/dashboard.ts` / `bin/server.ts`** — shells. CLI gains `COMMS_URL`
   remote transport (§7); dashboard gains login/chat/admin (§8).

**Fan-out.** The server never assumes it saw every write, and never relies on seeing writes at
all: **DB triggers** append to `events(seq INTEGER PRIMARY KEY AUTOINCREMENT, kind, msg_id,
agent_id, at)` on INSERT/UPDATE of `messages`, INSERT of `reads`, UPDATE of `agents`, UPDATE
of `tokens.revoked_at` (§4). Triggers live in the DB file, so **every** writer emits events —
new core, stale `comms.ts` checkout, manual `sqlite3`. One tailer
(`WHERE seq > ? ORDER BY seq`, 250 ms + immediate kick after in-process posts) feeds one SSE
broadcaster. SSE `id:` = `events.seq`. Why not `messages.rowid`: it misses UPDATEs (status,
receipts, presence, revoke — exactly what the UI shows); rowids may be renumbered by VACUUM
(implementation behavior, not contract), always renumber by `.dump`/restore, and tail-deleted
rowids are reused (probe-confirmed). AUTOINCREMENT never reuses; INTEGER PK survives VACUUM
and dump. **Epoch:** `meta(key='epoch')` = random id, rotated on every restore, echoed in
every cursor-bearing response; epoch change ⇒ client full-resyncs (stale cursor can never
silently skip).

## 4. Data model (additive; self-migrating on open, existing pattern)

```sql
ALTER TABLE agents ADD COLUMN kind TEXT DEFAULT 'agent';        -- agent|human|service (§5)
ALTER TABLE messages ADD COLUMN meta TEXT;                      -- JSON; {"as":principal} on impersonated posts (§5)
CREATE TABLE tokens(
  id INTEGER PRIMARY KEY,
  prefix TEXT UNIQUE NOT NULL,          -- first 12 chars AFTER 'ac_' (≥60 bits, display+locator)
  agent_id TEXT NOT NULL,
  salt BLOB NOT NULL,                   -- 16 random bytes = HMAC key
  key_hash BLOB NOT NULL,               -- HMAC-SHA256(key=salt, msg=token)
  scopes TEXT NOT NULL DEFAULT '',      -- sorted csv: read:all,post:as,tokens:admin,agents:admin
  created_at TEXT NOT NULL,
  last_used TEXT NOT NULL,              -- debounced ≤1/min (§9)
  revoked_at TEXT
);
CREATE TABLE events(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,                   -- msg|status|read|presence|token
  msg_id TEXT, agent_id TEXT, at TEXT NOT NULL
);
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);   -- 'epoch' lives here
CREATE TABLE idempotency(
  agent_id TEXT NOT NULL, key TEXT NOT NULL,     -- key capped 128 B; GC >24h (§6)
  msg_id TEXT NOT NULL, req_hash TEXT NOT NULL,  -- sha256(canonical(full params)), §6
  created_at TEXT NOT NULL,
  PRIMARY KEY(agent_id, key)
);
CREATE TABLE message_recipients(msg TEXT NOT NULL, target TEXT NOT NULL);
CREATE INDEX msg_rec_idx ON message_recipients(target, msg);
CREATE TABLE cursors(
  agent_id TEXT NOT NULL, consumer TEXT NOT NULL DEFAULT 'default',
  last_seq INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(agent_id, consumer)
);
```

Triggers (same self-migrating open):

```sql
CREATE TRIGGER msg_ai AFTER INSERT ON messages BEGIN
  INSERT INTO events(kind,msg_id,agent_id,at) VALUES('msg',NEW.id,NEW.sender,NEW.created_at);
  INSERT INTO message_recipients(msg,target)
    SELECT NEW.id, trim(j.value)
    FROM json_each('["' || replace(NEW.recipients, ',', '","') || '"]') j;
END;
CREATE TRIGGER msg_au AFTER UPDATE OF status ON messages BEGIN
  INSERT INTO events(kind,msg_id,agent_id,at) VALUES('status',NEW.id,NEW.sender,NEW.updated_at);
END;
CREATE TRIGGER reads_ai AFTER INSERT ON reads BEGIN
  INSERT INTO events(kind,msg_id,agent_id,at) VALUES('read',NEW.msg,NEW.agent,NEW.read_at);
END;
CREATE TRIGGER agents_au AFTER UPDATE ON agents BEGIN
  INSERT INTO events(kind,agent_id,at) VALUES('presence',NEW.id,NEW.last_seen);
END;
CREATE TRIGGER tokens_au AFTER UPDATE OF revoked_at ON tokens BEGIN
  INSERT INTO events(kind,agent_id,at) VALUES('token',NEW.agent_id,NEW.revoked_at);
END;
```

`message_recipients` is populated **by trigger, not by core code** — a stale-binary writer can
never drift it silently; migration does a one-shot backfill of pre-existing rows. CSV split
via json_each is safe: recipients are regex-validated ids/roles/`@all` (no quotes/commas).
`events.agent_id` is point-in-time audit history: **`rename`** rewrites
`tokens.agent_id`, `reads.agent`, `cursors.agent_id`, `idempotency.agent_id` **in one
transaction** but not `events`. Token scopes are stored sorted-normalized; membership checks
are comma-anchored (`instr(','||scopes||',', ',scope,')`) — never bare substring LIKE.

## 5. Identity, roles, authz

Three orthogonal axes (unchanged from v2, verified closed):

| axis | where | meaning |
|---|---|---|
| `agents.role` | agents table | **routing** label for `--to` (unchanged; `join --role` sets only this) |
| `agents.kind` | agents table | identity type `agent\|human\|service`; set **only** by `token.create`, never by `join` |
| `scopes` | tokens table | privileges: `read:all`, `post:as`, `tokens:admin`, `agents:admin` |

- `admin` = all scopes on a token (a **scope set**, never a kind). Web-UI humans get
  `read:all`; observers get `read:all` only. No client mints its own privileges.
- **Principal from the bearer token, never from params.** Server resolves
  `principal = {agentId, kind, scopes}`. **Per-parameter rules** (v2's blanket "present ⇒ must
  equal principal" is dead — it was unimplementable against §6):
  - `from`, and `join`/`read` `agent`: **assertion** — omit, or equal `principal.agentId`, else `-32002`.
  - `inbox`/`read` `for`: equals principal, **unless** token has `read:all` → **non-marking peek** (no `reads` row).
  - `token.create` `agent`: **admin target**, not an assertion; needs `tokens:admin`; validated against id regex.
  - `as`: **not** an assertion; requires `post:as`. Server sets `messages.sender = as-target`
    (impersonation is observable in `sender`) **and** `meta.as = principal.agentId` (audit;
    surfaced as `as:` in mirror front matter). Client-supplied body fields are never copied through.
  - Client `fingerprint` ignored server-side — the token *is* identity.
- **Human rows are minted by `token.create {kind:'human'}`** (row created at token-create
  time; server auto-register stays disabled — nothing mints rows inside `POST /rpc login`).
- **Status permission:** sender OR resolved intended recipient (id or role match via
  `recipientsMatch`) may `ack/done/status`; `agents:admin` may set any. Documented honesty
  (accepted for v1): role is self-granted via `join --role`, so role-based ack is
  self-grantable; `@all` messages can be closed by anyone; `status` is one global field.
  **Role must never become an authz input when ACLs land.**
- **Visibility (v1):** every token can read every message (consistent with AGENTS.md "assume
  everything is visible"); `read:all` gates exactly three things — `history`, stream
  `scope=all`, `for≠self` peek. It is **cost/UX control, not confidentiality**. SSE delivery
  is server-side filtered per subscriber: `scope=mine | channel:<x> | all`.
- **Token format:** `ac_` + base64url(32 random bytes). Lookup: locate row by `prefix` (first
  12 chars after `ac_`), compute `HMAC-SHA256(key=salt, msg=token)`,
  `crypto.timingSafeEqual` on the two 32-byte digests (never raw-token `===`; equal-length
  probe-confirmed). `Authorization` capped at 128 B before hashing. **No argon2** (would
  freeze the single Bun thread per request).
- **Bootstrap:** `comms token create --agent X --admin [--force]` is **local-transport-only**
  on the server host — filesystem write access to `comms.db` **is** the root of trust; HTTP
  has no bootstrap path, ever (`token.create` over HTTP requires `tokens:admin`). Guard inside
  the DB (not a lock file):

  ```sql
  BEGIN IMMEDIATE;
  SELECT count(*) FROM tokens WHERE revoked_at IS NULL
    AND instr(',' || scopes || ',', ',tokens:admin,') > 0;
  -- >0 ⇒ abort unless --force (print existing prefixes); else INSERT
  COMMIT;
  ```

  The second concurrent waiter blocks on the write lock, then sees the row. The guard stops
  **accidental or concurrent duplicate bootstrap — it is not a security boundary**. Revoking
  the last admin **correctly reopens local bootstrap**: that is the documented recovery path
  for a lost admin token, not a lockout.

## 6. JSON-RPC 2.0 API

`POST /rpc`, bearer auth (or session cookie for the web UI, §8), single request per call
(**batches rejected** — they bypass rate limits with undefined partial-failure semantics).
Methods mirror CLI verbs:

| method | params | notes |
|---|---|---|
| `join` | `{role?, caps?}` | id from token; `role` = routing label only |
| `who` | `{all?}` | |
| `post` | `{to[], type, subject?, body, thread?, re?, tags?, channel?, as?, idempotencyKey?}` | `body` plain string; `as` per §5; remote CLI auto-generates key (§7) |
| `inbox` | `{for?, open?, unread?, channel?, mark?}` | `for≠self` = non-marking peek, needs `read:all` (§5) |
| `read` | `{id, for?}` | marks reads for principal only |
| `thread` / `receipts` | `{id}` | ungated (§5 visibility) |
| `status` | `{id, state}` | permission per §5 |
| `channels` | `{}` | unions channels table (empty channels included) |
| `history` | `{channel?, sinceSeq?, limit?}` | needs `read:all`; SQL-filtered, indexed; returns high-water `cursor` (seq) for the stream handoff (§6-stream) |
| `login` / `logout` | `{token}` / `{}` | web UI only; sets/ clears HttpOnly session cookie; store **in-memory — restart = logout** |
| `stream.ticket` | `{}` | → `{ticket}` 60 s single-use — **non-cookie clients only** (§6-stream) |
| `inbox.wait` | `{for?, sinceSeq?, timeout?}` | long-poll ≤60 s → `{messages[], cursor}`; **never auto-advances** — client commits via `cursor.set` after processing (at-least-once); the primitive for scripts/MCP |
| `cursor.get` / `cursor.set` | `{consumer?}` / `{consumer, seq, force?}` | keyed `(agent_id, consumer)`; monotonic unless `force`; includes `epoch` in responses |
| `rename` | `{agent?, newId}` | self always; other targets need `agents:admin`; transactional (§4) |
| `token.create` | `{agent, kind?, admin?}` | `tokens:admin`; **creates the agent row** (`kind` per §5); returns full token once |
| `token.list` / `token.revoke` | `{agent?}` / `{prefix}` | `tokens:admin` |

**Errors:** `-32700` parse · `-32600` invalid request · `-32601` method not found · `-32602`
invalid params · `-32603` internal · `-32001` unauthorized · `-32002` forbidden (scope/id
assertion) · `-32003` not found · `-32004` rate limited (**HTTP 429 + `Retry-After`**) ·
`-32005` conflict (idempotency mismatch) · `-32006` contention (SQLITE_BUSY after retries,
**HTTP 503 + `Retry-After`** — deliberately *distinct* from rate-limit; no overloading).

**Idempotency:** table-backed (§4), scoped `(authenticated agent_id, key)` — admin `as` scopes
to the token. `req_hash = sha256(canonical(full params object))` — body, to, channel, type,
**subject, thread, re, tags, as**; canonical = UTF-8, object keys sorted, recipients sorted,
no insignificant whitespace. Key ≤ 128 B. Insert in the **same transaction** as the message;
the 16-bit PK collision retry also lives in that transaction and rewrites
`idempotency.msg_id` before commit (no committed key pointing at a rolled-back id). Replay of
same key+same hash → original `{id, thread, channel, file}`; same key+different hash →
`-32005`. GC sweeps rows >24 h at startup + hourly. Clients omitting the key get
at-least-once; **remote CLI generates one key per logical post, reused across its own retries**
(the threat model is flaky-LAN retry).

**Streaming:** `GET /stream?scope=mine|channel:<x>|all[&since=<seq>]`. Server-side predicate
per subscriber; **one shared tailer + broadcaster** (the dashboard's per-client
`setInterval(SELECT *)` pattern is explicitly not carried forward — event deltas + initial
`history` snapshot instead). `idleTimeout: 0` on Bun.serve; `: ping` every 20 s; ≤2 SSE per
token; resume via `Last-Event-ID` = seq, or explicit `?since=<seq>`. **Auth:** CLI/agents send
the bearer header; **browsers use the HttpOnly session cookie** — same-origin EventSource sends
it automatically, so native reconnect + `Last-Event-ID` work unmodified (single-use tickets in
a query string break exactly that: EventSource reuses the consumed URL, gets 401, and the spec
fails the connection permanently). Tickets remain for non-cookie clients that can't set
headers; the app then constructs a fresh EventSource with `?since=<seq>` from the last
delivered event. **Snapshot→stream handoff:** `history` returns its high-water `cursor`; the
stream opens at `since=cursor` — no gap, no overlap (seqs dedupe). Long-lived bearer never in
a query string (nginx logs).

**Watch durability:** `watch` persists its cursor via `cursors(agent_id, consumer)` so messages
between short-lived `watch --exit-on-new` runs aren't skipped. Local `watch` keeps today's
rebaseline quirk through M1 (goldens hold); cursor-backed watch lands in M3. `inbox.wait`
long-poll is the easy primitive for bash/Hermes/MCP.

## 7. CLI / transport compatibility

- Precedence: `COMMS_URL` set ⇒ remote; `--local` forces direct; else `COMMS_HOME` direct.
  **Transport banner** (`transport=local:<path> | remote:<url> as <id>(<scopes>)`) prints on
  stderr **only in remote mode, or when both COMMS_URL and COMMS_HOME are set (ambiguity), and
  always on `join`/`who`** — not on every command (agent-token cost, stderr goldens).
- In remote mode `--from/--agent` are assertions per §5; AGENTS.md usage stays verbatim *when
  ids match the token* — documented, not implied.
- Exit-code contract, mapped from typed core errors **and** the RPC table:

  | RPC error | HTTP | CLI exit |
  |---|---|---|
  | `-32001` / `-32002` | 401/403 | **3** (identity) |
  | `-32700/-32600/-32601/-32602` | 400/404 | **2** (usage) |
  | `-32003` | 404 | **1** |
  | `-32005` | 409 | **1** |
  | `-32004` | 429 | backoff per `Retry-After`, then **1** |
  | `-32006` | 503 | backoff per `Retry-After`, then **1** |
  | `-32603` | 500 | **1** |

  Local mode maps typed errors directly: not-found→1, usage→2, identity→3. The core exports
  the variant enum; the shell maps, never invents.
- Remote `join` does not stamp the server's `process.pid` (pid is a local-mode field).
- The `file` field in `post` results is **server-relative**; remote agents read the mirror via
  `GET /raw/messages/<channel>/<file>` (authed; `realpath` under `MSG_DIR`, filename must match
  `^msg-[A-Za-z0-9._-]+\.md$`) — or just use `read`. AGENTS.md updated in M6 to say so.

## 8. Web UI (human interface)

- Login: paste token → `login` RPC → HttpOnly session cookie
  (`SameSite=Strict; Secure; HttpOnly`); SSE rides the same cookie (§6). Token not persisted to
  localStorage. **CSRF:** cookie-authed requests must have `Content-Type: application/json`
  exactly (forces a CORS preflight the server never grants) **and** `Origin` == configured
  origin; bearer-authed requests exempt (SameSite ignores ports; text/plain form POSTs don't
  preflight).
- Human identity: **`token.create {kind:'human'}`** mints the agent row (e.g. `human-bakon`);
  DMs from humans are ordinary posts in agent inboxes — zero agent-side changes.
- Chat pane per channel + DM threads; admin panel: token list w/ scopes, create (shown once),
  revoke, per-agent presence, all-channel feed via `history`.
- Dashboard transport switch: direct-DB (standalone) vs `/rpc`+`/stream` (server). Receipts
  logic comes from the core, not the dashboard's duplicate `receiptsOf`.

## 9. Server mechanics & limits

- **Single-writer rule:** the hosted DB has exactly one writer — the server process.
  Local-direct mode is for standalone/dev buses and host-side admin ops (bootstrap, recovery).
  `.comms/` `0700`, `comms.db*` `0600`, owned by the service user; host CLI admin ops run as
  that user (else foreign-owned `-wal/-shm` ⇒ `SQLITE_READONLY`).
- **One connection** via `openBus()` at startup — no per-RPC `CREATE/ALTER/PRAGMA`.
- Server connection: `busy_timeout = 150` ms (not 5000 — a sync sleep on the only thread hangs
  everyone); `SQLITE_BUSY` after retries ⇒ `-32006`/503; `synchronous = NORMAL` (server WAL
  only). CLI keeps 5000 ms.
- Server-side `inbox/history` are **SQL-filtered via `message_recipients` + indexes**; the CLI
  keeps the JS filter until a fixture proves row-identity. `tokens.last_used` updated ≤1/min
  per token so reads stay WAL-readers.
- Mirror `writeFileSync` stays **outside** the DB transaction; insert-first (no orphan `.md` on
  failed insert). `post` retries the INSERT on 16-bit PK collision — id format unchanged.
- **Limits:** token buckets, separate budgets — write: burst 30, refill 2/s; read: burst 120,
  refill 10/s; SSE excluded from buckets but ≤2 streams/token; **unauthenticated 401s per-IP:
  burst 10, refill 1/s, checked before HMAC compute** (kills token-spraying as a hash oracle).
  `Content-Length` ≤ 256 KB pre-parse; `Authorization` ≤ 128 B; recipients ≤ 32; tags ≤ 20 ×
  32 B; subject ≤ 200 UTF-8 bytes; body ≤ 256 KB; idempotency key ≤ 128 B.
- **Identifier validation in the core:** agent id / channel `^[a-z0-9][a-z0-9_-]{0,31}$`, type
  `^[a-z0-9._-]{1,32}$`, enforced before any path join (fixes the local-mode traversal that
  today becomes remote arbitrary file write). Regexes gate **writes only** — legacy
  nonconforming rows stay readable/addressable; migration runs a **preflight** listing them.
- `readBody` (`-` stdin, `@file`) lives **only in the CLI shell** — core `post(body: string)`;
  a remote JSON body of `@/etc/shadow` is data, never a server file read.
- **Backups:** out-of-process as the service user (`sqlite3 … ".backup"`, `umask 077` — never
  on the server's own connection, it blocks the single thread); files 0600 (they contain token
  digests); **rotate `meta.epoch` on every restore**; never `cp` (WAL).

## 10. Milestones

- **M1 — core extraction + seams + contract harness.** `src/bus.ts` (typed results/errors,
  `ctx {principal, actor}` + mode at `openBus`, per-parameter assertions, identifier
  validation, `events` + all triggers, `message_recipients` trigger + backfill, `meta` column +
  `meta(key,value)` + epoch, `cursors(agent_id,consumer)`, canonical-hash helper). Injectable
  seams: clock (`nowIso/stamp`), rng (`shortHex/newId`), pid, mirror sink. `interface Bus` +
  `LocalBus`; contract-suite skeleton; goldens over a fixture DB with frozen clock capturing
  stdout+stderr+exit+mirror bytes. Known quirks preserved as behavior (two `stamp()` calls,
  ms-stripped `nowIso`, 16-bit suffix, unfiltered unresolved count, ON CONFLICT column set,
  watch rebaseline, rename non-atomicity *documented* — fixed in M2 transaction).
- **M2 — server.** `bin/server.ts`, auth mw, JSON-RPC, events tailer + SSE (seq ids, epoch,
  tickets, CSRF guards), `RpcBus`, contract suite green on both impls, integration tests (two
  processes, tokens, traversal probes, idempotency crash-window, 429/503, trigger fan-out from
  a foreign writer).
- **M3 — remote CLI.** `COMMS_URL` transport, conditional banner, auto idempotency keys,
  cursor-backed watch + consumer naming, `token` subcommand, exit table.
- **M4 — web UI.** login/session cookie, chat panes, admin token panel, receipts from core.
- **M5 — MCP adapter (stretch).** `bin/mcp.ts` `tools/list`/`tools/call` → same RPC methods
  (Hermes registers it natively like crw); `inbox.wait` is its natural tool shape.
- **M6 — deploy.** systemd unit (service user, 0700/0600), nginx TLS + `proxy_buffering off` +
  long `proxy_read_timeout` + HTTP/2, AGENTS.md/README updates, bootstrap + backup runbook.

## Appendix A — v1 findings → v2 resolutions

All 26 v1 findings resolved in v2 @85d3f52 and **verified closed by both reviewers** (grok's
"Closed — do not reopen": G1, G4–G12; claude confirmed all 4 v1 blockers resolved in
substance). The matrix lives in `git show 85d3f52:docs/RFC-001-server.md` Appendix A.

## Appendix B — v2-round findings → v2.1 resolution

| finding | sev | resolution |
|---|---|---|
| C17/G13 bootstrap `kind='admin'` never fires; revoke/reopen contradiction | **blocker (shared)** | §5 scope-membership predicate in BEGIN IMMEDIATE; guard = anti-duplicate, not security; revoke-reopens = documented recovery |
| C18 tailer misses UPDATEs; rowid not durable (VACUUM/dump/tail-delete) | major | §3′/§4 events + triggers + AUTOINCREMENT + epoch |
| C19 message_recipients drift from stale writers | major | §4 AFTER INSERT trigger + backfill |
| C20 local actor undefined; blanket assertion breaks `--from` + goldens | major | §3 ctx{principal,actor} + mode@openBus; §5 per-param rules |
| C21 single-use ticket breaks EventSource reconnect; handoff gap | major | §6 cookie SSE + since=<seq> + history cursor handoff |
| C22 cookie CSRF (text/plain, same-site ignores ports) | major | §6/§8 SameSite=Strict+Secure, CT+Origin checks, bearer exempt |
| C23 cursors(agent_id) collides across consumers | major | §4/§6 (agent_id,consumer), monotonic, inbox.wait at-least-once |
| G14 blanket vs inbox-for/token.create/as; sender-vs-as; human mint location | major | §5 four-rule table; sender=as-target + meta.as; token.create mints human |
| G15 HMAC concat contradiction | minor | §5 deleted; salt = HMAC key |
| G16 req_hash gaps; canonical undefined; key cap; retry txn | minor | §6 full canonical, 128 B cap, in-txn msg_id rewrite |
| C24/G17 SQLITE_BUSY overloading; no exit table; 401 bucket unbudgeted | minor | §6 -32006/503; §7 table; §9 burst 10 refill 1/s pre-HMAC |
| C25 banner costs tokens + stderr goldens | minor | §7 conditional banner |
| C26 backup blocks thread; digests; legacy rows; /raw symlinks | minor | §9 out-of-process/0600/preflight; §7 realpath+pattern |
| C27/G18 meta column absent; idempotency TTL+rename list; read:all creep; rename target; role honesty | minor | §4 meta col + idempotency in rename txn; §6 GC; §6 read:all trio; §6 rename{agent?}; §5 role note |
