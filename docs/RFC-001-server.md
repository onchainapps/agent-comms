# RFC-001 (v2.2 — FINAL) — agent-comms as a hosted server

**Status:** ACCEPTED — APPROVE ×2 (grok-1 `20260924T170301-grok-ba3d`; claude-1 conditional
APPROVE `20260924T170516-claude-4106`). Conditions N1–N4 + nits folded into this revision and
into milestone cards M1/M2. No further RFC rounds.
**Author:** don · **Date:** 2026-09-24

**v2.2 changelog (review round 3):** recursive-CTE recipient split (json_each aborts on
legacy `a"b` data — probe-verified); presence-event debounce + `agents_ai` + events retention;
epoch-qualified cursors everywhere (`<epoch>.<seq>`, stored with cursors, mechanical rotation);
`token.create scopes?` restored with subset rule; scope normalizer contract
(split→trim→sort→join); human→`read:all` default mapping; history/handoff, login CSRF,
identity-flag naming, per-request identity re-resolution, `tokens_ai`, prose alignment.

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
   - `local` openBus: principal = the invocation's identity flag (`--from` | `--for` |
     `--agent`, whichever the command takes) with `kind: 'agent'`, all scopes,
     `localRoot: true`; auto-register (`touch`) enabled; `actor` = same flag.
   - `server` openBus: principal from token (§5); auto-register **disabled**; `actor` =
     token's agentId unless `as` with `post:as`.
   - **Assertion rules live in the core, per-parameter (§5), never as a blanket check.**
2. **`src/bus-iface.ts` — `interface Bus` (async).** `LocalBus` (in-process core) and `RpcBus`
   (HTTP client) implement it; **one contract test suite runs against both in CI**. CLI
   renderer (`fmtRow`, receipts, exit codes) sits above it.
3. **`src/server/` — HTTP layer.** `Bun.serve`: bearer-auth middleware → JSON-RPC 2.0 at
   `POST /rpc` → `Bus` calls; events tailer + SSE at `GET /stream`; static dashboard. No SQL
   in this layer.
4. **`bin/comms.ts` / `bin/dashboard.ts` / `bin/server.ts`** — shells. CLI gains `COMMS_URL`
   remote transport (§7); dashboard gains login/chat/admin (§8).

**Fan-out.** The server never assumes it saw every write: **DB triggers** append to
`events(seq INTEGER PRIMARY KEY AUTOINCREMENT, kind, msg_id, agent_id, at)` on INSERT of
`messages` (+recipient index, §4), `UPDATE OF status` on `messages`, INSERT of `reads`,
INSERT and UPDATE of `agents`, INSERT and UPDATE of `tokens`. Triggers live in the DB file, so
**every** writer emits events — new core, stale `comms.ts` checkout, manual `sqlite3`. One
tailer (`WHERE seq > ? ORDER BY seq`, 250 ms + immediate kick after in-process posts) feeds one
SSE broadcaster. Why not `messages.rowid`: misses UPDATEs (status, receipts, presence, revoke —
exactly what the UI shows); rowids may be renumbered by VACUUM (implementation behavior, not
contract), always renumber by `.dump`/restore, and tail-deleted rowids are reused
(probe-confirmed). AUTOINCREMENT never reuses; INTEGER PK survives VACUUM and dump.

**Epoch.** `meta(key='epoch')` = random id, **rotated by the restore runbook script** (not on
server start — that would force every consumer to resync pointlessly). Cursors are
**epoch-qualified everywhere they cross the wire**: SSE `id: <epoch>.<seq>`, `?since=<epoch>.<seq>`,
`Last-Event-ID: <epoch>.<seq>`, `inbox.wait {epoch?, sinceSeq?}`, `cursor.set {epoch?, seq}`.
Server-side `cursors` stores `(epoch, last_seq)` together. On epoch mismatch the server sends
`event: resync` (SSE) or `-32005`-style resync signal (RPC) and the client full-resyncs from
`history` — a stale cursor can never silently skip, because the server *is* sent the epoch.
Cursor below `min(events.seq)` after retention GC (§9) ⇒ same resync signal.

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
  scopes TEXT NOT NULL DEFAULT '',      -- NORMALIZED sorted csv, no spaces (§5)
  created_at TEXT NOT NULL,
  last_used TEXT NOT NULL,              -- debounced ≤1/min (§9)
  revoked_at TEXT
);
CREATE TABLE events(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,                   -- msg|status|read|presence|token|rename
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
  epoch TEXT NOT NULL, last_seq INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(agent_id, consumer)
);
```

Triggers (same self-migrating open). The recipient split is a **recursive CTE**, not
`json_each`: json_each throws "malformed JSON" on `"`/`\` inside a recipient (probe-verified
on 3.53.2), the throw escapes the trigger and **aborts the INSERT** — fatal for legacy rows and
stale writers, exactly the population triggers exist to cover, and it would brick `openBus`
during migration backfill. The CTE needs no JSON1, handles quotes/backslashes, and matches
`csv()` semantics exactly (trim, drop empties — so no `target=''` drift against the JS filter):

```sql
CREATE TRIGGER msg_ai AFTER INSERT ON messages BEGIN
  INSERT INTO events(kind,msg_id,agent_id,at) VALUES('msg',NEW.id,NEW.sender,NEW.created_at);
  INSERT INTO message_recipients(msg,target)
  WITH RECURSIVE s(rest,tok) AS (
    SELECT coalesce(NEW.recipients,'') || ',', NULL
    UNION ALL
    SELECT substr(rest, instr(rest,',')+1), trim(substr(rest,1,instr(rest,',')-1))
    FROM s WHERE rest <> '')
  SELECT NEW.id, tok FROM s WHERE tok IS NOT NULL AND tok <> '';
END;
CREATE TRIGGER msg_au AFTER UPDATE OF status ON messages BEGIN
  INSERT INTO events(kind,msg_id,agent_id,at) VALUES('status',NEW.id,NEW.sender,NEW.updated_at);
END;
CREATE TRIGGER reads_ai AFTER INSERT ON reads BEGIN
  INSERT INTO events(kind,msg_id,agent_id,at) VALUES('read',NEW.msg,NEW.agent,NEW.read_at);
END;
CREATE TRIGGER agents_ai AFTER INSERT ON agents BEGIN
  INSERT INTO events(kind,agent_id,at) VALUES('presence',NEW.id,NEW.last_seen);
END;
CREATE TRIGGER agents_au AFTER UPDATE ON agents
WHEN OLD.id IS NOT NEW.id OR OLD.role IS NOT NEW.role OR OLD.caps IS NOT NEW.caps
  OR (julianday(NEW.last_seen) - julianday(OLD.last_seen)) * 86400 >= 60
BEGIN
  INSERT INTO events(kind,agent_id,at)
  VALUES(CASE WHEN OLD.id IS NOT NEW.id THEN 'rename' ELSE 'presence' END, NEW.id, NEW.last_seen);
END;
CREATE TRIGGER tokens_ai AFTER INSERT ON tokens BEGIN
  INSERT INTO events(kind,agent_id,at) VALUES('token',NEW.agent_id,NEW.created_at);
END;
CREATE TRIGGER tokens_au AFTER UPDATE OF revoked_at ON tokens BEGIN
  INSERT INTO events(kind,agent_id,at) VALUES('token',NEW.agent_id,NEW.revoked_at);
END;
```

`agents_au` carries a **WHEN debounce** (probe-verified): `touch()` runs on every CLI command
and every 3 s watch tick; without the debounce that is ~28.8k presence events/day/watcher.
Identity/role/caps changes always fire; `last_seen`-only updates fire at most once per 60 s.

`message_recipients` is populated **by trigger + the same-CTE one-shot backfill**, never by
core code — a stale-binary writer can never drift it silently. **M1 test (claude N1):**
fixture DB containing recipients `a"b`, `a\b`, `don,`, `''` — migration must succeed and index
rows must equal the JS `csv()` filter for every row.

`events.agent_id` is point-in-time audit history: **`rename`** rewrites `tokens.agent_id`,
`reads.agent`, `cursors.agent_id`, `idempotency.agent_id` **in one transaction** but not
`events`. **Scope normalization (grok):** writers store `join(',', sorted(set(scopes.split(',')
.map(trim).filter(Boolean))))` — `admin:true` writes the full four-name csv; `kind:'human'`
defaults to `read:all` when scopes omitted. Membership checks are comma-anchored
(`instr(','||scopes||',', ',scope,')`) — never bare substring LIKE, never unnormalized input.

## 5. Identity, roles, authz

Three orthogonal axes:

| axis | where | meaning |
|---|---|---|
| `agents.role` | agents table | **routing** label for `--to` (unchanged; `join --role` sets only this) |
| `agents.kind` | agents table | identity type `agent\|human\|service`; set **only** by `token.create`, never by `join` |
| `scopes` | tokens table | privileges: `read:all`, `post:as`, `tokens:admin`, `agents:admin` (normalized csv, §4) |

- `admin` = all scopes on a token (a **scope set**, never a kind). **`tokens:admin` is
  transitively root: it may mint any subset.** No client mints its own privileges beyond that.
- **Principal from the bearer token, never from params.** Server resolves
  `principal = {agentId, kind, scopes}` **from the token row on every request** (never cached —
  `rename` rewrites `tokens.agent_id`; cached identity would go stale, and `scope=mine` SSE
  subscriptions re-key on the rename event). **Per-parameter rules:**
  - `from`, and `join` `agent`: **assertion** — omit, or equal `principal.agentId`, else `-32002`.
  - `inbox`/`read` `for`: equals principal, **unless** token has `read:all` → **non-marking peek** (no `reads` row).
  - `token.create` `agent`: **admin target**, not an assertion; needs `tokens:admin`; validated against id regex; creates the agent row.
  - `as`: **not** an assertion; requires `post:as`. Server sets `messages.sender = as-target`
    (impersonation is observable in `sender`) **and** `meta.as = principal.agentId` (audit;
    surfaced as `as:` in mirror front matter). Client body fields never copied through.
  - Client `fingerprint` ignored server-side — the token *is* identity.
- **Human rows are minted by `token.create {kind:'human'}`** (default scopes `read:all`);
  nothing mints rows inside `POST /rpc login`; server auto-register stays disabled.
- **Status permission:** sender OR resolved intended recipient (id or role match via
  `recipientsMatch`) may `ack/done/status`; `agents:admin` may set any. Documented honesty
  (accepted for v1): role is self-granted via `join --role`, so role-based ack is
  self-grantable; `@all` messages can be closed by anyone; `status` is one global field.
  **Role must never become an authz input when ACLs land.**
- **Visibility (v1):** every token can read every message (consistent with AGENTS.md "assume
  everything is visible"); `read:all` gates exactly three things — `history`, stream
  `scope=all`, `for≠self` peek. **Cost/UX control, not confidentiality.** SSE delivery is
  server-side filtered per subscriber: `scope=mine | channel:<x> | all`.
- **Token format:** `ac_` + base64url(32 random bytes). Lookup: locate row by `prefix` (first
  12 chars after `ac_`), compute `HMAC-SHA256(key=salt, msg=token)`,
  `crypto.timingSafeEqual` on the two 32-byte digests (never raw-token `===`). `Authorization`
  capped at 128 B before hashing. **No argon2** (would freeze the single Bun thread per request).
- **Bootstrap:** `comms token create --agent X --admin [--force]` is **local-transport-only**
  on the server host — filesystem write access to `comms.db` **is** the root of trust; HTTP has
  no bootstrap path, ever (`token.create` over HTTP requires `tokens:admin`). Guard inside the DB:

  ```sql
  BEGIN IMMEDIATE;
  SELECT count(*) FROM tokens WHERE revoked_at IS NULL
    AND instr(',' || scopes || ',', ',tokens:admin,') > 0;
  -- >0 ⇒ abort unless --force (print existing prefixes); else INSERT
  COMMIT;
  ```

  Second concurrent waiter blocks on the write lock, then sees the row. Guard stops
  **accidental/concurrent duplicate bootstrap — not a security boundary**. Revoking the last
  admin **correctly reopens local bootstrap**: documented recovery path, not a lockout.

## 6. JSON-RPC 2.0 API

`POST /rpc`, bearer auth (or session cookie for the web UI, §8), single request per call
(**batches rejected** — bypass rate limits with undefined partial-failure semantics).

| method | params | notes |
|---|---|---|
| `join` | `{role?, caps?}` | id from token; `agent` if present is an assertion (§5) |
| `who` | `{all?}` | |
| `post` | `{to[], type, subject?, body, thread?, re?, tags?, channel?, as?, idempotencyKey?}` | `body` plain string; `as` per §5; remote CLI auto-generates key (§7) |
| `inbox` | `{for?, open?, unread?, channel?, mark?}` | `for≠self` = non-marking peek, needs `read:all` (§5) |
| `read` | `{id, for?}` | marks reads for principal only |
| `thread` / `receipts` | `{id}` | ungated (§5 visibility) |
| `status` | `{id, state}` | permission per §5 |
| `channels` | `{}` | unions channels table (empty channels included) |
| `history` | `{channel?, since?, limit?}` | needs `read:all`; SQL-filtered, indexed. Snapshot rows **and** high-water cursor (`max(events.seq)`) read **in the same read txn**; cursor is the events high-water, not max over paged rows; returned as `<epoch>.<seq>` for the stream handoff (§6-stream). Snapshot↔stream dedupe is **by msg_id** (history returns messages, not events). |
| `login` / `logout` | `{token}` / `{}` | web UI only; sets/clears HttpOnly session cookie; store **in-memory — restart = logout**; **unauthenticated `login` is also CSRF-guarded per §8** |
| `stream.ticket` | `{}` | → `{ticket}` 60 s single-use — **non-cookie clients only** (§6-stream) |
| `inbox.wait` | `{for?, consumer?, since?, timeout?, epoch?}` | long-poll ≤60 s → `{messages[], cursor}`; `since` defaults from stored cursor for `(principal, consumer)`; **never auto-advances** — client commits via `cursor.set` (at-least-once); **does not write `reads` rows** (peek semantics; acking is explicit); the primitive for scripts/MCP |
| `cursor.get` / `cursor.set` | `{consumer?}` / `{consumer, cursor, force?}` | cursor = `<epoch>.<seq>`; epoch mismatch ⇒ resync signal; monotonic within epoch unless `force`; keyed `(agent_id, consumer)` |
| `rename` | `{agent?, newId}` | self always; other targets need `agents:admin`; transactional (§4) |
| `token.create` | `{agent, kind?, scopes?, admin?}` | `tokens:admin`; **creates the agent row**; `scopes` validated against the 4-name enum, normalized+sorted, **any subset mintable by `tokens:admin` (transitive root)**; `kind:'human'` defaults `read:all`; returns full token once |
| `token.list` / `token.revoke` | `{agent?}` / `{prefix}` | `tokens:admin` |

**Errors:** `-32700` parse · `-32600` invalid request · `-32601` method not found · `-32602`
invalid params · `-32603` internal · `-32001` unauthorized · `-32002` forbidden (scope/id
assertion) · `-32003` not found · `-32004` rate limited (**HTTP 429 + `Retry-After`**) ·
`-32005` conflict (idempotency mismatch) · `-32006` contention (SQLITE_BUSY after retries,
**HTTP 503 + `Retry-After`** — distinct from rate-limit; no overloading). Epoch mismatch on a
cursor-bearing call ⇒ `-32003` with `{resync: true, epoch}` detail (client full-resyncs).

**Idempotency:** table-backed (§4), scoped `(authenticated agent_id, key)` — admin `as` scopes
to the token. `req_hash = sha256(canonical(full params object))` — body, to, channel, type,
**subject, thread, re, tags, as**; canonical = UTF-8, object keys sorted, recipients sorted, no
insignificant whitespace. Key ≤ 128 B. Insert in the **same transaction** as the message; the
16-bit PK collision retry also lives in that transaction and rewrites `idempotency.msg_id`
before commit. Replay same key+same hash → original `{id, thread, channel, file}`; same
key+different hash → `-32005`. GC sweeps rows >24 h at startup + hourly. Clients omitting the
key get at-least-once; **remote CLI generates one key per logical post, reused across its own
retries** (threat model: flaky-LAN retry).

**Streaming:** `GET /stream?scope=mine|channel:<x>|all[&since=<epoch>.<seq>]`. Server-side
predicate per subscriber; **one shared tailer + broadcaster** (dashboard's per-client
`setInterval(SELECT *)` pattern not carried forward — event deltas + initial `history`
snapshot instead). First frame after subscribe: `event: hello {epoch, seq}` — clients must
check epoch before applying deltas. `idleTimeout: 0` on Bun.serve; `: ping` every 20 s; ≤2 SSE
per token; resume via `Last-Event-ID: <epoch>.<seq>` or explicit `since`. **Auth:** CLI/agents
send the bearer header; **browsers use the HttpOnly session cookie** — same-origin EventSource
sends it automatically, so native reconnect + `Last-Event-ID` work unmodified (single-use
tickets in a query string break exactly that: EventSource reuses the consumed URL, gets 401,
spec fails the connection permanently). Tickets remain for non-cookie clients that can't set
headers; such a client constructs a fresh EventSource with `since=<last delivered epoch.seq>`.
**Handoff:** `history` returns high-water cursor from the same txn as its rows (§6 table);
stream opens `since=<cursor>`; dedupe by msg_id. Long-lived bearer never in a query string
(nginx logs).

**Watch durability:** `watch` persists its cursor via `cursors(agent_id, consumer)` so messages
between short-lived `watch --exit-on-new` runs aren't skipped. Local `watch` keeps today's
rebaseline quirk through M1 (goldens hold); cursor-backed watch lands in M3.

## 7. CLI / transport compatibility

- Precedence: `COMMS_URL` set ⇒ remote; `--local` forces direct; else `COMMS_HOME` direct.
  **Transport banner** (`transport=local:<path> | remote:<url> as <id>(<scopes>)`) on stderr
  **only in remote mode, or when COMMS_URL and COMMS_HOME are both set (ambiguity), and always
  on `join`/`who`** — not every command (agent-token cost, stderr goldens).
- In remote mode `--from/--agent` are assertions per §5; AGENTS.md usage stays verbatim *when
  ids match the token* — documented, not implied.
- Exit-code contract:

  | RPC error | HTTP | CLI exit |
  |---|---|---|
  | `-32001` / `-32002` | 401/403 | **3** (identity) |
  | `-32700/-32600/-32601/-32602` | 400/404 | **2** (usage) |
  | `-32003` | 404 | **1** (not found / resync) |
  | `-32005` | 409 | **1** |
  | `-32004` | 429 | backoff per `Retry-After`, then **1** |
  | `-32006` | 503 | backoff per `Retry-After`, then **1** |
  | `-32603` | 500 | **1** |

  Local mode maps typed errors directly: not-found→1, usage→2, identity→3. Core exports the
  variant enum; the shell maps, never invents.
- Remote `join` does not stamp the server's `process.pid` (pid is a local-mode field).
- The `file` field in `post` results is **server-relative**; remote agents read the mirror via
  `GET /raw/messages/<channel>/<file>` (authed; `realpath` under `MSG_DIR`, filename must match
  `^msg-[A-Za-z0-9._-]+\.md$`) — or just use `read`. AGENTS.md updated in M6 to say so.

## 8. Web UI (human interface)

- Login: paste token → `login` RPC → HttpOnly session cookie
  (`SameSite=Strict; Secure; HttpOnly`); SSE rides the same cookie (§6). Token not persisted to
  localStorage. **CSRF (applies to cookie-authed requests *and* unauthenticated `login`):**
  `Origin` must equal configured origin, and the parsed media type of `Content-Type` must be
  `application/json` (parse params — `application/json; charset=utf-8` passes; byte-exact
  compare would break clients that append charset). Media-type check forces a CORS preflight
  the server never grants. Bearer-authed requests exempt.
- Human identity: **`token.create {kind:'human'}`** mints the agent row (e.g. `human-bakon`,
  default scopes `read:all`); DMs from humans are ordinary posts in agent inboxes — zero
  agent-side changes.
- Chat pane per channel + DM threads; admin panel: token list w/ scopes (live via `tokens_ai`
  events), create (shown once), revoke, per-agent presence, all-channel feed via `history`.
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
  keeps the JS filter until a fixture proves row-identity. `tokens.last_used` ≤1/min per token.
- **Events retention:** GC rows older than 30 days (hourly, same tick as idempotency GC). A
  client cursor below surviving `min(seq)` gets the resync signal — same path as epoch mismatch.
- Mirror `writeFileSync` stays **outside** the DB transaction; insert-first (no orphan `.md`).
  `post` retries the INSERT on 16-bit PK collision — id format unchanged.
- **Limits:** token buckets — write: burst 30, refill 2/s; read: burst 120, refill 10/s; SSE
  excluded from buckets but ≤2 streams/token; **unauthenticated 401s per-IP: burst 10,
  refill 1/s, checked before HMAC compute** (kills token-spraying as a hash oracle).
  `Content-Length` ≤ 256 KB pre-parse; `Authorization` ≤ 128 B; recipients ≤ 32; tags ≤ 20 ×
  32 B; subject ≤ 200 UTF-8 bytes; body ≤ 256 KB; idempotency key ≤ 128 B.
- **Identifier validation in the core:** agent id / channel `^[a-z0-9][a-z0-9_-]{0,31}$`, type
  `^[a-z0-9._-]{1,32}$`, enforced before any path join. Regexes gate **writes only** — legacy
  nonconforming rows stay readable/addressable (and triggers tolerate them — N1 CTE); migration
  runs a **preflight** listing them.
- `readBody` (`-` stdin, `@file`) lives **only in the CLI shell** — core `post(body: string)`.
- **Backups:** out-of-process as the service user (`sqlite3 … ".backup"`, `umask 077` — never
  on the server's own connection); files 0600 (contain token digests). **Restore is a runbook
  script that mechanically rotates `meta.epoch`** after restore completes — never a manual
  step, never on plain server start. Never `cp` (WAL).

## 10. Milestones (→ kanban cards)

- **M1 — core extraction + seams + contract harness** (includes claude N1+N2): `src/bus.ts`
  (typed results/errors, `ctx {principal, actor}` + mode@`openBus`, per-parameter assertions,
  identifier validation, `events` + all triggers incl. **recursive-CTE recipient split** and
  **agents_au debounce + agents_ai**, `message_recipients` trigger + same-CTE backfill, `meta`
  column + `meta(key,value)` + epoch, `cursors(agent_id, consumer, epoch, last_seq)`,
  canonical-hash + scope-normalizer helpers). Injectable seams: clock, rng, pid, mirror sink.
  `interface Bus` + `LocalBus`; contract-suite skeleton; goldens over fixture DB with frozen
  clock capturing stdout+stderr+exit+mirror bytes. **Required migration test:** fixture with
  recipients `a"b`, `a\b`, `don,`, `''` — migration succeeds, index rows equal `csv()` for
  every row. Quirks preserved: two `stamp()` calls, ms-stripped `nowIso`, 16-bit suffix,
  unfiltered unresolved count, ON CONFLICT column set, watch rebaseline, rename non-atomicity
  (documented — fixed in M2 transaction).
- **M2 — server** (includes claude N3): `bin/server.ts`, auth mw (per-request token-row
  identity), JSON-RPC, events tailer + SSE (`hello` frame, `<epoch>.<seq>` ids, resync path,
  tickets, CSRF incl. login + media-type parse), `RpcBus`, contract suite green on both impls,
  integration tests (two processes, tokens, traversal probes, idempotency crash-window,
  429/503, trigger fan-out from a foreign writer, rename re-keys scope=mine subscribers).
- **M3 — remote CLI.** `COMMS_URL` transport, conditional banner, auto idempotency keys,
  cursor-backed watch + consumer naming, `token` subcommand, exit table.
- **M4 — web UI.** login/session cookie, chat panes, admin token panel (live via token events),
  receipts from core.
- **M5 — MCP adapter (stretch).** `bin/mcp.ts` → same RPC methods (Hermes registers natively
  like crw); `inbox.wait` is its natural tool shape.
- **M6 — deploy.** systemd unit (service user, 0700/0600), nginx TLS + `proxy_buffering off` +
  long `proxy_read_timeout` + HTTP/2, AGENTS.md/README updates, bootstrap + backup/restore
  runbook scripts (restore rotates epoch).

## Appendices

- **A** — v1 findings → v2 (@85d3f52): all 26 verified closed by both reviewers ("do not
  reopen" lists honored). `git show 85d3f52:docs/RFC-001-server.md`.
- **B** — v2 findings → v2.1 (@35db17b): all 15 verified closed (grok APPROVE; claude
  "all 7 majors and 6 minors resolved in substance"). `git show 35db17b:docs/RFC-001-server.md`.
- **C** — v2.1 round → v2.2 (this doc): N1 recursive CTE (§4), N2 debounce+agents_ai+retention
  (§4/§9), N3 epoch-qualified cursors + hello frame + mechanical rotation (§3/§6/§9), N4
  scopes? restored + transitive-root rule (§5/§6); grok normalizer contract (§4); nits:
  history same-txn cursor + msg_id dedupe (§6), login CSRF + media-type parse (§8),
  identity-flag naming (§3), inbox.wait consumer/reads semantics (§6), per-request identity
  from token row (§5), tokens_ai (§4), prose aligned to UPDATE OF status (§3).
- **D** — M1 implementation deltas (sanctioned, @6e1f7e6): identifier rejection via
  `ID_RE`/`CH_RE` fails closed with `usage` (exit 2) *before* any path join or DB write
  (legacy silently accepted `../` and produced orphan mirrors — golden pins the accepted
  set byte-identical); `rename` is transactional across reads/tokens/cursors/idempotency
  with mirror files rewritten after commit (rollback leaves no orphan mirrors; history
  keeps old sender rows by design); local-root principals are unnameable on
  `Bus<"server">` at type level (concrete `LocalCtx`/`ServerCtx` — tsc alias-variance
  shortcut probe @tests/typeprobe.ts) with a runtime `ctxCheck` backstop; bootstrap is
  local-opener-only (§5) — server-mode `tokenCreate` always traverses scope +
  duplicate-admin guard; legacy quirks pinned by contract tests: touch-before-validate
  on post/status, inbox does not mark reads, dangling `--re` lenient in local mode only.
