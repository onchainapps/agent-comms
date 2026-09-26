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
- ~~Channel ACLs~~ — superseded by Appendix G: dm-shaped channels carry a real
  `channel_members` ACL (`canSee`); public channels stay open to all tokens (§5). SSE
  already uses a per-subscriber predicate, so this was a predicate change, not protocol.

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
  kind TEXT NOT NULL,                   -- msg|status|read|presence|token|rename|group (§4/F fan-out)
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
.map(trim).filter(Boolean))))` — `admin:true` writes the full five-name csv (§5/G3);
`kind:'human'` defaults to `read:all,read:dm` when scopes omitted. Membership checks are
comma-anchored
(`instr(','||scopes||',', ',scope,')`) — never bare substring LIKE, never unnormalized input.

## 5. Identity, roles, authz

Three orthogonal axes:

| axis | where | meaning |
|---|---|---|
| `agents.role` | agents table | **routing** label for `--to` (unchanged; `join --role` sets only this) |
| `agents.kind` | agents table | identity type `agent\|human\|service`; set **only** by `token.create`, never by `join` |
| `scopes` | tokens table | privileges: `read:all`, `read:dm`, `post:as`, `tokens:admin`, `agents:admin` (normalized csv, §4; G3) |

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
- **Human rows are minted by `token.create {kind:'human'}`** (default scopes `read:all,read:dm` — G3; not `read:all` alone);
  nothing mints rows inside `POST /rpc login`; server auto-register stays disabled.
- **Status permission:** sender OR resolved intended recipient (id, role, or group match
  via `recipientsMatch` — F honesty: group membership grants ack on group-addressed mail)
  may `ack/done/status`; `agents:admin` may set any. On dm-shaped channels a non-party gets
  `not_found`, never `forbidden` (G2 — no existence oracle). Documented honesty
  (accepted for v1): role is self-granted via `join --role`, so role-based ack is
  self-grantable; `@all` messages can be closed by anyone; `status` is one global field.
  **Role must never become an authz input when ACLs land** (extended by F: group membership
  is likewise delivery, never a canSee input).
- **Visibility (v1):** every token can read every message on PUBLIC channels (consistent
  with AGENTS.md "assume everything is visible" — **M6 updates AGENTS.md to read** (claude
  m2: the file is unchanged today): DMs are private from other agents, NOT from operators
  holding `read:all`+`read:dm`); `read:all` gates
  the unfiltered `history` snapshot (channel/since views are ungated — §6 row, M3 ruling c), stream `scope=all`, `for≠self` peek; `read:dm` gates DM visibility (G2/G3).
  **dm-shaped channels are the one confidentiality boundary — `canSee` per Appendix G;
  everything else stays cost/UX control.** SSE delivery is
  server-side filtered per subscriber: `scope=mine | channel:<x> | all`, plus canSee on
  every event carrying a msg_id (G2).
- **Role grammar (F/G close the P2 class):** `joinAgent` — **both modes** (grok minor: the
  host CLI against the server DB is the same file; local join is the bypass) — validates
  `role` against `ID_RE` (no `:` `~` `@` — a role of `group:secret` must not impersonate a
  structured target via the bare-token match) and rejects `role == <another agent's id>`.
  **Symmetrically (claude N3, probe T2):** `token.create` and `rename` reject a NEW id when
  any OTHER agent holds `role ==` that id ⇒ `identity_conflict` — otherwise mallory takes
  `role=zed` first and inherits `alice→zed` mail when zed is later minted. **AND (claude
  R1, probe zz_fg3_retired_role.ts — reproduced on real code):** since G6 keeps old mail
  addressed to the retired id, `joinAgent` (both modes) ALSO rejects a `role` equal to any
  `agent_retired.id` ⇒ `identity_conflict` — else bob→carol frees the name `bob`, mallory
  takes `role=bob`, and the bare-token role arm delivers AND lets her ack every pre-rename
  alice→bob message. Triangle closed: id≠role, role≠id, role≠retired-id; preflight lists
  legacy roles colliding with retired ids. **All identity checks run INSIDE the write's
  IMMEDIATE txn, or as a single guarded statement (`INSERT … SELECT … WHERE NOT EXISTS`) —
  check-then-write across two connections (server + host CLI on one file) lets both writes
  land (claude m-c, probe: guarded form lets exactly one win).** Checks are
  write-path only; legacy rows stay; preflight lists collisions. **Local-mode byte-parity
  note:** legacy roles like `x:y` exist and local = root, so local mode warns (golden-safe;
  **hit-only** — no stderr on clean joins, so golden bytes don't move) rather than failing
  closed on the GRAMMAR and the role==id collision check (claude m-b: local `joinAgent`'s
  INSERT branch is a minting path by the paragraph's own reasoning, so both checks run
  there too). **The `agent_retired` check is NOT part of the warning: it rejects with
  `identity_conflict` in BOTH modes (grok: do not weaken R1 into the warning).**
  Recipient-token grammar pinned (§4):
  `'@all' | 'group:' ID | bare ID-or-role`.
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
| `post` | `{to[], type, subject?, body, thread?, re?, tags?, channel?, as?, idempotencyKey?}` | `body` plain string; `as` per §5; `thread`/`re` anchors validated against canSee in server mode (G2w-iv); remote CLI auto-generates key (§7) |
| `inbox` | `{for?, open?, unread?, channel?, mark?}` | `for≠self` = non-marking peek, needs `read:all` (§5) |
| `read` | `{id, for?}` | marks reads for principal only |
| `thread` / `receipts` | `{id}` | ungated on public channels; dm-shaped channels filtered by canSee (G2) — invisible ⇒ byte-identical `not_found` |
| `status` | `{id, state}` | permission per §5 |
| `channels` | `{}` | unions channels table (empty channels included); dm-shaped rows hidden unless member or `read:dm` (G2) |
| `history` | `{channel?, since?, limit?}` | **gate (claude M3 ruling c/m3):** the UNFILTERED global snapshot needs `read:all`; a `channel`-filtered or `since`-paged view is ungated — public rows are readable by every token (§5), consistent with stream `scope=channel:`. SQL-filtered, indexed; dm-shaped rows additionally row-filtered by canSee in BOTH modes (G2 — `read:all` alone does not satisfy canSee). **Snapshot mode (no `since`):** rows are MESSAGES (`created_at DESC, rowid DESC`, newest page, returned oldest-first) — messages stay visible even when their events are gc'd or predate events; cursor = `max(max(events.seq), gc_floor)` read **in the same txn** (floor-clamped so the handoff cursor is never below retention ⇒ never instant-resyncs/livelocks), returned as `<epoch>.<seq>`. **Since mode:** pages EVENTS ASC (oldest unseen first), cursor = last delivered event, paging to `hasMore=false` delivers every row exactly once. Snapshot↔stream dedupe is **by msg_id** (history returns messages, not events). `cursor.get` on a stored foreign-epoch row is a **resync** (never a silent seq-0 collapse); a missing row is `{epoch, seq: 0}`; an explicit current-epoch `cursor.set` is the recovery commit and is not blocked by the foreign row's monotonic check. `consumer` matches `[a-z0-9._#@~-]{1,128}` on `cursor.get`, `cursor.set` AND `inbox.wait` (grammar cap — caller-supplied keys must not grow unbounded; 128 covers the CLI's longest namespaced key `cli@<id>.all#<dm-name>` = 114 B). |
| `login` / `logout` | `{token}` / `{}` | web UI only; sets/clears HttpOnly session cookie; store **in-memory — restart = logout**; **unauthenticated `login` is also CSRF-guarded per §8** |
| `stream.ticket` | `{}` | → `{ticket}` 60 s single-use — **non-cookie clients only** (§6-stream) |
| `inbox.wait` | `{for?, consumer?, since?, timeout?, epoch?}` | long-poll ≤60 s → `{messages[], cursor}`; `since` defaults from stored cursor for `(principal, consumer)`; **never auto-advances** — client commits via `cursor.set` (at-least-once); **does not write `reads` rows** (peek semantics; acking is explicit); the primitive for scripts/MCP |
| `cursor.get` / `cursor.set` | `{consumer?}` / `{consumer, cursor, force?}` | cursor = `<epoch>.<seq>`; epoch mismatch ⇒ resync signal; monotonic within epoch unless `force`; keyed `(agent_id, consumer)` |
| `rename` | `{agent?, newId}` | self always; other targets need `agents:admin`; transactional (§4) |
| `token.create` | `{agent, kind?, scopes?, admin?}` | `tokens:admin`; **creates the agent row**; `scopes` validated against the 5-name enum, normalized+sorted, **any subset mintable by `tokens:admin` (transitive root)**; `kind:'human'` defaults `read:all,read:dm`; returns full token once |
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
**Handoff:** snapshot `history` returns the floor-clamped events high-water
(`max(max(seq), gc_floor)`) from the same txn as its rows (§6 table); stream opens
`since=<cursor>`; dedupe by msg_id. Long-lived bearer never in a query string
(nginx logs).

**Watch durability:** `watch` persists its cursor via `cursors(agent_id, consumer)` so messages
between short-lived `watch --exit-on-new` runs aren't skipped. Local `watch` keeps today's
rebaseline quirk through M1 (goldens hold); cursor-backed watch lands in M3.

## 7. CLI / transport compatibility

- Precedence: `COMMS_URL` set ⇒ remote; `--local` forces direct; else `COMMS_HOME` direct.
  **Transport banner** (`transport=local:<path> | remote:<url> as <id>(<scopes>)`) on stderr:
  **remote ⇒ every command** (identity rides the first response's `x-comms-*` headers — zero
  extra RPC, so the old agent-token cost does not apply); **ambiguity (`--local` while
  `COMMS_URL` is set) ⇒ once**; **plain local ⇒ never** (stderr goldens hold). Identity and
  scopes come from the token ROW, never from client claims.
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
  `^msg-[A-Za-z0-9._-]+\.md$`, then canSee on the resolved message — G2) — or just use `read`.
  AGENTS.md updated in M6 to say so.

## 8. Web UI (human interface)

- Login: paste token → `login` RPC → HttpOnly session cookie
  (`SameSite=Strict; Secure; HttpOnly`); SSE rides the same cookie (§6). Token not persisted to
  localStorage. **CSRF (applies to cookie-authed requests *and* unauthenticated `login`):**
  `Origin` must equal configured origin, and the parsed media type of `Content-Type` must be
  `application/json` (parse params — `application/json; charset=utf-8` passes; byte-exact
  compare would break clients that append charset). Media-type check forces a CORS preflight
  the server never grants. Bearer-authed requests exempt.
- Human identity: **`token.create {kind:'human'}`** mints the agent row (e.g. `human-bakon`,
  default scopes `read:all,read:dm` ⇒ omniview incl. DMs); DMs from humans are ordinary
  posts in agent inboxes — zero agent-side changes.
- Chat pane per channel + DM threads; admin panel: token list w/ scopes (live via `tokens_ai`
  events), create (shown once), revoke, per-agent presence, all-channel feed via `history`.
- Dashboard transport switch: direct-DB (standalone) vs `/rpc`+`/stream` (server). Receipts
  logic comes from the core, not the dashboard's duplicate `receiptsOf`.

## 9. Server mechanics & limits

- **Single-writer rule:** the hosted DB has exactly one writer — the server process.
  Local-direct mode is for standalone/dev buses and host-side admin ops (bootstrap, recovery).
  `.comms/` `0700`, `comms.db*` `0600`, owned by the service user; **`messages/` (the
  mirror, which now holds DM bodies) `0700` likewise — §9 permission covers it explicitly
  (claude m4)**; host CLI admin ops run as
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
- **D** — M1 implementation deltas (sanctioned, @6e1f7e6 + round-3): identifier rejection
  via `ID_RE` fails closed with `usage` (exit 2) *before* any path join or DB write **on the
  write paths** (post/join/rename/token.create); local `inbox`/`read` keep the legacy
  touch-before-validate quirk on unvalidated ids (pinned by test, byte-parity with de4ed3b); `rename` is transactional across reads/tokens/cursors/idempotency
  with mirror files rewritten after commit (rollback leaves no orphan mirrors; history
  keeps old sender rows by design); local-root principals are unnameable on
  `Bus<"server">` at type level (concrete `LocalCtx`/`ServerCtx` — tsc alias-variance
  shortcut probe @tests/typeprobe.ts) with a runtime `ctxCheck` backstop; bootstrap is
  local-opener-only (§5) — server-mode `tokenCreate` always traverses scope +
  duplicate-admin guard; legacy quirks pinned by contract tests: touch-before-validate
  on post/status, inbox does not mark reads, dangling `--re` lenient in local mode only.
- **E** — M1 round-3 deltas (sanctioned, post-rereview): triggers are versioned via
  `meta.schema_version` — on bump, all triggers DROP+CREATE inside one IMMEDIATE txn and
  `message_recipients` is rebuilt via the canonical SPLIT_SQL (old-generation rows had
  space-only trim); scope enum is exactly the 4 names (§4/§5) and mint validates each name
  (no commas, no unknowns — smuggled names verify inert on legacy rows) — **superseded by
  G3: a FIFTH name `read:dm` joins the enum when Appendix G lands (M1.5); until then the
  4-name validation is what M1 shipped**; `tokens:admin` is
  transitive-root for minting (§5, supersedes the round-2 "must hold" rule); scope failures
  are `forbidden` (-32002), `unauthorized` reserved for credential failure (§6); status
  "any" gate is `agents:admin`, not `read:all` (§5); `events.agent_id` is NOT rewritten by
  rename (§4, audit); history `since`-mode pages ASC oldest-first with no-hole cursor,
  snapshot mode keeps newest-page; ALL since-bearing entry points share one cursor parser
  that resyncs on epoch mismatch AND gc-floor breach (§9); collision retry retargets the
  DERIVED thread to the new id; dangling-re rejection precedes ensureChannel (no orphan
  rows); `db` demoted to `testDb`; post/rename return `internal` instead of raw-throwing.

- **E2** — M1 round-4 deltas (sanctioned, post-rereview; supersedes nothing in F):
  trigger migration is ONE IMMEDIATE txn with version re-read inside (DROP+CREATE+
  recipient rebuild+version), **forward-only** (stored > current ⇒ leave triggers —
  two binary generations can share one DB without flip-flop). The DROP set is a
  **hardcoded array** in bus.ts, not a sqlite_master scan — when F lands, `group_members_ai`
  and `group_members_ad` must be named in that array AND in TRIGGER_DDL in the same v3
  txn (claude n8 / grok minor: `CREATE TRIGGER IF NOT EXISTS` never upgrades a stale
  generation — the M6 bug). History SNAPSHOT mode
  pages over MESSAGES (`created_at DESC, rowid DESC`) with cursor = floor-clamped events
  high-water `max(max(seq), gc_floor)` read in the same txn — legacy and gc'd-event
  messages stay visible (§6 literal) and the handoff cursor can never sit below
  retention (else every since-entry point resyncs and the client livelocks);
  since-mode still pages over events; the cursor WIRE representation is exactly ONE
  string `value.cursor = "<epoch>.<seq>"` (Ok<T> carries no parallel field); a STORED
  cursor whose epoch ≠ current is a resync at EVERY entry point — waitStep no-since AND
  cursor.get (which returns Res, never a silent seq-0 collapse; the failed get must not
  rewrite the row, and an explicit current-epoch cursor.set is the recovery commit,
  unblocked by the foreign row) — rotateEpoch zeroes gc_floor, so the floor check alone
  cannot catch it;
  wrapSession ctx conditional is NON-DISTRIBUTIVE ([B["mode"]] extends ["local"]) so
  union/generic modes fail CLOSED to server ctx (H4-H6 probes).

### Appendix F — work-groups (v3, post-review; claude t_d227a815/t_04fdd7bb + grok t_6f64030f/t_6484c2a1 folded)

Agents addressing each other by literal ids or self-granted `role:*` is too coarse for
ad-hoc collaboration ("everyone touching the swap-migration spike, look at this").
**work-groups** — named, self-organizing recipient sets agents create based on the work
they're doing.

- **Model:** `groups(name PK, created_by, created_at TEXT NOT NULL)` +
  `group_members(grp, agent_id, joined_at, PK(grp, agent_id))` +
  `INDEX gm_agent(agent_id, grp)` — the PK answers "who is in group X", the secondary
  index answers the HOT path "which groups is agent Y in" (inbox/waitStep/SSE tick; PK
  alone SCANs). Names and members use `ID_RE` (`:` excluded ⇒ `group:<name>` parses
  unambiguously; ≤32 so `group:`+name fits any target cap).
- **Addressing:** `group:<name>` is a recipient target alongside ids, bare `role`, and
  `@all`. `message_recipients` stores the **literal** `group:x` (no post-time expansion —
  keeps §4 trigger-only population) and membership resolves **at delivery time** (late
  joiner sees earlier group traffic; mailing-list semantics). `@all` never matches a group
  target — both halves: an `@all` message is not a group delivery, and `group:x` does not
  reach non-members. **Joining is not a replay trigger:** SSE and `waitStep` evaluate
  membership at TAIL time, and a `since=` resume replays events against CURRENT membership
  — a just-joined agent's resume can surface earlier group traffic (that is the late-joiner
  semantics, not a bug), and a just-left agent's tail stops at once (claude m3).
  **Receipts honesty (claude m6):** group receipts are NOT historical — late joiners show
  as unread on old group messages, leavers drop out; the intended set is current members
  of the incarnation that existed when the message was posted, excluding sender (grok B2).
  That set is this query, not a snapshot of who was a member at post time (no
  membership-history table — do not add one; a recreated group contributes nobody):
  ```sql
  SELECT gm.agent_id FROM group_members gm
  JOIN groups g ON g.name = gm.grp
  WHERE gm.grp = ? AND g.created_at <= ? AND gm.agent_id != ?
  -- .all(groupName, message.created_at, sender)
  ```
- **Delivery SQL (both reviewers independently measured the draft's OR+LIKE+EXISTS form at
  SCAN, 145× slower at 300k rows; the draft's `'role:'||:role` arm also never matched —
  roles are stored BARE, and `@all` was missing entirely):**
  ```sql
  SELECT msg FROM message_recipients WHERE target = :agent
  UNION ALL SELECT msg FROM message_recipients WHERE target = :role    -- skip arm if role NULL
  UNION ALL SELECT msg FROM message_recipients WHERE target = '@all'
  UNION ALL SELECT r.msg FROM group_members gm
    JOIN groups g ON g.name = gm.grp
    JOIN message_recipients r ON r.target = ('group:' || gm.grp)
    JOIN messages m ON m.id = r.msg AND m.created_at >= g.created_at
    WHERE gm.agent_id = :agent
  ```
  Every arm is an index SEARCH; ONE statement (never one arm per group —
  `MAX_COMPOUND_SELECT`=500). Arms may OVERLAP (a message addressed `a,@all` matches two
  arms) ⇒ caller dedupes by msg id (or use `UNION` — still index-driven, adds a sort).
  The `created_at >=` guard means a delete+recreate never
  inherits the previous incarnation's backlog. `groups.created_at` is written with
  `nowIso` (millis stripped), the same function as `messages.created_at`. A NULL
  `created_at` makes the comparison unknown and drops the whole group arm. A millis
  timestamp and a stripped timestamp of the same instant compare false
  (`"2026-06-01T00:00:00.000Z" >= "2026-06-01T00:00:00Z"`). Resolution is one second:
  a delete+recreate in the same second as a backlog row inherits that row, and an NTP
  step-back widens the window. **Tombstone (claude n5) — `deleted_at + 1s` is not an
  operation, and `datetime(deleted_at, '+1 second')` is the wrong one.** That function
  returns `YYYY-MM-DD HH:MM:SS`. An ISO `messages.created_at` compares `>=` that string
  even when the message is older (probe, bun:sqlite 1.4.2:
  `'2026-06-01T00:00:00Z' >= '2026-06-01 00:00:01'` is 1). The guard fails OPEN and the
  previous incarnation's backlog is inherited — the leak this tombstone was added to
  close. `+1s` is `strftime('%Y-%m-%dT%H:%M:%SZ', deleted_at, '+1 second')`, or the JS
  equivalent of `nowIso` (parse, +1000 ms, strip millis). Both sides stay `>=` on that
  shape. A second delete must upsert the PK, not insert a sibling — a lookup that is
  not the PK (or `MAX(deleted_at)`) reads an arbitrary row and the +1s does not apply.
  ```sql
  CREATE TABLE IF NOT EXISTS group_tombstones(
    name TEXT PRIMARY KEY, deleted_at TEXT NOT NULL);
  -- inside the group-delete txn, same txn as DELETE FROM group_members:
  INSERT INTO group_tombstones(name, deleted_at) VALUES(?, ?)
    ON CONFLICT(name) DO UPDATE SET deleted_at = excluded.deleted_at
    WHERE excluded.deleted_at > group_tombstones.deleted_at;
  -- at create. No tombstone row: created_at = nowIso(). Else (claude m-a — do NOT stamp
  -- a FUTURE created_at via):
  SELECT max(?, strftime('%Y-%m-%dT%H:%M:%SZ', ?, '+1 second'));
  -- .get(nowIso(), tombstone.deleted_at). Both binds are ISO-Z. Never datetime().
  -- m-a resolution: if nowIso() <= deleted_at, REJECT the create with `contention`
  -- (retry in <=1s) instead of stamping created_at = T+1s — a future stamp silently
  -- drops the NEW incarnation's own posts in second T (delete; join; post scripts) via
  -- the same >= guard. The contention reject closes the same-second leak and the NTP
  -- step-back window without ever timestamping ahead of the clock; max()+strftime stays
  -- as the arithmetic if an implementation prefers stamping, but reject is the rule.
  ```
  **Bind:** bun:sqlite 1.4.2 does not bind `:agent` from `{agent}` or `{$agent}` —
  that call returns only the literal `'@all'` arm. **All RFC SQL is illustrative; code
  uses positional `?` (house style) — never copy named params into `.all({agent})` (n7).**
  Passing SQL NULL for the role slot is the skip (0 rows, equal to
  omitting the arm); do not string-build the statement.
  ```sql
  SELECT msg FROM message_recipients WHERE target = ?
  UNION ALL SELECT msg FROM message_recipients WHERE target = ? AND ? IS NOT NULL
  UNION ALL SELECT msg FROM message_recipients WHERE target = '@all'
  UNION ALL SELECT r.msg FROM group_members gm
    JOIN groups g ON g.name = gm.grp
    JOIN message_recipients r ON r.target = ('group:' || gm.grp)
    JOIN messages m ON m.id = r.msg AND m.created_at >= g.created_at
    WHERE gm.agent_id = ?
  -- .all(agent, role, role, agent). Role NULL: pass null, null for the two role slots.
  ```
- **JS parity:** `recipientsMatch(recips, agent, role, memberships, msgCreatedAt)` takes
  `memberships: Map<grp, groups.created_at>` (the incarnation time, never `joined_at`,
  never a name set) so no call site can forget the group arm — call sites that must learn
  it in lockstep: `inbox`, `waitStep`, `setStatus`, `receiptsForMsg`, `bin/comms.ts` watch,
  `bin/dashboard.ts` receipts. Group arm: token is `group:x` AND `memberships.has(x)` AND
  `msgCreatedAt >= memberships.get(x)`. A name set cannot express the per-message
  comparison, so the required fixture cannot pass against the SQL arm. The server computes
  the SQL group arm **from the same Map** the JS arm reads, so the two sides cannot drift
  (claude B2 refinement). That Map is the JOIN's membership read, taken in the same
  snapshot as the arm (`BEGIN`, then `SELECT gm.grp, g.created_at FROM group_members gm
  JOIN groups g ON g.name = gm.grp WHERE gm.agent_id = ?`). A cache filled on an earlier
  tick is not the Map — the rename path already requires a miss to load from the DB.
  A contract fixture
  proves JS ≡ SQL row sets (both directions), including old-incarnation exclusion.
- **Authz:** groups are self-organizing — **no scope** to create/join/leave; membership is
  delivery, NEVER a `canSee` input (extends the §5 role rule). `group.join/leave` take
  `agent?` as an **assertion** (self only — otherwise anyone subscribes others to 500
  groups). **Documented honesty (same class as role):** group membership DOES grant
  ack/done/status on group-addressed messages (delivery-time resolved, includes earlier
  ones). `delete` requires `agents:admin`. **No group rename in v1** (claude M4 ruling):
  rename would rewrite history (recipients CSV + index, which `msg_ai` won't re-fire);
  use delete+create — the `created_at` guard makes it safe. **Two documented consequences
  (claude n3/n4):** post addressed to `group:<nonexistent>` is rejected `usage` —
  otherwise anyone pre-addresses a name and creates the group later to squat traffic
  (groups are not a confidentiality boundary, so the usage-vs-not_found existence oracle
  that G2w-v forbids for DM channels is acceptable here); and deleting a group with
  unresolved backlog DROPS that backlog from members' inboxes — mailing-list semantics,
  not a bug.
- **Lifecycle:** `group_members.agent_id` cascades in the §4 rename txn; `groups` rows
  survive member renames. `delete` DELETEs `group_members` in the SAME txn (delivery reads
  members, not groups — orphans still receive).
- **Fan-out:** AFTER INSERT/DELETE triggers on `group_members` emit `events kind='group'`
  — triggers, NOT core inserts, preserving §3 "no writer can bypass fan-out" (claude M2;
  grok's explicit-insert option rejected: stale binaries would change membership silently
  and stale the SSE membership caches). This IS a trigger-generation bump (v3, one
  IMMEDIATE txn per E2). Per-subscriber membership caches invalidate on `group` events.
  **Rename is an UPDATE of `group_members.agent_id` — the INSERT/DELETE triggers do NOT
  fire (grok minor):** a membership cache miss must load from the DB, never be treated as
  empty, or group delivery for the renamed id silently drops until some other group event.
- **CLI:** `group create|join|leave|list|show|delete` + `post --to group:swap-migration`;
  `join --group <name>` = create-if-missing + join self.
- **Limits:** name `ID_RE`; ≤512 members/group; **≤64 groups per agent** (enforced in
  join — bounds the UNION arm's seeks; latency cap, not `MAX_VARIABLE_NUMBER`); ≤64 groups
  created per agent (squatting hygiene); group targets count toward recipients ≤ 32.
- **Tests:** late-joiner delivery, leave stops delivery, member-rename cascade, `@all` vs
  group both-halves, delete+recreate backlog isolation (`created_at` guard),
  `agents:admin` gate on delete, name traversal, csv()/index literal round-trip, JS≡SQL
  parity fixture, join agent-assertion.

### Appendix G — DMs + admin omniview (v3, post-review; supersedes parts of §5)

Requirements: agents DM each other; users/admins see all DMs and channels.

**G0 — rulings on the three open questions.**
(a) DM participant rename: **freeze the name, move the ACL** (claude). Not re-derive
(grok): re-deriving rewrites `messages.channel` + `file` + mirror dirs (crash window,
dangling `/raw`) and breaks `scope=channel:` subscribers and `history{channel}` bookmarks.
The name is a frozen label chosen at creation; `channel_members` is the authority;
`--dm <peer>` finds the channel **by member pair**, not by derived name. The printed
`IN (?,?) … HAVING count(*)=2` without a shape predicate matches every channel both
ids belong to (including `general`) and treats "both queried ids are members" as
"the channel has two members" (a third member still matches). Use:
```sql
SELECT c.name FROM channel_members cm INDEXED BY cm_agent
JOIN channels c ON c.name = cm.channel
WHERE cm.agent_id IN (?, ?) AND cm.channel GLOB 'dm~*'
GROUP BY c.name
HAVING count(*) = 2
   AND (SELECT count(*) FROM channel_members x WHERE x.channel = c.name) = 2
ORDER BY c.created_at DESC, c.rowid DESC
LIMIT 1
```
The statement without the join and without `ORDER BY` returns every match — probe
(bun:sqlite 1.4.2): one pair that sits in `dm~alice~bob` and `dm~alice~bob~1` returns
both rows. `.get()` on that result is not deterministic, which contradicts the
tie-break below. This form picks the newest `channels.created_at`; `rowid` breaks a
same-second tie (`nowIso` is one-second resolution). Plan: SEARCH `cm_agent` on
`agent_id` (the GLOB is a filter on that seek, not a table scan), SEARCH channels by
PK, cardinality subquery is a PK seek. `channels.created_at` for a DM is `nowIso`,
never `''` (that empty string is only the seeded `general` row, bus.ts:344).
Invariant, written in the same txn as the channel
row: a dm-shaped channel has exactly two `channel_members` rows. Tie-break when several dm channels match one pair (possible after rename +
`~n` reuse — at least one was created for a different incarnation): the `ORDER BY`
above, deterministic, no error (claude n1). After rename, history
`{channel}`, `scope=channel:`, and `/raw` use the stored name from this lookup — never
re-sort current ids into a name. (b) **Group-DM form dropped** (both): `dm~<creator>~<slug>`
is shape-indistinguishable from `dm~<lo>~<hi>`; multi-party = Appendix F groups on an
ordinary channel. (c) **Split the scope** (claude, overriding the draft's lean and grok's
"keep one"): `read:dm` is a FIFTH scope name. One scope would RETROACTIVELY widen every
existing `read:all` credential — minted under §5's "cost/UX control, not confidentiality"
promise — into a DM-omniview credential; splitting later protects nothing (existing tokens
either lose DMs or keep them). Cost now: one enum name + normalizer + default.

**G1 — DM = derived-label channel (zero new message machinery).**
1:1 DMs live in a canonical channel `dm~<lo>~<hi>` — participant ids sorted by **code-unit
comparison** (not `localeCompare`: locales order `-`/`_` differently), `lo == hi` rejected
(no self-DM); `DM_RE = /^dm~(ID)~(ID)(~[1-9][0-9]{0,3})?$/` — the `~n` suffix only on
creation collision after id-reuse. `~` is not in `ID_RE` ⇒ split unambiguous. Channel
validation widens to `ID_RE || DM_RE` via ONE helper used by `post()` (the write gate) and
`preflight()` (else every DM is flagged bad); `ensureChannel` stays policy-free. Read
filters (history/inbox/watch) do NOT widen (§9 gates writes only). DM names are up to 73
chars (`3 + 32 + 1 + 32 + 5` suffix) — no ≤64 channel-length assumption exists anywhere in
the schema, indexes, or mirror paths (claude m1). `--dm <peer>` sugar ⇒
channel + recipients = **the peer id only** (self is omitted from recipients at post time
by the existing post path — claude m8 wording fix); parties come from `channel_members`,
not recipients.

**G2 — `canSee` is the ONE predicate (replaces "sender, resolved recipients, or
read:all", which was role-spoofable: role is self-granted, so `join --role alice` read
alice's DMs — probe-verified end to end).**
```
canSee(principal, msg) = name shape of msg.channel is NOT dm-shaped
                       OR principal.agentId ∈ channel_members(msg.channel)
                       OR hasScope(principal, read:dm)
```
`channel_members(channel, agent_id, PK(channel, agent_id))` + `INDEX cm_agent(agent_id,
channel)`. Members are literal ids written at channel creation. **Role, group, and `@all`
are never consulted for access.** `allMessages`/`allMessageIds`/`tailEvents` stay raw local
dumps — the server never exposes them unscoped on the RPC surface; the SSE broadcaster
filters `tailEvents` through canSee per subscriber rather than shipping the raw tail
(grok minor; local `watch` keeps them). Delivery (inbox arms) and visibility (canSee) are
separate questions; a dm-channel message must satisfy BOTH. The `channels.kind` column is
at most a CHECK-enforced copy — the NAME SHAPE is the authority (GLOB in SQL), so a stale
writer's default can't flip a DM to public. `channel_members` has no trigger and no event
kind — `canSee` reads it LIVE (PK point lookup; cost is noise, grok probe 2.5 ms vs 2.4 ms)
or caches it invalidated by `rename` events; a cache miss loads from the DB, never empty
(claude n2 — the G6 member move is an UPDATE, so no trigger fires).
**Every path that returns or accepts a message id passes canSee, and "invisible" is
byte-identical to "missing"** (same error, same detail — no existence oracle):
`read` (check BEFORE the `reads` INSERT — else a non-party probe writes a reads row AND
emits a `kind=read` event leaking the id), `inbox` (confidentiality subject is the
CALLER, not `p.agent`; `for≠self` peek needs `read:all`, and seeing the other's DM rows
additionally needs `read:dm`), `threadOf` (threads span channels ⇒ per-row filter;
`not_found` only if ALL rows filtered; never build receipts for a hidden row),
`receipts` (`not_found` before `receiptsForMsg`), `waitStep`/stream `scope=mine` (JS
predicate per event), stream `scope=channel:x` (validate x; party check if dm-shaped),
SSE broadcaster (canSee on every event carrying a `msg_id`), `setStatus` (non-party on
dm ⇒ `not_found`, not `forbidden`), `post --re` (parent lookup applies canSee ⇒ B2 oracle
closed), `joinAgent`'s `unresolved` count (claude n6: computed under canSee — a non-party's
DM traffic count must not leak existence/volume), `/raw` (file → message → canSee),
`channels()` (dm-shaped rows hidden unless
member or `read:dm`; local mode lists all), `history` (method gate: `read:all` for the
unfiltered snapshot only — channel/since views ungated per M3 ruling c;
**canSee is the row predicate in both modes** — snapshot over messages and since over
events. `read:all` alone does not satisfy canSee. DM omniview of history = `read:all`
AND `read:dm`. There is NO "not row-filtered" exception — the loss IS what read:dm is for).
```sql
-- snapshot, no channel arg. $dm = 1 iff hasScope(read:dm). Positional:
SELECT * FROM messages m
WHERE m.channel NOT GLOB 'dm~*'
   OR EXISTS (SELECT 1 FROM channel_members cm
              WHERE cm.channel = m.channel AND cm.agent_id = ?)
   OR ? = 1
ORDER BY m.created_at DESC, rowid DESC LIMIT ?
-- since mode: same predicate on the joined message; events.seq stays the PK range.
-- The predicate goes in the WHERE, BEFORE LIMIT — NEVER a post-LIMIT JS filter
-- (claude m-d: bus.ts:950-954 post-filters today; a page entirely hidden would return
-- 0 rows with an unadvanced cursor and livelock).
-- history{channel}: canSee the channel ONCE, then the existing channel query.
-- A hidden dm channel returns the same empty page as a missing channel, not a new error.
```

**G2w — write side (draft was silent; a non-party could inject into any DM).**
(i) Posting INTO a dm-shaped channel requires the final sender (after `as`) ∈
`channel_members`; `read:all`/`read:dm` are READ-only omniview and never grant posting;
`post:as` impersonation stays audited via `meta.as`. (ii) dm-channel recipients must be a
**subset of the literal member ids** (wildcards `@all`, `role:*`, `group:*` rejected
`usage` -32602) — this is the rule the enforcement code checks; the `dm` CLI sugar sets
recipients to exactly the peer (G1), which satisfies it (C2 reconciliation: subset is the
predicate, equality is only what the sugar emits). (iii) parent lookup per G2.
(iv) **`thread` is an anchor too (claude N1 — today `thread=<any id>` is never
validated, and channel inheritance is `WHERE id=? OR thread=? ORDER BY created_at ASC`
(bus.ts:537), which can resolve to a squatter row rather than the named message).**
"Thread root" = the message whose `id` equals the `thread` value (claude m-e definition).
Legacy free-form explicit thread strings (any id-shaped value that never named a real
message) are rejected in server mode; local mode is unaffected, so golden is safe.
In server mode look up `messages WHERE id=?` for the thread param. Missing and
invisible are the same `not_found` with one detail string — do not copy the `re`
detail (`error: re -> unknown message id …`); it names `re`. "Byte-identical to a
missing `re`" means the same error code and the same missing-vs-hidden bytes, not
that string. If that row's channel is dm-shaped, the post's resolved channel must
EQUAL it, else `usage` (G7 "rejected"). That comparison is reached only after canSee
passes, so a non-party never sees `usage`. When `channel` is omitted, inheritance is
that id-row's `channel` column — not the `id OR thread` lookup. The inherited `--re`
path still skips the channel regex (that stays correct). (iii), (ii), and the G2w-i
membership check run on the resolved channel after that inheritance and BEFORE any
channel write. They apply only when the channel row already exists. Running them
against an empty `channel_members` rejects every first DM post — the helper has not
written the rows yet. As one unbranched sequence, (iv) and (v) cannot both be
implemented.
(v) **DM channel creation is ONE helper, both modes** (local `comms.ts` against the
server DB is the same file). `ensureChannel` is `INSERT OR IGNORE` and commits
outside the message txn (bus.ts:387, then the insert txn at :570), so a local first
post is the zero-member window G5 fail-closes. The helper is the other branch of
(iv), not a check that runs after (i):
- Canonicalize first (`lo`/`hi` by code unit; `lo==hi` rejected). A client-supplied
  `~n` is ignored. `dm~hi~lo` is the same request as `dm~lo~hi` — do not create a
  second channel, and do not return a different error for the non-canonical spelling.
- Member-pair lookup (the G0 statement) for `{lo,hi}`. If a channel already exists
  for that pair, post into the stored name. The requested name is not the key.
  Name-keyed create (`channel row missing ⇒ insert that string`) splits the pair.
- If no channel for the pair: create iff the final sender (after `as`, same word as
  G2w-i — not the token principal) ∈ `{lo,hi}` AND both ids exist in `agents` AND
  neither id is in `agent_retired`. Every other outcome is `not_found` with the same
  detail as a non-party naming an existing dm channel. `usage` vs `not_found` on this
  branch is an existence oracle; do not `usage`-reject a missing peer. "else rejected"
  in the previous sentence is this `not_found`, not a second code.
- One IMMEDIATE txn: channel row (`created_at = nowIso()`) + exactly 2
  `channel_members` rows + the message insert. Never call `ensureChannel` on a
  dm-shaped name. **Client-supplied `~n` is ignored (stripped before canonicalization),
  NOT a distinct reject (grok v4 binding)** — the pair, not the name, is the key, so
  naming `dm~x~y~1` resolves through the pair lookup like any spelling. `~n` is allocated
  ONLY inside the helper txn when the canonical name is taken by a DIFFERENT pair
  (claude m-e: a party minting a fresh duplicate + newest-created_at tie-break would
  silently re-route `--dm` for both = split conversation). With `agent_retired`
  blocking id reuse, the `~n` path is unreachable on server-mode DBs (it exists only
  for legacy/local ones); say so in the runbook. Renaming BACK to a retired id (the
  undo) is impossible by design — one-way door. On `UNIQUE` name collision: rollback, re-read the pair
  lookup, post into the winner if the sender is a member, else `not_found` — not
  `internal`.
- Recipients ⊆ `{lo,hi}` (or ⊆ the existing member ids if the lookup hit). Wildcards
  are `usage`, and that rejection rolls back the same txn so it does not leave a
  channel row.

**G3 — Authz matrix delta.** §5 "every token can read every message" is amended: public
channels unchanged; dm-shaped channels filtered by canSee. Scope enum becomes FIVE names:
`read:all, read:dm, post:as, tokens:admin, agents:admin`. `kind:'human'` defaults
`read:all,read:dm` (⇒ user/admin sees every DM and every channel — requirement met);
`admin:true` = all five; `read:dm` alone = DM rows on ungated paths only (`read`, `threadOf`, `receipts`,
`channels`, `/raw`, own inbox, and channel/since-scoped `history`) — it does not unlock the unfiltered `history` snapshot or stream `scope=all`;
`read:all` alone = channels only, no DM peek. Local mode keeps legacy see-all (host filesystem =
root of trust; byte-parity quirk pin).

**G4 — CLI/UI.** `comms.ts dm --from a --to b "text"` (post sugar); `comms.ts dms --for a`;
UI sidebar Channels / Groups / DMs; UI titles render from `channel_members`, never from
the frozen name; admin omniview = human token (`read:all,read:dm`) ⇒ `scope=all` default.

**G5 — Migration safety (draft failed OPEN — an older binary serving a v3 DB would show
every DM to every token).** `meta.acl_generation` (SEPARATE from `schema_version`, which
is the trigger generation): server-mode `openBus` REFUSES a DB whose `acl_generation`
exceeds the one it was compiled with (local mode may proceed — host is root). Lands in M2
before any DM code ships. A dm-shaped channel with ZERO `channel_members` rows fails
closed: `read:dm` only (not `read:all`), listed by preflight. `acl_generation` is a
DB-wide counter, not a per-channel stamp — do not "stamp and refuse the DB until an
admin assigns members" (that false-refuses a zero-DM DB such as `.review-bus`).
Missing key + no naked dm channel: write `acl_generation=1` inside the open
IMMEDIATE txn (marker re-read inside, `CREATE TABLE IF NOT EXISTS` outside the
`schema_version` gate) and proceed. Naked dm channel: canSee fails closed per
channel; do not refuse open. Key greater than compiled: server-mode `openBus`
refuses. The key binds only a binary that reads it — `openBus` today never does
(bus.ts:323). **Honesty (grok major + claude N4 concur): the key does NOT close M1
rollback — it only binds binaries N+1 onward.** Runbook: no pre-check server process may
open a DB that has a dm-shaped channel; replace the binary before the first DM row.
One concrete pre-G leak already shipped: **`bin/dashboard.ts`** is direct-DB,
unauthenticated, and `Bun.serve` with no hostname ⇒ binds 0.0.0.0 and re-serves every
message body to the LAN — "local = host is root of trust" covers the host USER, not LAN
visitors of a process that re-publishes the DB. FIX: standalone direct-DB dashboard binds
`127.0.0.1` by default and refuses (or strips dm-shaped rows) when the DB contains any
dm-shaped channel unless run `--omniview`; M6 runbook retires/replaces every pre-G binary
(comms.ts, dashboard.ts, host CLI checkouts) before the first dm row.
**Admin repair (C1):** for a zero-member dm-shaped channel left by a crash window, the
repair path is host-side local-mode SQL (or a future `channel.members` admin op); until
that method exists, G5 does not claim "an admin assigns members" — preflight listing is
the documented surface.

**G6 — rename interaction (ruling a).** The §4 rename txn additionally:
`UPDATE OR IGNORE group_members SET agent_id=new` (then delete leftovers), same for
`channel_members` (PK-safe via OR IGNORE + leftover delete; new id cannot pre-exist —
rename rejects it, and agent_retired below closes the re-mint path; **keep the leftover
delete** — under the alias/tombstone alternative `new` may already be a member via a
pre-rename join under the new id, claude n9). **No** rewrite of `message_recipients.target`, the recipients CSV,
`messages.channel/file`, mirror dirs, or `events` (audit). A plain
`UPDATE message_recipients SET target=new WHERE target=old` throws
`UNIQUE constraint failed` on `mr_uq` whenever any message already carries both
tokens; `OR IGNORE` leaves the stale old row. It is also core code writing
`message_recipients`, which §4 forbids, and it is the history mutation F rejects
for groups. Id-addressed mail stays addressed to the old id (honest, same as
`events.agent_id`). DM continuity is the member move plus the member-pair lookup,
not a recipient rewrite. So the old id cannot be re-minted:
```sql
CREATE TABLE IF NOT EXISTS agent_retired(
  id TEXT PRIMARY KEY, renamed_to TEXT NOT NULL, at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS agent_retired_new ON agent_retired(renamed_to, id);
-- inside the existing rename IMMEDIATE txn, after agents.id update.
-- Binds are (new, old) then (old, new, nowIso). Swapping the UPDATE binds
-- points the chain at the id being retired. Probe: alice→bob then bob→carol
-- leaves both rows renamed_to=carol. A second INSERT of alice throws
-- UNIQUE constraint failed: agent_retired.id — do not OR IGNORE that INSERT
-- (it would retarget the tombstone and reopen mail inheritance).
UPDATE agent_retired SET renamed_to = ? WHERE renamed_to = ?;  -- (new, old)
INSERT INTO agent_retired(id, renamed_to, at) VALUES(?, ?, ?); -- (old, new, nowIso)
-- EVERY path that inserts an agents row, both modes, BEFORE the write.
-- token.create (local root included), rename's new id, AND joinAgent's local
-- INSERT (bus.ts:461 — the bypass the role-grammar paragraph already names).
-- "token.create and rename (server)" leaves that INSERT open. Hit ⇒ identity_conflict.
SELECT 1 FROM agent_retired WHERE id = ?;
```
Do not add an alias arm to delivery unless inbox continuity of id-addressed mail is
an explicit requirement. If it is, this arm is SEARCH on `agent_retired_new` then
`msg_rec_idx` — still no history mutation:
```sql
UNION ALL SELECT r.msg FROM agent_retired ar
  JOIN message_recipients r ON r.target = ar.id
  WHERE ar.renamed_to = ?
```
The JS map must carry the same old ids or the fixture splits. Frozen names +
member-pair lookup ⇒ renamed agent keeps the SAME DM conversation, no split.

**G7 — Tests (contract suite, both directions).** canonicalization (alice↔bob same
channel, code-unit order); self-DM rejected; `~` collision impossible; non-party
read/inbox/threadOf/receipts/waitStep/setStatus → byte-identical to a nonexistent id AND
no `reads` row written; party visible; `read:all`-only human sees channels but NOT DMs
(incl. history BOTH modes — the row-filter, not just the method gate);
`read:dm` human (default) sees everything incl. history both modes + `channels()`; local
non-party sees all (quirk pin); `--re` into dm by non-party `not_found`; **`thread=<dm
root>` by a non-party ⇒ `not_found`; cross-channel thread attach into a dm ⇒ rejected
(claude N1); `thread=<nonexistent>` rejected in server mode; `thread=<own public root>`
accepted**; post `@all` into
dm rejected; SSE event filtered for non-party subscriber; `/raw` 404-equivalent; rename
keeps conversation via member-pair; acl_generation refusal; non-canonical dm name does
not create a second channel; first post by a party creates via the helper with no
pre-existing `channel_members` row; non-party first post is `not_found` and writes no
channel row; group delete+recreate in the same second does not inherit (tombstone +1s
is ISO-Z, not `datetime()`); local join of a retired id is `identity_conflict`.
