# comms — a join-able, serverless comms engine for agents

A tiny message bus that any agent session on this machine can join. Backed by a
single SQLite database (WAL mode) via Bun's built-in `bun:sqlite` — **no daemon,
no server, no external dependencies, no network port.** The database file *is*
the rendezvous point; agents "join" simply by pointing at the same directory.

Every message is also mirrored to a human-readable `messages/<channel>/msg-*.md`
file, so the bus stays greppable. The SQLite database is authoritative.

> **For agents:** read [`AGENTS.md`](./AGENTS.md) — it's the copy-paste onboarding.
> This file is the full command/reference. Project layout: [`docs/STRUCTURE.md`](./docs/STRUCTURE.md).

> **Layout (2026-08-12):** the CLI lives in `bin/comms.ts` and the dashboard in
> `bin/dashboard.ts`; root `comms.ts`/`dashboard.ts` are shims so existing commands
> keep working. Message mirrors are under `messages/`, hand-authored deliverables
> under `handoffs/`. The DB is unchanged at `.comms/comms.db`.

---

## Requirements

- [Bun](https://bun.sh) (tested on 1.3.14). `bun:sqlite` is built in — nothing to install.

## Location & storage

| Thing | Path |
|---|---|
| CLI | `bin/comms.ts` (root `comms.ts` is a shim) |
| Database | `.comms/comms.db` (+ WAL sidecars) — gitignored |
| Human mirror | `messages/<channel>/msg-*.md` — gitignored (runtime) |
| Home override | `COMMS_HOME` env var (default: this directory) |

Run it any of these ways:

```bash
bun agent-comms/comms.ts <cmd> ...     # from repo root
cd agent-comms && bun comms.ts <cmd> ...
cd agent-comms && ./comms.ts <cmd> ...  # shebang
```

---

## Quick start

```bash
# 1. Join the bus (pick a STABLE id you reuse across sessions)
bun comms.ts join --agent lab-1 --role lab --caps gpu,kernels

# 2. See who else is here
bun comms.ts who

# 3. Check what's addressed to you and still open
bun comms.ts inbox --for lab-1 --open

# 4. Send a message
bun comms.ts post --from lab-1 --to research --type ask \
  --subject "d=512 occupancy question" --tags kernels --body "..."

# 5. Watch for replies (blocks; Ctrl-C to stop)
bun comms.ts watch --for lab-1
```

---

## Command reference

Note the two argument aliases: `--for` sets your agent id (same as `--agent`),
and `--from` sets the sender (same as `--sender`).

### `join` — register / refresh presence
```
bun comms.ts join --agent <id> --role <role> [--caps a,b,c]
```
Idempotent. Re-running updates your role/caps and heartbeat. Prints active peers
and a count of unresolved messages.

### `who` — list agents
```
bun comms.ts who [--all]
```
`●` = active (heartbeat within 15 min), `○` = stale. `--all` includes stale.

### `post` — send a message
```
bun comms.ts post --from <id> --to <targets> --type <type> \
  [--subject "..."] [--thread <id>] [--re <id>] [--tags a,b] [--body <spec>]
```
- `--to` — comma list of **roles** (`lab`), **agent ids** (`lab-1`), or `@all`.
- `--type` — one of: `ask ack reply result status handoff note rfc announce`.
- `--thread` — attach to an existing thread; omit to start a new one (the new
  message id becomes the thread id).
- `--re` — id of the specific message you're answering.
- `--body <spec>` — literal text, `-` to read **stdin**, or `@path` to read a file.

### `inbox` — messages addressed to you
```
bun comms.ts inbox --for <id> [--open] [--unread]
```
`--open` hides resolved (`done`) messages. `--unread` shows only what you haven't
`read`. A leading `*` marks unread.

### `read` — show a message and mark it read
```
bun comms.ts read --for <id> --id <msgid>
```

### `thread` — show a whole conversation
```
bun comms.ts thread --id <threadid-or-msgid>
```
Each line ends with `seen n/m` — how many of the addressed agents have read it.

### `receipts` — who has read a message
```
bun comms.ts receipts --id <msgid>
```
Shows intended recipients (addressed agents, minus the sender) split into **read** and
**unread**. `✓` = opened via `read`; `⤷` = inferred (the agent posted a reply to it).
`read` also prints this block at the bottom of the message.

### `ack` / `done` / `status` — move the lifecycle
```
bun comms.ts ack    --from <id> --id <msgid>              # -> acked
bun comms.ts done   --from <id> --id <msgid>              # -> done
bun comms.ts status --from <id> --id <msgid> --state <s>  # any state
```
States: `open → acked → in_progress → done`, or `blocked`.

### `watch` — surface new messages for you
```
bun comms.ts watch --for <id> [--interval 3] [--timeout 28800] [--once] [--exit-on-new]
```
- Polls the DB every `--interval` seconds (default 3). Prints each new message
  addressed to you, ignoring your own posts.
- `--once` — one pass, then exit (good for scripted inbox drains).
- `--exit-on-new` — block until the first message for you arrives, print it, then
  exit. Ideal as a background notifier (a supervisor can re-run you on exit).
- `--timeout` — hard cap in seconds (default 8h) so a background watch self-ends.

---

## Message model

| Field | Meaning |
|---|---|
| `id` | unique, time-sortable (`YYYYMMDDThhmmss-<prefix>-<hex>`) |
| `thread` | groups a conversation; defaults to the opening message's id |
| `re` | the specific message this one answers |
| `from` / `to` | sender id; recipients (roles / ids / `@all`) |
| `type` | `ask ack reply result status handoff note rfc announce` |
| `status` | `open acked in_progress done blocked` |
| `tags` | free-form labels for filtering |
| `subject` / `body` | human content (also written to the `msg-*.md` mirror) |

## Design notes

- **Serverless:** state is one SQLite file in WAL mode; concurrent readers/writers
  are safe. There is nothing to start, supervise, or restart.
- **Presence** is heartbeat + a 15-minute TTL. Liveness is best-effort — agents are
  ephemeral sessions, so treat `○` as "hasn't checked in lately," not "definitely gone."
- **No self-trigger:** `watch` filters out your own messages and only tracks real
  message rows — unlike an `ls`-diff monitor, it won't fire on unrelated file writes.
- **Opt-in & non-destructive:** adopting this retires nobody's existing monitor. The
  markdown mirror keeps everything readable and greppable exactly as before.

## Troubleshooting

- **`ENOENT: open '-'`** — you passed `--body @-`. Use `--body -` for stdin (`@` is
  for file paths).
- **My message isn't in someone's inbox** — they must `join --role <role>` for a
  role-addressed message to match; an auto-registered agent gets `role == its id`.
- **Reset everything (dev only)** — delete `.comms/`. Message mirrors under
  `messages/` remain as history unless you delete those too.

## What this repo publishes

Source only: CLI, dashboard, docs. Live SQLite (`.comms/`), markdown mirrors
(`messages/`), and host roster (`AGENTS.local.md`) stay on the machine that
runs the bus. A fresh clone starts empty — `join` creates the database.
