# Project structure

```
agent-comms/
├── bin/
│   ├── comms.ts        # the CLI engine (join/who/post/inbox/read/thread/ack/done/status/watch)
│   └── dashboard.ts    # live web dashboard (SSE, read-only)
├── comms.ts            # shim → bin/comms.ts   (keeps old invocation paths working)
├── dashboard.ts        # shim → bin/dashboard.ts
├── messages/           # Markdown mirror per bus message (runtime; gitignored)
│   └── <channel>/      #   one folder per channel, created on first post
├── handoffs/           # optional human-authored notes (runtime; gitignored)
├── docs/               # design & change notes (this file)
├── .comms/             # live SQLite DB (comms.db + WAL) — runtime state, gitignored
├── package.json        # bun manifest + scripts
├── README.md           # full command reference
└── AGENTS.md           # how an agent joins & uses the bus
```

## Invocation

All three forms work (root shims forward to `bin/`, and the CLI auto-detects the
project root, so the DB path is stable no matter where it's called from):

```bash
bun agent-comms/comms.ts who         # absolute-ish
cd agent-comms && bun comms.ts who   # via shim
cd agent-comms && bun run comms who  # via package.json script
bun agent-comms/dashboard.ts --port 8787
```

## Data model

- **Source of truth:** `.comms/comms.db` (SQLite, WAL). Never edit by hand.
- **`messages/<channel>/`** is a human/git-friendly *mirror* of each message,
  foldered by the message's channel; the DB is authoritative. The mirror is not a
  machine API — read the DB, not the files. Deleting a mirror file does not remove
  the message. New channels get their own folder automatically on first post.
- **Root detection:** the CLI/dashboard walk up from their location until they
  find `package.json` or `.comms/`, so moving code between `bin/` and the root
  never changes which database they use. Override with `COMMS_HOME`.

## 2026-08-12 reorg

Flat directory → this layout. Non-destructive: DB path unchanged, all history
preserved, old commands kept working via root shims. Bus mirrors moved to
`messages/`; hand-authored `.md` moved to `handoffs/` (today) and
`handoffs/archive/` (prior). Added `package.json`, `.gitignore`, `bin/`, `docs/`.

## 2026-08-13 per-project foldering

`messages/` and `handoffs/` split into per-project subfolders matching the channel
labels (`general`, `gemma4-26b`). Non-destructive: 265 message mirrors relocated by
their DB channel, the display-only `file` column normalized to
`messages/<channel>/<name>`, DB/CLI/dashboard behavior identical (they key by id,
not path). New posts auto-write into `messages/<channel>/`; new channels get a
folder on first use.
