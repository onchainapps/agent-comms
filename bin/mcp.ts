#!/usr/bin/env bun
/**
 * bin/mcp.ts — MCP adapter over the hosted server (RFC-001 §10-M5, stretch).
 *
 * stdio MCP (newline-delimited JSON-RPC 2.0) → the SAME /rpc methods the CLI
 * uses, via RpcBus. A Hermes-style client registers this like any other MCP
 * server (command: bun bin/mcp.ts, env COMMS_URL + COMMS_TOKEN) and can then
 * join / post / inbox / read / await entirely over MCP — no bus CLI, no DB.
 *
 * Design (mirrors the RFC rulings):
 * - thin: argv/env + tool schema + text rendering live HERE; all behavior is
 *   the server's (this file contains no SQL and no bus logic).
 * - inbox.wait is the watch tool (long-poll ≤60 s, never auto-advances);
 *   cursor.set commits AFTER the client has processed the batch (at-least-
 *   once, §6). tools/call maps 1:1 onto RPC methods — same params, and the
 *   typed bus errors come back as isError content with the variant name, so
 *   exit-code semantics (§7) translate to a model-readable form.
 * - identity is the token row's (§5); this process never claims an agent id.
 * - stderr only for logs (stdout is the protocol channel).
 *
 * usage: COMMS_URL=http://host:8700 COMMS_TOKEN=*** bun bin/mcp.ts [--timeout-ms N]
 */
import { RpcBus } from "../src/rpc-bus.ts";

const URL_ = process.env.COMMS_URL ?? "";
const TOKEN = process.env["COMMS" + "_TOKEN"] ?? "";
const argTimeout = process.argv.includes("--timeout-ms") ? Number(process.argv[process.argv.indexOf("--timeout-ms") + 1]) : undefined;
// grok M5 fold: comms_wait long-polls CLIENT-side up to 60 s per call, so the
// per-request fetch timeout must exceed the longest single HTTP hold (server
// long-poll ≤60 s) plus the longest adapter hold — 90 s, not 65 s.
const TIMEOUT = Number.isFinite(argTimeout) && argTimeout! > 0 ? argTimeout! : 90_000;

if (!URL_ || !TOKEN) {
  console.error("error: COMMS_URL and COMMS_TOKEN are required (agent-comms MCP adapter)");
  process.exit(2);
}

const rpc = new RpcBus(URL_, TOKEN, TIMEOUT);

// ---------- tool table: name → { desc, schema, call } ----------
type Tool = { desc: string; schema: object; call: (a: Record<string, any>) => Promise<{ text: string; isError?: boolean }> };

const j = (v: unknown) => JSON.stringify(v, null, 2);

/** one wrapper: RPC call → text content; typed errors stay typed in text. */
async function exec(method: string, params: Record<string, unknown>): Promise<{ text: string; isError?: boolean }> {
  // cred is explicit: RpcBus.call attaches the bearer ONLY per call (the
  // constructor token feeds session(); a bare call() was silently anonymous
  // ⇒ every tool returned unauthorized).
  const r = await rpc.call(method, params, TOKEN);
  if (r.error) return { text: `error(${r.error}): ${r.detail}${r.data ? " " + JSON.stringify(r.data) : ""}`, isError: true };
  return { text: j(r.value) };
}

const TOOLS: Record<string, Tool> = {
  comms_join: {
    desc: "Join/register this agent on the bus (id comes from the token; role/caps optional).",
    schema: { type: "object", properties: { role: { type: "string" }, caps: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("join", clean({ role: a.role, caps: a.caps })),
  },
  comms_post: {
    desc: "Post a message. to = comma-separated ids / role:<r> / group:<g> / @all. dm=<peer> sends a DM (to must equal peer). re/thread for replies (thread = parent's thread). channel defaults to general.",
    schema: {
      type: "object",
      required: ["to", "type", "body"],
      properties: {
        to: { type: "string" }, type: { type: "string", enum: ["ack", "announce", "ask", "handoff", "note", "reply", "result", "rfc", "status"] },
        body: { type: "string" }, subject: { type: "string" }, channel: { type: "string" },
        thread: { type: "string" }, re: { type: "string" }, tags: { type: "string" }, dm: { type: "string" },
        as: { type: "string", description: "post as another sender (§5, requires post:as; sender = as, meta.as records the principal)" },
        idempotencyKey: { type: "string", description: "optional; retries with the same key + params return the original result" },
      },
      additionalProperties: false,
    },
    call: (a) => exec("post", clean(a)),
  },
  comms_inbox: {
    desc: "List the caller's inbox (open/unread marks in the rows). mark=true marks them read (default false — peek).",
    schema: {
      type: "object",
      properties: { open: { type: "boolean" }, unread: { type: "boolean" }, channel: { type: "string" }, mark: { type: "boolean" } },
      additionalProperties: false,
    },
    call: (a) => exec("inbox", clean(a)),
  },
  comms_read: {
    desc: "Read one message BY ID (marks it read for the caller) with receipts. For a non-marking peek use comms_receipts.",
    schema: { type: "object", required: ["id"], properties: { id: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("read", { id: a.id }),
  },
  comms_thread: {
    desc: "All messages in a thread (thread id or any member id) with receipts.",
    schema: { type: "object", required: ["id"], properties: { id: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("thread", { id: a.id }),
  },
  comms_receipts: {
    desc: "Message row + receipts (intended/readers/unread) WITHOUT marking anything read.",
    schema: { type: "object", required: ["id"], properties: { id: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("receipts", { id: a.id }),
  },
  comms_status: {
    desc: "Set message status (acked/done/in_progress/blocked/open) — recipient or agents:admin only.",
    schema: { type: "object", required: ["id", "state"], properties: { id: { type: "string" }, state: { type: "string", enum: ["acked", "done", "in_progress", "blocked", "open"] } }, additionalProperties: false },
    call: (a) => exec("status", { id: a.id, state: a.state }),
  },
  comms_channels: {
    desc: "List channels visible to this token (dm~ channels only for members / read:dm).",
    schema: { type: "object", properties: {}, additionalProperties: false },
    call: () => exec("channels", {}),
  },
  comms_who: {
    desc: "List agents. all=false (default) ⇒ active only (presence TTL).",
    schema: { type: "object", properties: { all: { type: "boolean" } }, additionalProperties: false },
    call: (a) => exec("who", { all: !!a.all }),
  },
  comms_history: {
    desc: "History: one newest page (≤1000) as {rows, hasMore, cursor}; cursor is the STREAM HANDOFF point — pass it as comms_wait since= for zero-gap live delivery. channel/since are ungated views; unfiltered snapshot needs read:all.",
    schema: { type: "object", properties: { channel: { type: "string" }, since: { type: "string", pattern: "^[0-9a-f]{8,64}\\.\\d+$" }, limit: { type: "number", minimum: 1, maximum: 1000 } }, additionalProperties: false },
    call: (a) => exec("history", clean(a)),
  },
  comms_wait: {
    desc: "Long-poll ≤60 s for new messages addressed to this agent: {messages, cursor}. At-least-once: NOTHING is committed — after processing, call comms_cursor_set(cursor). since defaults from the stored cursor for (this agent, consumer). resync error ⇒ re-baseline via comms_history then commit epoch.floor.",
    schema: { type: "object", properties: { consumer: { type: "string", description: "durable-cursor lane, default 'mcp'" }, since: { type: "string" }, timeout: { type: "number", minimum: 1, maximum: 60, description: "long-poll seconds, default 30" } }, additionalProperties: false },
    // grok M5 #1: server inbox.wait is waitStep — ONE scan, transport-side
    // long-poll is THIS adapter's job (§6: inbox.wait is the MCP watch
    // primitive). Loop steps (client-side since advance, never committed)
    // until messages, deadline, or stdin close; 500 ms park between steps —
    // the server is the single writer, no busy-spinning it.
    call: async (a) => {
      const consumer = a.consumer ?? "mcp";
      const timeout = typeof a.timeout === "number" ? a.timeout : 30; // documented default, not 0
      const deadline = Date.now() + timeout * 1000;
      let since = a.since;
      for (;;) {
        const r = await exec("inbox.wait", clean({ consumer, since }));
        if (r.isError) return r; // resync/usage propagate immediately — never swallowed by the loop
        const v = JSON.parse(r.text);
        if ((v.messages?.length ?? 0) > 0 || Date.now() >= deadline || stdinEnded) return r;
        since = v.cursor; // advance the SCAN position only; commit stays the caller's comms_cursor_set
        await Bun.sleep(Math.max(0, Math.min(500, deadline - Date.now())));
      }
    },
  },
  comms_cursor_get: {
    desc: "Read the stored durable cursor for (this agent, consumer). resync error on foreign epoch.",
    schema: { type: "object", properties: { consumer: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("cursor.get", clean({ consumer: a.consumer ?? "mcp" })),
  },
  comms_cursor_set: {
    desc: "Commit the durable cursor '<epoch>.<seq>' for (this agent, consumer) AFTER processing a batch (at-least-once). Non-monotonic ⇒ conflict unless force.",
    schema: { type: "object", required: ["cursor"], properties: { consumer: { type: "string" }, cursor: { type: "string", pattern: "^[0-9a-f]{8,64}\\.\\d+$" }, force: { type: "boolean" } }, additionalProperties: false },
    call: (a) => exec("cursor.set", clean({ consumer: a.consumer ?? "mcp", cursor: a.cursor, force: a.force })),
  },
  comms_group_list: {
    desc: "List work-groups (name, created_by, created_at, members count, mine).",
    schema: { type: "object", properties: {}, additionalProperties: false },
    call: () => exec("group.list", {}),
  },
  comms_group_show: {
    desc: "Show one group's members.",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("group.show", { name: a.name }),
  },
  comms_group_join: {
    desc: "Join a work-group (creates it if absent — self-organizing; groups are delivery, not ACL). Optional agent joins on behalf of another (agents:admin).",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" }, agent: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("group.join", clean({ name: a.name, agent: a.agent })),
  },
  comms_group_leave: {
    desc: "Leave a work-group. Optional agent leaves on behalf of another (agents:admin).",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" }, agent: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("group.leave", clean({ name: a.name, agent: a.agent })),
  },
  comms_dm_members: {
    desc: "Members of a dm~ channel (members array; non-party ⇒ not_found like a missing channel).",
    schema: { type: "object", required: ["channel"], properties: { channel: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("dm.members", { channel: a.channel }),
  },
  // grok M5 #2: tools/list ↔ RPC method parity (§10-M5 "same RPC methods").
  // These dispatch live in src/server/mod.ts but were never exposed here.
  comms_rename: {
    desc: "Rename an agent id (default: the caller's own; agents:admin may rename others). Transactional across reads/tokens/cursors/idempotency/groups/channels; old id is retired forever.",
    schema: { type: "object", required: ["to"], properties: { to: { type: "string" }, agent: { type: "string", description: "target agent, default self (renaming others needs agents:admin)" } }, additionalProperties: false },
    call: (a) => exec("rename", clean({ to: a.to, agent: a.agent })),
  },
  comms_token_create: {
    desc: "Mint an API token for an agent (requires tokens:admin; the secret shows ONCE). kind:'human' defaults to read:all,read:dm. admin:true grants all scopes — refused without force if an admin token already exists (bootstrap guard).",
    schema: { type: "object", required: ["agent"], properties: { agent: { type: "string" }, kind: { type: "string", enum: ["agent", "human"] }, label: { type: "string" }, scopes: { type: "array", items: { type: "string", enum: ["read:all", "read:dm", "post:as", "tokens:admin", "agents:admin"] } }, admin: { type: "boolean" }, force: { type: "boolean" } }, additionalProperties: false },
    call: (a) => exec("token.create", clean(a)),
  },
  comms_token_list: {
    desc: "List token rows (id, prefix, agent, scopes, label, revoked) — never the secrets. Requires tokens:admin.",
    schema: { type: "object", properties: {}, additionalProperties: false },
    call: () => exec("token.list", {}),
  },
  comms_token_revoke: {
    desc: "Revoke a token by numeric id (from comms_token_list). Requires tokens:admin.",
    schema: { type: "object", required: ["id"], properties: { id: { type: "number" } }, additionalProperties: false },
    call: (a) => exec("token.revoke", { id: a.id }),
  },
  comms_group_create: {
    desc: "Create a work-group (idempotent; join also creates). Optional agent attributes creation to someone else (agents:admin).",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" }, agent: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("group.create", clean(a)),
  },
  comms_group_delete: {
    desc: "Delete a work-group (requires agents:admin). Same-second re-create returns contention; tombstone ≥1 s.",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" } }, additionalProperties: false },
    call: (a) => exec("group.delete", { name: a.name }),
  },
};

function clean(o: Record<string, unknown>): Record<string, unknown> {
  const r: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(o)) if (v !== undefined && v !== null) r[k] = v;
  return r;
}

// ---------- stdio MCP loop (newline-delimited JSON-RPC 2.0) ----------
type Req = { jsonrpc: "2.0"; id?: number | string | null; method: string; params?: any };
const PROTOCOL = "2024-11-05";

// keep-alive while async tool calls are in flight: with no timers registered,
// Bun exits when stdin closes even if a fetch is still running (pipe clients
// that send-then-close would lose the last responses).
let inflight = 0;
let ka: ReturnType<typeof setInterval> | null = null;
function track<T>(p: Promise<T>): Promise<T> {
  inflight++;
  ka ??= setInterval(() => {}, 500);
  return p.finally(() => { if (--inflight === 0 && ka) { clearInterval(ka); ka = null; } });
}

function send(msg: object) { process.stdout.write(JSON.stringify(msg) + "\n"); }
function reply(id: number | string | null, result: object) { send({ jsonrpc: "2.0", id, result }); }
function replyErr(id: number | string | null, code: number, message: string) { send({ jsonrpc: "2.0", id, error: { code, message } }); }

let initialized = false;
async function handle(req: Req) {
  const id = req.id ?? null;
  switch (req.method) {
    case "initialize":
      initialized = true;
      return reply(id, {
        protocolVersion: PROTOCOL,
        capabilities: { tools: {} },
        serverInfo: { name: "agent-comms", version: "0.1.0" },
        instructions: "Cardano-side agent comms bus. Identity = the token row. Await work with comms_wait, commit with comms_cursor_set.",
      });
    case "notifications/initialized":
    case "initialized":
      return; // notification — no response
    case "ping":
      return reply(id, {});
    case "tools/list":
      return reply(id, {
        tools: Object.entries(TOOLS).map(([name, t]) => ({ name, description: t.desc, inputSchema: t.schema })),
      });
    case "tools/call": {
      const name = String(req.params?.name ?? "");
      const tool = TOOLS[name];
      if (!tool) return replyErr(id, -32602, `unknown tool: ${name}`);
      const args = req.params?.arguments ?? {};
      // server-side schema sanity (inputSchema is advisory to many clients):
      const bad = validate(name, tool, args);
      if (bad) return replyErr(id, -32602, bad);
      try {
        const r = await tool.call(args);
        return reply(id, { content: [{ type: "text", text: r.text }], ...(r.isError ? { isError: true } : {}) });
      } catch (e: any) {
        return reply(id, { content: [{ type: "text", text: `error(internal): ${String(e?.message ?? e)}` }], isError: true });
      }
    }
    default:
      if (req.id === undefined || req.id === null) return; // unknown notification: ignore per spec
      return replyErr(id, -32601, `method not found: ${req.method}`);
  }
}

function validate(name: string, tool: Tool, args: any): string | null {
  if (typeof args !== "object" || args === null || Array.isArray(args)) return `${name}: arguments must be an object`;
  const s = tool.schema as any;
  for (const r of s.required ?? [])
    if (args[r] === undefined) return `${name}: missing required '${r}'`;
  for (const k of Object.keys(args))
    if (!(k in (s.properties ?? {}))) return `${name}: unknown property '${k}'`;
  for (const [k, spec] of Object.entries((s.properties ?? {}) as Record<string, any>)) {
    const v = args[k];
    if (v === undefined) continue;
    if (spec.type === "string" && typeof v !== "string") return `${name}: '${k}' must be a string`;
    if (spec.type === "boolean" && typeof v !== "boolean") return `${name}: '${k}' must be a boolean`;
    if (spec.type === "number" && typeof v !== "number") return `${name}: '${k}' must be a number`;
    if (spec.enum && !spec.enum.includes(v)) return `${name}: '${k}' must be one of ${spec.enum.join("|")}`;
    if (spec.pattern && typeof v === "string" && !new RegExp(spec.pattern).test(v)) return `${name}: '${k}' must match ${spec.pattern}`;
    if (spec.maximum !== undefined && typeof v === "number" && v > spec.maximum) return `${name}: '${k}' ≤ ${spec.maximum}`;
    if (spec.minimum !== undefined && typeof v === "number" && v < spec.minimum) return `${name}: '${k}' ≥ ${spec.minimum}`;
  }
  return null;
}

let buf = "";
process.stdin.setEncoding("utf8");
let pending = new Set<Promise<void>>();
// (declared early: comms_wait's poll loop bails when stdin closes)
let stdinEnded = false;
function maybeExit() { if (stdinEnded && pending.size === 0) process.exit(0); }
process.stdin.on("data", (chunk: string) => {
  buf += chunk;
  let nl: number;
  while ((nl = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, nl).trim();
    buf = buf.slice(nl + 1);
    if (!line) continue;
    let msg: Req;
    try { msg = JSON.parse(line); } catch { replyErr(null, -32700, "parse error"); continue; }
    if (Array.isArray(msg)) { replyErr(null, -32600, "batches not supported"); continue; }
    // drain before exit: a pipe client that sends-then-closes must still get
    // the responses for requests already accepted (the fetch may be in flight).
    const p = track(handle(msg).catch((e) => replyErr(msg.id ?? null, -32603, String(e?.message ?? e))).then(() => { pending.delete(p); maybeExit(); }));
    pending.add(p);
  }
});
process.stdin.on("end", () => { stdinEnded = true; maybeExit(); });
console.error(`agent-comms mcp: ${URL_} (stdio, tools=${Object.keys(TOOLS).length}, timeout=${TIMEOUT}ms)`);
