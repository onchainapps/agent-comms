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
 * - comms_wait is the watch tool. The server's inbox.wait is ONE non-blocking
 *   scan step (≤500 events; the wait LOOP is transport-side per the core's
 *   waitStep contract — the CLI's watch loop is the other transport), so the
 *   long-poll lives HERE: step with since=<last scanned cursor>, drain without
 *   sleeping while the cursor advances, else poll every WAIT_POLL_MS until a
 *   message or the deadline. Never commits except the §6 resync recovery
 *   commit; cursor.set commits AFTER the client has processed the batch
 *   (at-least-once). Every other tool maps 1:1 onto an RPC method — same
 *   params, and the typed bus errors come back as isError content with the
 *   variant name, so exit-code semantics (§7) translate to a model-readable form.
 * - transport policy (§7, same as the CLI shell): rate_limited/contention
 *   retried per Retry-After, ambiguous transport failures retried for
 *   replay-safe methods only, post gets an auto idempotency key per logical
 *   call — all inside a bounded window.
 * - identity is the token row's (§5); this process never claims an agent id.
 * - stderr only for logs (stdout is the protocol channel).
 *
 * usage: COMMS_URL=http://host:8700 COMMS_TOKEN=*** bun bin/mcp.ts [--timeout-ms N]
 */
import { RpcBus } from "../src/rpc-bus.ts";
import type { Res } from "../src/bus.ts";

const URL_ = process.env.COMMS_URL ?? "";
const TOKEN = process.env["COMMS" + "_TOKEN"] ?? "";
const argTimeout = process.argv.includes("--timeout-ms") ? Number(process.argv[process.argv.indexOf("--timeout-ms") + 1]) : undefined;
// claude M5 B1/(e): no RPC long-polls (comms_wait loops over short steps), so
// per-RPC deadline matches the CLI's 10 s rather than outliving a 60 s poll.
const TIMEOUT = Number.isFinite(argTimeout) && argTimeout! > 0 ? argTimeout! : 10_000;
const RETRY_WINDOW_MS = 30_000;
const WAIT_POLL_MS = Number(process.env.COMMS_MCP_POLL_MS ?? 1000) || 1000;
const CURSOR_PAT = "^[0-9a-f]{8,64}\\.\\d+$";
const CONSUMER_PAT = "^[a-z0-9._#@~-]{1,128}$";

if (!URL_ || !TOKEN) {
  console.error("error: COMMS_URL and COMMS_TOKEN are required (agent-comms MCP adapter)");
  process.exit(2);
}

const rpc = new RpcBus(URL_, TOKEN, TIMEOUT);

// ---------- tool table: name → { desc, schema, call } ----------
type Out = { text: string; isError?: boolean };
type Tool = { desc: string; schema: object; call: (a: Record<string, any>, signal: AbortSignal) => Promise<Out> };

// compact: tool text lands in a model's context window (indent=2 cost ~17%).
const j = (v: unknown) => JSON.stringify(v);
const errText = (r: { error: string; detail?: string; data?: unknown }, hint = ""): Out =>
  ({ text: `error(${r.error}): ${r.detail}${r.data ? " " + JSON.stringify(r.data) : ""}${hint}`, isError: true });

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((res) => {
    if (signal?.aborted) return res();
    const t = setTimeout(res, Math.max(0, ms));
    signal?.addEventListener("abort", () => { clearTimeout(t); res(); }, { once: true });
  });
}

/** §7 transport policy — the SAME rules as bin/comms.ts remoteCall (the
 *  retry policy lives in the shell; this adapter is a shell). `refused` never
 *  reached the server ⇒ fail fast. group.* / token.* are not replay-safe. */
const REPLAY_SAFE = new Set(["join", "who", "post", "inbox", "read", "thread", "receipts", "status", "channels", "history", "inbox.wait", "cursor.get", "cursor.set", "group.list", "group.show", "dm.members"]);
async function call(method: string, params: Record<string, unknown>, signal?: AbortSignal): Promise<Res<any>> {
  const deadline = Date.now() + RETRY_WINDOW_MS;
  let backoff = 250;
  for (;;) {
    let retryAfterMs = 1000;
    // cred is explicit: RpcBus.call attaches the bearer ONLY per call.
    const r = await rpc.call(method, params, TOKEN, (h) => {
      const ra = Number(h.get("retry-after"));
      if (Number.isFinite(ra) && ra > 0) retryAfterMs = ra * 1000;
    });
    if (!r.error || Date.now() >= deadline || signal?.aborted) return r;
    if (r.error === "rate_limited" || r.error === "contention") { await sleep(Math.min(Math.max(retryAfterMs, 250), 5000), signal); continue; }
    if (r.error === "unavailable" && REPLAY_SAFE.has(method) && (r.data as any)?.transport !== "refused") {
      await sleep(backoff, signal); backoff = Math.min(backoff * 2, 5000); continue;
    }
    return r;
  }
}

/** one wrapper: RPC call → text content; typed errors stay typed in text. */
async function exec(method: string, params: Record<string, unknown>, signal?: AbortSignal): Promise<Out> {
  const r = await call(method, params, signal);
  if (r.error) return errText(r);
  return { text: j(r.value) };
}

/** long-poll over waitStep (see header). Returns as soon as a step yields
 *  messages; the cursor returned is the SCANNED-TO position, so committing
 *  it after an empty result is correct and cheap (skips the scanned noise). */
async function waitLoop(a: Record<string, any>, signal: AbortSignal): Promise<Out> {
  const consumer: string = a.consumer ?? "mcp";
  const deadline = Date.now() + (a.timeout ?? 30) * 1000;
  let since: string | undefined = a.since;
  const seqOf = (c?: string) => (c ? Number(c.slice(c.lastIndexOf(".") + 1)) : null);
  for (;;) {
    const r = await call("inbox.wait", clean({ consumer, since, noAll: a.noAll }), signal);
    if (r.error === "resync" && a.since === undefined) {
      // §6 recovery commit (the CLI watch does exactly this): everything
      // below the floor is gone; resume from the retained floor. Only on the
      // stored-cursor path — an explicit bad `since` is the caller's to fix.
      const d = (r.data ?? {}) as { epoch?: string; floor?: number };
      const cur = `${d.epoch}.${Number(d.floor ?? 0)}`;
      const s = await call("cursor.set", { consumer, cursor: cur, force: true }, signal);
      if (s.error) return errText(s);
      return { text: j({ messages: [], cursor: cur, done: false, resynced: true,
        note: "epoch rotated or cursor fell below retention: durable cursor re-baselined to the retained floor (§6). Events before it are gone — check comms_inbox {unread:true} for anything still open, then call comms_wait again." }) };
    }
    if (r.error) return errText(r);
    const v = r.value as { messages: unknown[]; cursor: string; done: boolean };
    if (v.messages.length || signal.aborted || Date.now() >= deadline) return { text: j(v) };
    // a FULL scan page (≥500 seqs) means backlog remains ⇒ step again now.
    // Unknown start (stored cursor) ⇒ one immediate re-step to learn it.
    // Anything less is live trickle ⇒ sleep (else steady noise traffic spins
    // this loop at RPC speed and drains the 120-token read bucket).
    const prev = seqOf(since);
    const cur = seqOf(v.cursor)!;
    since = v.cursor;
    if (prev === null || cur - prev >= 500) continue;
    await sleep(Math.min(WAIT_POLL_MS, deadline - Date.now()), signal);
  }
}

const TOOLS: Record<string, Tool> = {
  comms_join: {
    desc: "Join / refresh presence for this agent (id comes from the token row). role is required by the bus; caps is REPLACED on every join (omit ⇒ cleared).",
    schema: { type: "object", required: ["role"], properties: { role: { type: "string" }, caps: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("join", clean({ role: a.role, caps: a.caps }), s),
  },
  comms_post: {
    desc: "Post a message. to = comma-separated ids / role:<r> / group:<g> / @all. dm=<peer> sends a DM (to must equal peer). re/thread for replies (thread = parent's thread). channel defaults to general. Retries are safe: an idempotency key is generated per call unless you pass one (pass your own to make a RE-ISSUED call idempotent too).",
    schema: {
      type: "object",
      required: ["to", "type", "body"],
      properties: {
        to: { type: "string" }, type: { type: "string", enum: ["ack", "announce", "ask", "handoff", "note", "reply", "result", "rfc", "status"] },
        body: { type: "string" }, subject: { type: "string" }, channel: { type: "string" },
        thread: { type: "string" }, re: { type: "string" }, tags: { type: "string" }, dm: { type: "string" },
        as: { type: "string", description: "post as another sender (§5, requires post:as; sender = as, meta.as records the principal)" },
        idempotencyKey: { type: "string", maxLength: 128, description: "optional; the same key + params returns the original result (auto-generated per logical post when omitted — §7)" },
      },
      additionalProperties: false,
    },
    // §6/§7: ONE key per logical post, reused across this call's own
    // transport retries (a reset/timeout after commit must not double-post).
    call: (a, s) => exec("post", clean({ ...a, idempotencyKey: a.idempotencyKey ?? `mcp:${crypto.randomUUID()}` }), s),
  },
  comms_inbox: {
    desc: "List the caller's inbox, NEWEST `limit` rows (default 50) plus total/truncated — filter with unread/open/channel rather than raising limit. mark=true (default false — peek) marks EVERY matching row read server-side, so it disables truncation: combine it with unread/channel filters. noAll=true (E1) drops the @all broadcast arm from delivery — only id/role/group-addressed rows (auditor view; does NOT change ack permission).",
    schema: {
      type: "object",
      properties: { open: { type: "boolean" }, unread: { type: "boolean" }, channel: { type: "string" }, mark: { type: "boolean" }, noAll: { type: "boolean" }, limit: { type: "integer", minimum: 1, maximum: 500 } },
      additionalProperties: false,
    },
    call: async (a, s) => {
      const { limit = 50, ...p } = a;
      const r = await call("inbox", clean(p), s);
      if (r.error) return errText(r);
      // rendering only (the server has no inbox limit): a 300-row inbox was
      // ~270 KB of tool text — larger than most context windows can spare.
      // Never truncate a marking call: rows marked read but never shown to
      // the model would silently vanish from its --unread view.
      const v = r.value as { rows: { id: string }[]; unreadIds: string[] };
      const rows = p.mark ? v.rows : v.rows.slice(-limit);
      const shown = new Set(rows.map((x) => x.id));
      return { text: j({ rows, unreadIds: v.unreadIds.filter((id) => shown.has(id)), total: v.rows.length, truncated: v.rows.length - rows.length }) };
    },
  },
  comms_read: {
    desc: "Read one message BY ID (marks it read for the caller) with receipts. For a non-marking peek use comms_receipts.",
    schema: { type: "object", required: ["id"], properties: { id: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("read", { id: a.id }, s),
  },
  comms_thread: {
    desc: "All messages in a thread (thread id or any member id) with receipts.",
    schema: { type: "object", required: ["id"], properties: { id: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("thread", { id: a.id }, s),
  },
  comms_receipts: {
    desc: "Message row + receipts (intended/readers/unread) WITHOUT marking anything read.",
    schema: { type: "object", required: ["id"], properties: { id: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("receipts", { id: a.id }, s),
  },
  comms_status: {
    desc: "Set message status (acked/done/in_progress/blocked/open) — recipient or agents:admin only.",
    schema: { type: "object", required: ["id", "state"], properties: { id: { type: "string" }, state: { type: "string", enum: ["acked", "done", "in_progress", "blocked", "open"] } }, additionalProperties: false },
    call: (a, s) => exec("status", { id: a.id, state: a.state }, s),
  },
  comms_channels: {
    desc: "List channels visible to this token (dm~ channels only for members / read:dm).",
    schema: { type: "object", properties: {}, additionalProperties: false },
    call: (_a, s) => exec("channels", {}, s),
  },
  comms_who: {
    desc: "List agents. all=false (default) ⇒ active only (presence TTL).",
    schema: { type: "object", properties: { all: { type: "boolean" } }, additionalProperties: false },
    call: (a, s) => exec("who", { all: !!a.all }, s),
  },
  comms_history: {
    desc: "History as {rows, hasMore, cursor}. WITHOUT since: the newest `limit` rows (default 200, ≤1000) and cursor = the live handoff point — pass it as comms_wait since= for zero-gap delivery. WITH since: rows after that cursor, oldest first; page by re-passing cursor until hasMore=false. channel/since views are open to every token; the unfiltered snapshot needs read:all.",
    schema: { type: "object", properties: { channel: { type: "string" }, since: { type: "string", pattern: CURSOR_PAT }, limit: { type: "integer", minimum: 1, maximum: 1000 } }, additionalProperties: false },
    call: (a, s) => exec("history", clean(a), s),
  },
  comms_wait: {
    desc: "Block up to `timeout` s (default 30, ≤50 so the reply beats a 60 s client request timeout) until messages addressed to this agent arrive: {messages, cursor}. At-least-once: NOTHING is committed — after processing, call comms_cursor_set(cursor); an empty result's cursor is safe to commit too (it skips scanned noise). since defaults from the stored cursor for (this agent, consumer). If the stored cursor is stale (epoch rotated / below retention) the adapter performs the §6 recovery commit itself and returns resynced:true. noAll=true (E1) drops the @all broadcast arm — use a DISTINCT consumer (e.g. 'mcp.noall') so the two predicates never share a cursor row.",
    schema: { type: "object", properties: { consumer: { type: "string", pattern: CONSUMER_PAT, description: "durable-cursor lane, default 'mcp'" }, since: { type: "string", pattern: CURSOR_PAT }, timeout: { type: "number", minimum: 0, maximum: 50, description: "seconds, default 30; 0 = one non-blocking step" }, noAll: { type: "boolean" } }, additionalProperties: false },
    // grok M5 #1 + claude B1: server inbox.wait is ONE non-blocking waitStep;
    // the long-poll is THIS adapter's job (claude's waitLoop supersedes the
    // earlier 500 ms loop: full 500-event pages re-step without sleeping, live
    // trickle parks WAIT_POLL_MS, abort honoured, §6 recovery commit inline).
    call: (a, s) => waitLoop(a, s),
  },
  comms_cursor_get: {
    desc: "Read the stored durable cursor for (this agent, consumer). resync error on foreign epoch.",
    schema: { type: "object", properties: { consumer: { type: "string", pattern: CONSUMER_PAT } }, additionalProperties: false },
    call: (a, s) => exec("cursor.get", clean({ consumer: a.consumer ?? "mcp" }), s),
  },
  comms_cursor_set: {
    desc: "Commit the durable cursor '<epoch>.<seq>' for (this agent, consumer) AFTER processing a batch (at-least-once). Non-monotonic ⇒ conflict unless force.",
    schema: { type: "object", required: ["cursor"], properties: { consumer: { type: "string", pattern: CONSUMER_PAT }, cursor: { type: "string", pattern: CURSOR_PAT }, force: { type: "boolean" } }, additionalProperties: false },
    call: (a, s) => exec("cursor.set", clean({ consumer: a.consumer ?? "mcp", cursor: a.cursor, force: a.force }), s),
  },
  comms_group_list: {
    desc: "List work-groups (name, created_by, created_at, members count, mine).",
    schema: { type: "object", properties: {}, additionalProperties: false },
    call: (_a, s) => exec("group.list", {}, s),
  },
  comms_group_show: {
    desc: "Show one group's members.",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("group.show", { name: a.name }, s),
  },
  comms_group_join: {
    desc: "Join a work-group (creates it if absent — self-organizing; groups are delivery, not ACL). Optional agent joins on behalf of another (agents:admin).",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" }, agent: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("group.join", clean({ name: a.name, agent: a.agent }), s),
  },
  comms_group_leave: {
    desc: "Leave a work-group. Optional agent leaves on behalf of another (agents:admin).",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" }, agent: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("group.leave", clean({ name: a.name, agent: a.agent }), s),
  },
  comms_dm_members: {
    desc: "Members of a dm~ channel (members array; non-party ⇒ not_found like a missing channel).",
    schema: { type: "object", required: ["channel"], properties: { channel: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("dm.members", { channel: a.channel }, s),
  },
  // grok M5 #2: tools/list ↔ RPC method parity (§10-M5 "same RPC methods").
  // These dispatch live in src/server/mod.ts but were never exposed here.
  comms_rename: {
    desc: "Rename an agent id (default: the caller's own; agents:admin may rename others). Transactional across reads/tokens/cursors/idempotency/groups/channels; old id is retired forever.",
    schema: { type: "object", required: ["to"], properties: { to: { type: "string" }, agent: { type: "string", description: "target agent, default self (renaming others needs agents:admin)" } }, additionalProperties: false },
    call: (a, s) => exec("rename", clean({ to: a.to, agent: a.agent }), s),
  },
  comms_token_create: {
    desc: "Mint an API token for an agent (requires tokens:admin; the secret shows ONCE — note: through MCP the plaintext secret lands in the model transcript, so rotate it if the transcript is shared). kind:'human' defaults to read:all,read:dm. admin:true grants all scopes — refused without force if an admin token already exists (bootstrap guard).",
    schema: { type: "object", required: ["agent"], properties: { agent: { type: "string" }, kind: { type: "string", enum: ["agent", "human"] }, label: { type: "string" }, scopes: { type: "array", items: { type: "string", enum: ["read:all", "read:dm", "post:as", "tokens:admin", "agents:admin"] } }, admin: { type: "boolean" }, force: { type: "boolean" } }, additionalProperties: false },
    call: (a, s) => exec("token.create", clean(a), s),
  },
  comms_token_list: {
    desc: "List token rows (id, prefix, agent, scopes, label, revoked) — never the secrets. Requires tokens:admin.",
    schema: { type: "object", properties: {}, additionalProperties: false },
    call: (_a, s) => exec("token.list", {}, s),
  },
  comms_token_revoke: {
    desc: "Revoke a token by numeric id (from comms_token_list). Requires tokens:admin.",
    schema: { type: "object", required: ["id"], properties: { id: { type: "integer", minimum: 1 } }, additionalProperties: false },
    call: (a, s) => exec("token.revoke", { id: a.id }, s),
  },
  comms_group_create: {
    desc: "Create a work-group (idempotent; join also creates). Optional agent attributes creation to someone else (agents:admin).",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" }, agent: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("group.create", clean(a), s),
  },
  comms_group_delete: {
    desc: "Delete a work-group (requires agents:admin). Same-second re-create returns contention; tombstone ≥1 s.",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("group.delete", { name: a.name }, s),
  },
  comms_channel_create: {
    desc: "Create a channel explicitly (E2, the blessed path). Exact name is idempotent (created:false). A NEW name colliding with an existing channel modulo case/[-_] is refused with the existing name in the detail — post to THAT name instead. post --channel <new> still auto-creates, but through the same guard.",
    schema: { type: "object", required: ["name"], properties: { name: { type: "string" }, purpose: { type: "string" } }, additionalProperties: false },
    call: (a, s) => exec("channel.create", clean({ name: a.name, purpose: a.purpose }), s),
  },
};

function clean(o: Record<string, unknown>): Record<string, unknown> {
  const r: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(o)) if (v !== undefined && v !== null) r[k] = v;
  return r;
}

// ---------- stdio MCP loop (newline-delimited JSON-RPC 2.0) ----------
type Id = number | string | null;
type Req = { jsonrpc: "2.0"; id?: Id; method: string; params?: any };
// Negotiation (spec lifecycle): echo the client's version when we speak it,
// else offer our oldest. The surface (tools + text content + isError) is
// valid unchanged in all three revisions.
const PROTOCOLS = ["2024-11-05", "2025-03-26", "2025-06-18"];

// keep-alive while async tool calls are in flight: with no timers registered,
// Bun exits when stdin closes even if a fetch is still running (pipe clients
// that send-then-close would lose the last responses).
let ka: ReturnType<typeof setInterval> | null = null;
/** in-flight tools/call by request id — notifications/cancelled aborts one;
 *  stdin EOF aborts all (a comms_wait must not hold a dead client for 60 s). */
const inflightCalls = new Map<string, AbortController>();
const cancelled = new Set<string>();
const keyOf = (id: Id) => JSON.stringify(id);

function send(msg: object) { process.stdout.write(JSON.stringify(msg) + "\n"); }
function reply(id: Id, result: object) { send({ jsonrpc: "2.0", id, result }); }
function replyErr(id: Id, code: number, message: string) { send({ jsonrpc: "2.0", id, error: { code, message } }); }

let initialized = false;
async function handle(req: Req) {
  const id = req.id ?? null;
  const isNotification = req.id === undefined;
  switch (req.method) {
    case "initialize": {
      initialized = true;
      const want = req.params?.protocolVersion;
      return reply(id, {
        protocolVersion: PROTOCOLS.includes(want) ? want : PROTOCOLS[0],
        capabilities: { tools: {} },
        serverInfo: { name: "agent-comms", version: "0.2.0" },
        instructions: "Agent comms bus. Identity = the token row. Await work with comms_wait, then commit its cursor with comms_cursor_set after processing.",
      });
    }
    case "notifications/initialized":
    case "initialized":
      return; // notification — no response
    case "notifications/cancelled": {
      const k = keyOf(req.params?.requestId ?? null);
      const ac = inflightCalls.get(k);
      if (ac) { cancelled.add(k); ac.abort(); }
      return;
    }
  }
  if (isNotification) return; // unknown notification: ignore per spec
  if (req.method === "ping") return reply(id, {}); // allowed before initialize
  if (!initialized) return replyErr(id, -32002, "server not initialized");
  switch (req.method) {
    case "tools/list":
      return reply(id, {
        tools: Object.entries(TOOLS).map(([name, t]) => ({ name, description: t.desc, inputSchema: t.schema })),
      });
    case "tools/call": {
      const name = String(req.params?.name ?? "");
      const tool = Object.hasOwn(TOOLS, name) ? TOOLS[name] : undefined;
      if (!tool) return replyErr(id, -32602, `unknown tool: ${name}`);
      const args = req.params?.arguments ?? {};
      // edge validation (inputSchema is advisory to many clients). Rendered
      // as a tool execution error — the §7 `usage` variant, like every other
      // typed failure — so the MODEL sees it and can self-correct (MCP
      // 2025-11-25 moved input-validation errors to isError for this reason);
      // unknown tool stays a protocol error.
      const bad = validate(name, tool, args);
      if (bad) return reply(id, { content: [{ type: "text", text: `error(usage): ${bad}` }], isError: true });
      const k = keyOf(id);
      const ac = new AbortController();
      if (stdinEnded) ac.abort();
      inflightCalls.set(k, ac);
      try {
        const r = await tool.call(args, ac.signal);
        if (cancelled.has(k)) return; // spec: no response to a cancelled request
        return reply(id, { content: [{ type: "text", text: r.text }], ...(r.isError ? { isError: true } : {}) });
      } catch (e: any) {
        if (cancelled.has(k)) return;
        return reply(id, { content: [{ type: "text", text: `error(internal): ${String(e?.message ?? e)}` }], isError: true });
      } finally { inflightCalls.delete(k); cancelled.delete(k); }
    }
    default:
      return replyErr(id, -32601, `method not found: ${req.method}`);
  }
}

function validate(name: string, tool: Tool, args: any): string | null {
  if (typeof args !== "object" || args === null || Array.isArray(args)) return `${name}: arguments must be an object`;
  const s = tool.schema as any;
  const props = (s.properties ?? {}) as Record<string, any>;
  for (const r of s.required ?? [])
    if (args[r] === undefined) return `${name}: missing required '${r}'`;
  for (const k of Object.keys(args)) // own keys only: `k in props` let toString/constructor/__proto__ through
    if (!Object.hasOwn(props, k)) return `${name}: unknown property '${k}'`;
  for (const [k, spec] of Object.entries(props)) {
    const v = args[k];
    if (v === undefined) continue;
    if (spec.type === "string" && typeof v !== "string") return `${name}: '${k}' must be a string`;
    if (spec.type === "boolean" && typeof v !== "boolean") return `${name}: '${k}' must be a boolean`;
    if (spec.type === "number" && typeof v !== "number") return `${name}: '${k}' must be a number`;
    if (spec.type === "integer" && !Number.isInteger(v)) return `${name}: '${k}' must be an integer`;
    if (spec.enum && !spec.enum.includes(v)) return `${name}: '${k}' must be one of ${spec.enum.join("|")}`;
    if (spec.pattern && typeof v === "string" && !new RegExp(spec.pattern).test(v)) return `${name}: '${k}' must match ${spec.pattern}`;
    if (spec.maxLength !== undefined && typeof v === "string" && v.length > spec.maxLength) return `${name}: '${k}' length ≤ ${spec.maxLength}`;
    if (spec.maximum !== undefined && typeof v === "number" && v > spec.maximum) return `${name}: '${k}' ≤ ${spec.maximum}`;
    if (spec.minimum !== undefined && typeof v === "number" && v < spec.minimum) return `${name}: '${k}' ≥ ${spec.minimum}`;
  }
  return null;
}

/** envelope check BEFORE dispatch: a valid-JSON non-request line (`null`,
 *  `3`, `{}`) used to throw inside the error handler and crash the process. */
function envelope(msg: unknown): { req: Req } | { err: { id: Id; message: string } } | { ignore: true } {
  if (typeof msg !== "object" || msg === null) return { err: { id: null, message: "invalid request" } };
  const m = msg as Record<string, unknown>;
  const idOk = m.id === undefined || m.id === null || typeof m.id === "string" || typeof m.id === "number";
  const id = idOk && m.id !== undefined ? (m.id as Id) : null;
  if (typeof m.method !== "string") {
    // a response object (we never send requests) — nothing to answer
    if ("result" in m || "error" in m) return { ignore: true };
    return { err: { id, message: "invalid request" } };
  }
  if (m.jsonrpc !== "2.0" || !idOk) return { err: { id, message: "invalid request" } };
  return { req: m as unknown as Req };
}

let buf = "";
process.stdin.setEncoding("utf8");
const pending = new Set<Promise<void>>();
let stdinEnded = false;
function maybeExit() {
  if (!stdinEnded || pending.size) return;
  // NOT process.exit(): it discards stdout bytes still queued for a slow pipe
  // reader (probe: a 233 KB response arrived as 64 KB). Dropping the last ref
  // lets the runtime flush stdout and exit 0 on its own.
  if (ka) { clearInterval(ka); ka = null; }
}
process.stdin.on("data", (chunk: string) => {
  buf += chunk;
  let nl: number;
  while ((nl = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, nl).trim();
    buf = buf.slice(nl + 1);
    if (!line) continue;
    let msg: unknown;
    try { msg = JSON.parse(line); } catch { replyErr(null, -32700, "parse error"); continue; }
    if (Array.isArray(msg)) { replyErr(null, -32600, "batches not supported"); continue; }
    const env = envelope(msg);
    if ("ignore" in env) continue;
    if ("err" in env) { replyErr(env.err.id, -32600, env.err.message); continue; }
    const req = env.req;
    // drain before exit: a pipe client that sends-then-closes must still get
    // the responses for requests already accepted (the fetch may be in flight).
    ka ??= setInterval(() => {}, 500);
    const p: Promise<void> = handle(req)
      .catch((e) => { if (req.id !== undefined) replyErr(req.id ?? null, -32603, String(e?.message ?? e)); })
      .finally(() => { pending.delete(p); maybeExit(); });
    pending.add(p);
  }
});
process.stdin.on("end", () => {
  stdinEnded = true;
  for (const ac of inflightCalls.values()) ac.abort(); // waits return their current state now
  maybeExit();
});
console.error(`agent-comms mcp: ${URL_} (stdio, tools=${Object.keys(TOOLS).length}, rpc-timeout=${TIMEOUT}ms)`);
