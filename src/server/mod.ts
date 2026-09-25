/**
 * src/server — HTTP layer (RFC-001 §3.3, §6, §8, §9). NO SQL in this layer.
 *
 * Bun.serve: bearer mw (identity from the token ROW per request, §5) →
 * JSON-RPC 2.0 at POST /rpc → Bus calls; one events tailer (250 ms + in-proc
 * kick) → one SSE broadcaster at GET /stream; tickets, CSRF (cookie + login),
 * rate-limit buckets (§9). The core is opened ONCE in server mode.
 */
import { openBus, RPC_CODES, type BusErrorCode, type Scope, type Cred } from "../bus.ts";
import { serverHandle, wrapSession, type BusHandle, type Session } from "../bus-iface.ts";
import type { Seams } from "../seams.ts";

// ---------- §9 limits ----------
export const LIMITS = {
  maxBody: 256 * 1024,        // Content-Length cap pre-parse
  maxAuth: 128,               // Authorization header cap before hashing
  writeBurst: 30, writeRefill: 2 / 1000,   // tokens/ms
  readBurst: 120, readRefill: 10 / 1000,
  unauthBurst: 10, unauthRefill: 1 / 1000, // per-IP 401 bucket, pre-HMAC
  maxStreamsPerToken: 2,
  tailerMs: 250,
  pingMs: 20_000,
  ticketTtlMs: 60_000,
};

const WRITE_METHODS = new Set(["join", "post", "status", "rename", "token.create", "token.revoke", "group.create", "group.join", "group.leave", "group.delete", "cursor.set", "login"]);

// ---------- JSON-RPC plumbing ----------
const J = {
  parse: -32700, invalid: -32600, noMethod: -32601, badParams: -32602, internal: -32603,
};
const HTTP_FOR_CODE: Record<number, number> = {
  [-32001]: 401, [-32002]: 403, [-32003]: 404, [-32004]: 429, [-32005]: 409,
  [-32006]: 503, [-32600]: 400, [-32601]: 404, [-32602]: 400, [-32603]: 500, [-32700]: 400,
};
function rpcErr(code: number, message: string, data?: Record<string, unknown>) {
  return { code, message, ...(data ? { data } : {}) };
}
/** BusError → JSON-RPC error object: code from the shared table, the EXACT
 *  variant on data.busError so RpcBus reconstructs it losslessly (the contract
 *  suite compares variants, not codes). */
function busToRpc(e: { error: BusErrorCode; detail: string; data?: Record<string, unknown> }) {
  return rpcErr(RPC_CODES[e.error], e.error, { busError: e.error, detail: e.detail, ...(e.data ?? {}) });
}

// ---------- token buckets (§9) ----------
class Bucket {
  private tokens: number; private last = Date.now();
  constructor(private burst: number, private refillPerMs: number) { this.tokens = burst; }
  take(n = 1): { ok: true } | { ok: false; retryAfterMs: number } {
    const now = Date.now();
    this.tokens = Math.min(this.burst, this.tokens + (now - this.last) * this.refillPerMs);
    this.last = now;
    if (this.tokens >= n) { this.tokens -= n; return { ok: true }; }
    return { ok: false, retryAfterMs: Math.ceil((n - this.tokens) / this.refillPerMs) };
  }
}

export type ServerOpts = {
  home: string;
  port?: number;              // 0 ⇒ ephemeral (tests)
  hostname?: string;          // default 127.0.0.1 (single-writer host; nginx terminates)
  origin?: string;            // §8 CSRF exact-origin pin; unset ⇒ must equal request Host
  seams?: Seams;
  limits?: Partial<typeof LIMITS>; // tests shrink buckets/tailer; prod uses defaults
};

export type RunningServer = {
  port: number; url: string; home: string;
  stop(): void;
  /** test/admin hooks (same demotion rationale as core.testDb) */
  handle: BusHandle;
  sessions: Map<string, { agentId: string; scopes: Scope[]; kind: "agent" | "human" }>;
  tailNow(): void;
  streamCount(tokenId: number): number;
};

export function startServer(opts: ServerOpts): RunningServer {
  const core = openBus({ home: opts.home, mode: "server", seams: opts.seams });
  const handle = serverHandle(core);
  const hostname = opts.hostname ?? "127.0.0.1";
  const LIM = { ...LIMITS, ...(opts.limits ?? {}) };

  // §8 in-memory session store — restart = logout (documented).
  const sessions = new Map<string, { agentId: string; scopes: Scope[]; kind: "agent" | "human"; tokenId: number }>();
  // §6 non-cookie tickets: 60 s single-use.
  const tickets = new Map<string, { tokenId: number; agentId: string; scopes: Scope[]; kind: "agent" | "human"; exp: number }>();

  const writeBuckets = new Map<number, Bucket>();
  const readBuckets = new Map<number, Bucket>();
  const ipBuckets = new Map<string, Bucket>();
  const streamsByToken = new Map<number, number>();

  // ---------- events tailer → one broadcaster ----------
  type Sub = {
    push(frame: string): void; close(): void; refresh(): void;
    scope: string; agentId: string; scopes: Scope[]; tokenId: number;
    seq: number; epoch: string; alive: boolean;
  };
  const subs = new Set<Sub>();
  let tailTimer: ReturnType<typeof setInterval> | null = null;

  function tick() {
    if (!subs.size) return;
    const minSeq = Math.min(...[...subs].map((s) => s.seq));
    for (const s of subs) s.refresh(); // §5: rename rewrites tokens.agent_id —
    // scope=mine subscriptions RE-KEY on the rename event, so identity is
    // re-read from the token row every tick, never cached at subscribe time.
    const evs = core.tailEvents(minSeq, 500);
    for (const e of evs) {
      for (const s of subs) {
        if (!s.alive || e.seq <= s.seq) continue;
        const frame = frameFor(s, e);
        if (frame) s.push(`${frame}`);
      }
      for (const s of subs) if (e.seq > s.seq) s.seq = e.seq;
    }
  }

  function frameFor(s: Sub, e: { seq: number; kind: string; msg_id: string | null; agent_id: string | null }): string | null {
    const idLine = `id: ${s.epoch}.${e.seq}\n`;
    if (e.kind === "msg" || e.kind === "status") {
      if (!e.msg_id) return null;
      const m = core.messageById(e.msg_id);
      if (!m) return null; // deleted/never visible
      if (!core.canSeeChannel({ principal: { agentId: s.agentId, kind: "agent", scopes: s.scopes }, actor: s.agentId }, m.channel)) return null; // G2
      if (!scopeMatch(s, m)) return null;
      const body = JSON.stringify({ id: m.id, channel: m.channel, sender: m.sender, type: m.type, subject: m.subject, status: m.status, created_at: m.created_at, updated_at: m.updated_at, re: m.re, thread: m.thread });
      return `event: ${e.kind}\n${idLine}data: ${body}\n\n`;
    }
    if (e.kind === "read") {
      const m = e.msg_id ? core.messageById(e.msg_id) : null;
      if (m && !core.canSeeChannel({ principal: { agentId: s.agentId, kind: "agent", scopes: s.scopes }, actor: s.agentId }, m.channel)) return null;
      if (s.scope !== "all" && m && !scopeMatch(s, m)) return null;
      return `event: read\n${idLine}data: ${JSON.stringify({ msg: e.msg_id, agent: e.agent_id })}\n\n`;
    }
    if (e.kind === "presence") return `event: presence\n${idLine}data: ${JSON.stringify({ agent: e.agent_id, active: core.isActive(e.agent_id ?? "") })}\n\n`;
    if (e.kind === "token") return `event: token\n${idLine}data: ${JSON.stringify({ agent: e.agent_id })}\n\n`;
    if (e.kind === "group") return `event: group\n${idLine}data: ${JSON.stringify({ agent: e.agent_id })}\n\n`;
    return null; // future kinds: ignore, cursor still advances
  }

  function scopeMatch(s: Sub, m: { channel: string; recipients: string; sender: string; created_at: string }): boolean {
    if (s.scope === "all") return true;
    if (s.scope.startsWith("channel:")) return m.channel === s.scope.slice(8);
    // mine — same predicate as waitStep delivery (recipientsMatch, F honesty).
    return m.sender === s.agentId ||
      core.recipientsMatch(m.recipients, s.agentId, core.roleOf(s.agentId), core.membershipsOf(s.agentId), m.created_at);
  }

  // ---------- auth (§5: principal from the token ROW, every request) ----------
  type Authed = { session: Session; tokenId: number; agentId: string; scopes: Scope[]; via: "bearer" | "cookie" };
  function bearerOf(req: Request): string | null {
    const h = req.headers.get("authorization") ?? "";
    if (!h.startsWith("Bearer ")) return null;
    const t = h.slice(7).trim();
    return t.length ? t : null;
  }
  function cookieSid(req: Request): string | null {
    const c = req.headers.get("cookie") ?? "";
    const m = /(?:^|;\s*)comms_session=([^;\s]+)/.exec(c);
    return m ? m[1] : null;
  }
  function auth(req: Request): { ok: Authed } | { err: ReturnType<typeof rpcErr>; http: number } {
    const tok = bearerOf(req);
    if (tok !== null) {
      if (tok.length > LIM.maxAuth) return { err: rpcErr(-32001, "unauthorized", { detail: "Authorization too long" }), http: 401 };
      const v = core.tokenVerify(tok);
      if (v.error) return { err: busToRpc(v), http: 401 };
      return { ok: { session: wrapSession(core as any, { principal: { agentId: v.value.agentId, kind: v.value.kind, scopes: v.value.scopes }, actor: v.value.agentId } as any), tokenId: v.value.tokenId, agentId: v.value.agentId, scopes: v.value.scopes, via: "bearer" } };
    }
    const sid = cookieSid(req);
    if (sid !== null) {
      const s = sessions.get(sid);
      if (!s) return { err: rpcErr(-32001, "unauthorized", { detail: "unknown session" }), http: 401 };
      return { ok: { session: wrapSession(core as any, { principal: { agentId: s.agentId, kind: s.kind, scopes: s.scopes }, actor: s.agentId } as any), tokenId: s.tokenId, agentId: s.agentId, scopes: s.scopes, via: "cookie" } };
    }
    return { err: rpcErr(-32001, "unauthorized", { detail: "missing bearer or session cookie" }), http: 401 };
  }

  // ---------- CSRF (§8): cookie requests + unauthenticated login ----------
  function csrfOk(req: Request): boolean {
    const ct = (req.headers.get("content-type") ?? "").split(";")[0].trim().toLowerCase();
    if (ct !== "application/json") return false; // parsed media type, params stripped
    const o = req.headers.get("origin");
    if (o === null) return true; // non-browser client (CLI): no Origin, no CSRF surface
    if (opts.origin) return o === opts.origin;
    try { return new URL(o).host === new URL(req.url).host; } catch { return false; }
  }

  // ---------- RPC dispatch ----------
  async function rpcCall(session: Session, method: string, p: any): Promise<{ ok: true; result: unknown } | { ok: false; err: ReturnType<typeof busToRpc> }> {
    const call = async (r: Promise<{ error?: BusErrorCode; detail?: string; value?: unknown; data?: Record<string, unknown> }>) => {
      const res = await r;
      if (res.error) return { ok: false as const, err: busToRpc(res as any) };
      return { ok: true as const, result: res.value };
    };
    switch (method) {
      case "join": return call(session.joinAgent({ agent: p.agent ?? session.agentId, role: p.role ?? "", caps: p.caps, fingerprint: p.fingerprint }));
      case "who": return call(session.listAgents(!p.all));
      case "post": return call(session.post({ from: p.from ?? session.agentId, to: Array.isArray(p.to) ? p.to.join(",") : String(p.to ?? ""), type: String(p.type ?? ""), subject: p.subject, body: String(p.body ?? ""), thread: p.thread, re: p.re, tags: p.tags, channel: p.channel, as: p.as, idempotencyKey: p.idempotencyKey, dm: p.dm }));
      case "inbox": return call(session.inbox({ agent: p.for ?? p.agent ?? session.agentId, open: p.open, unread: p.unread, channel: p.channel, mark: p.mark }));
      case "read": return call(session.read({ agent: p.for ?? p.agent ?? session.agentId, id: String(p.id ?? "") }));
      case "thread": return call(session.threadOf(String(p.id ?? "")));
      case "receipts": return call(session.receipts(String(p.id ?? "")));
      case "status": return call(session.setStatus({ agent: session.agentId, id: String(p.id ?? ""), state: String(p.state ?? "") }));
      case "channels": return call(session.channels());
      case "history": return call(session.history({ channel: p.channel, since: p.since, limit: p.limit }));
      case "rename": return call(session.rename({ agent: p.agent ?? session.agentId, to: String(p.to ?? p.newId ?? "") }));
      case "inbox.wait": return call(session.waitStep({ for: p.for, consumer: p.consumer, since: p.since }));
      case "cursor.get": return call(session.cursorGet({ consumer: p.consumer }));
      case "cursor.set": return call(session.cursorSet({ consumer: String(p.consumer ?? ""), cursor: String(p.cursor ?? ""), force: p.force }));
      case "token.create": return call(session.tokenCreate({ agent: String(p.agent ?? ""), kind: p.kind, label: p.label, scopes: p.scopes, admin: p.admin, force: p.force }));
      case "token.list": return call(session.tokenList());
      case "token.revoke": return call(session.tokenRevoke({ id: Number(p.id) }));
      case "group.create": return call(session.groupCreate({ name: String(p.name ?? ""), agent: p.agent }));
      case "group.join": return call(session.groupJoin({ name: String(p.name ?? ""), agent: p.agent }));
      case "group.leave": return call(session.groupLeave({ name: String(p.name ?? ""), agent: p.agent }));
      case "group.delete": return call(session.groupDelete({ name: String(p.name ?? "") }));
      case "group.list": return call(session.groupList());
      case "group.show": return call(session.groupShow({ name: String(p.name ?? "") }));
      case "dm.members": return call(session.dmMembers(String(p.channel ?? "")));
      default: return { ok: false, err: rpcErr(J.noMethod, "method not found") };
    }
  }

  // ---------- HTTP handler ----------
  let server: ReturnType<typeof Bun.serve> | null = null;

  function json(status: number, body: object, headers: Record<string, string> = {}) {
    return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
  }

  async function handleRpc(req: Request): Promise<Response> {
    // §8 CSRF BEFORE any work: applies to cookie-authed requests AND
    // unauthenticated ones (the login surface); bearer requests are exempt.
    if (bearerOf(req) === null && !csrfOk(req))
      return json(403, { jsonrpc: "2.0", error: rpcErr(-32002, "forbidden", { busError: "forbidden", detail: "CSRF check failed (§8)" }), id: null });
    const cl = Number(req.headers.get("content-length") ?? 0);
    if (cl > LIM.maxBody) return json(413, { jsonrpc: "2.0", error: rpcErr(J.invalid, "body too large"), id: null });
    let text: string;
    try { text = await req.text(); } catch { return json(400, { jsonrpc: "2.0", error: rpcErr(J.parse, "unreadable body"), id: null }); }
    if (text.length > LIM.maxBody) return json(413, { jsonrpc: "2.0", error: rpcErr(J.invalid, "body too large"), id: null });
    let body: any;
    try { body = JSON.parse(text); } catch { return json(400, { jsonrpc: "2.0", error: rpcErr(J.parse, "parse error"), id: null }); }
    if (Array.isArray(body)) return json(400, { jsonrpc: "2.0", error: rpcErr(J.invalid, "batches rejected (§6)"), id: null });
    if (!body || typeof body !== "object" || body.jsonrpc !== "2.0" || typeof body.method !== "string" || !("id" in body))
      return json(400, { jsonrpc: "2.0", error: rpcErr(J.invalid, "invalid request"), id: body?.id ?? null });
    const id = body.id;
    const p = body.params ?? {};

    // login/logout are the cookie bootstrap — special-cased (§8). CSRF for
    // them already ran at the top of handleRpc (bearer-exempt pre-check).
    if (body.method === "login" || body.method === "logout") {
      if (body.method === "logout") {
        const sid = cookieSid(req);
        if (sid) sessions.delete(sid);
        return json(200, { jsonrpc: "2.0", result: { loggedOut: true }, id }, { "set-cookie": "comms_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict; Secure" });
      }
      const ip = ipBucket(req);
      const v = core.tokenVerify(String(p.token ?? ""));
      if (v.error) { ip.take(); return json(401, { jsonrpc: "2.0", error: busToRpc(v), id }); }
      const sid = Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("hex");
      sessions.set(sid, { agentId: v.value.agentId, scopes: v.value.scopes, kind: v.value.kind, tokenId: v.value.tokenId });
      return json(200, { jsonrpc: "2.0", result: { agentId: v.value.agentId, scopes: v.value.scopes }, id },
        { "set-cookie": `comms_session=${sid}; Path=/; HttpOnly; SameSite=Strict; Secure` });
    }

    const a = auth(req);
    if ("err" in a) {
      const ip = ipBucket(req);
      ip.take(); // failed auths burn the per-IP bucket BEFORE any HMAC oracle value
      return json(a.http, { jsonrpc: "2.0", error: a.err, id });
    }
    // cookie requests passed CSRF at the top (bearer-exempt pre-check).

    // §9 buckets keyed by token id.
    const bucket = bucketFor(a.ok.tokenId, body.method);
    const t = bucket.take();
    if (!t.ok)
      return json(429, { jsonrpc: "2.0", error: rpcErr(-32004, "rate limited", { busError: "rate_limited", detail: "rate limit exceeded" }), id },
        { "retry-after": String(Math.max(1, Math.ceil(t.retryAfterMs / 1000))) });

    core.tokenTouch(a.ok.tokenId); // last_used ≤ 1/min inside core

    let out: Awaited<ReturnType<typeof rpcCall>>;
    try {
      out = await rpcCall(a.ok.session, body.method, p);
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      if (/SQLITE_BUSY|database is locked/i.test(msg))
        return json(503, { jsonrpc: "2.0", error: rpcErr(-32006, "contention", { busError: "contention", detail: "db busy; retry" }), id }, { "retry-after": "1" });
      return json(500, { jsonrpc: "2.0", error: rpcErr(J.internal, "internal", { detail: msg }), id });
    }
    if (!out.ok) {
      const http = HTTP_FOR_CODE[out.err.code] ?? 400;
      const headers: Record<string, string> = { "x-comms-agent": a.ok.agentId, "x-comms-scopes": a.ok.scopes.join(",") };
      if (out.err.code === -32004 || out.err.code === -32006) headers["retry-after"] = "1";
      return json(http, { jsonrpc: "2.0", error: out.err, id }, headers);
    }
    // in-process kick: our own writes need no 250 ms wait (§3 fan-out).
    if (WRITE_METHODS.has(body.method)) queueMicrotask(tick);
    return json(200, { jsonrpc: "2.0", result: out.result, id }, { "x-comms-agent": a.ok.agentId, "x-comms-scopes": a.ok.scopes.join(",") });
  }

  function ipBucket(req: Request): Bucket {
    const ip = (req.headers.get("x-forwarded-for") ?? req.headers.get("cf-connecting-ip") ?? "local").split(",")[0].trim();
    let b = ipBuckets.get(ip);
    if (!b) { b = new Bucket(LIM.unauthBurst, LIM.unauthRefill); ipBuckets.set(ip, b); }
    return b;
  }
  function bucketFor(tokenId: number, method: string): Bucket {
    const map = WRITE_METHODS.has(method) ? writeBuckets : readBuckets;
    let b = map.get(tokenId);
    if (!b) { b = new Bucket(WRITE_METHODS.has(method) ? LIM.writeBurst : LIM.readBurst, WRITE_METHODS.has(method) ? LIM.writeRefill : LIM.readRefill); map.set(tokenId, b); }
    return b;
  }

  // ---------- SSE (§6-stream) ----------
  function handleStream(req: Request): Response {
    const a = auth(req);
    let tokenId = 0; let agentId = ""; let scopes: Scope[] = [];
    if ("err" in a) {
      // ticket fallback for non-cookie clients that can't set headers
      const tk = new URL(req.url).searchParams.get("ticket");
      const rec = tk ? tickets.get(tk) : null;
      if (!rec || rec.exp < Date.now()) return new Response("unauthorized", { status: 401 });
      tickets.delete(tk!); // single-use
      tokenId = rec.tokenId; agentId = rec.agentId; scopes = rec.scopes;
    } else { tokenId = a.ok.tokenId; agentId = a.ok.agentId; scopes = a.ok.scopes; }

    const count = streamsByToken.get(tokenId) ?? 0;
    if (count >= LIM.maxStreamsPerToken) return new Response("too many streams (≤2/token)", { status: 429, headers: { "retry-after": "5" } });

    const u = new URL(req.url);
    const scope = u.searchParams.get("scope") ?? "mine";
    if (scope === "all" && !scopes.includes("read:all")) return new Response("scope=all requires read:all", { status: 403 });
    streamsByToken.set(tokenId, count + 1);
    const lastId = req.headers.get("last-event-id") ?? u.searchParams.get("since");

    const ep = core.epoch();
    // fresh subscribe (no cursor): start AT the hello high-water — hello
    // carries {epoch,seq} and deltas apply AFTER it (§6); replaying history
    // to a client that never asked for it would double-deliver against the
    // history snapshot (§6 handoff).
    let from = currentSeq();
    let resync: { epoch: string; floor: number } | null = null;
    if (lastId) {
      const m = /^([0-9a-f]{8,64})\.(\d+)$/.exec(lastId);
      if (!m) return new Response("bad Last-Event-ID/since", { status: 400 });
      if (m[1] !== ep) resync = { epoch: ep, floor: core.gcFloor() };
      else if (Number(m[2]) < core.gcFloor()) resync = { epoch: ep, floor: core.gcFloor() };
      else from = Number(m[2]);
    }

    const enc = new TextEncoder();
    let detached = false;
    let ping: ReturnType<typeof setInterval> | null = null;
    let ctrl: ReadableStreamDefaultController | null = null;
    const sub: Sub = {
      scope, agentId, scopes, tokenId, seq: from, epoch: ep, alive: true,
      push: (f) => { try { ctrl?.enqueue(enc.encode(f)); } catch { /* client gone */ } },
      close() { sub.alive = false; try { ctrl?.close(); } catch { } },
      refresh() {
        const t = core.tokenById(tokenId);
        if (!t || t.revoked) { sub.alive = false; detach(); return; }
        sub.agentId = t.agentId; sub.scopes = t.scopes;
      },
    };
    function detach() {
      if (detached) return;
      detached = true;
      subs.delete(sub);
      if (ping !== null) clearInterval(ping);
      const n = (streamsByToken.get(tokenId) ?? 1) - 1;
      if (n <= 0) streamsByToken.delete(tokenId); else streamsByToken.set(tokenId, n);
      sub.alive = false;
    }
    const stream = new ReadableStream({
      start(c) {
        ctrl = c;
        subs.add(sub);
        if (resync) {
          sub.push(`event: resync\ndata: ${JSON.stringify({ resync: true, ...resync })}\n\n`);
          detach();
          try { c.close(); } catch { }
          return;
        }
        sub.push(`event: hello\ndata: ${JSON.stringify({ epoch: ep, seq: currentSeq() })}\n\n`);
        tick();
        ping = setInterval(() => { if (detached) return; try { c.enqueue(enc.encode(": ping\n\n")); } catch { } }, LIM.pingMs);
      },
      cancel() { detach(); try { sub.close(); } catch { } },
    });
    req.signal.addEventListener("abort", () => { detach(); sub.close(); });
    return new Response(stream, {
      headers: { "content-type": "text/event-stream", "cache-control": "no-cache", "x-accel-buffering": "no" },
    });
  }

  function currentSeq(): number {
    return core.eventsHighWater();
  }

  // ---------- fetch router ----------
  const server0 = Bun.serve({
    hostname,
    port: opts.port ?? 8700,
    idleTimeout: 0, // §6: SSE streams are long-lived
    async fetch(req) {
      const url = new URL(req.url);
      if (req.method === "GET" && url.pathname === "/health") return json(200, { ok: true, epoch: core.epoch() });
      if (req.method === "POST" && url.pathname === "/rpc") return handleRpc(req);
      if (req.method === "GET" && url.pathname === "/stream") return handleStream(req);
      if (req.method === "POST" && url.pathname === "/stream.ticket") {
        const a = auth(req);
        if ("err" in a) return json(a.http, { jsonrpc: "2.0", error: a.err, id: null });
        const tk = "t_" + Buffer.from(crypto.getRandomValues(new Uint8Array(16))).toString("hex");
        tickets.set(tk, { tokenId: a.ok.tokenId, agentId: a.ok.agentId, scopes: a.ok.scopes, kind: "agent", exp: Date.now() + LIM.ticketTtlMs });
        return json(200, { jsonrpc: "2.0", result: { ticket: tk }, id: null });
      }
      return json(404, { jsonrpc: "2.0", error: rpcErr(J.noMethod, "no route"), id: null });
    },
  });
  server = server0;
  tailTimer = setInterval(tick, LIM.tailerMs);

  return {
    port: server0.port as number, url: `http://${hostname}:${server0.port}`, home: opts.home,
    handle, sessions,
    tailNow: tick,
    streamCount: (tokenId) => streamsByToken.get(tokenId) ?? 0,
    stop() {
      if (tailTimer) clearInterval(tailTimer);
      for (const s of subs) s.close();
      subs.clear();
      server?.stop(true);
      core.close();
    },
  };
}
