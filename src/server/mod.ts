/**
 * src/server — HTTP layer (RFC-001 §3.3, §6, §8, §9). NO SQL in this layer.
 *
 * Bun.serve: bearer mw (identity from the token ROW per request, §5) →
 * JSON-RPC 2.0 at POST /rpc → Bus calls; one events tailer (250 ms + in-proc
 * kick) → one SSE broadcaster at GET /stream; tickets, CSRF (cookie + login),
 * rate-limit buckets (§9). The core is opened ONCE in server mode.
 */
import { openBus, serverCtx, validChannelName, RPC_CODES, type BusErrorCode, type Scope, type Cred } from "../bus.ts";
import { serverHandle, wrapSession, type BusHandle, type Session } from "../bus-iface.ts";
import { UI_HTML, UI_CSP } from "./ui.ts";
import type { Seams } from "../seams.ts";
import { join, sep } from "node:path";
import { realpathSync } from "node:fs";

// ---------- §9 limits ----------
export const LIMITS = {
  maxBody: 256 * 1024,        // Content-Length cap pre-parse
  maxAuth: 128,               // Authorization header cap before hashing
  writeBurst: 30, writeRefill: 2 / 1000,   // tokens/ms
  readBurst: 120, readRefill: 10 / 1000,
  unauthBurst: 10, unauthRefill: 1 / 1000, // per-IP 401 bucket, pre-HMAC
  maxStreamsPerToken: 2,
  maxQueuedFrames: 5_000,     // per-stream unsent frames before a stalled reader is dropped
  tailerMs: 250,
  pingMs: 20_000,
  ticketTtlMs: 60_000,
  gcMs: 3_600_000,            // §6/§9 idempotency+events GC: startup + hourly (0 disables — tests)
  // claude M4 M-e: cookie sessions were immortal until logout/restart and
  // unbounded (probe: 2000 logins ⇒ sessions.size 2000). Idle + absolute
  // expiry, and a per-token cap (oldest evicted).
  sessionIdleMs: 12 * 3_600_000,
  sessionMaxMs: 7 * 24 * 3_600_000,
  maxSessionsPerToken: 8,
};

const WRITE_METHODS = new Set(["join", "post", "status", "rename", "token.create", "token.revoke", "group.create", "group.join", "group.leave", "group.delete", "cursor.set", "login"]);

// §7 /raw filename gate: msg-<...>.md, no '/' (regex runs before any join).
const FILE_RE = /^msg-[A-Za-z0-9._-]+\.md$/;

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
  private refill() {
    const now = Date.now();
    this.tokens = Math.min(this.burst, this.tokens + (now - this.last) * this.refillPerMs);
    this.last = now;
  }
  take(n = 1): { ok: true } | { ok: false; retryAfterMs: number } {
    this.refill();
    if (this.tokens >= n) { this.tokens -= n; return { ok: true }; }
    return { ok: false, retryAfterMs: Math.ceil((n - this.tokens) / this.refillPerMs) };
  }
  /** claude M2: non-consuming check — the per-IP 401 bucket is CHECKED before
   *  the HMAC (§9 letter) and only CHARGED on failure (a valid token never
   *  pays for its neighbours' typos). */
  peek(n = 1): { ok: true } | { ok: false; retryAfterMs: number } {
    this.refill();
    return this.tokens >= n ? { ok: true } : { ok: false, retryAfterMs: Math.ceil((n - this.tokens) / this.refillPerMs) };
  }
  get full(): boolean { this.refill(); return this.tokens >= this.burst; }
}

export type ServerOpts = {
  home: string;
  port?: number;              // 0 ⇒ ephemeral (tests)
  hostname?: string;          // default 127.0.0.1 (single-writer host; nginx terminates)
  origin?: string;            // §8 CSRF exact-origin pin; unset ⇒ must equal request Host
  /** claude M2: per-IP 401 bucket key. false (default) ⇒ the socket peer
   *  (server.requestIP) — X-Forwarded-For is client-controlled and would let a
   *  sprayer mint a fresh bucket per request. true ⇒ the RIGHTMOST XFF entry
   *  (the one nginx's proxy_add_x_forwarded_for appended), never the leftmost. */
  trustProxy?: boolean;
  seams?: Seams;
  limits?: Partial<typeof LIMITS>; // tests shrink buckets/tailer; prod uses defaults
};

export type RunningServer = {
  port: number; url: string; home: string;
  stop(): void;
  /** test/admin hooks (same demotion rationale as core.testDb) */
  handle: BusHandle;
  sessions: Map<string, { tokenId: number; created: number; seen: number }>;
  tailNow(): void;
  streamCount(tokenId: number): number;
};

export function startServer(opts: ServerOpts): RunningServer {
  const core = openBus({ home: opts.home, mode: "server", seams: opts.seams });
  const handle = serverHandle(core);
  const hostname = opts.hostname ?? "127.0.0.1";
  const LIM = { ...LIMITS, ...(opts.limits ?? {}) };

  // §8 in-memory session store — restart = logout (documented). claude M2 B1:
  // stores ONLY the token id; the principal is re-read from the token row on
  // EVERY cookie request (§5), so revoke/rename/retire bite immediately.
  const sessions = new Map<string, { tokenId: number; created: number; seen: number }>();
  const sessionLive = (s: { created: number; seen: number }, now = Date.now()) =>
    now - s.seen < LIM.sessionIdleMs && now - s.created < LIM.sessionMaxMs;
  // §6 non-cookie tickets: 60 s single-use; re-resolved from the row at open.
  const tickets = new Map<string, { tokenId: number; exp: number }>();

  const writeBuckets = new Map<number, Bucket>();
  const readBuckets = new Map<number, Bucket>();
  const ipBuckets = new Map<string, Bucket>();
  const streamsByToken = new Map<number, number>();

  // ---------- events tailer → one broadcaster ----------
  type Sub = {
    push(frame: string): void; close(): void; refresh(): void; resync(epoch: string, floor: number): void;
    scope: string; agentId: string; scopes: Scope[]; tokenId: number;
    seq: number; epoch: string; alive: boolean;
    sid: string | null; // cookie-authed stream ⇒ dies with its session (logout/expiry)
  };
  const subs = new Set<Sub>();
  let tailTimer: ReturnType<typeof setInterval> | null = null;
  let gcTimer: ReturnType<typeof setInterval> | null = null;
  // claude M2 M3: ONE shared head cursor for the live tail. The old global
  // min(sub.seq) let a single resuming laggard pin every live subscriber to
  // the laggard's 500-event page per tick (probe: 2.7 s live latency behind a
  // 6k-event resume; a 30-day backlog ⇒ ~minutes). Laggards now catch up on
  // their OWN page, bounded by head, then join the shared tail.
  let head = core.eventsHighWater();
  let headEpoch = core.epoch();
  const PAGE = 500;

  function deliver(s: Sub, e: { seq: number; kind: string; msg_id: string | null; agent_id: string | null }) {
    if (!s.alive || e.seq <= s.seq) return;
    const frame = frameFor(s, e);
    if (frame) s.push(frame);
    s.seq = e.seq;
  }

  function tick() {
    try {
      // epoch rotated / DB restored under a live server: seqs may REGRESS, so
      // the shared head is re-read and every open stream gets resync (their
      // ids carry the dead epoch). A laggard whose position was gc'd since it
      // subscribed would otherwise skip silently ⇒ resync too (§3/§9).
      const ep = core.epoch();
      if (ep !== headEpoch) { headEpoch = ep; head = core.eventsHighWater(); }
      if (!subs.size) { head = core.eventsHighWater(); return; }
      const floor = core.gcFloor();
      for (const s of [...subs]) if (s.epoch !== ep || s.seq < floor) s.resync(ep, floor);
      for (const s of [...subs]) s.refresh(); // §5: rename rewrites tokens.agent_id —
      // scope=mine subscriptions RE-KEY on the rename event, so identity is
      // re-read from the token row every tick, never cached at subscribe time.
      // 1) laggards (resumed below head): one private page each, capped at head.
      for (const s of subs) {
        if (!s.alive || s.seq >= head) continue;
        for (const e of core.tailEvents(s.seq, PAGE)) { if (e.seq > head) break; deliver(s, e); }
      }
      // 2) shared live tail: read ONCE, fan out to every caught-up subscriber.
      for (let pages = 0; pages < 8; pages++) {
        const evs = core.tailEvents(head, PAGE);
        for (const e of evs) {
          for (const s of subs) if (s.seq >= head) deliver(s, e); // caught-up only; laggards get it via (1)
          head = e.seq;
        }
        if (evs.length < PAGE) break;
      }
    } catch (e) {
      // a timer callback must never take the process down (SQLITE_BUSY on a
      // read under a writer, a corrupt row): next tick retries from `head`.
      core.seams.warn?.(`agent-comms tailer: ${String((e as any)?.message ?? e)}`);
    }
  }

  const principalOf = (s: Sub) => ({ principal: { agentId: s.agentId, kind: "agent" as const, scopes: s.scopes }, actor: s.agentId });

  function frameFor(s: Sub, e: { seq: number; kind: string; msg_id: string | null; agent_id: string | null }): string | null {
    const idLine = `id: ${s.epoch}.${e.seq}\n`;
    if (e.kind === "msg" || e.kind === "status") {
      if (!e.msg_id) return null;
      const m = core.messageById(e.msg_id);
      if (!m) return null; // deleted/never visible
      if (!core.canSeeChannel(principalOf(s), m.channel)) return null; // G2
      if (!scopeMatch(s, m)) return null;
      const body = JSON.stringify({ id: m.id, channel: m.channel, sender: m.sender, type: m.type, subject: m.subject, status: m.status, created_at: m.created_at, updated_at: m.updated_at, re: m.re, thread: m.thread });
      return `event: ${e.kind}\n${idLine}data: ${body}\n\n`;
    }
    if (e.kind === "read") {
      // claude M2 m: a read event whose message is gone/unknown carries no
      // visibility proof ⇒ drop (was: delivered to every scope).
      const m = e.msg_id ? core.messageById(e.msg_id) : null;
      if (!m || !core.canSeeChannel(principalOf(s), m.channel)) return null;
      if (s.scope !== "all" && !scopeMatch(s, m)) return null;
      return `event: read\n${idLine}data: ${JSON.stringify({ msg: e.msg_id, agent: e.agent_id })}\n\n`;
    }
    if (e.kind === "presence") return `event: presence\n${idLine}data: ${JSON.stringify({ agent: e.agent_id, active: core.isActive(e.agent_id ?? "") })}\n\n`;
    // claude M2 m: token mint/revoke is tokens:admin data (token.list gate) —
    // a non-admin sees only its own agent's token events.
    if (e.kind === "token") return s.scopes.includes("tokens:admin") || e.agent_id === s.agentId
      ? `event: token\n${idLine}data: ${JSON.stringify({ agent: e.agent_id })}\n\n` : null;
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
  type AuthFail = { err: ReturnType<typeof rpcErr>; http: number; retryAfter?: number };
  /** claude M2 m: auth-scheme is case-insensitive (RFC 9110 §11.1) — a
   *  `bearer x` header was silently treated as NO credential, which then took
   *  the cookie/CSRF branch with a confusing 403. */
  function bearerOf(req: Request): string | null {
    const h = req.headers.get("authorization") ?? "";
    const m = /^bearer[ \t]+(\S+)[ \t]*$/i.exec(h);
    return m ? m[1] : null;
  }
  function cookieSid(req: Request): string | null {
    const c = req.headers.get("cookie") ?? "";
    const m = /(?:^|;\s*)comms_session=([0-9a-f]{64})(?:;|\s|$)/.exec(c);
    return m ? m[1] : null;
  }
  const UNAUTH = (detail: string): AuthFail => ({ err: rpcErr(-32001, "unauthorized", { busError: "unauthorized", detail }), http: 401 });
  /** principal from the token ROW by id — the ONE resolver for cookie
   *  sessions, tickets and SSE refresh (tokenById.live = not revoked AND not
   *  retired, the same predicate tokenVerify applies). */
  function fromRow(tokenId: number, via: Authed["via"]): { ok: Authed } | AuthFail {
    const t = core.tokenById(tokenId);
    if (!t || !t.live) return UNAUTH("unknown or revoked token");
    const session = wrapSession(core, serverCtx(t.agentId, t.scopes, t.kind));
    return { ok: { session, tokenId, agentId: t.agentId, scopes: t.scopes, via } };
  }
  /** §9: unauthenticated 401s are metered per IP and the bucket is CHECKED
   *  BEFORE the HMAC (RFC letter — an exhausted IP never reaches the hash),
   *  CHARGED only on failure. Applies to every credential-bearing path:
   *  /rpc, login, /stream, /stream.ticket. */
  function auth(req: Request, ip: string): { ok: Authed } | AuthFail {
    const tok = bearerOf(req);
    const sid = tok === null ? cookieSid(req) : null;
    const bucket = ipBucket(ip);
    const gate = bucket.peek();
    if (!gate.ok && (tok !== null || sid !== null)) {
      const f = UNAUTH("too many failed authentications from this address");
      return { ...f, http: 429, err: rpcErr(-32004, "rate limited", { busError: "rate_limited", detail: f.err.data!.detail }), retryAfter: Math.max(1, Math.ceil(gate.retryAfterMs / 1000)) };
    }
    const fail = (f: AuthFail) => { bucket.take(); return f; };
    if (tok !== null) {
      if (tok.length > LIM.maxAuth) return fail(UNAUTH("Authorization too long"));
      const v = core.tokenVerify(tok);
      if (v.error) return fail({ err: busToRpc(v), http: 401 });
      return { ok: { session: wrapSession(core, serverCtx(v.value.agentId, v.value.scopes, v.value.kind)), tokenId: v.value.tokenId, agentId: v.value.agentId, scopes: v.value.scopes, via: "bearer" } };
    }
    if (sid !== null) {
      const s = sessions.get(sid);
      if (!s) return fail(UNAUTH("unknown session"));
      if (!sessionLive(s)) { sessions.delete(sid); return fail(UNAUTH("session expired")); }
      const r = fromRow(s.tokenId, "cookie");
      if ("err" in r) { sessions.delete(sid); return fail(r); } // revoked/retired ⇒ session dies with the token
      s.seen = Date.now();
      return r;
    }
    return UNAUTH("missing bearer or session cookie");
  }

  // ---------- CSRF (§8): cookie requests + unauthenticated login ----------
  function csrfOk(req: Request): boolean {
    const ct = (req.headers.get("content-type") ?? "").split(";")[0].trim().toLowerCase();
    if (ct !== "application/json") return false; // parsed media type, params stripped
    const o = req.headers.get("origin");
    // claude M2 m: §8 says Origin MUST equal the configured origin. With a pin
    // configured, a missing Origin on a cookie/login request fails closed
    // (browsers always send Origin on cross-origin POST; SameSite=Strict is
    // the belt, this is the braces). Without a pin (dev/loopback), absent
    // Origin = non-browser client and passes as before.
    if (o === null) return !opts.origin;
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
  const failResp = (f: AuthFail, id: unknown) =>
    json(f.http, { jsonrpc: "2.0", error: f.err, id }, f.retryAfter ? { "retry-after": String(f.retryAfter) } : {});
  // envelope errors carry busError too, so RpcBus never degrades a wire-level
  // reject to "internal" where LocalBus would say "usage" (parity by construction)
  const envErr = (code: number, message: string) => rpcErr(code, message, { busError: code === J.internal ? "internal" : "usage", detail: message });
  const isStrOpt = (v: unknown) => v === undefined || v === null || typeof v === "string";

  async function handleRpc(req: Request, ip: string): Promise<Response> {
    // §8 CSRF BEFORE any work: applies to cookie-authed requests AND
    // unauthenticated ones (the login surface); bearer requests are exempt BY
    // HEADER PRESENCE — safe because a present bearer disables the cookie path
    // entirely in auth() (bad bearer + valid cookie ⇒ 401, never cookie-authed),
    // and Authorization is not CORS-safelisted (cross-site needs a preflight we
    // never grant).
    if (bearerOf(req) === null && !csrfOk(req))
      return json(403, { jsonrpc: "2.0", error: rpcErr(-32002, "forbidden", { busError: "forbidden", detail: "CSRF check failed (§8)" }), id: null });
    const cl = Number(req.headers.get("content-length") ?? 0);
    if (cl > LIM.maxBody) return json(413, { jsonrpc: "2.0", error: envErr(J.invalid, "body too large"), id: null });
    let text: string;
    // Bun.serve maxRequestBodySize (set below) is the hard cap for chunked
    // bodies with no Content-Length — req.text() rejects past it.
    try { text = await req.text(); } catch { return json(413, { jsonrpc: "2.0", error: envErr(J.invalid, "body too large or unreadable"), id: null }); }
    if (text.length > LIM.maxBody) return json(413, { jsonrpc: "2.0", error: envErr(J.invalid, "body too large"), id: null });
    let body: any;
    try { body = JSON.parse(text); } catch { return json(400, { jsonrpc: "2.0", error: envErr(J.parse, "parse error"), id: null }); }
    if (Array.isArray(body)) return json(400, { jsonrpc: "2.0", error: envErr(J.invalid, "batches rejected (§6)"), id: null });
    if (!body || typeof body !== "object" || body.jsonrpc !== "2.0" || typeof body.method !== "string" || !("id" in body))
      return json(400, { jsonrpc: "2.0", error: envErr(J.invalid, "invalid request"), id: body?.id ?? null });
    const id = body.id;
    const p = body.params ?? {};
    if (typeof p !== "object" || p === null || Array.isArray(p))
      return json(400, { jsonrpc: "2.0", error: envErr(J.badParams, "params must be an object"), id });

    // login/logout are the cookie bootstrap — special-cased (§8). CSRF for
    // them already ran at the top of handleRpc (bearer-exempt pre-check).
    if (body.method === "login" || body.method === "logout") {
      if (body.method === "logout") {
        const sid = cookieSid(req);
        if (sid) {
          sessions.delete(sid);
          // claude M4 M-e: logout also ends the session's open /stream (probe:
          // after logout /rpc was 401 but the cookie EventSource kept
          // receiving msg frames until restart).
          for (const s of [...subs]) if (s.sid === sid) s.close();
        }
        return json(200, { jsonrpc: "2.0", result: { loggedOut: true }, id }, { "set-cookie": "comms_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict; Secure" });
      }
      const bucket = ipBucket(ip);
      const gate = bucket.peek(); // §9: checked BEFORE the HMAC
      if (!gate.ok) return json(429, { jsonrpc: "2.0", error: rpcErr(-32004, "rate limited", { busError: "rate_limited", detail: "too many failed authentications from this address" }), id }, { "retry-after": String(Math.max(1, Math.ceil(gate.retryAfterMs / 1000))) });
      const tokStr = typeof p.token === "string" ? p.token : "";
      const v = tokStr.length > LIM.maxAuth ? { error: "unauthorized" as const, detail: "token too long" } : core.tokenVerify(tokStr);
      if (v.error) { bucket.take(); return json(401, { jsonrpc: "2.0", error: busToRpc(v), id }); }
      const sid = Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("hex");
      // a re-login from the same browser replaces its old session, and each
      // token holds at most maxSessionsPerToken (oldest evicted, its stream too).
      const prior = cookieSid(req);
      if (prior && sessions.delete(prior)) for (const s of [...subs]) if (s.sid === prior) s.close();
      const mine = [...sessions].filter(([, s]) => s.tokenId === v.value.tokenId).sort((a, b) => a[1].created - b[1].created);
      for (const [k] of mine.slice(0, Math.max(0, mine.length - LIM.maxSessionsPerToken + 1))) {
        sessions.delete(k);
        for (const s of [...subs]) if (s.sid === k) s.close();
      }
      const now = Date.now();
      sessions.set(sid, { tokenId: v.value.tokenId, created: now, seen: now });
      return json(200, { jsonrpc: "2.0", result: { agentId: v.value.agentId, scopes: v.value.scopes }, id },
        { "set-cookie": `comms_session=${sid}; Path=/; Max-Age=${Math.floor(LIM.sessionMaxMs / 1000)}; HttpOnly; SameSite=Strict; Secure` });
    }

    const a = auth(req, ip);
    if ("err" in a) return failResp(a, id); // per-IP bucket already checked (pre-HMAC) + charged inside auth()
    // cookie requests passed CSRF at the top (bearer-exempt pre-check).

    // claude M3 M1: ONE identity-header builder for every post-auth response
    // (429 bucket, 400 param check, 503, 500, dispatch error) — the banner is
    // transport truth only if no authed path can omit half of it.
    const ident = (o: Authed) => ({ "x-comms-agent": o.agentId, "x-comms-scopes": o.scopes.join(",") });
    // §9 buckets keyed by token id.
    const bucket = bucketFor(a.ok.tokenId, body.method);
    const t = bucket.take();
    if (!t.ok)
      return json(429, { jsonrpc: "2.0", error: rpcErr(-32004, "rate limited", { busError: "rate_limited", detail: "rate limit exceeded" }), id },
        { "retry-after": String(Math.max(1, Math.ceil(t.retryAfterMs / 1000))), ...ident(a.ok) });

    // wire types are erased (finding 1 class): string-typed post fields that
    // arrive as objects/numbers must be usage, not a TypeError → 500.
    if (body.method === "post" && !(["from", "type", "subject", "body", "thread", "re", "tags", "channel", "as", "idempotencyKey", "dm"].every((k) => isStrOpt(p[k]))
        && (isStrOpt(p.to) || (Array.isArray(p.to) && p.to.every((x: unknown) => typeof x === "string")))))
      return json(400, { jsonrpc: "2.0", error: busToRpc({ error: "usage", detail: "invalid params: post fields must be strings (to: string[])" }), id }, ident(a.ok));

    let out: Awaited<ReturnType<typeof rpcCall>>;
    try {
      // last_used ≤ 1/min inside core — a WRITE: inside the BUSY→503 mapping.
      // claude M3 M7: the same debounced write refreshes PRESENCE (UPDATE-only
      // touch in server mode — never inserts, never resurrects a retired id):
      // server-mode core never touch()es, so a remote agent running `watch`
      // for hours went ○ after 15 min while local watch stays ●.
      if (core.tokenTouch(a.ok.tokenId)) core.touch(a.ok.agentId);
      out = await rpcCall(a.ok.session, body.method, p);
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      if (/SQLITE_BUSY|database is locked/i.test(msg))
        return json(503, { jsonrpc: "2.0", error: rpcErr(-32006, "contention", { busError: "contention", detail: "db busy; retry" }), id }, { "retry-after": "1", ...ident(a.ok) });
      core.seams.warn?.(`agent-comms rpc ${body.method}: ${msg}`); // server log only — no exception text on the wire
      return json(500, { jsonrpc: "2.0", error: rpcErr(J.internal, "internal", { busError: "internal", detail: "internal error" }), id }, ident(a.ok));
    }
    if (!out.ok) {
      const http = HTTP_FOR_CODE[out.err.code] ?? 400;
      const headers: Record<string, string> = { ...ident(a.ok) };
      if (out.err.code === -32004 || out.err.code === -32006) headers["retry-after"] = "1";
      return json(http, { jsonrpc: "2.0", error: out.err, id }, headers);
    }
    // in-process kick: our own writes need no 250 ms wait (§3 fan-out).
    if (WRITE_METHODS.has(body.method)) queueMicrotask(tick);
    // x-comms-agent = the token ROW's agent AFTER the call (a self-rename
    // returns the NEW id, not the pre-call one). claude fold n1: read ONCE.
    const row = body.method === "rename" ? core.tokenById(a.ok.tokenId) : null;
    const after = row?.agentId ?? a.ok.agentId;
    const afterScopes = row?.scopes ?? a.ok.scopes;
    return json(200, { jsonrpc: "2.0", result: out.result, id }, { "x-comms-agent": after, "x-comms-scopes": afterScopes.join(",") });
  }

  /** claude M2: key = socket peer by default. XFF is client-controlled (a
   *  sprayer rotating it got a fresh bucket per request — and the old code
   *  never even consulted take()'s verdict). trustProxy ⇒ RIGHTMOST XFF hop
   *  (appended by our own nginx), never the leftmost (client-supplied). */
  function clientIp(req: Request, peer: string | null): string {
    if (opts.trustProxy) {
      const xff = req.headers.get("x-forwarded-for");
      if (xff) { const hops = xff.split(",").map((s) => s.trim()).filter(Boolean); if (hops.length) return hops[hops.length - 1]; }
    }
    return peer ?? "unknown";
  }
  function ipBucket(ip: string): Bucket {
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
  function handleStream(req: Request, ip: string): Response {
    const u = new URL(req.url);
    let who: Authed;
    const tk = u.searchParams.get("ticket");
    if (tk !== null && bearerOf(req) === null && cookieSid(req) === null) {
      // ticket path for non-cookie clients that can't set headers — single-use,
      // then the principal is re-read from the token ROW (a ticket minted
      // before a revoke/rename must not carry the stale identity).
      const rec = tickets.get(tk);
      tickets.delete(tk); // single-use, consumed even when expired
      if (!rec || rec.exp < Date.now()) return new Response("unauthorized", { status: 401 });
      const r = fromRow(rec.tokenId, "bearer");
      if ("err" in r) return new Response("unauthorized", { status: 401 });
      who = r.ok;
    } else {
      const a = auth(req, ip);
      if ("err" in a) return new Response(String(a.err.data?.detail ?? "unauthorized"), { status: a.http, headers: a.retryAfter ? { "retry-after": String(a.retryAfter) } : {} });
      who = a.ok;
    }
    const { tokenId, agentId, scopes } = who;
    const streamSid = who.via === "cookie" ? cookieSid(req) : null;

    // validate EVERYTHING before taking a stream slot (claude M2: a 400 on a
    // bad Last-Event-ID used to leak the slot — two bad resumes = token
    // locked out of streaming until restart).
    const scope = u.searchParams.get("scope") ?? "mine";
    if (!(scope === "mine" || scope === "all" || (scope.startsWith("channel:") && scope.length > 8)))
      return new Response("scope must be mine | all | channel:<name>", { status: 400 });
    if (scope === "all" && !scopes.includes("read:all")) return new Response("scope=all requires read:all", { status: 403 });
    const lastId = req.headers.get("last-event-id") ?? u.searchParams.get("since");
    const ep = core.epoch();
    const hw = currentSeq();
    // fresh subscribe (no cursor): start AT the high-water — deltas apply
    // AFTER hello.seq (§6); replaying history to a client that never asked
    // for it would double-deliver against the history snapshot (§6 handoff).
    let from = hw;
    let resync: { epoch: string; floor: number } | null = null;
    if (lastId) {
      const m = /^([0-9a-f]{8,64})\.(\d+)$/.exec(lastId);
      if (!m) return new Response("bad Last-Event-ID/since", { status: 400 });
      const seq = Number(m[2]);
      // a same-epoch cursor ABOVE the high-water cannot be legitimate (seq is
      // AUTOINCREMENT) and would silently swallow every event up to it ⇒ resync.
      if (m[1] !== ep || seq < core.gcFloor() || seq > hw) resync = { epoch: ep, floor: core.gcFloor() };
      else from = seq;
    }
    const count = streamsByToken.get(tokenId) ?? 0;
    if (!resync && count >= LIM.maxStreamsPerToken) return new Response("too many streams (≤2/token)", { status: 429, headers: { "retry-after": "5" } });

    const enc = new TextEncoder();
    let detached = false;
    let counted = false;
    let ping: ReturnType<typeof setInterval> | null = null;
    let ctrl: ReadableStreamDefaultController | null = null;
    const sub: Sub = {
      scope, agentId, scopes, tokenId, seq: from, epoch: ep, alive: true, sid: streamSid,
      push: (f) => {
        if (!ctrl || detached) return;
        // bounded server-side queue: a stalled reader must not grow memory
        // without limit — drop it; EventSource reconnects with Last-Event-ID.
        if ((ctrl.desiredSize ?? 0) < -LIM.maxQueuedFrames) { sub.close(); return; }
        try { ctrl.enqueue(enc.encode(f)); } catch { sub.close(); }
      },
      close() { detach(); try { ctrl?.close(); } catch { } },
      refresh() {
        // §5 + claude M2: SAME liveness predicate as tokenVerify (revoked OR
        // retired ⇒ gone) and the socket is actually CLOSED — the old code
        // only unsubscribed, leaving a silent zombie connection holding the
        // client (EventSource never reconnects, never learns it was revoked).
        const t = core.tokenById(tokenId);
        const sess = sub.sid !== null ? sessions.get(sub.sid) : null;
        const sessionGone = sub.sid !== null && (!sess || !sessionLive(sess));
        if (!t || !t.live || sessionGone || (sub.scope === "all" && !t.scopes.includes("read:all"))) {
          sub.push(`event: revoked\ndata: {}\n\n`);
          sub.close();
          return;
        }
        sub.agentId = t.agentId; sub.scopes = t.scopes;
      },
      resync(epoch: string, floor: number) {
        sub.push(`event: resync\ndata: ${JSON.stringify({ resync: true, epoch, floor })}\n\n`);
        sub.close();
      },
    };
    function detach() {
      if (detached) return;
      detached = true;
      sub.alive = false;
      subs.delete(sub);
      if (ping !== null) clearInterval(ping);
      if (counted) {
        const n = (streamsByToken.get(tokenId) ?? 1) - 1;
        if (n <= 0) streamsByToken.delete(tokenId); else streamsByToken.set(tokenId, n);
      }
    }
    const stream = new ReadableStream({
      start(c) {
        ctrl = c;
        if (resync) { sub.resync(resync.epoch, resync.floor); return; }
        counted = true;
        streamsByToken.set(tokenId, (streamsByToken.get(tokenId) ?? 0) + 1);
        subs.add(sub);
        // hello.seq = the point deltas apply AFTER — the RESUME cursor on a
        // resume (claude M2: was the high-water, so a client deduping by
        // seq<=hello.seq dropped the entire replay).
        // grok M4 B2: hello also sets the SSE id, so a native EventSource
        // reconnect on a QUIET bus (no id'd frame ever arrived ⇒ browser has
        // no Last-Event-ID) still resumes at the handoff point instead of
        // re-subscribing at the high-water and silently dropping the gap.
        // resync frames deliberately carry NO id (the client must drop the
        // dead cursor; claude M4 M-f handles that side).
        sub.push(`event: hello\nid: ${ep}.${from}\ndata: ${JSON.stringify({ epoch: ep, seq: from })}\n\n`);
        tick();
        ping = setInterval(() => sub.push(": ping\n\n"), LIM.pingMs);
      },
      cancel() { sub.close(); },
    });
    req.signal.addEventListener("abort", () => sub.close());
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
    // hard cap for bodies WITHOUT Content-Length (chunked): req.text() rejects
    // past this instead of buffering unbounded pre-auth (probe: 8 MB accepted).
    maxRequestBodySize: LIM.maxBody + 64 * 1024,
    async fetch(req, srv) {
      const url = new URL(req.url);
      const ip = clientIp(req, srv.requestIP(req)?.address ?? null);
      if (req.method === "GET" && url.pathname === "/health") return json(200, { ok: true, epoch: core.epoch() });
      // §8 M4: the web UI is a single static page; all data rides /rpc + /stream
      // on the session cookie. no-store: restart = logout, a cached shell must
      // never outlive its session; X-Frame-Options: a foreign frame could not
      // drive RPCs (CSRF) but should not sniff the login surface either.
      if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html"))
        return new Response(UI_HTML, { headers: {
          "content-type": "text/html; charset=utf-8", "cache-control": "no-store", "x-frame-options": "DENY", "referrer-policy": "no-referrer",
          // claude M4 m-c: the policy rides the HEADER (a meta CSP cannot
          // carry frame-ancestors, and is applied only after parsing starts).
          // script-src is the inline script's sha256, not 'unsafe-inline'.
          "content-security-policy": UI_CSP, "x-content-type-options": "nosniff",
        } });
      if (req.method === "POST" && url.pathname === "/rpc") return handleRpc(req, ip);
      if (req.method === "GET" && url.pathname === "/stream") return handleStream(req, ip);
      // §7 GET /raw/messages/<channel>/<file> — mirror bytes for remote agents.
      // Auth: same bearer/cookie chain as /rpc. File regex + channel regex run
      // BEFORE any path join; realpath must stay under MSG_DIR; the resolved
      // message must pass canSee (invisible == missing).
      if (req.method === "GET" && url.pathname.startsWith("/raw/messages/")) {
        const rest = url.pathname.slice("/raw/messages/".length);
        const seg = rest.split("/");
        // match on the RAW percent-encoded segments (decodeURIComponent could
        // smuggle %00/%2F past the regex), then decode the safe remainder.
        if (seg.length !== 2 || !FILE_RE.test(seg[1]) || !validChannelName(seg[0]))
          return json(404, { jsonrpc: "2.0", error: rpcErr(-32003, "not found"), id: null });
        const chan = decodeURIComponent(seg[0]);
        const file = decodeURIComponent(seg[1]);
        const a = auth(req, ip);
        if ("err" in a) return failResp(a, null);
        const t = bucketFor(a.ok.tokenId, "raw").take(); // read bucket
        if (!t.ok) return json(429, { jsonrpc: "2.0", error: rpcErr(-32004, "rate limited", { busError: "rate_limited", detail: "rate limit exceeded" }), id: null }, { "retry-after": String(Math.max(1, Math.ceil(t.retryAfterMs / 1000))) });
        const msg = core.messageByFile(chan, file);
        if (!msg || !core.canSeeChannel(serverCtx(a.ok.agentId, a.ok.scopes), msg.channel))
          return json(404, { jsonrpc: "2.0", error: rpcErr(-32003, "not found"), id: null });
        const abs = join(core.MSG_DIR, chan, file);
        let real: string | null = null;
        try { real = realpathSync(abs); } catch { /* missing file on disk ⇒ not found */ }
        const rootReal = realpathSync(core.MSG_DIR);
        if (real === null || !(real === rootReal || real.startsWith(rootReal + sep)))
          return json(404, { jsonrpc: "2.0", error: rpcErr(-32003, "not found"), id: null });
        const f = Bun.file(real);
        return new Response(f, { headers: { "content-type": "text/markdown; charset=utf-8", "x-comms-agent": a.ok.agentId, "x-comms-scopes": a.ok.scopes.join(",") } });
      }
      if (req.method === "POST" && url.pathname === "/stream.ticket") {
        // §6: tickets are for NON-COOKIE clients only ⇒ bearer required (a
        // cookie-authed mint was also a CSRF-unchecked write).
        if (bearerOf(req) === null) return json(401, { jsonrpc: "2.0", error: rpcErr(-32001, "unauthorized", { busError: "unauthorized", detail: "stream.ticket requires a bearer token" }), id: null });
        const a = auth(req, ip);
        if ("err" in a) return failResp(a, null);
        const t = bucketFor(a.ok.tokenId, "stream.ticket").take(); // §9 read bucket — minting was unmetered
        if (!t.ok) return json(429, { jsonrpc: "2.0", error: rpcErr(-32004, "rate limited", { busError: "rate_limited", detail: "rate limit exceeded" }), id: null }, { "retry-after": String(Math.max(1, Math.ceil(t.retryAfterMs / 1000))) });
        const now = Date.now();
        for (const [k, v] of tickets) if (v.exp < now) tickets.delete(k); // expired tickets never used were never swept
        const tk = "t_" + Buffer.from(crypto.getRandomValues(new Uint8Array(16))).toString("hex");
        tickets.set(tk, { tokenId: a.ok.tokenId, exp: now + LIM.ticketTtlMs });
        return json(200, { jsonrpc: "2.0", result: { ticket: tk }, id: null });
      }
      return json(404, { jsonrpc: "2.0", error: rpcErr(J.noMethod, "no route"), id: null });
    },
  });
  server = server0;
  tailTimer = setInterval(tick, LIM.tailerMs);
  // §6/§9: idempotency + events GC "at startup + hourly" IN the server — it is
  // the single writer (§9); an out-of-process cron would be a second writer on
  // the hosted DB. Same tick sweeps full per-IP buckets (bounded memory).
  const housekeeping = () => {
    try { core.gc(); } catch (e) { core.seams.warn?.(`agent-comms gc: ${String((e as any)?.message ?? e)}`); }
    for (const [k, b] of ipBuckets) if (b.full) ipBuckets.delete(k);
    const now = Date.now();
    for (const [k, s] of sessions) if (!sessionLive(s, now)) sessions.delete(k); // streams close on next refresh()
  };
  if (LIM.gcMs > 0) { housekeeping(); gcTimer = setInterval(housekeeping, LIM.gcMs); }

  return {
    port: server0.port as number, url: `http://${hostname}:${server0.port}`, home: opts.home,
    handle, sessions,
    tailNow: tick,
    streamCount: (tokenId) => streamsByToken.get(tokenId) ?? 0,
    stop() {
      if (tailTimer) clearInterval(tailTimer);
      if (gcTimer) clearInterval(gcTimer);
      for (const s of [...subs]) s.close();
      subs.clear();
      server?.stop(true);
      core.close();
    },
  };
}
