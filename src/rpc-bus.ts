/**
 * RpcBus — the `Bus` interface over HTTP JSON-RPC (RFC-001 §3.2).
 *
 * Holds NOTHING but {url, token}: the interface is transport-implementable
 * (finding 10a). Every method returns Res; the server carries the exact
 * BusError variant on error.data.busError so the reconstructed variant is
 * lossless — the contract suite compares variants, so any wire drift between
 * LocalBus and RpcBus is a CI failure BY CONSTRUCTION.
 */
import type { BusErrorCode, Res, Scope, Cred } from "./bus.ts";
import type { BusHandle, Session } from "./bus-iface.ts";

export class RpcError extends Error {
  constructor(readonly code: number, message: string, readonly data?: Record<string, unknown>) { super(message); }
}

export class RpcBus implements BusHandle {
  readonly mode = "server" as const;
  constructor(private url: string, private token?: string) {}
  readonly _kick = { n: 0 }; // §6: post→stream handoff counter (M4 UI uses it)

  async call<T = unknown>(method: string, params: Record<string, unknown> = {}, cred?: string, onMeta?: (h: Headers) => void): Promise<Res<T>> {
    let res: Response;
    try {
      res = await fetch(`${this.url}/rpc`, {
        method: "POST",
        headers: { "content-type": "application/json", ...(cred ? { authorization: `Bearer ${cred}` } : {}) },
        body: JSON.stringify({ jsonrpc: "2.0", method, params, id: ++this._kick.n }),
      });
    } catch (e) {
      return { error: "unavailable", detail: `rpc transport: ${String(e)}` };
    }
    onMeta?.(res.headers); // x-comms-agent: per-request identity from the token ROW (§5)
    let body: any;
    try { body = await res.json(); } catch { return { error: "internal", detail: `rpc: non-JSON response (HTTP ${res.status})` }; }
    if (body.error) {
      const d = body.error.data ?? {};
      const variant = (d.busError ?? wireToVariant(body.error.code)) as BusErrorCode;
      return { error: variant, detail: String(d.detail ?? body.error.message ?? variant), ...(d.resync !== undefined || d.epoch !== undefined || d.floor !== undefined ? { data: d } : {}) } as Res<never>;
    }
    return { value: body.result as T };
  }

  resolve(cred: Cred): Res<Session> | Promise<Res<Session>> {
    if (!("token" in cred)) return { error: "unauthorized", detail: "RpcBus speaks bearer only" };
    // verify over the wire: any authed method resolves iff the token row
    // verifies (channels = ungated read). Callers must await (LocalBus
    // resolves synchronously; the contract suite awaits both uniformly).
    const sess = this.session({ token: cred.token });
    return this.call("channels", {}, cred.token).then((r) =>
      r.error ? (r as Res<Session>) : { value: sess });
  }
  session(cred?: Cred): Session {
    const token = cred && "token" in cred ? cred.token : this.token;
    if (!token) throw new Error("RpcBus session requires a token");
    return makeSession(this, token);
  }
  close(): void {} // stateless
}

function wireToVariant(code: number): BusErrorCode {
  // fallback when a server predates data.busError (never within this repo's
  // contract pair — kept so an old server degrades to a sane variant)
  return code === -32001 ? "unauthorized" : code === -32002 ? "forbidden" : code === -32003 ? "not_found"
    : code === -32004 ? "rate_limited" : code === -32005 ? "conflict" : code === -32006 ? "contention"
    : code === -32602 ? "usage" : "internal";
}

type MutableSession = { -readonly [K in keyof Session]: Session[K] };

function makeSession(bus: RpcBus, token: string): Session {
  // identity arrives on EVERY response (x-comms-agent, from the token ROW):
  // rename rewrites tokens.agent_id, so a client-cached identity would go
  // stale (§5). agentId is "" until the first round-trip completes.
  const meta = (h: Headers) => { const a = h.get("x-comms-agent"); if (a) sess.agentId = a; };
  const c = <T>(m: string, p?: Record<string, unknown>) => bus.call<T>(m, p ?? {}, token, meta);
  const sess: MutableSession = {
    agentId: "",
    joinAgent: (p) => c("join", p as Record<string, unknown>),
    listAgents: (activeOnly) => c("who", { all: !activeOnly }),
    post: (p) => c("post", { to: csvSplit(p.to), ...strip(p as Record<string, unknown>, "to") }),
    inbox: (p) => c("inbox", p as Record<string, unknown>),
    read: (p) => c("read", p as Record<string, unknown>),
    threadOf: (id) => c("thread", { id }),
    receipts: (id) => c("receipts", { id }),
    setStatus: (p) => c("status", p as Record<string, unknown>),
    channels: () => c("channels"),
    rename: (p) => c("rename", p as Record<string, unknown>),
    history: (p) => c("history", p as Record<string, unknown>),
    waitStep: (p) => c("inbox.wait", p as Record<string, unknown>),
    cursorGet: (p) => c("cursor.get", p as Record<string, unknown>),
    cursorSet: (p) => c("cursor.set", p as Record<string, unknown>),
    tokenCreate: (p) => c("token.create", p as Record<string, unknown>),
    tokenList: () => c("token.list"),
    tokenRevoke: (p) => c("token.revoke", p as Record<string, unknown>),
    groupCreate: (p) => c("group.create", p as Record<string, unknown>),
    groupJoin: (p) => c("group.join", p as Record<string, unknown>),
    groupLeave: (p) => c("group.leave", p as Record<string, unknown>),
    groupDelete: (p) => c("group.delete", p as Record<string, unknown>),
    groupList: () => c("group.list"),
    groupShow: (p) => c("group.show", p as Record<string, unknown>),
  };
  return sess;
}
const csvSplit = (to: string) => (to ? to.split(",").map((s) => s.trim()).filter(Boolean) : []);
function strip(p: Record<string, unknown>, key: string) {
  const { [key]: _drop, ...rest } = p;
  return rest;
}
