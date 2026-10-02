/**
 * interface Bus — the seam that keeps dual mode honest (RFC-001 §3).
 *
 * A BusHandle hands out Sessions bound to a credential (finding 10a: the
 * interface is transport-implementable — RpcBus holds nothing but {url, token}).
 * wrapSession(core, ctx) is the SAME adapter the M2 server uses after token
 * verification: one code path, zero drift. Methods are async so RpcBus
 * implements the identical interface over fetch; LocalBus resolves immediately.
 * EVERY method returns Res (finding 10b: transports must represent failures).
 * ONE contract suite (tests/contract.suite.ts) runs against every impl.
 */
import { openBus, localCtx, serverCtx, type Bus, type Ctx, type Mode, type MsgRow, type Receipts, type AgentRow, type Res, type Scope, type Cred } from "./bus.ts";

export interface Session {
  readonly agentId: string;
  joinAgent(p: { agent: string; role: string; caps?: string; fingerprint?: string | null }): Promise<Res<{ agent: AgentRow; active: AgentRow[]; unresolved: number }>>;
  listAgents(activeOnly: boolean): Promise<Res<AgentRow[]>>;
  post(p: { from: string; to: string; type: string; subject?: string; body: string; thread?: string | null; re?: string | null; tags?: string; channel?: string | null; as?: string | null; idempotencyKey?: string | null; dm?: string | null }): Promise<Res<{ id: string; channel: string; thread: string; file: string }>>;
  inbox(p: { agent: string; open?: boolean; unread?: boolean; channel?: string | null; mark?: boolean; noAll?: boolean }): Promise<Res<{ rows: MsgRow[]; unreadIds: string[] }>>;
  read(p: { agent: string; id: string }): Promise<Res<MsgRow & { receipts: Receipts }>>;
  threadOf(id: string): Promise<Res<{ rows: MsgRow[]; receipts: Receipts[] }>>;
  receipts(id: string): Promise<Res<MsgRow & { receipts: Receipts }>>;
  setStatus(p: { agent: string; id: string; state: string }): Promise<Res<{ id: string; status: string }>>;
  channels(): Promise<Res<{ name: string; n: number; last: string | null; purpose: string | null }[]>>;
  rename(p: { agent: string; to: string; fingerprint?: string | null }): Promise<Res<{ announced: MsgRow }>>;
  history(p: { channel?: string | null; limit?: number; since?: string }): Promise<Res<{ rows: MsgRow[]; hasMore: boolean; cursor: string }>>;
  waitStep(p: { for?: string; consumer?: string; since?: string; noAll?: boolean }): Promise<Res<{ messages: MsgRow[]; cursor: string; done: boolean }>>;
  cursorGet(p: { consumer?: string }): Promise<Res<{ epoch: string; seq: number }>>;
  cursorSet(p: { consumer: string; cursor: string; force?: boolean }): Promise<Res<null>>;
  tokenCreate(p: { agent: string; kind?: "agent" | "human"; label?: string; scopes?: Scope[]; admin?: boolean; force?: boolean }): Promise<Res<{ id: number; token: string; prefix: string; agentId: string; scopes: string }>>;
  tokenList(): Promise<Res<{ tokens: { id: number; agentId: string; kind: string; prefix: string; scopes: Scope[]; created_at: string; last_used: string; revoked_at: string | null }[] }>>;
  tokenRevoke(p: { id: number }): Promise<Res<{ revoked: boolean }>>;
  groupCreate(p: { name: string; agent?: string }): Promise<Res<{ name: string; created: boolean }>>;
  channelCreate(p: { name: string; purpose?: string }): Promise<Res<{ name: string; created: boolean }>>;
  groupJoin(p: { name: string; agent?: string }): Promise<Res<{ name: string; members: string[] }>>;
  groupLeave(p: { name: string; agent?: string }): Promise<Res<{ name: string; left: boolean }>>;
  groupDelete(p: { name: string }): Promise<Res<{ name: string; deleted: boolean }>>;
  groupList(): Promise<Res<{ groups: { name: string; created_by: string; created_at: string; members: number; mine: boolean }[] }>>;
  groupShow(p: { name: string }): Promise<Res<{ name: string; created_by: string; created_at: string; members: string[] }>>;
  dmMembers(channel: string): Promise<Res<string[]>>;
}

export interface BusHandle {
  readonly mode: Mode;
  /** cred binds the session to a credential. Local handle: no cred ⇒ root
   *  (the CLI is its own authority); {token} ⇒ same resolution as the server. */
  session(cred?: Cred): Session;
  /** typed credential resolution for transports (finding 10c): a bad token is
   *  a Res error (→ -32001), not an exception. Transports without a local DB
   *  (RpcBus) must verify over the wire ⇒ callers AWAIT (await on a sync Res
   *  is a no-op; the contract suite awaits uniformly). */
  resolve(cred: Cred): Res<Session> | Promise<Res<Session>>;
  close(): void;
}

/** Wrap an already-resolved ctx over the core as a Session — used by LocalBus
 *  AND by the M2 server post-token-verification (one adapter, no drift).
 *  The `as never` casts only satisfy tsc's deferred conditional when M is
 *  generic (Bus<M> methods take Ctx<M>); the ctx reaching here is always the
 *  mode-correct one (serverCtx/localCtx constructors + ServerOnly param types). */
export function wrapSession<B extends { readonly mode: Mode }>(bus: B, ctx: [B["mode"]] extends ["local"] ? Ctx<"local"> : Ctx<"server">): Session {
  return wrapSessionImpl(bus as unknown as Bus<Mode>, ctx as Ctx<Mode>);
}
function wrapSessionImpl<M extends Mode>(bus: Bus<M>, ctx: Ctx<M>): Session {
  const a = <T>(r: Res<T>) => Promise.resolve(r);
  const c = ctx as never;
  return {
    agentId: ctx.principal.agentId,
    joinAgent: (p) => a(bus.joinAgent(c, p)),
    listAgents: (activeOnly) => a({ value: bus.listAgents(activeOnly) }),
    post: (p) => a(bus.post(c, p)),
    inbox: (p) => {
      const r = bus.inbox(c, p);
      return Promise.resolve(r.error ? r : { value: { rows: r.value.rows, unreadIds: [...r.value.unreadIds] } });
    },
    read: (p) => a(bus.read(c, p)),
    threadOf: (id) => a(bus.threadOf(c, id)),
    receipts: (id) => a(bus.receipts(c, id)),
    setStatus: (p) => a(bus.setStatus(c, p)),
    channels: () => a(bus.channels(c)),
    rename: (p) => a(bus.rename(c, p)),
    history: (p) => a(bus.history(c, p)),
    waitStep: (p) => a(bus.waitStep(c, p)),
    cursorGet: (p) => a(bus.cursorGet(ctx.principal.agentId, p.consumer ?? "default")),
    cursorSet: (p) => {
      const m = /^([0-9a-f]{8,64})\.(\d+)$/.exec(p.cursor);
      if (!m) return Promise.resolve({ error: "usage" as const, detail: "cursor must be <epoch>.<seq>" });
      return a(bus.cursorSet(ctx.principal.agentId, p.consumer, m[1], Number(m[2]), p.force));
    },
    tokenCreate: (p) => a(bus.tokenCreate(c, p)),
    tokenList: () => a(bus.tokenList(c)),
    tokenRevoke: (p) => a(bus.tokenRevoke(c, p)),
    groupCreate: (p) => a(bus.groupCreate(c, p)),
    channelCreate: (p) => a(bus.channelCreate(c, p)),
    groupJoin: (p) => a(bus.groupJoin(c, p)),
    groupLeave: (p) => a(bus.groupLeave(c, p)),
    groupDelete: (p) => a(bus.groupDelete(c, p)),
    groupList: () => a(bus.groupList(c)),
    groupShow: (p) => a(bus.groupShow(c, p)),
    dmMembers: (channel) => a(bus.dmMembersFor(c, channel)),
  };
}

/** Local-mode handle: session() = root (CLI is its own authority);
 *  session({token}) = verified token — same resolution path as the server. */
export class LocalBus implements BusHandle {
  private constructor(private core: Bus<"local">) {}
  static open(opts: { home: string; seams?: Parameters<typeof openBus>[0]["seams"] }): LocalBus {
    return new LocalBus(openBus({ ...opts, mode: "local" }));
  }
  readonly mode = "local" as const;
  resolve(cred: Cred): Res<Session> {
    if (!("token" in cred)) return { error: "unauthorized", detail: "session cookie not supported on a local bus" };
    const v = this.core.tokenVerify(cred.token);
    if (v.error) return v;
    return { value: wrapSession(this.core, serverCtx(v.value.agentId, v.value.scopes, v.value.kind, cred)) };
  }
  session(cred?: Cred): Session {
    if (!cred) return wrapSession(this.core, localCtx("local"));
    const r: Res<Session> = this.resolve(cred); // LocalBus.resolve is sync — narrowed by annotation
    if (r.error) throw new Error(r.detail);
    return r.value;
  }
  close() { this.core.close(); }
  /** test/admin escape hatch (bootstrap, backup) — local handles only */
  get raw(): Bus<"local"> { return this.core; }
}

/** Server-mode handle over the core: every session is credential-bound and
 *  resolved via tokenVerify on EVERY request (finding: rename rewrites
 *  tokens.agent_id, so a cached principal would go stale). */
export function serverHandle(core: Bus<"server">): BusHandle & { raw: Bus<"server"> } {
  if (core.mode !== "server") throw new Error("serverHandle requires a server-mode core (mode discriminant, round-2 M1)");
  const resolve = (cred: Cred): Res<Session> => {
    if (!("token" in cred)) return { error: "unauthorized", detail: "server sessions require a token credential" };
    const v = core.tokenVerify(cred.token);
    if (v.error) return v;
    return { value: wrapSession(core, serverCtx(v.value.agentId, v.value.scopes, v.value.kind, cred)) };
  };
  return {
    mode: "server" as const,
    raw: core,
    resolve,
    session(cred?: Cred): Session {
      if (!cred) throw new Error("server sessions require a token credential");
      const r = resolve(cred); // local sync closure — the interface widens for RpcBus, not here
      if (r.error) throw new Error(r.detail);
      return r.value;
    },
    close(): void { core.close(); },
  };
}

/** Transport-independent admin/test primitive (§5 bootstrap path):
 *  root.token.create mints the agent row + token, then join under the MINTED
 *  credential sets the role. Returns the credential too (finding 10a). */
export async function seedAgent(handle: BusHandle, root: Session, id: string, role: string, scopes: Scope[] = []): Promise<Res<{ session: Session; token: string }>> {
  const tc = await root.tokenCreate({ agent: id, scopes });
  if (tc.error) return tc;
  const session = handle.session({ token: tc.value.token });
  const j = await session.joinAgent({ agent: id, role });
  if (j.error) return { error: j.error, detail: j.detail };
  return { value: { session, token: tc.value.token } };
}

export { localCtx, serverCtx };
export type { Ctx, Scope, Cred, Mode, Res };
