/**
 * interface Bus — the seam that keeps dual mode honest (RFC-001 §3).
 * LocalBus wraps the sync core; RpcBus (M2) speaks HTTP. ONE contract suite
 * (tests/contract.ts) runs against every implementation in CI.
 */
import { openBus, localCtx, type Bus, type Ctx, type MsgRow, type Receipts, type AgentRow, type Res, type Scope } from "./bus.ts";

export interface BusInterface {
  joinAgent(ctx: Ctx, p: { agent: string; role: string; caps?: string; fingerprint?: string | null }): Promise<Res<{ agent: AgentRow; active: AgentRow[]; unresolved: number }>>;
  listAgents(activeOnly: boolean): Promise<AgentRow[]>;
  post(ctx: Ctx, p: { from: string; to: string; type: string; subject?: string; body: string; thread?: string | null; re?: string | null; tags?: string; channel?: string | null; as?: string | null; idempotencyKey?: string | null }): Promise<Res<{ id: string; channel: string; thread: string; file: string }>>;
  inbox(ctx: Ctx, p: { agent: string; open?: boolean; unread?: boolean; channel?: string | null }): Promise<Res<{ rows: MsgRow[]; unreadIds: string[] }>>;
  read(ctx: Ctx, p: { agent: string; id: string }): Promise<Res<MsgRow & { receipts: Receipts }>>;
  threadOf(ctx: Ctx, id: string): Promise<Res<{ rows: MsgRow[]; receipts: Receipts[] }>>;
  receipts(ctx: Ctx, id: string): Promise<Res<MsgRow & { receipts: Receipts }>>;
  setStatus(ctx: Ctx, p: { agent: string; id: string; state: string }): Promise<Res<{ id: string; status: string }>>;
  channels(ctx: Ctx): Promise<{ name: string; n: number; last: string | null; purpose: string | null }[]>;
  rename(ctx: Ctx, p: { agent: string; to: string; fingerprint?: string | null }): Promise<Res<{ announced: MsgRow }>>;
  history(ctx: Ctx, p: { channel?: string | null; limit?: number }): Promise<Res<{ rows: MsgRow[]; cursor: { epoch: string; seq: number } }>>;
  inboxWait(ctx: Ctx, p: { for?: string; consumer?: string; since?: string; timeout?: number }): Promise<Res<{ messages: MsgRow[]; cursor: string }>>;
  cursorGet(ctx: Ctx, p: { consumer?: string }): Promise<{ epoch: string; seq: number }>;
  cursorSet(ctx: Ctx, p: { consumer: string; cursor: string; force?: boolean }): Promise<Res<null>>;
  /** admin/test primitive: token.create mints the agent row + a token, then join sets role.
   *  RpcBus implements it with token.create + join under the minted token. */
  seedAgent(root: Ctx, id: string, role: string, scopes?: Scope[]): Promise<Res<Ctx>>;
  close(): Promise<void>;
}

export class LocalBus implements BusInterface {
  constructor(private bus: Bus) {}
  static open(opts: Parameters<typeof openBus>[0]): LocalBus { return new LocalBus(openBus(opts)); }

  async joinAgent(ctx: Ctx, p: any) { return this.bus.joinAgent(ctx, p); }
  async listAgents(activeOnly: boolean) { return this.bus.listAgents(activeOnly); }
  async post(ctx: Ctx, p: any) {
    const r = await this.bus.post(ctx, p);
    return r.error ? r : { value: { ...r.value } };
  }
  async inbox(ctx: Ctx, p: any) {
    const r = this.bus.inbox(ctx, p);
    return r.error ? r : { value: { rows: r.value.rows, unreadIds: [...r.value.unreadIds] } };
  }
  async read(ctx: Ctx, p: any) { return this.bus.read(ctx, p); }
  async threadOf(ctx: Ctx, id: string) { return this.bus.threadOf(ctx, id); }
  async receipts(ctx: Ctx, id: string) { return this.bus.receipts(ctx, id); }
  async setStatus(ctx: Ctx, p: any) { return this.bus.setStatus(ctx, p); }
  async channels(ctx: Ctx) { return this.bus.channels(ctx); }
  async rename(ctx: Ctx, p: any) { return this.bus.rename(ctx, p); }
  async history(ctx: Ctx, p: any) { return this.bus.history(ctx, p); }
  async inboxWait(ctx: Ctx, p: { for?: string; consumer?: string; since?: string; timeout?: number }) {
    // local impl: resolve cursor, poll until messages or timeout (seams-free fast path)
    const consumer = p.consumer ?? "default";
    const target = p.for ?? ctx.actor;
    const cur = p.since ? parseCursor(p.since) : this.bus.cursorGet(ctx.principal.agentId, consumer);
    const deadline = Date.now() + Math.min(p.timeout ?? 30, 60) * 1000;
    for (;;) {
      const last = this.bus.tailEvents(cur.seq, 500);
      const msgs: MsgRow[] = [];
      let seq = cur.seq;
      for (const e of last) {
        seq = e.seq;
        if (e.kind !== "msg" || !e.msg_id) continue;
        const m = this.bus.db.query("SELECT * FROM messages WHERE id=?").get(e.msg_id) as MsgRow | null;
        if (m && m.sender !== target && this.bus.recipientsMatch(m.recipients, target, this.bus.roleOf(target)))
          msgs.push(m);
      }
      if (msgs.length || Date.now() >= deadline)
        return { value: { messages: msgs, cursor: `${cur.epoch}.${seq}` } };
      await Bun.sleep(250);
    }
  }
  async cursorGet(ctx: Ctx, p: { consumer?: string }) {
    return this.bus.cursorGet(ctx.principal.agentId, p.consumer ?? "default");
  }
  async cursorSet(ctx: Ctx, p: { consumer: string; cursor: string; force?: boolean }) {
    const { epoch, seq } = parseCursor(p.cursor);
    return this.bus.cursorSet(ctx.principal.agentId, p.consumer, epoch, seq, p.force);
  }
  async seedAgent(root: Ctx, id: string, role: string, scopes: Scope[] = []) {
    const tc = this.bus.tokenCreate({ agent: id, scopes });
    if (tc.error) return tc;
    const v = this.bus.tokenVerify(tc.value.token);
    if (v.error) return v;
    const ctx: Ctx = { principal: { agentId: v.value.agentId, kind: v.value.kind as any, scopes: v.value.scopes }, actor: v.value.agentId };
    const j = this.bus.joinAgent(ctx, { agent: id, role });
    if (j.error) return j;
    return { value: ctx };
  }
  async close() { this.bus.close(); }
  /** escape hatch for local-only admin ops (bootstrap, backup) */
  get raw(): Bus { return this.bus; }
}

function parseCursor(c: string): { epoch: string; seq: number } {
  const i = c.lastIndexOf(".");
  return { epoch: c.slice(0, i), seq: Number(c.slice(i + 1)) };
}

export { localCtx };
export type { Ctx, Scope };
