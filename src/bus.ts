/**
 * Bus core — all DB logic of agent-comms, extracted from bin/comms.ts (RFC-001 §3).
 *
 * Synchronous, typed results and typed errors. No console.log, no process.exit,
 * no stdin, no @file resolution, no ambient env reads, no process.pid.
 * Determinism flows through the injectable seams (clock/rng/pid/mirror/sleep).
 *
 * mode (local|server) is bound at openBus() and is TYPE-PARAMETRIC:
 * Bus<"server"> methods only accept Ctx<"server">, whose principal cannot carry
 * localRoot (branded); Ctx<"local"> can. Server mode ALSO rejects a localRoot
 * principal at runtime (types erase at the RPC boundary).
 *
 * Legacy schema is preserved exactly (in-place upgrade from de4ed3b DBs);
 * documented quirks are behavior-locked by tests/golden.ts.
 */
import { Database } from "bun:sqlite";
import { join } from "node:path";
import { mkdirSync } from "node:fs";
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { defaultSeams, type Seams } from "./seams.ts";

// ---------- public types ----------

export type Mode = "local" | "server";
// §4/§5: exactly the four-name scope enum. Any other token (presence:all,
// tokens:admin:human, backup, restore, channel:*) is NOT a scope in the accepted
// RFC and was removed in the round-3 audit (M8c) — presence/token admin/backup
// are governed by tokens:admin + agents:admin + the restore runbook, not scopes.
export type Scope = "read:all" | "post:as" | "tokens:admin" | "agents:admin";
export const ALL_SCOPES: readonly Scope[] = ["read:all", "post:as", "tokens:admin", "agents:admin"];
const SCOPE_SET: ReadonlySet<string> = new Set(ALL_SCOPES);
/** §4 normalizer contract: writers must never store unnormalized input. A scope
 *  is one enum name — never empty, never comma-bearing (which would smuggle a
 *  second name through normalizeScopes and verify as a scope the minter never
 *  named). Rejects `read:all,agents:admin` and `bogus` alike (M8a). */
export function validScope(s: string): s is Scope {
  return SCOPE_SET.has(s);
}

export type PrincipalBase = { agentId: string; kind: "agent" | "human"; scopes: Scope[] };
export type LocalPrincipal = PrincipalBase & { localRoot?: true };
export type ServerPrincipal = PrincipalBase & { localRoot?: undefined };
export type Principal<M extends Mode> = M extends "server" ? ServerPrincipal : LocalPrincipal;

/** Opaque transport credential (finding 10a): what a client presents; the
 *  server resolves it to a Principal via tokenVerify. Local sessions carry none. */
export type Cred = { token: string } | { session: string };

// CONCRETE per-mode ctx types — NOT one generic alias with a conditional
// member. tsc compares two instantiations of the same alias via a variance
// shortcut that WRONGLY accepts Ctx<"local"> where Ctx<"server"> is required
// (probe-verified on tsc 7.0.2); distinct named targets make the assignment
// check structural and the localRoot leak fails closed (finding B1).
export type LocalCtx = { principal: LocalPrincipal; actor: string; cred?: Cred };
export type ServerCtx = { principal: ServerPrincipal; actor: string; cred?: Cred };
export type Ctx<M extends Mode = Mode> = M extends "server" ? ServerCtx : LocalCtx;

export type BusErrorCode =
  | "usage" | "not_found" | "forbidden" | "unauthorized" | "conflict" | "gone"
  | "identity_conflict" | "rate_limited" | "unavailable" | "internal" | "resync" | "contention";
export type BusError = { error: BusErrorCode; detail: string; data?: Record<string, unknown> };
export type Ok<T> = { error?: undefined; value: T }; // n3 (round 3): the ONLY cursor representation is value.cursor (string "<epoch>.<seq>") — no parallel top-level field for RpcBus to forget.
export type Res<T> = Ok<T> | BusError;

/** §7 mappings — the shell and the server map, never invent. */
export const RPC_CODES: Record<BusErrorCode, number> = {
  usage: -32602, not_found: -32003, forbidden: -32002, unauthorized: -32001,
  conflict: -32005, gone: -32004, identity_conflict: -32005, rate_limited: -32004,
  unavailable: -32006, internal: -32603, resync: -32003, contention: -32006,
};
export const EXIT_CODES: Record<BusErrorCode, number> = {
  usage: 2, not_found: 1, forbidden: 3, unauthorized: 3, conflict: 1, gone: 1,
  identity_conflict: 3, rate_limited: 1, unavailable: 1, internal: 1, resync: 1, contention: 1,
};

export type AgentRow = {
  id: string; role: string | null; caps: string | null; pid: number | null;
  joined_at: string | null; last_seen: string | null; meta: string | null; fingerprint: string | null;
};
export type MsgRow = {
  id: string; thread: string; re: string | null; sender: string; recipients: string;
  type: string; status: string; tags: string; subject: string; body: string; file: string;
  created_at: string; updated_at: string; channel: string; meta: string | null;
};
export type Receipts = { intended: string[]; readers: { id: string; at: string | null }[]; unread: string[] };

export const MSG_TYPES = ["ack", "announce", "ask", "handoff", "note", "reply", "result", "rfc", "status"] as const;
export const STATES = ["acked", "blocked", "done", "in_progress", "open"] as const;

// identifier validation (§9) — gates WRITES only
export const ID_RE = /^[a-z0-9][a-z0-9_-]{0,31}$/;
export const TYPE_RE = /^[a-z0-9._-]{1,32}$/;
export const PRESENCE_TTL_MS = 15 * 60 * 1000;

// ---------- helpers (quirk-exact from bin/comms.ts) ----------

const mkNowIso = (s: Seams) => () => s.now().toISOString().replace(/\.\d{3}Z$/, "Z");
const mkStamp = (nowIso: () => string) => () =>
  nowIso().replace(/[-:TZ]/g, "").slice(0, 15).replace(/(\d{8})(\d{6})/, "$1T$2");
const mkShortHex = (s: Seams) => (n = 2) =>
  Array.from(s.rng(n), (b) => b.toString(16).padStart(2, "0")).join("");
export const csv = (str?: string | null) => (str ?? "").split(",").map((x) => x.trim()).filter(Boolean);

export const normalizeScopes = (scopes: Iterable<string>): string =>
  [...new Set([...scopes].map((s) => s.trim()).filter(Boolean))].sort().join(",");

export function canonicalJson(v: unknown): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v) ?? "null";
  if (Array.isArray(v)) return "[" + v.map(canonicalJson).join(",") + "]";
  return "{" + Object.keys(v).sort().map((k) => JSON.stringify(k) + ":" + canonicalJson((v as any)[k])).join(",") + "}";
}
export function sha256hex(text: string): string {
  return createHash("sha256").update(new TextEncoder().encode(text)).digest("hex");
}

/** The JS whitespace set as a SQLite trim() char-set expression (finding 5):
 *  SQLite trim() strips only U+0020 by default while JS .trim() strips the full
 *  Unicode set — the recipient split must use ALL of them to match csv() BY
 *  CONSTRUCTION. bun:sqlite (1.4.2) has no custom-function API, so the set is
 *  inlined: TAB VT FF CR SP NBSP OGHAM-SP EN-QUAD..HAIR-SP LSEP PSEP NNBSP
 *  MMSP IDEOGRAPHIC-SP ZWNBSP. */
const WSET = `char(9)||char(10)||char(11)||char(12)||char(13)||char(32)||char(160)||char(5760)||char(8192)||char(8193)||char(8194)||char(8195)||char(8196)||char(8197)||char(8198)||char(8199)||char(8200)||char(8201)||char(8202)||char(8232)||char(8233)||char(8239)||char(8287)||char(12288)||char(65279)`;
const jtrim = (x: string) => `trim(${x},${WSET})`;

/** The ONE recipient-split statement: used by the msg_ai trigger AND the
 *  one-shot backfill, so index rows follow identical semantics by CONSTRUCTION
 *  (§4 N1, finding 5). */
const SPLIT_SQL = `WITH RECURSIVE s(rest,tok) AS (
    SELECT coalesce(?,'') || ',', NULL
    UNION ALL
    SELECT substr(rest, instr(rest,',')+1), ${jtrim("substr(rest,1,instr(rest,',')-1)")} FROM s WHERE rest <> '')
  SELECT tok FROM s WHERE tok IS NOT NULL AND tok <> ''`;

export function recipientsMatch(recips: string, agent: string, role?: string | null): boolean {
  const toks = new Set(csv(recips));
  if (toks.has("@all") || toks.has(agent)) return true;
  return !!role && toks.has(role);
}

export type BusOpts = {
  home: string;
  mode: Mode;
  seams?: Seams;
  busyTimeoutMs?: number;
};

export function openBus<M extends Mode>(opts: { home: string; mode: M; seams?: Seams; busyTimeoutMs?: number }): Bus<M> {
  return openBusCore(opts.home, opts.mode, opts.seams ?? defaultSeams, opts.busyTimeoutMs) as unknown as Bus<M>;
}

type Core = ReturnType<typeof openBusCore>;
/** Server-mode surface: every ctx-taking method accepts ONLY Ctx<"server"> —
 *  a localRoot principal is unnameable here (finding B1). Signatures are
 *  spelled out explicitly: the core methods are generic over M, and inferring
 *  through them would widen the ctx to the union and let localRoot slip back
 *  in (probe-verified with tsc). */
export type Bus<M extends Mode = Mode> = M extends "server" ? Omit<Core,
  "joinAgent" | "post" | "inbox" | "read" | "threadOf" | "receipts" | "setStatus" | "channels" | "rename" | "history" | "waitStep" | "tokenCreate" | "tokenList" | "tokenRevoke" | "mode"
> & ServerOnly : Omit<Core, "mode"> & { readonly mode: "local" };
interface ServerOnly {
  // discriminant so Bus<"local"> is NOT structurally assignable to Bus<"server">
  // (H2/H3, round-2 M1): without it, core methods taking Ctx<M> are a supertype
  // and nothing else distinguishes the modes at the handle seam.
  readonly mode: "server";
  // property (arrow) syntax, NOT method syntax: strictFunctionTypes is only
  // contravariant for properties — method params are bivariant and would let
  // a Ctx<"local"> slip back in (probe-verified).
  joinAgent: (ctx: Ctx<"server">, p: { agent: string; role: string; caps?: string; fingerprint?: string | null }) => Res<{ agent: AgentRow; active: AgentRow[]; unresolved: number }>;
  post: (ctx: Ctx<"server">, p: { from: string; to: string; type: string; subject?: string; body: string; thread?: string | null; re?: string | null; tags?: string; channel?: string | null; as?: string | null; idempotencyKey?: string | null }) => Res<{ id: string; channel: string; thread: string; file: string }>;
  inbox: (ctx: Ctx<"server">, p: { agent: string; open?: boolean; unread?: boolean; channel?: string | null; mark?: boolean }) => Res<{ rows: MsgRow[]; unreadIds: Set<string> }>;
  read: (ctx: Ctx<"server">, p: { agent: string; id: string }) => Res<MsgRow & { receipts: Receipts }>;
  threadOf: (ctx: Ctx<"server">, id: string) => Res<{ rows: MsgRow[]; receipts: Receipts[] }>;
  receipts: (ctx: Ctx<"server">, id: string) => Res<MsgRow & { receipts: Receipts }>;
  setStatus: (ctx: Ctx<"server">, p: { agent: string; id: string; state: string }) => Res<{ id: string; status: string }>;
  channels: (ctx: Ctx<"server">) => Res<{ name: string; n: number; last: string | null; purpose: string | null }[]>;
  rename: (ctx: Ctx<"server">, p: { agent: string; to: string; fingerprint?: string | null }) => Res<{ announced: MsgRow }>;
  history: (ctx: Ctx<"server">, p: { channel?: string | null; limit?: number; since?: string }) => Res<{ rows: MsgRow[]; hasMore: boolean; cursor: string }>;
  waitStep: (ctx: Ctx<"server">, p: { for?: string; consumer?: string; since?: string }) => Res<{ messages: MsgRow[]; cursor: string; done: boolean }>;
  tokenCreate: (ctx: Ctx<"server">, p: { agent: string; kind?: "agent" | "human"; label?: string; scopes?: Scope[]; admin?: boolean; force?: boolean }) => Res<{ id: number; token: string; prefix: string; agentId: string; scopes: string }>;
  tokenList: (ctx: Ctx<"server">) => Res<{ tokens: { id: number; agentId: string; kind: string; prefix: string; scopes: Scope[]; created_at: string; last_used: string; revoked_at: string | null }[] }>;
  tokenRevoke: (ctx: Ctx<"server">, p: { id: number }) => Res<{ revoked: boolean }>;
}

// ---------- open / schema ----------

function openBusCore<M extends Mode>(home: string, mode: M, seams: Seams, busyTimeoutMs?: number) {
  const nowIso = mkNowIso(seams);
  const stamp = mkStamp(nowIso);
  const shortHex = mkShortHex(seams);
  const newId = (prefix: string) => `${stamp()}-${prefix}-${shortHex(2)}`;
  const DB_PATH = join(home, ".comms", "comms.db");
  const MSG_DIR = join(home, "messages");

  mkdirSync(join(home, ".comms"), { recursive: true });
  const d = new Database(DB_PATH, { create: true });
  d.exec("PRAGMA journal_mode = WAL");
  d.exec(`PRAGMA busy_timeout = ${busyTimeoutMs ?? (mode === "server" ? 150 : 5000)}`);
  if (mode === "server") d.exec("PRAGMA synchronous = NORMAL");

  d.exec(`
    CREATE TABLE IF NOT EXISTS agents(
      id TEXT PRIMARY KEY NOT NULL, role TEXT, caps TEXT, pid INTEGER,
      joined_at TEXT, last_seen TEXT, meta TEXT, fingerprint TEXT);
    CREATE TABLE IF NOT EXISTS messages(
      id TEXT PRIMARY KEY, thread TEXT, re TEXT, sender TEXT,
      recipients TEXT, type TEXT, status TEXT, tags TEXT,
      subject TEXT, body TEXT, file TEXT, created_at TEXT, updated_at TEXT,
      channel TEXT NOT NULL DEFAULT 'general');
    CREATE TABLE IF NOT EXISTS reads(
      agent TEXT, msg TEXT, read_at TEXT, PRIMARY KEY(agent, msg));
    CREATE TABLE IF NOT EXISTS channels(
      name TEXT PRIMARY KEY NOT NULL, purpose TEXT, created_at TEXT, created_by TEXT);
    CREATE INDEX IF NOT EXISTS idx_msg_thread  ON messages(thread);
    CREATE INDEX IF NOT EXISTS idx_msg_created ON messages(created_at);
    CREATE INDEX IF NOT EXISTS idx_msg_channel ON messages(channel);
  `);

  const mcols = d.query("PRAGMA table_info(messages)").all() as any[];
  if (!mcols.some((c) => c.name === "channel"))
    d.exec("ALTER TABLE messages ADD COLUMN channel TEXT NOT NULL DEFAULT 'general'");
  if (!mcols.some((c) => c.name === "meta"))
    d.exec("ALTER TABLE messages ADD COLUMN meta TEXT");
  const acols = d.query("PRAGMA table_info(agents)").all() as any[];
  if (!acols.some((c) => c.name === "fingerprint"))
    d.exec("ALTER TABLE agents ADD COLUMN fingerprint TEXT");
  if (!acols.some((c) => c.name === "kind"))
    d.exec("ALTER TABLE agents ADD COLUMN kind TEXT DEFAULT 'agent'");

  d.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_fp ON agents(fingerprint) WHERE fingerprint IS NOT NULL;
    CREATE TABLE IF NOT EXISTS tokens(
      id INTEGER PRIMARY KEY,
      prefix TEXT UNIQUE NOT NULL,
      agent_id TEXT NOT NULL,
      salt BLOB NOT NULL,
      key_hash BLOB NOT NULL,
      scopes TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL,
      last_used TEXT NOT NULL,
      revoked_at TEXT
    );
    CREATE TABLE IF NOT EXISTS events(
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      kind TEXT NOT NULL,
      msg_id TEXT, agent_id TEXT, at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS idempotency(
      agent_id TEXT NOT NULL, key TEXT NOT NULL,
      msg_id TEXT NOT NULL, req_hash TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY(agent_id, key)
    );
    CREATE TABLE IF NOT EXISTS message_recipients(msg TEXT NOT NULL, target TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS msg_rec_idx ON message_recipients(target, msg);
    CREATE TABLE IF NOT EXISTS cursors(
      agent_id TEXT NOT NULL, consumer TEXT NOT NULL DEFAULT 'default',
      epoch TEXT NOT NULL, last_seq INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(agent_id, consumer)
    );
  `);
  // dedupe key for the recipient index; legacy DBs may hold duplicates from the
  // racing-backfill bug — clean them first so the unique index can exist.
  try {
    d.exec("DELETE FROM message_recipients WHERE rowid NOT IN (SELECT MIN(rowid) FROM message_recipients GROUP BY msg,target)");
    d.exec("CREATE UNIQUE INDEX IF NOT EXISTS mr_uq ON message_recipients(msg,target)");
  } catch { /* non-fatal: index stays non-unique on pathological legacy data */ }

  // triggers (§4). Timestamps coalesced (finding 6): a NULL created_at/last_seen/
  // revoked_at from a manual or stale writer must not abort the write.
  // M6 (round 2): triggers are VERSIONED. CREATE TRIGGER IF NOT EXISTS would
  // silently keep old-generation triggers forever on existing DBs (the live
  // review bus hit exactly this). On SCHEMA_VERSION bump: DROP + recreate all
  // triggers inside BEGIN IMMEDIATE, and rebuild message_recipients (rows the
  // old trigger wrote had wrong trim semantics).
  const SCHEMA_VERSION = 2;
  const readVersion = () => Number((d.query("SELECT value FROM meta WHERE key='schema_version'").get() as any)?.value ?? 0);
  const NOW_SQL = `strftime('%Y-%m-%dT%H:%M:%SZ','now')`;
  const TRIGGER_DDL = (`
    CREATE TRIGGER IF NOT EXISTS msg_ai AFTER INSERT ON messages BEGIN
      INSERT INTO events(kind,msg_id,agent_id,at) VALUES('msg',NEW.id,NEW.sender,coalesce(NEW.created_at,${NOW_SQL}));
      INSERT OR IGNORE INTO message_recipients(msg,target)
      WITH RECURSIVE s(rest,tok) AS (
        SELECT coalesce(NEW.recipients,'') || ',', NULL
        UNION ALL
        SELECT substr(rest, instr(rest,',')+1), ${jtrim("substr(rest,1,instr(rest,',')-1)")} FROM s WHERE rest <> '')
      SELECT NEW.id, tok FROM s WHERE tok IS NOT NULL AND tok <> '';
    END;
    CREATE TRIGGER IF NOT EXISTS msg_au AFTER UPDATE OF status ON messages BEGIN
      INSERT INTO events(kind,msg_id,agent_id,at) VALUES('status',NEW.id,NEW.sender,coalesce(NEW.updated_at,${NOW_SQL}));
    END;
    CREATE TRIGGER IF NOT EXISTS reads_ai AFTER INSERT ON reads BEGIN
      INSERT INTO events(kind,msg_id,agent_id,at) VALUES('read',NEW.msg,NEW.agent,coalesce(NEW.read_at,${NOW_SQL}));
    END;
    CREATE TRIGGER IF NOT EXISTS agents_ai AFTER INSERT ON agents BEGIN
      INSERT INTO events(kind,agent_id,at) VALUES('presence',NEW.id,coalesce(NEW.last_seen,NEW.joined_at,${NOW_SQL}));
    END;
    CREATE TRIGGER IF NOT EXISTS agents_au AFTER UPDATE ON agents
    WHEN OLD.id IS NOT NEW.id OR OLD.role IS NOT NEW.role OR OLD.caps IS NOT NEW.caps
      OR (NEW.last_seen IS NOT NULL AND (OLD.last_seen IS NULL
          OR (julianday(NEW.last_seen) - julianday(OLD.last_seen)) * 86400 >= 60))
    BEGIN
      INSERT INTO events(kind,agent_id,at)
      VALUES(CASE WHEN OLD.id IS NOT NEW.id THEN 'rename' ELSE 'presence' END, NEW.id, coalesce(NEW.last_seen,${NOW_SQL}));
    END;
    CREATE TRIGGER IF NOT EXISTS tokens_ai AFTER INSERT ON tokens BEGIN
      INSERT INTO events(kind,agent_id,at) VALUES('token',NEW.agent_id,coalesce(NEW.created_at,${NOW_SQL}));
    END;
    CREATE TRIGGER IF NOT EXISTS tokens_au AFTER UPDATE OF revoked_at ON tokens
    WHEN OLD.revoked_at IS NOT NEW.revoked_at BEGIN
      -- emits on revoke AND un-revoke (audit trail is complete either way)
      INSERT INTO events(kind,agent_id,at) VALUES('token',NEW.agent_id,coalesce(NEW.revoked_at,${NOW_SQL}));
    END;
  `);
  // round-3 fix (don-claude): DROP + CREATE + recipient rebuild + version write
  // are ONE IMMEDIATE txn, version re-checked inside it, forward-only. No
  // trigger-less window for concurrent writers; an older binary never
  // downgrades a newer DB's triggers.
  if (readVersion() < SCHEMA_VERSION || !d.query("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='msg_ai'").get()) {
    d.exec("BEGIN IMMEDIATE");
    try {
      const v = readVersion();
      if (v > SCHEMA_VERSION) { /* newer binary already upgraded: leave its triggers */ }
      else if (v < SCHEMA_VERSION) {
        for (const trg of ["msg_ai", "msg_au", "reads_ai", "agents_ai", "agents_au", "tokens_ai", "tokens_au"])
          d.exec(`DROP TRIGGER IF EXISTS ${trg}`);
        d.exec(TRIGGER_DDL);
        d.exec("DELETE FROM message_recipients");
        const splitStmt = d.query(SPLIT_SQL);
        for (const r of d.query("SELECT id, recipients FROM messages").all() as any[])
          for (const t of splitStmt.all(String(r.recipients ?? "")) as any[])
            d.run("INSERT OR IGNORE INTO message_recipients(msg,target) VALUES(?,?)", [r.id, t.tok]);
        d.run("INSERT OR REPLACE INTO meta(key,value) VALUES('backfill_recipients',?)", [nowIso()]);
        d.run("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version',?)", [String(SCHEMA_VERSION)]);
      } else d.exec(TRIGGER_DDL); // v == current but triggers missing (fresh DB race) — IF NOT EXISTS
      d.exec("COMMIT");
    } catch (e) { d.exec("ROLLBACK"); throw e; }
  }
  // N2 (round 3): no events seed — history snapshot pages over MESSAGES (§6),
  // so pre-events legacy rows and gc'd-event rows stay visible without
  // fabricating seqs.

  d.run(
    "INSERT OR IGNORE INTO channels(name,purpose,created_at,created_by) VALUES('general','tooling / meta / cross-project chatter','',?)",
    ["system"],
  );

  // epoch: rotate ONLY on restore (runbook), never here (§3 N3)
  if (!d.query("SELECT value FROM meta WHERE key='epoch'").get())
    d.run("INSERT INTO meta(key,value) VALUES('epoch',?)", [hex(seams.rng(16))]);

  // one-shot backfill: marker re-checked INSIDE the txn (finding 5), and the
  // split runs through the SAME SPLIT_SQL statement as the trigger (no JS trim).
  if (!d.query("SELECT value FROM meta WHERE key='backfill_recipients'").get()) {
    d.exec("BEGIN IMMEDIATE");
    try {
      if (!d.query("SELECT value FROM meta WHERE key='backfill_recipients'").get()) {
        const splitStmt = d.query(SPLIT_SQL);
        for (const r of d.query("SELECT id, recipients FROM messages").all() as any[])
          for (const t of splitStmt.all(String(r.recipients ?? "")) as any[])
            d.run("INSERT OR IGNORE INTO message_recipients(msg,target) VALUES(?,?)", [r.id, t.tok]);
        d.run("INSERT OR REPLACE INTO meta(key,value) VALUES('backfill_recipients',?)", [nowIso()]);
      }
      d.exec("COMMIT");
    } catch (e) { d.exec("ROLLBACK"); throw e; }
  }

  // ---------- internals ----------

  const isActive = (lastSeen: string | null) => {
    const t = lastSeen ? Date.parse(lastSeen) : NaN;
    return Number.isFinite(t) && seams.now().getTime() - t <= PRESENCE_TTL_MS;
  };

  function touch(agent: string) {
    if (!agent) return;
    const t = nowIso();
    const r = d.run("UPDATE agents SET last_seen=? WHERE id=?", [t, agent]);
    if (r.changes === 0 && mode === "local") {
      d.run(
        "INSERT OR IGNORE INTO agents(id,role,caps,pid,joined_at,last_seen,meta) VALUES(?,?,?,?,?,?,?)",
        [agent, agent, "", seams.pid(), t, t, "{}"],
      );
    }
  }

  function ensureChannel(name: string, by: string) {
    d.run("INSERT OR IGNORE INTO channels(name,purpose,created_at,created_by) VALUES(?,?,?,?)", [name, "", nowIso(), by]);
  }

  const roleOf = (agent: string): string | null =>
    (d.query("SELECT role FROM agents WHERE id=?").get(agent) as any)?.role ?? null;

  function receiptsForMsg(msg: MsgRow): Receipts {
    const agents = d.query("SELECT id, role FROM agents WHERE id IS NOT NULL").all() as any[];
    const intended = agents
      .filter((a) => a.id !== msg.sender && recipientsMatch(msg.recipients, a.id, a.role))
      .map((a) => a.id);
    const readMap = new Map<string, string | null>();
    for (const r of d.query("SELECT agent, read_at FROM reads WHERE msg=?").all(msg.id) as any[])
      readMap.set(r.agent, r.read_at);
    for (const r of d.query("SELECT DISTINCT sender FROM messages WHERE re=?").all(msg.id) as any[])
      if (!readMap.has(r.sender)) readMap.set(r.sender, null);
    return {
      intended,
      readers: intended.filter((id) => readMap.has(id)).map((id) => ({ id, at: readMap.get(id) ?? null })),
      unread: intended.filter((id) => !readMap.has(id)),
    };
  }

  function renderMd(m: MsgRow): string {
    const fm: Record<string, unknown> = {
      id: m.id, channel: m.channel ?? "general", thread: m.thread, re: m.re, from: m.sender,
      to: csv(m.recipients), type: m.type, status: m.status,
      tags: csv(m.tags), created_at: m.created_at,
    };
    const as = m.meta ? (JSON.parse(m.meta)?.as ?? null) : null;
    if (as) fm.as = as;
    return `---\n${JSON.stringify(fm, null, 2)}\n---\n\n# ${m.subject || m.type}\n\n${m.body}\n`;
  }

  // ---------- §5 guards (finding 1: runtime backstop for erased types) ----------

  type AnyCtx = { principal: PrincipalBase & { localRoot?: boolean }; actor: string };
  function ctxCheck(ctx: AnyCtx): BusError | null {
    if (mode === "server" && (ctx.principal as any).localRoot === true)
      return { error: "internal", detail: "local-root principal in server mode (types erased at RPC boundary)" };
    return null;
  }
  const isRootCtx = (ctx: AnyCtx) => mode === "local" && (ctx.principal as any).localRoot === true;
  const hasScope = (ctx: AnyCtx, s: Scope) =>
    isRootCtx(ctx) || (ctx.principal.scopes as Scope[]).includes(s);

  // ---------- commands ----------

  function joinAgent(ctx: Ctx<M>, p: { agent: string; role: string; caps?: string; fingerprint?: string | null }): Res<{ agent: AgentRow; active: AgentRow[]; unresolved: number }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!p.agent || !p.role) return { error: "usage", detail: "error: join requires --agent and --role" };
    if (!ID_RE.test(p.agent)) return { error: "usage", detail: `invalid agent id: ${p.agent}` };
    const t = nowIso();
    if (mode === "server") {
      // finding 12: assertion id, UPDATE-only (no auto-register), fingerprint IGNORED.
      if (p.agent !== ctx.principal.agentId)
        return { error: "forbidden", detail: `agent assertion '${p.agent}' != principal '${ctx.principal.agentId}'` };
      const r = d.run("UPDATE agents SET role=?, caps=?, last_seen=? WHERE id=?", [p.role, p.caps ?? "", t, p.agent]);
      if (r.changes === 0)
        return { error: "not_found", detail: `unknown agent '${p.agent}' — rows are minted by token.create on the server` };
    } else {
      const fp = p.fingerprint ?? null;
      if (fp) {
        const byFp = d.query("SELECT id FROM agents WHERE fingerprint=?").get(fp) as any;
        if (byFp && byFp.id !== p.agent)
          return { error: "identity_conflict", detail:
            `error: this runtime already joined as '${byFp.id}'. One id per agent.\n` +
            `  Reconnect as yourself:  --agent ${byFp.id}\n` +
            `  Or change your name (announces it to @all):  bun comms.ts rename --agent ${byFp.id} --to ${p.agent} --fingerprint <fp>` };
        const byId = d.query("SELECT fingerprint FROM agents WHERE id=?").get(p.agent) as any;
        if (byId && byId.fingerprint && byId.fingerprint !== fp)
          return { error: "identity_conflict", detail: `error: id '${p.agent}' is already claimed by another runtime. Pick a different id, or coordinate a rename.` };
      }
      d.run(
        `INSERT INTO agents(id,role,caps,pid,joined_at,last_seen,meta,fingerprint,kind) VALUES(?,?,?,?,?,?,?,?,?)
         ON CONFLICT(id) DO UPDATE SET role=excluded.role, caps=excluded.caps, pid=excluded.pid, last_seen=excluded.last_seen,
           fingerprint=COALESCE(excluded.fingerprint, agents.fingerprint)`,
        [p.agent, p.role, p.caps ?? "", seams.pid(), t, t, "{}", fp,
         (d.query("SELECT kind FROM agents WHERE id=?").get(p.agent) as any)?.kind ?? "agent"],
      );
    }
    const row = d.query("SELECT * FROM agents WHERE id=?").get(p.agent) as AgentRow;
    const unresolved = (d.query(
      "SELECT COUNT(*) c FROM messages WHERE status IN ('open','acked','in_progress') AND sender!=?",
    ).get(p.agent) as any).c;
    return { value: { agent: row, active: listAgents(true), unresolved } };
  }

  function listAgents(activeOnly: boolean): AgentRow[] {
    return (d.query("SELECT * FROM agents ORDER BY last_seen DESC, rowid ASC").all() as AgentRow[])
      .filter((r) => !!r.id)
      .filter((r) => (activeOnly ? isActive(r.last_seen) : true));
  }

  function post(ctx: Ctx<M>, p: {
    from: string; to: string; type: string; subject?: string; body: string;
    thread?: string | null; re?: string | null; tags?: string; channel?: string | null;
    as?: string | null; idempotencyKey?: string | null;
  }): Res<{ id: string; channel: string; thread: string; file: string }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const rootCtx = isRootCtx(ctx);
    if (!p.from || !p.to) return { error: "usage", detail: "error: post requires --from and --to" };

    // QUIRK restored (finding 14): legacy touched the sender BEFORE validating
    // type/channel/etc, so a rejected post still registered the agent. Server
    // mode never auto-registers (touch is UPDATE-only there).
    if (mode === "local") touch(p.from);

    if (p.as !== null && p.as !== undefined && !ID_RE.test(p.as))
      return { error: "usage", detail: `invalid as: ${p.as}` }; // finding 3 (B3): before any path join
    if (!ID_RE.test(p.from)) return { error: "usage", detail: `invalid from id: ${p.from}` };
    if (!MSG_TYPES.includes(p.type as any)) return { error: "usage", detail: `error: --type must be one of ${[...MSG_TYPES].sort()}` };
    if (!TYPE_RE.test(p.type)) return { error: "usage", detail: `invalid type: ${p.type}` };
    if (p.channel !== undefined && p.channel !== null && !ID_RE.test(p.channel))
      return { error: "usage", detail: `invalid channel: ${p.channel}` };

    if (mode === "server" || !rootCtx) {
      // finding 2 (B2): `from` is ALWAYS an assertion — even with post:as.
      // post:as governs `as` only. sender = as-target ?? principal.
      if (p.from !== ctx.principal.agentId)
        return { error: "forbidden", detail: `from '${p.from}' must equal authenticated principal` };
      if (p.as && !ctx.principal.scopes.includes("post:as"))
        return { error: "forbidden", detail: "as requires post:as scope" };
    }

    const t = nowIso();
    let idem: { key: string; hash: string } | null = null;
    if ((mode === "server" || !rootCtx) && p.idempotencyKey) { // finding 1: mode-gated, not localRoot-gated
      const key = String(p.idempotencyKey);
      if (key.length > 128) return { error: "usage", detail: "idempotency key > 128 bytes" };
      const hash = sha256hex(canonicalJson({
        body: p.body, to: csv(p.to).sort(), channel: p.channel ?? null, type: p.type,
        subject: p.subject ?? "", thread: p.thread ?? null, re: p.re ?? null,
        tags: p.tags ?? "", as: p.as ?? null,
      }));
      idem = { key, hash };
      const prev = d.query("SELECT msg_id, req_hash FROM idempotency WHERE agent_id=? AND key=?")
        .get(ctx.principal.agentId, key) as any;
      if (prev) {
        if (prev.req_hash !== hash) return { error: "conflict", detail: "idempotency key reused with different params" };
        const m = d.query("SELECT id, thread, channel, file FROM messages WHERE id=?").get(prev.msg_id) as any;
        return m ? { value: m } : { error: "conflict", detail: "idempotency row points at missing message" };
      }
    }

    let channel: string | null = p.channel ?? null;
    if (!channel) {
      const parentId = p.thread || p.re;
      if (parentId) {
        const par = d.query("SELECT channel FROM messages WHERE id=? OR thread=? ORDER BY created_at ASC LIMIT 1")
          .get(parentId, parentId) as any;
        channel = par?.channel ?? null;
      }
    }
    // m3 (round 2): server rejects dangling --re BEFORE ensureChannel, so a
    // rejected post leaves no orphan channel row (X3/N-h).
    if (p.re && mode === "server") {
      const reRow = d.query("SELECT id FROM messages WHERE id=?").get(p.re);
      if (!reRow) return { error: "not_found", detail: `error: re -> unknown message id '${p.re}'` };
    }
    channel = channel || "general";
    ensureChannel(channel, p.from);

    const sender = rootCtx ? (p.as ?? p.from) : (p.as || ctx.principal.agentId); // §5: sender = as-target
    const asAudit = p.as ? ctx.principal.agentId : null;                        // N4: meta.as = principal
    const mid = newId(sender.split("-")[0]);
    // M5 (round 2): thread is DERIVED from the final id — recomputed on every
    // collision retry; an explicit p.thread always wins and never retargets.
    let thread = p.thread || mid;
    let fname = `msg-${stamp()}-${sender}-${p.type}-${mid.split("-").pop()}.md`;
    const m: MsgRow = {
      id: mid, thread, re: p.re ?? null, sender, recipients: p.to,
      type: p.type, status: "open", tags: p.tags ?? "", subject: p.subject ?? "",
      body: p.body, file: "", created_at: t, updated_at: t, channel,
      meta: asAudit ? JSON.stringify({ as: asAudit }) : null,
    };
    // QUIRK (legacy §10): a dangling --re inserts silently in local mode;
    // server mode rejects it ABOVE (m3) before any channel row is written.

    // insert-first (§9): DB txn commits (message + idempotency + PK-collision
    // retry), THEN mirror write outside the txn.
    try {
      d.exec("BEGIN IMMEDIATE");
      try {
        let id = mid, tries = 0;
        for (;;) {
          try {
            d.run(
              `INSERT INTO messages(id,thread,re,sender,recipients,type,status,tags,subject,body,file,created_at,updated_at,channel,meta)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
              [id, thread, m.re, sender, p.to, p.type, "open", m.tags, m.subject, p.body, "", t, t, channel,
               asAudit ? JSON.stringify({ as: asAudit }) : null],
            );
            m.id = id;
            m.thread = thread;
            break;
          } catch (e: any) {
            if (!String(e?.message ?? e).includes("UNIQUE") || ++tries >= 8) throw e;
            id = newId(sender.split("-")[0]);
            fname = `msg-${stamp()}-${sender}-${p.type}-${id.split("-").pop()}.md`; // finding 13: fname follows id
            if (!p.thread) thread = id; // M5 (round 2): derived thread follows the new id
          }
        }
        if (idem) {
          d.run("INSERT INTO idempotency(agent_id,key,msg_id,req_hash,created_at) VALUES(?,?,?,?,?)",
            [ctx.principal.agentId, idem.key, m.id, idem.hash, t]);
        }
        d.exec("COMMIT");
      } catch (e) { d.exec("ROLLBACK"); throw e; }
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      if (msg.includes("UNIQUE") && idem) return { error: "conflict", detail: "idempotency key race (single-writer rule; retry)" };
      if (msg.includes("SQLITE_BUSY") || msg.includes("database is locked")) return { error: "contention", detail: "busy after retries" };
      return { error: "internal", detail: `post: ${msg}` }; // m6 (round 2): never raw-throw over Res
    }

    const content = renderMd(m);
    seams.mirror(join(MSG_DIR, channel), fname, content);
    const file = join("messages", channel, fname);
    d.run("UPDATE messages SET file=? WHERE id=?", [file, m.id]);
    m.file = file;
    return { value: { id: m.id, channel, thread, file } };
  }

  function inbox(ctx: Ctx<M>, p: { agent: string; open?: boolean; unread?: boolean; channel?: string | null; mark?: boolean }): Res<{ rows: MsgRow[]; unreadIds: Set<string> }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const rootCtx = isRootCtx(ctx);
    if ((mode === "server" || !rootCtx) && p.agent !== ctx.principal.agentId && !hasScope(ctx, "read:all"))
      return { error: "forbidden", detail: "for≠self requires read:all" };
    const peek = !rootCtx && p.agent !== ctx.principal.agentId; // non-marking peek
    // QUIRK (legacy §10): inbox does NOT mark reads — only `read` does.
    // mark:true opts in (server consumers use it explicitly).
    const mark = p.mark === true && !peek;
    if (mode === "local") touch(p.agent);
    const role = roleOf(p.agent);
    const rows = d.query("SELECT * FROM messages ORDER BY created_at ASC, rowid ASC").all() as MsgRow[];
    const readIds = new Set((d.query("SELECT msg FROM reads WHERE agent=?").all(p.agent) as any[]).map((x) => x.msg));
    const out = rows.filter((r) => {
      if (r.sender === p.agent) return false;
      if (p.channel && r.channel !== p.channel) return false;
      if (!recipientsMatch(r.recipients, p.agent, role)) return false;
      if (p.open && !["open", "acked", "in_progress"].includes(r.status)) return false;
      if (p.unread && readIds.has(r.id)) return false;
      return true;
    });
    if (mark) for (const r of out) d.run("INSERT OR REPLACE INTO reads(agent,msg,read_at) VALUES(?,?,?)", [p.agent, r.id, nowIso()]);
    return { value: { rows: out, unreadIds: new Set(out.map((r) => r.id).filter((id) => !readIds.has(id))) } };
  }

  function read(ctx: Ctx<M>, p: { agent: string; id: string }): Res<MsgRow & { receipts: Receipts }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const rootCtx = isRootCtx(ctx);
    if ((mode === "server" || !rootCtx) && p.agent !== ctx.principal.agentId && !hasScope(ctx, "read:all"))
      return { error: "forbidden", detail: "read for another agent requires read:all" }; // finding 2 (B2)
    const r = d.query("SELECT * FROM messages WHERE id=?").get(p.id) as MsgRow | null;
    if (!r) return { error: "not_found", detail: `no such message: ${p.id}` };
    if (mode === "local") touch(p.agent);
    const peek = !rootCtx && p.agent !== ctx.principal.agentId; // with read:all: NON-marking peek
    if (!peek) d.run("INSERT OR REPLACE INTO reads(agent,msg,read_at) VALUES(?,?,?)", [p.agent, p.id, nowIso()]);
    return { value: { ...r, receipts: receiptsForMsg(r) } };
  }

  function threadOf(ctx: Ctx<M>, id: string): Res<{ rows: MsgRow[]; receipts: Receipts[] }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    // QUIRK (legacy): thread = explicit thread OR own id (de4ed3b semantics).
    const rows = d.query("SELECT * FROM messages WHERE thread=? OR id=? ORDER BY created_at ASC, rowid ASC").all(id, id) as MsgRow[];
    if (!rows.length) return { error: "not_found", detail: `no thread: ${id}` };
    return { value: { rows, receipts: rows.map(receiptsForMsg) } };
  }

  function receipts(ctx: Ctx<M>, id: string): Res<MsgRow & { receipts: Receipts }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const r = d.query("SELECT * FROM messages WHERE id=?").get(id) as MsgRow | null;
    if (!r) return { error: "not_found", detail: `no such message: ${id}` };
    return { value: { ...r, receipts: receiptsForMsg(r) } };
  }

  function setStatus(ctx: Ctx<M>, p: { agent: string; id: string; state: string }): Res<{ id: string; status: string }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const rootCtx = isRootCtx(ctx);
    // QUIRK restored (finding 14): legacy touched the agent BEFORE validating
    // state/existence, so a rejected status still registered the agent.
    const agent = rootCtx ? p.agent : ctx.principal.agentId; // finding B2: server acts AS principal
    if (mode === "local") touch(agent);
    if (!STATES.includes(p.state as any)) return { error: "usage", detail: `error: state must be one of ${[...STATES].sort()}` };
    const r = d.query("SELECT * FROM messages WHERE id=?").get(p.id) as MsgRow | null;
    if (!r) return { error: "not_found", detail: `no such message: ${p.id}` };
    if (!rootCtx) {
      // §5: sender OR resolved intended recipient may ack/done/status;
      // agents:admin may set ANY. read:all is visibility (history/stream/peek),
      // NOT confidentiality — it must not gate status (round-2 M2).
      const maySet = agent === r.sender
        || recipientsMatch(r.recipients, agent, roleOf(agent))
        || hasScope(ctx, "agents:admin");
      if (!maySet) return { error: "forbidden", detail: "status: not sender, not recipient (needs agents:admin)" };
    }
    d.run("UPDATE messages SET status=?, updated_at=? WHERE id=?", [p.state, nowIso(), p.id]);
    return { value: { id: p.id, status: p.state } };
  }

  function channels(ctx: Ctx<M>): Res<{ name: string; n: number; last: string | null; purpose: string | null }[]> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const counts = d.query("SELECT channel name, COUNT(*) n, MAX(created_at) last FROM messages GROUP BY channel").all() as any[];
    const cmap = new Map<string, any>(counts.map((c) => [c.name, c]));
    for (const c of d.query("SELECT name, purpose FROM channels").all() as any[])
      if (!cmap.has(c.name)) cmap.set(c.name, { name: c.name, n: 0, last: null, purpose: c.purpose });
    for (const c of counts) c.purpose = (d.query("SELECT purpose FROM channels WHERE name=?").get(c.name) as any)?.purpose ?? "";
    return { value: [...cmap.values()].sort((a, b) => String(b.last ?? "").localeCompare(String(a.last ?? ""))) };
  }

  function rename(ctx: Ctx<M>, p: { agent: string; to: string; fingerprint?: string | null }): Res<{ announced: MsgRow }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!p.agent || !p.to) return { error: "usage", detail: "error: rename requires --agent <old> --to <new>" };
    if (!ID_RE.test(p.agent) || !ID_RE.test(p.to)) return { error: "usage", detail: "invalid id" };
    const old = d.query("SELECT * FROM agents WHERE id=?").get(p.agent) as any;
    if (!old) return { error: "not_found", detail: `error: no such agent '${p.agent}'` };
    if (isRootCtx(ctx) || mode === "server") {
      if (!isRootCtx(ctx) && p.agent !== ctx.principal.agentId && !hasScope(ctx, "agents:admin"))
        return { error: "forbidden", detail: "renaming others requires agents:admin" };
    } else if (old.fingerprint && old.fingerprint !== (p.fingerprint ?? null)) {
      return { error: "identity_conflict", detail: `error: rename of '${p.agent}' must come from the same runtime (fingerprint mismatch).` };
    }
    if (d.query("SELECT id FROM agents WHERE id=?").get(p.to))
      return { error: "identity_conflict", detail: `error: id '${p.to}' already exists — pick a free name.` };
    const t = nowIso();
    ensureChannel("general", p.to);
    const mid = newId(String(p.to).split("-")[0]);
    const fname = `msg-${stamp()}-${p.to}-announce-${mid.split("-").pop()}.md`;
    const m: MsgRow = {
      id: mid, thread: mid, re: null, sender: p.agent, recipients: "@all",
      type: "announce", status: "open", tags: "identity,rename",
      subject: `Agent rename: ${p.agent} -> ${p.to}`,
      body: `Identity change: ${p.agent} is now ${p.to} (same runtime). Please update routing; historical messages keep sender=${p.agent}.`,
      file: join("messages", "general", fname), created_at: t, updated_at: t, channel: "general", meta: null,
    };
    // finding 16: mirror AFTER commit — no orphan .md on rollback.
    try {
      d.exec("BEGIN IMMEDIATE");
      try {
        d.run(
          `INSERT INTO messages(id,thread,re,sender,recipients,type,status,tags,subject,body,file,created_at,updated_at,channel,meta)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
          [m.id, m.thread, m.re, m.sender, m.recipients, m.type, m.status, m.tags, m.subject, m.body, m.file, m.created_at, m.updated_at, m.channel, null],
        );
        d.run("UPDATE agents SET id=?, last_seen=? WHERE id=?", [p.to, t, p.agent]);
        d.run("UPDATE reads SET agent=? WHERE agent=?", [p.to, p.agent]);
        d.run("UPDATE tokens SET agent_id=? WHERE agent_id=?", [p.to, p.agent]);
        d.run("UPDATE cursors SET agent_id=? WHERE agent_id=?", [p.to, p.agent]);
        d.run("UPDATE idempotency SET agent_id=? WHERE agent_id=?", [p.to, p.agent]);
        // M7 (round 2): events are point-in-time audit history (§4) — the OLD
        // id is NOT rewritten; the agents_au rename event links old→new.
        d.exec("COMMIT");
      } catch (e) { d.exec("ROLLBACK"); throw e; }
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      if (msg.includes("SQLITE_BUSY")) return { error: "contention", detail: "busy" };
      return { error: "internal", detail: `rename: ${msg}` }; // m6 (round 2): never raw-throw over Res
    }
    seams.mirror(join(MSG_DIR, "general"), fname, renderMd(m));
    return { value: { announced: m } };
  }

  // ---------- server-mode token ops (§5) ----------

  function tokenCreate(ctx: Ctx<M>, p: { agent: string; kind?: "agent" | "human"; label?: string; scopes?: Scope[]; admin?: boolean; force?: boolean }): Res<{ id: number; token: string; prefix: string; agentId: string; scopes: string }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!ID_RE.test(p.agent)) return { error: "usage", detail: `invalid agent id: ${p.agent}` };
    const kind = p.kind ?? "agent";
    const rootCtx = isRootCtx(ctx);
    // §5: token.create requires tokens:admin (local root is the bootstrap path).
    // M9 (round 2): a VALID credential lacking the scope is forbidden (-32002),
    // never unauthorized (-32001 = bad/missing credential only).
    if (!rootCtx && !ctx.principal.scopes.includes("tokens:admin"))
      return { error: "forbidden", detail: "token.create requires tokens:admin" };
    // §4 normalizer contract (M8a): every minted scope must be one enum name —
    // no commas, no unknown names. Otherwise normalizeScopes would store a
    // smuggled second name that later verifies as a real scope.
    if (p.scopes) for (const s of p.scopes)
      if (!validScope(s)) return { error: "usage", detail: `invalid scope: ${JSON.stringify(s)} (must be one of ${ALL_SCOPES.join("|")}, no commas)` };
    let scopes: Scope[];
    if (p.admin) scopes = [...ALL_SCOPES];
    else if (p.scopes) scopes = p.scopes;
    else scopes = kind === "human" ? ["read:all"] : [];
    // §5: tokens:admin is TRANSITIVELY ROOT — it may mint any subset, including
    // scopes it does not itself hold (the round-2 M8b "must hold" rule was a
    // contradiction of §5 and is removed). Minting tokens:admin itself is still
    // gated by the bootstrap guard below.
    const norm = normalizeScopes(scopes);
    const bytes = seams.rng(32);
    const token = "ac_" + b64url(bytes);
    const prefix = token.slice(3, 15);
    const salt = seams.rng(16);
    const keyHash = createHmac("sha256", Buffer.from(salt)).update(token).digest();
    const t = nowIso();
    try {
      d.exec("BEGIN IMMEDIATE");
      try {
        // bootstrap guard ACTUALLY aborts (finding 2): second admin needs force.
        if (norm.split(",").includes("tokens:admin") && !p.force) {
          const existingAdmin = d.query(
            "SELECT count(*) c FROM tokens WHERE revoked_at IS NULL AND instr(',' || scopes || ',', ',tokens:admin,') > 0",
          ).get() as any;
          if (existingAdmin.c) { d.exec("ROLLBACK"); return { error: "conflict", detail: "bootstrap guard: an admin token already exists (pass force to mint another)" }; }
        }
        // NOTE (finding 21): on an EXISTING agent row, kind is NOT overwritten —
        // the row's kind wins; minting a second token never rewrites identity.
        d.run("INSERT OR IGNORE INTO agents(id,role,caps,pid,joined_at,last_seen,meta,kind) VALUES(?,?,?,?,?,?,?,?)",
          [p.agent, p.agent, "", null, t, t, "{}", kind]);
        const r = d.run("INSERT INTO tokens(prefix,agent_id,salt,key_hash,scopes,created_at,last_used) VALUES(?,?,?,?,?,?,?)",
          [prefix, p.agent, salt, keyHash, norm, t, t]);
        d.exec("COMMIT");
        return { value: { id: Number(r.lastInsertRowid), token, prefix, agentId: p.agent, scopes: norm } };
      } catch (e) { d.exec("ROLLBACK"); throw e; }
    } catch (e: any) {
      if (String(e?.message ?? e).includes("UNIQUE")) return { error: "conflict", detail: "prefix collision (60-bit; retry)" };
      throw e;
    }
  }

  function tokenVerify(token: string): Res<{ agentId: string; scopes: Scope[]; kind: "agent" | "human"; tokenId: number }> {
    if (!token.startsWith("ac_") || token.length < 20) return { error: "unauthorized", detail: "malformed token" };
    const prefix = token.slice(3, 15);
    const row = d.query("SELECT * FROM tokens WHERE prefix=? AND revoked_at IS NULL").get(prefix) as any;
    if (!row) return { error: "unauthorized", detail: "unknown or revoked token" };
    const digest = createHmac("sha256", Buffer.from(row.salt)).update(token).digest();
    if (digest.length !== row.key_hash.length || !timingSafeEqual(digest, Buffer.from(row.key_hash)))
      return { error: "unauthorized", detail: "bad token" };
    const kind = ((d.query("SELECT kind FROM agents WHERE id=?").get(row.agent_id) as any)?.kind ?? "agent") as "agent" | "human";
    return { value: { agentId: row.agent_id, scopes: normalizeScopes(csv(row.scopes)).split(",").filter(validScope) as Scope[], kind, tokenId: row.id } }; // finding 20: tokenId; legacy 8-name rows: unknown tokens are inert (M8)
  }

  function tokenList(ctx: Ctx<M>): Res<{ tokens: { id: number; agentId: string; kind: string; prefix: string; scopes: Scope[]; created_at: string; last_used: string; revoked_at: string | null }[] }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!isRootCtx(ctx) && !ctx.principal.scopes.includes("tokens:admin"))
      return { error: "forbidden", detail: "token.list requires tokens:admin" }; // M9
    // m1 (round 2): kind lives on agents, not tokens — JOIN for it.
    const rows = d.query("SELECT t.*, a.kind AS agent_kind FROM tokens t LEFT JOIN agents a ON a.id=t.agent_id ORDER BY t.id").all() as any[];
    return { value: { tokens: rows.map((r) => ({ id: r.id, agentId: r.agent_id, kind: r.agent_kind ?? "agent", prefix: r.prefix, scopes: normalizeScopes(csv(r.scopes)).split(",").filter(validScope) as Scope[], created_at: r.created_at, last_used: r.last_used, revoked_at: r.revoked_at })) } };
  }

  function tokenRevoke(ctx: Ctx<M>, p: { id: number }): Res<{ revoked: boolean }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!isRootCtx(ctx) && !ctx.principal.scopes.includes("tokens:admin"))
      return { error: "forbidden", detail: "token.revoke requires tokens:admin" }; // M9
    const r = d.run("UPDATE tokens SET revoked_at=? WHERE id=? AND revoked_at IS NULL", [nowIso(), p.id]);
    if (r.changes === 0) return { error: "not_found", detail: `no active token ${p.id}` };
    return { value: { revoked: true } };
  }

  function tokenTouch(id: number) {
    const row = d.query("SELECT last_used FROM tokens WHERE id=?").get(id) as any;
    if (!row) return;
    const prev = Date.parse(row.last_used);
    if (Number.isFinite(prev) && seams.now().getTime() - prev < 60_000) return;
    d.run("UPDATE tokens SET last_used=? WHERE id=?", [nowIso(), id]);
  }

  // ---------- cursors (§6) ----------

  function epoch(): string {
    return (d.query("SELECT value FROM meta WHERE key='epoch'").get() as any).value;
  }
  function gcFloor(): number {
    return Number((d.query("SELECT value FROM meta WHERE key='gc_floor'").get() as any)?.value ?? 0);
  }

  function cursorRaw(agentId: string, consumer: string): { epoch: string; seq: number } | null {
    const row = d.query("SELECT epoch, last_seq FROM cursors WHERE agent_id=? AND consumer=?").get(agentId, consumer) as any;
    return row ? { epoch: row.epoch, seq: row.last_seq } : null;
  }
  // M4 (grok round 4): a STORED foreign-epoch row is a resync signal, NOT a silent
  // {epoch: current, seq: 0} collapse — that let clients feed cursor.get straight
  // into waitStep(since) and skip GC'd unconsumed events, and let cursor.set stamp
  // them caught up. Missing row stays {epoch, seq: 0}. Row is never rewritten here.
  function cursorGet(agentId: string, consumer: string): Res<{ epoch: string; seq: number }> {
    const e = epoch();
    const row = cursorRaw(agentId, consumer);
    if (!row) return { value: { epoch: e, seq: 0 } };
    if (row.epoch !== e)
      return { error: "resync", detail: "cursor epoch stale (epoch rotated)", data: { resync: true, epoch: e } };
    return { value: { epoch: e, seq: row.seq } };
  }

  function cursorSet(agentId: string, consumer: string, ep: string, seq: number, force = false): Res<null> {
    if (!Number.isFinite(seq) || !/^[0-9a-f]{8,64}$/.test(ep))
      return { error: "usage", detail: "cursor must be <hex-epoch>.<int-seq>" };
    const e = epoch();
    if (ep !== e) return { error: "resync", detail: "epoch mismatch; resync required", data: { resync: true, epoch: e } };
    if (seq < gcFloor()) return { error: "resync", detail: `cursor below retention floor`, data: { resync: true, epoch: e, floor: gcFloor() } };
    // M4: monotonic check compares against the RAW stored row ONLY when it shares
    // the target epoch — a foreign-epoch row must not block the recovery commit.
    const cur = cursorRaw(agentId, consumer);
    if (!force && cur && cur.epoch === ep && seq < cur.seq) return { error: "conflict", detail: "cursor not monotonic (use force)" };
    d.run("INSERT INTO cursors(agent_id,consumer,epoch,last_seq) VALUES(?,?,?,?) ON CONFLICT(agent_id,consumer) DO UPDATE SET epoch=excluded.epoch,last_seq=excluded.last_seq",
      [agentId, consumer, ep, seq]);
    return { value: null };
  }

  // ---------- streaming/history (§6) ----------

  /** M3 (round 2): ONE cursor parser for every since-bearing entry point
   *  (history, waitStep, cursorSet). Format → usage; foreign epoch → resync;
   *  seq below gc_floor → resync with data.floor — a stale Last-Event-ID or
   *  explicit since can NEVER silently skip past deleted events (§9/N3). */
  function parseCursor(since: string): Res<{ ep: string; seq: number }> {
    const m = /^([0-9a-f]{8,64})\.(\d+)$/.exec(since);
    if (!m) return { error: "usage", detail: "cursor must be <epoch>.<seq>" };
    const e = epoch();
    if (m[1] !== e) return { error: "resync", detail: "epoch mismatch", data: { resync: true, epoch: e } };
    const seq = Number(m[2]);
    if (seq < gcFloor()) return { error: "resync", detail: "cursor below retention floor", data: { resync: true, epoch: e, floor: gcFloor() } };
    return { value: { ep: e, seq } };
  }

  function history(ctx: Ctx<M>, p: { channel?: string | null; limit?: number; since?: string }): Res<{ rows: MsgRow[]; hasMore: boolean; cursor: string }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!isRootCtx(ctx) && !ctx.principal.scopes.includes("read:all"))
      return { error: "forbidden", detail: "history requires read:all" };
    const limit = Math.min(Math.max(p.limit ?? 200, 1), 1000);
    let seqFrom = 0;
    if (p.since) {
      const c = parseCursor(p.since);
      if (c.error) return c;
      seqFrom = c.value.seq;
    }
    // M4 (round 2): TWO modes. WITH since the client is catching up ⇒ OLDEST
    // unseen first (ASC), cursor = last delivered, hasMore = more after it —
    // paging to hasMore=false delivers every row exactly once, no hole.
    // WITHOUT since ⇒ newest-page snapshot (finding 8), cursor = newest.
    const ascending = !!p.since;
    d.exec("BEGIN");
    try {
      if (!ascending) {
        // N2 (round 3): SNAPSHOT pages over MESSAGES (§6 "history returns
        // messages, not events") — events are retention-bounded and absent for
        // pre-events legacy rows. Cursor = events high-water read in the SAME
        // txn (§6), so the stream handoff has no hole; dedupe by msg_id.
        const q = p.channel
          ? d.query("SELECT * FROM messages WHERE channel=? ORDER BY created_at DESC, rowid DESC LIMIT ?")
          : d.query("SELECT * FROM messages ORDER BY created_at DESC, rowid DESC LIMIT ?");
        const got = (p.channel ? q.all(p.channel, limit + 1) : q.all(limit + 1)) as MsgRow[];
        const hw = Math.max((d.query("SELECT coalesce(max(seq),0) m FROM events").get() as any).m as number, gcFloor());
        d.exec("COMMIT");
        const rows = got.slice(0, limit).reverse();
        return { value: { rows, hasMore: got.length > limit, cursor: `${epoch()}.${hw}` } };
      }
      const dir = ascending ? "ASC" : "DESC";
      const evs = (p.channel
        ? d.query(`SELECT e.seq,e.msg_id FROM events e JOIN messages m ON m.id=e.msg_id WHERE e.kind='msg' AND e.seq>? AND m.channel=? ORDER BY e.seq ${dir} LIMIT ?`)
        : d.query(`SELECT seq,msg_id FROM events WHERE kind='msg' AND seq>? ORDER BY seq ${dir} LIMIT ?`)) as any;
      const got = (p.channel ? evs.all(seqFrom, p.channel, limit + 1) : evs.all(seqFrom, limit + 1)) as { seq: number; msg_id: string }[];
      const hasMore = got.length > limit;
      const page = ascending ? got.slice(0, limit) : got.slice(0, limit).reverse();
      const rows = page.map((e) => d.query("SELECT * FROM messages WHERE id=?").get(e.msg_id) as MsgRow).filter(Boolean);
      const lastSeq = page.length ? page[page.length - 1].seq : seqFrom;
      d.exec("COMMIT");
      return { value: { rows, hasMore, cursor: `${epoch()}.${lastSeq}` } };
    } catch (e) {
      // M4 (grok round 3 nit): history must not throw out of the Res contract
      // (post/rename precedent) — a SQL failure is `internal`, same as everywhere.
      d.exec("ROLLBACK");
      return { error: "internal", detail: `history: ${String(e)}` };
    }
  }

  /** finding 9/11: the inboxWait SCAN lives here — server-reachable, fully
   *  asserted (read:all gate, epoch check, malformed cursor). The wait LOOP
   *  (backoff) is transport-side via seams.sleep. */
  function waitStep(ctx: Ctx<M>, p: { for?: string; consumer?: string; since?: string }): Res<{ messages: MsgRow[]; cursor: string; done: boolean }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const target = p.for ?? ctx.principal.agentId;
    if (target !== ctx.principal.agentId && !hasScope(ctx, "read:all"))
      return { error: "forbidden", detail: "inbox.wait for another agent requires read:all" };
    const consumer = p.consumer ?? "default";
    let ep = epoch(); let seq: number;
    if (p.since) {
      const c = parseCursor(p.since); // M3: epoch AND gc_floor checked
      if (c.error) return c;
      ep = c.value.ep; seq = c.value.seq;
    } else {
      // grok round-3 #1: a STORED cursor from a previous epoch is a resync
      // signal, not a silent reset to seq 0 — rotateEpoch zeroes gc_floor, so
      // the floor check alone can never fire after rotation. Epoch mismatch
      // is the same resync path as a below-retention cursor (§6/§9).
      const row = cursorRaw(ctx.principal.agentId, consumer);
      if (row && row.epoch !== ep)
        return { error: "resync", detail: "cursor epoch stale (epoch rotated)", data: { resync: true, epoch: ep, floor: gcFloor() } };
      seq = row?.seq ?? 0;
      if (seq < gcFloor()) return { error: "resync", detail: "cursor below retention floor", data: { resync: true, epoch: ep, floor: gcFloor() } };
    }
    const role = roleOf(target);
    const messages: MsgRow[] = [];
    let cur = seq;
    for (const e of tailEvents(seq, 500)) {
      cur = e.seq;
      if (e.kind !== "msg" || !e.msg_id) continue;
      const m = d.query("SELECT * FROM messages WHERE id=?").get(e.msg_id) as MsgRow | null;
      if (m && m.sender !== target && recipientsMatch(m.recipients, target, role)) messages.push(m);
    }
    return { value: { messages, cursor: `${ep}.${cur}`, done: messages.length > 0 } };
  }

  function tailEvents(afterSeq: number, limit = 200): { seq: number; kind: string; msg_id: string | null; agent_id: string | null; at: string }[] {
    return d.query("SELECT seq,kind,msg_id,agent_id,at FROM events WHERE seq>? ORDER BY seq LIMIT ?").all(afterSeq, limit) as any;
  }

  function rotateEpoch(): string {
    const e = hex(seams.rng(16));
    d.run("INSERT INTO meta(key,value) VALUES('epoch',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [e]);
    d.run("INSERT INTO meta(key,value) VALUES('gc_floor','0') ON CONFLICT(key) DO UPDATE SET value='0'");
    return e;
  }

  function gc(): { events: number; idempotency: number; floor: number } {
    return db_txn(() => {
      const cutE = new Date(seams.now().getTime() - 30 * 86400_000).toISOString().replace(/\.\d{3}Z$/, "Z");
      const cutI = new Date(seams.now().getTime() - 86400_000).toISOString().replace(/\.\d{3}Z$/, "Z");
      const maxDel = (d.query("SELECT coalesce(max(seq),0) m FROM events WHERE at < ?").get(cutE) as any).m;
      const a = d.run("DELETE FROM events WHERE at < ? AND seq <= ?", [cutE, maxDel]);
      if (a.changes > 0) // finding 7: floor advances so stale cursors RESYNC, never skip silently
        d.run("INSERT INTO meta(key,value) VALUES('gc_floor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [String(maxDel)]);
      const b = d.run("DELETE FROM idempotency WHERE created_at < ?", [cutI]);
      return { events: a.changes, idempotency: b.changes, floor: Number(maxDel) };
    });
  }

  function preflight(): { badIds: string[]; badChannels: string[] } {
    const badIds = (d.query("SELECT DISTINCT id FROM agents").all() as any[])
      .map((r) => r.id).filter((id: string) => id && !ID_RE.test(id));
    const badChannels = (d.query("SELECT DISTINCT sender FROM messages").all() as any[])
      .map((r) => r.sender).filter((s: string) => s && !ID_RE.test(s))
      .concat((d.query("SELECT name FROM channels").all() as any[]).map((r) => r.name).filter((n: string) => n && !ID_RE.test(n)));
    return { badIds, badChannels: [...new Set(badChannels)] };
  }

  // local watch helpers (finding 9: no SQL above the core)
  function allMessages(): MsgRow[] {
    return d.query("SELECT * FROM messages ORDER BY created_at ASC, rowid ASC").all() as MsgRow[];
  }
  function allMessageIds(): string[] {
    return (d.query("SELECT id FROM messages").all() as any[]).map((r) => r.id);
  }

  function db_txn<T>(fn: () => T): T {
    d.exec("BEGIN IMMEDIATE");
    try { const v = fn(); d.exec("COMMIT"); return v; }
    catch (e) { d.exec("ROLLBACK"); throw e; }
  }

  function close() { d.close(); }

  return {
    // m5 (round 2): db is demoted to an explicit test accessor — production
    // shells/adapters must never reach for it (M2 RpcBus has no DB at all).
    testDb: d, home, mode, seams, DB_PATH, MSG_DIR, nowIso, stamp, newId,
    joinAgent, listAgents, post, inbox, read, threadOf, receipts, setStatus, channels, rename,
    tokenCreate, tokenVerify, tokenList, tokenRevoke, tokenTouch,
    cursorGet, cursorSet, history, waitStep, tailEvents, epoch, gcFloor, rotateEpoch, gc, preflight,
    allMessages, allMessageIds, ensureChannel,
    isActive, recipientsMatch, roleOf, receiptsForMsg, renderMd, touch, close,
  };
}

function hex(b: Uint8Array) { return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join(""); }
function b64url(b: Uint8Array) {
  return btoa(String.fromCharCode(...b)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
export function localCtx(actor: string): Ctx<"local"> {
  return { principal: { agentId: actor, kind: "agent", scopes: [...ALL_SCOPES], localRoot: true }, actor };
}
export function serverCtx(agentId: string, scopes: Scope[] = [], kind: "agent" | "human" = "agent", cred?: Cred): Ctx<"server"> {
  return { principal: { agentId, kind, scopes }, actor: agentId, cred };
}
