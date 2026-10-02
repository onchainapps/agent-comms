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
// M1.5 (App G ruling c): FIFTH name read:dm — split so an existing read:all
// credential (minted under "cost/UX control, not confidentiality") is never
// retroactively widened into a DM-omniview credential.
export type Scope = "read:all" | "read:dm" | "post:as" | "tokens:admin" | "agents:admin";
export const ALL_SCOPES: readonly Scope[] = ["read:all", "read:dm", "post:as", "tokens:admin", "agents:admin"];
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
// M1.5 App G1: dm-shaped channel. ~ is NOT in ID_RE ⇒ the split is unambiguous.
// The ~n suffix exists only for legacy/local DBs (agent_retired blocks id reuse
// on server DBs, so the collision path is unreachable there).
export const DM_RE = new RegExp(`^dm~(${ID_RE.source.slice(1, -1)})~(${ID_RE.source.slice(1, -1)})(~[1-9][0-9]{0,3})?$`);
/** G2: name SHAPE is the authority for canSee — a cheap shape test mirroring
 *  SQL GLOB 'dm~*'. (DM_RE additionally validates the id halves; the shape test
 *  alone is what routes a channel through the ACL.) */
export const DM_SHAPED_RE = /^dm~/;
/** App G1: ONE helper for the write gate (post/preflight). Read filters do NOT
 *  widen (§9: regexes gate writes only). Name SHAPE is the authority for canSee. */
export const validChannelName = (n: string) => ID_RE.test(n) || DM_RE.test(n);
/** App G1: canonical dm channel name — code-unit sort (NOT localeCompare:
 *  locales order -/_ differently), lo==hi rejected by the caller. */
export function dmChannelName(a: string, b: string, suffix?: string): string {
  const [lo, hi] = a < b ? [a, b] : [b, a];
  return `dm~${lo}~${hi}${suffix ? `~${suffix}` : ""}`;
}

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

/** M1.5 F: memberships is Map<grp, groups.created_at> — the INCARNATION time,
 *  never joined_at, never a name set (a name set cannot express the per-message
 *  comparison, so the JS≡SQL fixture could not pass against it). Explicit param
 *  so no call site can forget the group arm. Group arm: token is group:x AND
 *  memberships.has(x) AND msgCreatedAt >= memberships.get(x). */
export function recipientsMatch(
  recips: string, agent: string, role?: string | null,
  memberships?: Map<string, string>, msgCreatedAt?: string,
  opts?: { ignoreAll?: boolean },
): boolean {
  const toks = new Set(csv(recips));
  if (toks.has(agent)) return true;
  // E1: @all is delivery by default (rename/announce depend on it reaching
  // every inbox). ignoreAll opts a SINGLE consumer out of the broadcast arm —
  // auditors watching a chatty bus must not subscribe to every announce.
  // Same function, one flag: no second predicate can drift (card t_bd5d63cf).
  if (!opts?.ignoreAll && toks.has("@all")) return true;
  if (role && toks.has(role)) return true;
  if (memberships?.size)
    for (const t of toks)
      if (t.startsWith("group:")) {
        const grp = t.slice(6);
        const since = memberships.get(grp);
        // F n3: group:<nonexistent> never lands here via delivery (post rejects
        // it up front); a stale incarnation still resolves via the created_at guard.
        if (since !== undefined && msgCreatedAt !== undefined && msgCreatedAt >= since) return true;
      }
  return false;
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
  "joinAgent" | "post" | "inbox" | "read" | "threadOf" | "receipts" | "setStatus" | "channels" | "rename" | "history" | "waitStep" | "tokenCreate" | "tokenList" | "tokenRevoke" | "mode" |
  "groupCreate" | "groupJoin" | "groupLeave" | "groupDelete" | "groupList" | "groupShow" | "channelCreate"
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
  post: (ctx: Ctx<"server">, p: { from: string; to: string; type: string; subject?: string; body: string; thread?: string | null; re?: string | null; tags?: string; channel?: string | null; as?: string | null; idempotencyKey?: string | null; dm?: string | null }) => Res<{ id: string; channel: string; thread: string; file: string }>;
  inbox: (ctx: Ctx<"server">, p: { agent: string; open?: boolean; unread?: boolean; channel?: string | null; mark?: boolean; noAll?: boolean }) => Res<{ rows: MsgRow[]; unreadIds: Set<string> }>;
  read: (ctx: Ctx<"server">, p: { agent: string; id: string }) => Res<MsgRow & { receipts: Receipts }>;
  threadOf: (ctx: Ctx<"server">, id: string) => Res<{ rows: MsgRow[]; receipts: Receipts[] }>;
  receipts: (ctx: Ctx<"server">, id: string) => Res<MsgRow & { receipts: Receipts }>;
  setStatus: (ctx: Ctx<"server">, p: { agent: string; id: string; state: string }) => Res<{ id: string; status: string }>;
  channels: (ctx: Ctx<"server">) => Res<{ name: string; n: number; last: string | null; purpose: string | null }[]>;
  rename: (ctx: Ctx<"server">, p: { agent: string; to: string; fingerprint?: string | null }) => Res<{ announced: MsgRow }>;
  history: (ctx: Ctx<"server">, p: { channel?: string | null; limit?: number; since?: string }) => Res<{ rows: MsgRow[]; hasMore: boolean; cursor: string }>;
  waitStep: (ctx: Ctx<"server">, p: { for?: string; consumer?: string; since?: string; noAll?: boolean }) => Res<{ messages: MsgRow[]; cursor: string; done: boolean }>;
  tokenCreate: (ctx: Ctx<"server">, p: { agent: string; kind?: "agent" | "human"; label?: string; scopes?: Scope[]; admin?: boolean; force?: boolean }) => Res<{ id: number; token: string; prefix: string; agentId: string; scopes: string }>;
  tokenList: (ctx: Ctx<"server">) => Res<{ tokens: { id: number; agentId: string; kind: string; prefix: string; scopes: Scope[]; created_at: string; last_used: string; revoked_at: string | null }[] }>;
  tokenRevoke: (ctx: Ctx<"server">, p: { id: number }) => Res<{ revoked: boolean }>;
  groupCreate: (ctx: Ctx<"server">, p: { name: string; agent?: string }) => Res<{ name: string; created: boolean }>;
  channelCreate: (ctx: Ctx<"server">, p: { name: string; purpose?: string }) => Res<{ name: string; created: boolean }>;
  groupJoin: (ctx: Ctx<"server">, p: { name: string; agent?: string }) => Res<{ name: string; members: string[] }>;
  groupLeave: (ctx: Ctx<"server">, p: { name: string; agent?: string }) => Res<{ name: string; left: boolean }>;
  groupDelete: (ctx: Ctx<"server">, p: { name: string }) => Res<{ name: string; deleted: boolean }>;
  groupList: (ctx: Ctx<"server">) => Res<{ groups: { name: string; created_by: string; created_at: string; members: number; mine: boolean }[] }>;
  groupShow: (ctx: Ctx<"server">, p: { name: string }) => Res<{ name: string; created_by: string; created_at: string; members: string[] }>;
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
    -- M1.5 App F: work-groups. PK answers "who is in group X"; gm_agent answers
    -- the HOT path "which groups is agent Y in" (PK alone would SCAN).
    CREATE TABLE IF NOT EXISTS groups(
      name TEXT PRIMARY KEY, created_by TEXT NOT NULL, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS group_members(
      grp TEXT NOT NULL, agent_id TEXT NOT NULL, joined_at TEXT NOT NULL,
      created_by TEXT,
      PRIMARY KEY(grp, agent_id));
    CREATE INDEX IF NOT EXISTS gm_agent ON group_members(agent_id, grp);
    -- F n5: delete+recreate backlog guard (upserted PK, never siblings).
    CREATE TABLE IF NOT EXISTS group_tombstones(
      name TEXT PRIMARY KEY, deleted_at TEXT NOT NULL);
    -- M1.5 App G: the ONE confidentiality boundary. Literal member ids only —
    -- role/group/@all are NEVER consulted for access.
    CREATE TABLE IF NOT EXISTS channel_members(
      channel TEXT NOT NULL, agent_id TEXT NOT NULL,
      PRIMARY KEY(channel, agent_id));
    CREATE INDEX IF NOT EXISTS cm_agent ON channel_members(agent_id, channel);
    -- G6: retired-id tombstone (mail inheritance + id-reuse door closed).
    CREATE TABLE IF NOT EXISTS agent_retired(
      id TEXT PRIMARY KEY, renamed_to TEXT NOT NULL, at TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS agent_retired_new ON agent_retired(renamed_to, id);
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
  // v3 (M1.5 F): group_members triggers added — E2: these names must appear in
  // BOTH TRIGGER_DDL and the hardcoded DROP array (IF NOT EXISTS never upgrades
  // a stale generation).
  const SCHEMA_VERSION = 3;
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
    -- v3 (M1.5 F): membership changes fan out via TRIGGERS, not core inserts —
    -- "no writer can bypass fan-out" (§3). Rename is an UPDATE: these do NOT
    -- fire there; membership cache misses must load from the DB (never empty).
    CREATE TRIGGER IF NOT EXISTS group_members_ai AFTER INSERT ON group_members BEGIN
      INSERT INTO events(kind,agent_id,at) VALUES('group',NEW.agent_id,${NOW_SQL});
    END;
    CREATE TRIGGER IF NOT EXISTS group_members_ad AFTER DELETE ON group_members BEGIN
      INSERT INTO events(kind,agent_id,at) VALUES('group',OLD.agent_id,${NOW_SQL});
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
        for (const trg of ["msg_ai", "msg_au", "reads_ai", "agents_ai", "agents_au", "tokens_ai", "tokens_au", "group_members_ai", "group_members_ad"])
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

  // M1.5 G5 — acl_generation (DB-wide counter, SEPARATE from schema_version).
  // Compiled generation is 1 (this binary understands channel_members).
  const ACL_GENERATION = 1;
  {
    const cur = (d.query("SELECT value FROM meta WHERE key='acl_generation'").get() as any)?.value;
    if (cur === undefined) {
      const nakedDm = d.query("SELECT 1 FROM channels WHERE name GLOB 'dm~*' LIMIT 1").get();
      if (!nakedDm) {
        // marker re-read INSIDE the txn (backfill precedent)
        d.exec("BEGIN IMMEDIATE");
        try {
          if (!d.query("SELECT value FROM meta WHERE key='acl_generation'").get())
            d.run("INSERT INTO meta(key,value) VALUES('acl_generation','1')");
          d.exec("COMMIT");
        } catch (e) { d.exec("ROLLBACK"); throw e; }
      }
      // else: pre-existing dm channel without the key (crash window) — canSee
      // fails closed PER CHANNEL; do not refuse open (false-refuses nothing here).
    } else if (Number(cur) > ACL_GENERATION && mode === "server") {
      d.close();
      throw new Error(`openBus: DB acl_generation ${cur} exceeds compiled ${ACL_GENERATION} — replace the binary before serving this DB (G5)`);
    }
  }

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
      // G6 one-way door: touch() is an agents-insert path too — a RETIRED id
      // must never be resurrected by post/inbox/read/setStatus touching (B1).
      if (d.query("SELECT 1 FROM agent_retired WHERE id=?").get(agent)) return;
      d.run(
        "INSERT OR IGNORE INTO agents(id,role,caps,pid,joined_at,last_seen,meta) VALUES(?,?,?,?,?,?,?)",
        [agent, agent, "", seams.pid(), t, t, "{}"],
      );
    }
  }

  function ensureChannel(name: string, by: string) {
    d.run("INSERT OR IGNORE INTO channels(name,purpose,created_at,created_by) VALUES(?,?,?,?)", [name, "", nowIso(), by]);
  }

  // E2 (t_52610023, wildw_client lesson): canonical channel identity stays
  // EXACT (no silent [-_] rewrite — that would retroactively change identity),
  // but a NEW name must not collide with an existing one modulo case/[-_].
  // The guard is a tiny scan (channels are few); dm-shaped rows are excluded
  // (system-managed pair names, not human-typed lanes).
  const chanNorm = (n: string) => n.toLowerCase().replace(/[-_]/g, "");
  function channelDup(name: string): string | null {
    const norm = chanNorm(name);
    for (const r of d.query("SELECT name FROM channels").all() as any[])
      if (r.name !== name && !DM_SHAPED_RE.test(r.name) && chanNorm(r.name) === norm) return r.name;
    return null;
  }

  function channelCreate(ctx: Ctx<M>, p: { name: string; purpose?: string }): Res<{ name: string; created: boolean }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!ID_RE.test(p.name)) return { error: "usage", detail: `invalid channel name: ${p.name} (ID_RE; ':' and '~' excluded)` };
    if (d.query("SELECT 1 FROM channels WHERE name=?").get(p.name)) return { value: { name: p.name, created: false } };
    const dup = channelDup(p.name);
    if (dup) return { error: "usage", detail: `channel '${p.name}' already exists as '${dup}' (near-duplicate — post to the existing name, or choose a distinct one)` };
    d.run("INSERT INTO channels(name,purpose,created_at,created_by) VALUES(?,?,?,?)",
      [p.name, p.purpose ?? "", nowIso(), ctx.principal.agentId]);
    return { value: { name: p.name, created: true } };
  }

  const roleOf = (agent: string): string | null =>
    (d.query("SELECT role FROM agents WHERE id=?").get(agent) as any)?.role ?? null;

  // ---------- M1.5 F/G: memberships + canSee ----------

  /** F JS-parity Map: grp -> groups.created_at (incarnation time). THIS is the
   *  JOIN's membership read — a cache filled on an earlier tick is not the Map;
   *  a miss loads from the DB, never empty (rename UPDATE fires no trigger). */
  function membershipsOf(agent: string): Map<string, string> {
    const m = new Map<string, string>();
    for (const r of d.query(
      "SELECT gm.grp, g.created_at FROM group_members gm JOIN groups g ON g.name = gm.grp WHERE gm.agent_id = ?",
    ).all(agent) as any[]) m.set(r.grp, r.created_at);
    return m;
  }

  /** The probe-verified delivery statement (F): one UNION ALL, every arm an
   *  index SEARCH, positional binds (house style — bun:sqlite does NOT bind
   *  named params from {agent}). Role NULL ⇒ arm returns 0 rows (skip). */
  // NIT-3 (claude t_58d62457): this SQL twin has no noAll knob — if one is ever
  // added, the JS≡SQL parity fuzz MUST thread the same opts through both sides
  // or it will pin the wrong equivalence.
  function deliveredMsgIds(agent: string, role: string | null): Set<string> {
    const rows = d.query(`
      SELECT msg FROM message_recipients WHERE target = ?
      UNION ALL SELECT msg FROM message_recipients WHERE target = ? AND ? IS NOT NULL
      UNION ALL SELECT msg FROM message_recipients WHERE target = '@all'
      UNION ALL SELECT r.msg FROM group_members gm
        JOIN groups g ON g.name = gm.grp
        JOIN message_recipients r ON r.target = ('group:' || gm.grp)
        JOIN messages m ON m.id = r.msg AND m.created_at >= g.created_at
        WHERE gm.agent_id = ?
    `).all(agent, role, role, agent) as any[];
    return new Set(rows.map((r) => r.msg));
  }

  const dmMembers = (channel: string): string[] =>
    (d.query("SELECT agent_id FROM channel_members WHERE channel=? ORDER BY agent_id").all(channel) as any[]).map((r) => r.agent_id);

  /** G4 CLI (`dms`): member list gated by the ONE visibility predicate. Local
   *  see-all quirk applies via canSeeChannel; server mode: member or read:dm,
   *  else invisible ⇒ byte-identical not_found (G2). */
  function dmMembersFor(ctx: AnyCtx, channel: string): Res<string[]> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!DM_SHAPED_RE.test(channel))
      return { error: "usage", detail: "not a dm channel" };
    if (!canSeeChannel(ctx, channel))
      return { error: "not_found", detail: `no such channel: ${channel}` };
    return { value: dmMembers(channel) };
  }

  /** G2 — the ONE visibility predicate. Name SHAPE is the authority (GLOB);
   *  channels.kind is at most a CHECK-enforced copy. Role/group/@all NEVER
   *  consulted. Local mode = host is root of trust ⇒ see-all (pinned quirk). */
  function canSeeChannel(ctx: AnyCtx, channel: string): boolean {
    if (mode === "local") return true; // quirk pin: local sees all (G5/G7)
    if (!DM_SHAPED_RE.test(channel)) return true;
    if (hasScope(ctx, "read:dm")) return true;
    return d.query("SELECT 1 FROM channel_members WHERE channel=? AND agent_id=?").get(channel, ctx.principal.agentId) !== null;
  }

  /** G0 pair lookup — GLOB + cardinality, deterministic tie-break newest
   *  channels.created_at then rowid (nowIso is one-second resolution). */
  function dmChannelForPair(lo: string, hi: string): string | null {
    const row = d.query(`
      SELECT c.name FROM channel_members cm INDEXED BY cm_agent
      JOIN channels c ON c.name = cm.channel
      WHERE cm.agent_id IN (?, ?) AND cm.channel GLOB 'dm~*'
      GROUP BY c.name
      HAVING count(*) = 2
         AND (SELECT count(*) FROM channel_members x WHERE x.channel = c.name) = 2
      ORDER BY c.created_at DESC, c.rowid DESC
      LIMIT 1
    `).get(lo, hi) as any;
    return row?.name ?? null;
  }

  /** G2w-v — the ONE dm creation helper, BOTH modes. Canonicalize first (client
   *  ~n is STRIPPED, not a distinct reject — the PAIR is the key). One IMMEDIATE
   *  txn: channel row + exactly 2 member rows. Returns the stored name. */
  function dmEnsure(lo: string, hi: string, by: string): Res<{ channel: string }> {
    const existing = dmChannelForPair(lo, hi);
    if (existing) return { value: { channel: existing } };
    const t = nowIso();
    let chan = dmChannelName(lo, hi);
    try {
      d.exec("BEGIN IMMEDIATE");
      try {
        const hit = dmChannelForPair(lo, hi); // re-check inside txn
        if (hit) { d.exec("COMMIT"); return { value: { channel: hit } }; }
        if (d.query("SELECT 1 FROM channels WHERE name=?").get(chan)) {
          // canonical name held by a DIFFERENT pair (legacy id-reuse path only —
          // agent_retired blocks id reuse on server DBs): allocate ~n inside txn.
          let n = 1;
          while (d.query("SELECT 1 FROM channels WHERE name=?").get(`dm~${lo}~${hi}~${n}`)) n++;
          chan = `dm~${lo}~${hi}~${n}`;
        }
        d.run("INSERT INTO channels(name,purpose,created_at,created_by) VALUES(?,?,?,?)", [chan, "", t, by]);
        d.run("INSERT INTO channel_members(channel,agent_id) VALUES(?,?)", [chan, lo]);
        d.run("INSERT INTO channel_members(channel,agent_id) VALUES(?,?)", [chan, hi]);
        d.exec("COMMIT");
      } catch (e) { d.exec("ROLLBACK"); throw e; }
      return { value: { channel: chan } };
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      if (msg.includes("SQLITE_BUSY")) return { error: "contention", detail: "busy" };
      return { error: "internal", detail: `dmEnsure: ${msg}` };
    }
  }

  /** M2/M3 split of dmResolvePair: the GATE is a pure predicate (no writes) so
   *  post() can defer channel creation into the message txn. not_found detail
   *  is built from the REQUESTED pair's canonical name — never a stored name —
   *  so channel existence is never an oracle (G2w-v). */
  function dmGate(lo: string, hi: string, sender: string, requireAgents = true): BusError | null {
    const agentExists = (id: string) => d.query("SELECT 1 FROM agents WHERE id=?").get(id) !== null;
    // requireAgents = the server-mode oracle closure (G2w-v). Local mode is the
    // bypass (host = root; ids mint implicitly everywhere), so only the retired
    // check applies there.
    if (!sender || sender !== lo && sender !== hi || (requireAgents && (!agentExists(lo) || !agentExists(hi))) ||
        d.query("SELECT 1 FROM agent_retired WHERE id IN (?,?)").get(lo, hi))
      return { error: "not_found", detail: `no such channel: ${dmChannelName(lo, hi)}` };
    return null;
  }

  /** G2w-v branch of (iv): pair lookup first; missing ⇒ gate then create. */
  function dmResolvePair(lo: string, hi: string, sender: string, requireAgents = true): Res<{ channel: string; members: string[] }> {
    const hit = dmChannelForPair(lo, hi);
    if (hit) return { value: { channel: hit, members: dmMembers(hit) } };
    const g = dmGate(lo, hi, sender, requireAgents);
    if (g) return g;
    const en = dmEnsure(lo, hi, sender);
    if (en.error) return en;
    return { value: { channel: en.value.channel, members: dmMembers(en.value.channel) } };
  }

  /** M3: txn-LESS channel creation for the message's IMMEDIATE txn. Caller is
   *  already inside BEGIN IMMEDIATE. ~n allocation stays inside this txn only. */
  function dmCreateInTxn(lo: string, hi: string, by: string): string {
    const t = nowIso();
    let chan = dmChannelName(lo, hi);
    if (d.query("SELECT 1 FROM channels WHERE name=?").get(chan)) {
      let n = 1;
      while (d.query("SELECT 1 FROM channels WHERE name=?").get(`dm~${lo}~${hi}~${n}`)) n++;
      chan = `dm~${lo}~${hi}~${n}`;
    }
    d.run("INSERT INTO channels(name,purpose,created_at,created_by) VALUES(?,?,?,?)", [chan, "", t, by]);
    d.run("INSERT INTO channel_members(channel,agent_id) VALUES(?,?)", [chan, lo]);
    d.run("INSERT INTO channel_members(channel,agent_id) VALUES(?,?)", [chan, hi]);
    return chan;
  }

  // ---------- F: group operations (self-organizing — NO scope to create/join/leave) ----------

  /** create-if-missing core. Tombstone rule (m-a/grok): if now <= deleted_at the
   *  create REJECTS with contention (retry <=1s) — never stamp ahead of clock. */
  function groupEnsure(name: string, by: string): Res<{ name: string; created: boolean }> {
    const ex = d.query("SELECT name FROM groups WHERE name=?").get(name);
    if (ex) return { value: { name, created: false } };
    let created = false;
    try {
      d.exec("BEGIN IMMEDIATE");
      try {
        const r = groupEnsureInTxn(name, by);
        if ("error" in r) { d.exec("ROLLBACK"); return r; }
        created = r.created;
        d.exec("COMMIT");
      } catch (e) { d.exec("ROLLBACK"); throw e; }
      return { value: { name, created } };
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      if (msg.includes("SQLITE_BUSY")) return { error: "contention", detail: "busy" };
      return { error: "internal", detail: `groupEnsure: ${msg}` };
    }
  }

  /** txn-LESS core — caller already holds BEGIN IMMEDIATE (m9: groupJoin runs
   *  ensure+cap+insert as ONE txn so a concurrent delete can't orphan us). */
  function groupEnsureInTxn(name: string, by: string): { created: boolean } | BusError {
    if (d.query("SELECT name FROM groups WHERE name=?").get(name)) return { created: false };
    const t = nowIso();
    const tomb = (d.query("SELECT deleted_at FROM group_tombstones WHERE name=?").get(name) as any)?.deleted_at as string | undefined;
    if (tomb !== undefined && t <= tomb) return { error: "contention", detail: `group '${name}' deleted <1s ago; retry after the second boundary` };
    const created = (d.query("SELECT count(*) c FROM groups WHERE created_by=?").get(by) as any).c;
    if (created >= 64) return { error: "usage", detail: "group squatting cap: 64 groups created per agent" };
    d.run("INSERT INTO groups(name,created_by,created_at) VALUES(?,?,?)", [name, by, t]);
    return { created: true };
  }

  function groupCreate(ctx: Ctx<M>, p: { name: string; agent?: string }): Res<{ name: string; created: boolean }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!ID_RE.test(p.name)) return { error: "usage", detail: `invalid group name: ${p.name} (ID_RE, ':' and '~' excluded)` };
    const self = p.agent ?? ctx.principal.agentId;
    if (self !== ctx.principal.agentId && !isRootCtx(ctx))
      return { error: "forbidden", detail: "group agent is an assertion — self only" };
    return groupEnsure(p.name, self);
  }

  /** join = create-if-missing + add self. Caps: <=64 groups/agent (counting
   *  NEW memberships only — m8: re-joining an existing group must not fail at
   *  the cap), <=512 members/group (m1). m9: ensure+cap+insert in ONE txn so a
   *  concurrent groupDelete cannot orphan a membership onto the NEXT
   *  incarnation, and the cap is not TOCTOU. */
  function groupJoin(ctx: Ctx<M>, p: { name: string; agent?: string }): Res<{ name: string; members: string[] }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!ID_RE.test(p.name)) return { error: "usage", detail: `invalid group name: ${p.name}` };
    const self = p.agent ?? ctx.principal.agentId;
    if (self !== ctx.principal.agentId && !isRootCtx(ctx))
      return { error: "forbidden", detail: "group agent is an assertion — self only (else anyone subscribes others to 500 groups)" };
    const t = nowIso();
    try {
      d.exec("BEGIN IMMEDIATE");
      try {
        const en = groupEnsureInTxn(p.name, self);
        if ("error" in en) { d.exec("ROLLBACK"); return en; }
        const already = d.query("SELECT 1 FROM group_members WHERE grp=? AND agent_id=?").get(p.name, self);
        if (!already) {
          const nGroups = (d.query("SELECT count(*) c FROM group_members WHERE agent_id=?").get(self) as any).c;
          if (nGroups >= 64) { d.exec("ROLLBACK"); return { error: "usage", detail: "cap: 64 groups per agent (bounds the delivery arm's seeks)" }; }
          const nMembers = (d.query("SELECT count(*) c FROM group_members WHERE grp=?").get(p.name) as any).c;
          if (nMembers >= 512) { d.exec("ROLLBACK"); return { error: "usage", detail: "cap: 512 members per group" }; }
          d.run("INSERT INTO group_members(grp,agent_id,joined_at,created_by) VALUES(?,?,?,?)", [p.name, self, t, self]);
        }
        d.exec("COMMIT");
      } catch (e) { d.exec("ROLLBACK"); throw e; }
      return { value: { name: p.name, members: dmMembersLike(p.name) } };
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      if (msg.includes("SQLITE_BUSY")) return { error: "contention", detail: "busy" };
      return { error: "internal", detail: `groupJoin: ${msg}` };
    }
  }

  const dmMembersLike = (grp: string) =>
    (d.query("SELECT agent_id FROM group_members WHERE grp=? ORDER BY agent_id").all(grp) as any[]).map((r) => r.agent_id);

  function groupLeave(ctx: Ctx<M>, p: { name: string; agent?: string }): Res<{ name: string; left: boolean }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!ID_RE.test(p.name)) return { error: "usage", detail: `invalid group name: ${p.name}` };
    const self = p.agent ?? ctx.principal.agentId;
    if (self !== ctx.principal.agentId && !isRootCtx(ctx))
      return { error: "forbidden", detail: "group agent is an assertion — self only" };
    const r = d.run("DELETE FROM group_members WHERE grp=? AND agent_id=?", [p.name, self]);
    return { value: { name: p.name, left: r.changes > 0 } };
  }

  /** delete requires agents:admin; DELETEs members in the SAME txn (delivery
   *  reads members, not groups — orphans must not still receive); upserts the
   *  tombstone PK (never a sibling row). */
  function groupDelete(ctx: Ctx<M>, p: { name: string }): Res<{ name: string; deleted: boolean }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!ID_RE.test(p.name)) return { error: "usage", detail: `invalid group name: ${p.name}` };
    if (!isRootCtx(ctx) && !hasScope(ctx, "agents:admin"))
      return { error: "forbidden", detail: "group.delete requires agents:admin" };
    const t = nowIso();
    try {
      d.exec("BEGIN IMMEDIATE");
      try {
        const g = d.query("SELECT name FROM groups WHERE name=?").get(p.name);
        if (!g) { d.exec("ROLLBACK"); return { error: "not_found", detail: `no such group: ${p.name}` }; }
        d.run("DELETE FROM group_members WHERE grp=?", [p.name]);
        d.run("DELETE FROM groups WHERE name=?", [p.name]);
        d.run(`INSERT INTO group_tombstones(name,deleted_at) VALUES(?,?)
               ON CONFLICT(name) DO UPDATE SET deleted_at = excluded.deleted_at
               WHERE excluded.deleted_at > group_tombstones.deleted_at`, [p.name, t]);
        d.exec("COMMIT");
      } catch (e) { d.exec("ROLLBACK"); throw e; }
      return { value: { name: p.name, deleted: true } };
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      if (msg.includes("SQLITE_BUSY")) return { error: "contention", detail: "busy" };
      return { error: "internal", detail: `groupDelete: ${msg}` };
    }
  }

  function groupList(ctx: Ctx<M>): Res<{ groups: { name: string; created_by: string; created_at: string; members: number; mine: boolean }[] }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const mine = new Set((d.query("SELECT grp FROM group_members WHERE agent_id=?").all(ctx.principal.agentId) as any[]).map((r) => r.grp));
    const rows = (d.query("SELECT name, created_by, created_at FROM groups ORDER BY name").all() as any[])
      .map((g) => ({ name: g.name, created_by: g.created_by, created_at: g.created_at, members: dmMembersLike(g.name).length, mine: mine.has(g.name) }));
    return { value: { groups: rows } };
  }

  function groupShow(ctx: Ctx<M>, p: { name: string }): Res<{ name: string; created_by: string; created_at: string; members: string[] }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const g = d.query("SELECT name, created_by, created_at FROM groups WHERE name=?").get(p.name) as any;
    if (!g) return { error: "not_found", detail: `no such group: ${p.name}` };
    return { value: { name: g.name, created_by: g.created_by, created_at: g.created_at, members: dmMembersLike(g.name) } };
  }

  /** F receipts honesty: the intended set is current members of the
   *  incarnation that existed when the message was posted, excluding sender —
   *  NOT a membership snapshot (no history table; a recreated group
   *  contributes nobody). G2: receipts never build for a hidden dm row — the
   *  intended set is filtered by canSee per reader (server mode). */
  function receiptsForMsg(msg: MsgRow, ctx?: AnyCtx): Receipts {
    const agents = d.query("SELECT id, role FROM agents WHERE id IS NOT NULL").all() as any[];
    const memCache = new Map<string, Map<string, string>>();
    const memFor = (id: string) => {
      let m = memCache.get(id);
      if (!m) { m = membershipsOf(id); memCache.set(id, m); }
      return m;
    };
    let intended = agents
      .filter((a) => a.id !== msg.sender && recipientsMatch(msg.recipients, a.id, a.role, memFor(a.id), msg.created_at))
      .map((a) => a.id);
    // G2: never build receipts for a hidden row's readers.
    if (ctx && mode === "server") intended = intended.filter((id) => canSeeReader(msg.channel, id, ctx));
    const readMap = new Map<string, string | null>();
    for (const r of d.query("SELECT agent, read_at FROM reads WHERE msg=?").all(msg.id) as any[])
      readMap.set(r.agent, r.read_at);
    // m7 (claude): reply-inference must respect canSee — a reply hidden in a DM
    // must not mark its sender "seen (replied)" to a caller who cannot read the
    // REPLY row itself. Rule: inference only from rows the caller can see.
    for (const r of d.query("SELECT sender, channel FROM messages WHERE re=?").all(msg.id) as any[])
      if (!readMap.has(r.sender) && (!ctx || mode === "local" || canSeeChannel(ctx, r.channel)))
        readMap.set(r.sender, null);
    return {
      intended,
      readers: intended.filter((id) => readMap.has(id)).map((id) => ({ id, at: readMap.get(id) ?? null })),
      unread: intended.filter((id) => !readMap.has(id)),
    };
  }

  /** canSee from the READER's side for receipts: a dm row is visible to its
   *  literal members; the omniview overlay (read:dm) belongs to whoever asks,
   *  so per-reader filtering here is membership-only (the caller's scope was
   *  already checked at the path gate). */
  function canSeeReader(channel: string, readerId: string, _ctx: AnyCtx): boolean {
    if (mode === "local") return true;
    if (!DM_SHAPED_RE.test(channel)) return true;
    return d.query("SELECT 1 FROM channel_members WHERE channel=? AND agent_id=?").get(channel, readerId) !== null;
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

  /** §5 role grammar (F/G close the P2/R1 class) — checks on a role WRITE:
   *  (1) ID_RE grammar — server rejects, local warns (hit-only, golden-safe);
   *  (2) role == ANOTHER AGENT'S ID (T2 direction) — server identity_conflict,
   *      local warns; (3) role == any agent_retired.id (R1) — identity_conflict
   *      in BOTH modes (grok binding: never weakened into the warning).
   *  Returns { error } to reject, { warnings } hit-only notices, or null. */
  function roleWriteChecks(role: string, selfId: string): { error?: BusError; warnings?: string[] } | null {
    const warnings: string[] = [];
    if (!ID_RE.test(role)) {
      if (mode === "server") return { error: { error: "usage", detail: `invalid role: ${role} (must match ID_RE — no ':' '~' '@': a role of 'group:secret' must not impersonate a structured target)` } };
      warnings.push(`warning: legacy role '${role}' fails ID_RE grammar (local mode = root; will be rejected in server mode)`);
    }
    const clash = (d.query("SELECT id FROM agents WHERE id=? AND id!=?").get(role, selfId) as any)?.id;
    if (clash) {
      if (mode === "server") return { error: { error: "identity_conflict", detail: `role '${role}' equals agent id '${clash}' — would inherit that agent's bare-token mail (N3/T2)` } };
      warnings.push(`warning: role '${role}' equals existing agent id — server mode rejects this (N3)`);
    }
    if (d.query("SELECT 1 FROM agent_retired WHERE id=?").get(role))
      return { error: { error: "identity_conflict", detail: `role '${role}' equals a retired agent id — would inherit pre-rename mail through the bare-token arm (R1)` } };
    return warnings.length ? { warnings } : null;
  }

  /** §5 N3 symmetric + G6 checks on a NEW agent id (token.create INSERT, rename
   *  target, local joinAgent mint): the id must not be retired (HARD both modes —
   *  a re-minted id would collide agent_retired PK AND inherit old mail); no
   *  OTHER agent may hold role == newId (server hard; local warn). Runs INSIDE
   *  the caller's IMMEDIATE txn (m-c). */
  function idMintChecks(newId: string): { error?: BusError; warnings?: string[] } | null {
    if (d.query("SELECT 1 FROM agent_retired WHERE id=?").get(newId))
      return { error: { error: "identity_conflict", detail: `id '${newId}' was retired by a rename — ids are not reused (one-way door, G6)` } };
    const holder = (d.query("SELECT id FROM agents WHERE role=? AND id!=?").get(newId, newId) as any)?.id;
    if (holder) {
      if (mode === "server") return { error: { error: "identity_conflict", detail: `id '${newId}' is held as ROLE by '${holder}' — minting it would inherit that agent's bare-token mail (N3/T2)` } };
      return { warnings: [`warning: id '${newId}' is held as role by '${holder}' — server mode rejects this mint (N3)`] };
    }
    return null;
  }

  function joinAgent(ctx: Ctx<M>, p: { agent: string; role: string; caps?: string; fingerprint?: string | null }): Res<{ agent: AgentRow; active: AgentRow[]; unresolved: number }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    if (!p.agent || !p.role) return { error: "usage", detail: "error: join requires --agent and --role" };
    if (!ID_RE.test(p.agent)) return { error: "usage", detail: `invalid agent id: ${p.agent}` };
    const t = nowIso();
    if (mode === "server") {
      // finding 12: assertion id, UPDATE-only (no auto-register), fingerprint IGNORED.
      if (p.agent !== ctx.principal.agentId)
        return { error: "forbidden", detail: `agent assertion '${p.agent}' != principal '${ctx.principal.agentId}'` };
      // m-c: identity checks INSIDE the write's IMMEDIATE txn.
      try { d.exec("BEGIN IMMEDIATE"); } catch (e: any) {
        if (String(e?.message ?? e).includes("SQLITE_BUSY")) return { error: "contention", detail: "busy" };
        return { error: "internal", detail: `join: ${String(e?.message ?? e)}` };
      }
      try {
        const rc = roleWriteChecks(p.role, p.agent);
        if (rc?.error) { d.exec("ROLLBACK"); return rc.error; }
        for (const w of rc?.warnings ?? []) seams.warn?.(w);
        const r = d.run("UPDATE agents SET role=?, caps=?, last_seen=? WHERE id=?", [p.role, p.caps ?? "", t, p.agent]);
        if (r.changes === 0) { d.exec("ROLLBACK"); return { error: "not_found", detail: `unknown agent '${p.agent}' — rows are minted by token.create on the server` }; }
        d.exec("COMMIT");
      } catch (e: any) { d.exec("ROLLBACK"); return { error: "internal", detail: `join: ${String(e?.message ?? e)}` }; }
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
      const isNew = !d.query("SELECT id FROM agents WHERE id=?").get(p.agent);
      // §5 role grammar applies to EVERY join (both modes); mint checks only on
      // the INSERT branch (m-b). m3 (claude): checks + INSERT in ONE IMMEDIATE
      // txn (m-c parity with the server branch) so a concurrent rename cannot
      // TOCTOU between the retired/role read and the write.
      const rc = roleWriteChecks(p.role, p.agent);
      if (rc?.error) return rc.error;
      if (isNew) {
        const mc = idMintChecks(p.agent);
        if (mc?.error) return mc.error;
        for (const w of [...(mc?.warnings ?? []), ...(rc?.warnings ?? [])]) seams.warn?.(w);
        try { d.exec("BEGIN IMMEDIATE"); } catch (e: any) {
          if (String(e?.message ?? e).includes("SQLITE_BUSY")) return { error: "contention", detail: "busy" };
          return { error: "internal", detail: `join: ${String(e?.message ?? e)}` };
        }
        try {
          if (d.query("SELECT 1 FROM agent_retired WHERE id=?").get(p.agent)) { d.exec("ROLLBACK"); return { error: "identity_conflict", detail: `agent id '${p.agent}' is retired (renamed away) — one-way door (G6)` }; }
          d.run(
            `INSERT INTO agents(id,role,caps,pid,joined_at,last_seen,meta,fingerprint,kind) VALUES(?,?,?,?,?,?,?,?,?)
             ON CONFLICT(id) DO UPDATE SET role=excluded.role, caps=excluded.caps, pid=excluded.pid, last_seen=excluded.last_seen,
               fingerprint=COALESCE(excluded.fingerprint, agents.fingerprint)`,
            [p.agent, p.role, p.caps ?? "", seams.pid(), t, t, "{}", fp,
             (d.query("SELECT kind FROM agents WHERE id=?").get(p.agent) as any)?.kind ?? "agent"],
          );
          d.exec("COMMIT");
        } catch (e) { d.exec("ROLLBACK"); throw e; }
      } else {
        for (const w of rc?.warnings ?? []) seams.warn?.(w);
        d.run(
          `INSERT INTO agents(id,role,caps,pid,joined_at,last_seen,meta,fingerprint,kind) VALUES(?,?,?,?,?,?,?,?,?)
           ON CONFLICT(id) DO UPDATE SET role=excluded.role, caps=excluded.caps, pid=excluded.pid, last_seen=excluded.last_seen,
             fingerprint=COALESCE(excluded.fingerprint, agents.fingerprint)`,
          [p.agent, p.role, p.caps ?? "", seams.pid(), t, t, "{}", fp,
           (d.query("SELECT kind FROM agents WHERE id=?").get(p.agent) as any)?.kind ?? "agent"],
        );
      }
    }
    const row = d.query("SELECT * FROM agents WHERE id=?").get(p.agent) as AgentRow;
    // n6 (claude): unresolved count under canSee — a non-party's DM traffic must
    // not leak existence/volume through join output. Local mode: legacy formula
    // verbatim (golden parity).
    let unresolved: number;
    if (mode === "server") {
      unresolved = (d.query(
        "SELECT COUNT(*) c FROM messages WHERE status IN ('open','acked','in_progress') AND sender!=?",
      ).get(p.agent) as any).c
        - (d.query(
          "SELECT COUNT(*) c FROM messages WHERE status IN ('open','acked','in_progress') AND sender!=? AND channel GLOB 'dm~*' AND NOT EXISTS (SELECT 1 FROM channel_members cm WHERE cm.channel=messages.channel AND cm.agent_id=?) AND ? != 1",
        ).get(p.agent, p.agent, hasScope(ctx, "read:dm") ? 1 : 0) as any).c;
    } else {
      unresolved = (d.query(
        "SELECT COUNT(*) c FROM messages WHERE status IN ('open','acked','in_progress') AND sender!=?",
      ).get(p.agent) as any).c;
    }
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
    as?: string | null; idempotencyKey?: string | null; dm?: string | null;
  }): Res<{ id: string; channel: string; thread: string; file: string }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const rootCtx = isRootCtx(ctx);
    if (!p.from || !p.to) return { error: "usage", detail: "error: post requires --from and --to" };
    if (p.dm !== undefined && p.dm !== null && !ID_RE.test(p.dm))
      return { error: "usage", detail: `invalid dm peer: ${p.dm}` };

    // QUIRK restored (finding 14): legacy touched the sender BEFORE validating
    // type/channel/etc, so a rejected post still registered the agent. Server
    // mode never auto-registers (touch is UPDATE-only there).
    if (mode === "local") touch(p.from);

    if (p.as !== null && p.as !== undefined && !ID_RE.test(p.as))
      return { error: "usage", detail: `invalid as: ${p.as}` }; // finding 3 (B3): before any path join
    if (!ID_RE.test(p.from)) return { error: "usage", detail: `invalid from id: ${p.from}` };
    if (!MSG_TYPES.includes(p.type as any)) return { error: "usage", detail: `error: --type must be one of ${[...MSG_TYPES].sort()}` };
    if (!TYPE_RE.test(p.type)) return { error: "usage", detail: `invalid type: ${p.type}` };
    if (p.channel !== undefined && p.channel !== null && !validChannelName(p.channel))
      return { error: "usage", detail: `invalid channel: ${p.channel}` };

    // m1 (claude/grok): §9 recipients cap — group: tokens count as one target.
    if (csv(p.to).length > 32) return { error: "usage", detail: "recipients cap: 32 targets per message" };

    // F n3: post addressed to group:<nonexistent> is rejected up front — nobody
    // pre-addresses a name and squats traffic by creating the group later.
    for (const t0 of csv(p.to))
      if (t0.startsWith("group:") && !d.query("SELECT 1 FROM groups WHERE name=?").get(t0.slice(6)))
        return { error: "usage", detail: `post to ${t0}: no such group (create it first — pre-addressing would squat traffic)` };

    // G2w-i/ii: dm write-side rules (server-enforced; local root bypasses —
    // host is root of trust). (iii) parent lookup applies canSee (below).
    const dmRules = mode === "server" || !rootCtx;

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
        // m5 (claude): dm participates in the hash ONLY when set, so hashes of
        // pre-M1.5 rows stay stable across the upgrade (same-key dm:bob vs
        // dm:carol must conflict per §6).
        ...(p.dm !== undefined && p.dm !== null ? { dm: p.dm } : {}),
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

    // §5: sender = as-target; N4: meta.as = principal. Computed BEFORE channel
    // resolution — G2w rules speak of the FINAL sender (after `as`).
    const sender = rootCtx ? (p.as ?? p.from) : (p.as || ctx.principal.agentId);
    const asAudit = p.as ? ctx.principal.agentId : null;

    // ---------- M1.5 G2w: anchors + dm channel resolution ----------

    // (iv) thread anchor: server mode requires it to reference an EXISTING,
    // canSee-visible message (kills injection + future-id squat + legacy
    // free-form thread strings). Local keeps legacy leniency (golden parity).
    let threadRoot: MsgRow | null = null;
    if (p.thread) {
      threadRoot = d.query("SELECT * FROM messages WHERE id=?").get(p.thread) as MsgRow | null;
      if (mode === "server" && (!threadRoot || !canSeeChannel(ctx, threadRoot.channel)))
        return { error: "not_found", detail: `no such message: ${p.thread}` };
    }

    // (iii) re anchor: existence (m3) AND canSee — invisible == missing, same
    // detail string (B2 oracle closed). Local dangling --re stays a silent
    // insert (legacy quirk, pinned).
    let reRow: MsgRow | null = null;
    if (p.re) {
      reRow = d.query("SELECT * FROM messages WHERE id=?").get(p.re) as MsgRow | null;
      if (mode === "server" && (!reRow || !canSeeChannel(ctx, reRow.channel)))
        return { error: "not_found", detail: `error: re -> unknown message id '${p.re}'` };
    }
    // E3 (grok field report #3): in SERVER mode `re` alone anchors the thread
    // to the replied row's thread root — a reply silently starting its own
    // thread was scattering threads across channels. Local keeps the legacy
    // own-id quirk (golden parity, §10). The reRow doubles as threadRoot so the
    // dm cross-channel attach rule below applies to derived threads too.
    let derivedThread: string | null = null;
    if (mode === "server" && !p.thread && reRow) {
      derivedThread = reRow.thread || reRow.id;
      threadRoot = reRow;
    }

    let channel: string | null = p.channel ?? null;
    // Channel inheritance: server = the ANCHOR ROW's channel column (never the
    // legacy id-OR-thread lookup, which can resolve to a squatter row); local =
    // legacy lookup verbatim (golden quirk pin).
    if (!channel) {
      if (mode === "server") channel = threadRoot?.channel ?? reRow?.channel ?? null;
      else {
        const parentId = p.thread || p.re;
        if (parentId) {
          const par = d.query("SELECT channel FROM messages WHERE id=? OR thread=? ORDER BY created_at ASC LIMIT 1")
            .get(parentId, parentId) as any;
          channel = par?.channel ?? null;
        }
      }
    }

    // G1 dm sugar: channel + recipients = the peer (pair-keyed, like everything dm).
    // M3 (claude/grok round 1): creation writes are DEFERRED into the message's
    // IMMEDIATE txn — a usage reject must leave NO channel row. Channel writes
    // for a missing pair happen only after every validation has passed.
    let dmMembersResolved: string[] | null = null;
    let dmPending: { lo: string; hi: string } | null = null;
    let requestedChannel = channel; // M2: not_found detail is request-derived, never stored-name
    if (p.dm !== undefined && p.dm !== null) {
      const me = sender, peer = p.dm;
      if (me === peer) return { error: "usage", detail: "self-DM rejected (lo == hi)" };
      // m6 (claude): dm is mutually exclusive with channel; to must be omitted
      // or equal the peer — a silent reroute public→dm (or to=@all) is a footgun.
      if (p.channel !== undefined && p.channel !== null)
        return { error: "usage", detail: "dm and channel are mutually exclusive" };
      const toToks = csv(p.to);
      if (toToks.length && (toToks.length !== 1 || toToks[0] !== peer))
        return { error: "usage", detail: `with dm, to must be omitted or equal the peer: ${p.to}` };
      const [lo, hi] = me < peer ? [me, peer] : [peer, me];
      const hit = dmChannelForPair(lo, hi);
      if (hit) {
        channel = hit;
        dmMembersResolved = dmMembers(hit);
      } else {
        const g = dmGate(lo, hi, sender, dmRules);
        if (g) return g;
        dmPending = { lo, hi };
        channel = dmChannelName(lo, hi); // tentative; ~n may allocate inside the txn
        dmMembersResolved = [lo, hi];
      }
      requestedChannel = dmChannelName(lo, hi);
      p = { ...p, to: peer };
    }

    // dm-shaped channel: an EXISTING channel row is used under its stored name
    // as-is — the name is a frozen label and channel_members is the authority
    // (G0: never re-sort current ids into a name after rename). Only a MISSING
    // dm channel routes through the pair-keyed creation path (client ~n is
    // stripped there — the pair, not the name, is the key).
    if (channel !== null && DM_SHAPED_RE.test(channel) && dmMembersResolved === null) {
      // M2-residual: detail is the CANONICAL pair name of the request (sorted,
      // ~n stripped) on EVERY branch — raw echo on hit vs canonical on miss is
      // a pair-existence oracle for reversed / suffixed names.
      const rq = DM_RE.exec(channel);
      if (rq) requestedChannel = dmChannelName(rq[1], rq[2]);
      if (d.query("SELECT 1 FROM channels WHERE name=?").get(channel)) {
        dmMembersResolved = dmMembers(channel);
      } else {
        const dm = DM_RE.exec(channel);
        if (!dm) return { error: "usage", detail: `invalid dm channel name: ${channel}` };
        const [lo, hi] = dm[1] < dm[2] ? [dm[1], dm[2]] : [dm[2], dm[1]];
        if (lo === hi) return { error: "usage", detail: "self-DM channel rejected (lo == hi)" };
        const hit = dmChannelForPair(lo, hi); // stored-name reuse under the frozen label
        if (hit) {
          channel = hit;
          dmMembersResolved = dmMembers(hit);
        } else {
          const g = dmGate(lo, hi, sender, dmRules);
          if (g) return g;
          dmPending = { lo, hi };
          channel = dmChannelName(lo, hi);
          dmMembersResolved = [lo, hi];
        }
      }
    }

    // (iv) dm thread-root equality — reached only after canSee passed above,
    // so a non-party never sees `usage` here.
    if (dmRules && threadRoot && DM_SHAPED_RE.test(threadRoot.channel) && channel !== threadRoot.channel)
      return { error: "usage", detail: "cross-channel attach into a dm thread is rejected" };

    // (i)+(ii) dm write rules on the RESOLVED channel, BEFORE any channel
    // write: invisible == missing (not_found, no oracle); wildcard/out-of-pair
    // recipients from a PARTY are usage.
    if (channel !== null && DM_SHAPED_RE.test(channel)) {
      const members = dmMembersResolved ?? dmMembers(channel);
      if (dmRules) {
        // M2: detail is REQUEST-derived (requestedChannel), never the stored
        // frozen name — a non-party must not learn the DM exists or its label.
        if (!members.includes(sender))
          return { error: "not_found", detail: `no such channel: ${requestedChannel ?? channel}` }; // == missing
        for (const t0 of csv(p.to)) {
          if (t0 === "@all" || t0.startsWith("group:") || !members.includes(t0))
            return { error: "usage", detail: `dm recipients must be literal member ids (subset of members; no @all/group/wildcards): ${t0}` };
        }
      }
    } else {
      channel = channel || "general";
      // E2: implicit create (post to an unknown channel) goes THROUGH the
      // near-duplicate guard — a typo'd lane costs the poster 1 s, not the
      // fleet a week (wildw_client lesson). Exact-name creates are untouched.
      if (channel !== "general" && !d.query("SELECT 1 FROM channels WHERE name=?").get(channel)) {
        const dup = channelDup(channel);
        if (dup) return { error: "usage", detail: `channel '${channel}' already exists as '${dup}' (near-duplicate — post to the existing name, or channel create a distinct one)` };
      }
      ensureChannel(channel, sender); // policy-free for public channels (G1)
    }
    const mid = newId(sender.split("-")[0]);
    // M5 (round 2): thread is DERIVED from the final id — recomputed on every
    // collision retry; an explicit p.thread always wins and never retargets.
    // E3: server-mode derivedThread (from re) also outranks the own-id default
    // and survives id-collision retries unchanged.
    let thread = p.thread || derivedThread || mid;
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
        // M3: missing-pair dm creation happens HERE — channel row + exactly 2
        // members + message in ONE txn (G2w-v). A reject before this point left
        // no channel row; a UNIQUE collision here re-reads the pair and posts
        // into the winner (never internal).
        if (dmPending) {
          const win = dmChannelForPair(dmPending.lo, dmPending.hi);
          if (win) channel = win;
          else {
            // M3-residual: re-run the pure gate INSIDE the txn — a rename that
            // committed between the pre-check and BEGIN must not let us mint a
            // dm with a now-retired member (m-c parity).
            const g2 = dmGate(dmPending.lo, dmPending.hi, sender, dmRules);
            if (g2) { d.exec("ROLLBACK"); return g2; }
            try { channel = dmCreateInTxn(dmPending.lo, dmPending.hi, sender); }
            catch (e: any) {
              if (!String(e?.message ?? e).includes("UNIQUE")) throw e;
              const w2 = dmChannelForPair(dmPending.lo, dmPending.hi);
              if (!w2) throw e;
              channel = w2;
            }
          }
          m.channel = channel;
          if (!dmMembers(channel).includes(sender)) {
            d.exec("ROLLBACK"); // never return out of an open txn
            return { error: "not_found", detail: `no such channel: ${requestedChannel ?? channel}` };
          }
        }
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
            if (!p.thread && !derivedThread) thread = id; // M5 (round 2): derived thread follows the new id (E3 derivedThread excepted)
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

  function inbox(ctx: Ctx<M>, p: { agent: string; open?: boolean; unread?: boolean; channel?: string | null; mark?: boolean; noAll?: boolean }): Res<{ rows: MsgRow[]; unreadIds: Set<string> }> {
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
    const mem = membershipsOf(p.agent);
    // G2: confidentiality subject is the CALLER. A for≠self peek (read:all)
    // sees the other's DM rows only with read:dm too; a self inbox sees own DMs
    // as member. Local mode: see-all quirk (canSeeChannel returns true).
    // n2 (claude): one peek rule everywhere — a DM row is peek-visible when the
    // CALLER holds read:dm OR is itself a member of that channel (matching the
    // read/waitStep behavior; the party exemption was missing only here).
    const callerSeesDm = (ch: string) =>
      canSeeChannel(ctx, ch) &&
      (p.agent === ctx.principal.agentId || isRootCtx(ctx) || !DM_SHAPED_RE.test(ch) || hasScope(ctx, "read:dm") ||
       dmMembers(ch).includes(ctx.principal.agentId));
    const rows = d.query("SELECT * FROM messages ORDER BY created_at ASC, rowid ASC").all() as MsgRow[];
    const readIds = new Set((d.query("SELECT msg FROM reads WHERE agent=?").all(p.agent) as any[]).map((x) => x.msg));
    const out = rows.filter((r) => {
      if (r.sender === p.agent) return false;
      if (p.channel && r.channel !== p.channel) return false;
      if (!callerSeesDm(r.channel)) return false;
      if (!recipientsMatch(r.recipients, p.agent, role, mem, r.created_at, p.noAll ? { ignoreAll: true } : undefined)) return false;
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
    // G2: canSee BEFORE the reads INSERT — a non-party probe must not write a
    // reads row nor emit a kind=read event leaking the id. Invisible == missing.
    if (!canSeeChannel(ctx, r.channel)) return { error: "not_found", detail: `no such message: ${p.id}` };
    if (mode === "local") touch(p.agent);
    const peek = !rootCtx && p.agent !== ctx.principal.agentId; // with read:all: NON-marking peek
    if (peek && DM_SHAPED_RE.test(r.channel) && !hasScope(ctx, "read:dm") && !dmMembers(r.channel).includes(p.agent))
      return { error: "not_found", detail: `no such message: ${p.id}` }; // peek needs read:dm for DMs
    if (!peek) d.run("INSERT OR REPLACE INTO reads(agent,msg,read_at) VALUES(?,?,?)", [p.agent, p.id, nowIso()]);
    return { value: { ...r, receipts: receiptsForMsg(r, ctx) } };
  }

  function threadOf(ctx: Ctx<M>, id: string): Res<{ rows: MsgRow[]; receipts: Receipts[] }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    // QUIRK (legacy): thread = explicit thread OR own id (de4ed3b semantics).
    const rows = d.query("SELECT * FROM messages WHERE thread=? OR id=? ORDER BY created_at ASC, rowid ASC").all(id, id) as MsgRow[];
    // G2: threads span channels ⇒ per-row canSee filter; not_found only if ALL
    // rows filtered; never build receipts for a hidden row.
    const vis = rows.filter((r) => canSeeChannel(ctx, r.channel));
    if (!vis.length) return { error: "not_found", detail: `no thread: ${id}` };
    return { value: { rows: vis, receipts: vis.map((r) => receiptsForMsg(r, ctx)) } };
  }

  function receipts(ctx: Ctx<M>, id: string): Res<MsgRow & { receipts: Receipts }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const r = d.query("SELECT * FROM messages WHERE id=?").get(id) as MsgRow | null;
    if (!r) return { error: "not_found", detail: `no such message: ${id}` };
    if (!canSeeChannel(ctx, r.channel)) return { error: "not_found", detail: `no such message: ${id}` }; // not_found BEFORE receiptsForMsg
    return { value: { ...r, receipts: receiptsForMsg(r, ctx) } };
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
    // G2: non-party on a dm row ⇒ not_found, NOT forbidden (no existence oracle).
    if (!canSeeChannel(ctx, r.channel)) return { error: "not_found", detail: `no such message: ${p.id}` };
    if (!rootCtx) {
      // §5: sender OR resolved intended recipient may ack/done/status;
      // agents:admin may set ANY. read:all is visibility (history/stream/peek),
      // NOT confidentiality — it must not gate status (round-2 M2).
      // F honesty: group membership DOES grant ack on group-addressed mail
      // (delivery-time resolved, includes earlier ones).
      const maySet = agent === r.sender
        || recipientsMatch(r.recipients, agent, roleOf(agent), membershipsOf(agent), r.created_at)
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
    // G2: dm-shaped rows hidden unless member or read:dm (local mode lists all).
    return { value: [...cmap.values()]
      .filter((c) => canSeeChannel(ctx, c.name))
      .sort((a, b) => String(b.last ?? "").localeCompare(String(a.last ?? ""))) };
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
    // G6: renaming TO a retired id is a one-way door; N3 symmetric: no other
    // agent may hold role == the new id.
    if (d.query("SELECT 1 FROM agent_retired WHERE id=?").get(p.to))
      return { error: "identity_conflict", detail: `error: id '${p.to}' was retired by an earlier rename — one-way door (G6).` };
    // B1 extension (claude probe 3): a legacy row resurrected by the old
    // touch() bug must get identity_conflict here, not internal UNIQUE failure
    // on agent_retired.id — the half-open door must refuse, not crash.
    if (d.query("SELECT 1 FROM agent_retired WHERE id=?").get(p.agent))
      return { error: "identity_conflict", detail: `error: id '${p.agent}' is retired (renamed away) — cannot rename a retired id.` };
    if ((d.query("SELECT id FROM agents WHERE role=? AND id!=?").get(p.to, p.agent) as any)?.id)
      return { error: "identity_conflict", detail: `error: '${p.to}' is held as a role by another agent (N3) — pick a free name.` };
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
        // M5 (claude NEW, ruling adopted): an implicit role==own-former-id is
        // an alias arm through the bare-token role match — contradicts C + R1
        // (no agent may hold role == a retired id) and floods preflight noise.
        // Rewrite the DEFAULT role (role == old id) to the new id. Explicit
        // roles are untouched. Golden unaffected (fixture roles != ids).
        d.run("UPDATE agents SET role=? WHERE id=? AND role=?", [p.to, p.to, p.agent]);
        d.run("UPDATE reads SET agent=? WHERE agent=?", [p.to, p.agent]);
        d.run("UPDATE tokens SET agent_id=? WHERE agent_id=?", [p.to, p.agent]);
        d.run("UPDATE cursors SET agent_id=? WHERE agent_id=?", [p.to, p.agent]);
        d.run("UPDATE idempotency SET agent_id=? WHERE agent_id=?", [p.to, p.agent]);
        // G6 (ruling a): membership cascades. OR IGNORE + leftover delete keeps
        // the PK safe; the leftover delete is REQUIRED (under the alias
        // alternative `new` may already be a member — claude n9).
        d.run("UPDATE OR IGNORE group_members SET agent_id=? WHERE agent_id=?", [p.to, p.agent]);
        d.run("DELETE FROM group_members WHERE agent_id=?", [p.agent]);
        d.run("UPDATE OR IGNORE channel_members SET agent_id=? WHERE agent_id=?", [p.to, p.agent]);
        d.run("DELETE FROM channel_members WHERE agent_id=?", [p.agent]);
        // G6: retire the old id (chains existing tombstones forward). Binds are
        // (new, old) then (old, new, nowIso) — swapping points the chain wrong.
        // NOT OR IGNORE: retargeting would reopen mail inheritance.
        d.run("UPDATE agent_retired SET renamed_to=? WHERE renamed_to=?", [p.to, p.agent]);
        d.run("INSERT INTO agent_retired(id,renamed_to,at) VALUES(?,?,?)", [p.agent, p.to, t]);
        // NO rewrite of message_recipients.target / recipients CSV /
        // messages.channel / mirror dirs / events — msg_ai won't re-fire and
        // mr_uq would throw when a message carries both ids (claude G6 probe).
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
    // G3: human defaults to BOTH read scopes (operators need DM visibility).
    else scopes = kind === "human" ? ["read:all", "read:dm"] : [];
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
        // G6/N3: EVERY agents-insert path runs the mint checks BEFORE the write,
        // inside this IMMEDIATE txn (m-c): retired-id reuse hard-rejects; an id
        // held as another agent's role hard-rejects (server) — local root is
        // the bootstrap path, so checks apply here too (INSERT OR IGNORE means
        // an EXISTING row is not a mint: checks only fire for new ids).
        // G6/N3 + B1: the retired check runs EVEN IF an agents row exists —
        // the exists-skip is the latch that let a touch()-resurrected id get
        // credentialed. A row this bug already created must not be mintable.
        if (d.query("SELECT 1 FROM agent_retired WHERE id=?").get(p.agent)) {
          d.exec("ROLLBACK");
          return { error: "identity_conflict", detail: `agent id '${p.agent}' is retired (renamed away) — cannot mint a token for it` };
        }
        if (!d.query("SELECT 1 FROM agents WHERE id=?").get(p.agent)) {
          const mc = idMintChecks(p.agent);
          if (mc?.error) { d.exec("ROLLBACK"); return mc.error; }
          for (const w of mc?.warnings ?? []) seams.warn?.(w);
        }
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
    // B1-legacy (claude r2): a token minted for a touch()-resurrected retired id
    // under b922c1f is still live — the one-way door fails CLOSED at verify.
    if (d.query("SELECT 1 FROM agent_retired WHERE id=?").get(row.agent_id))
      return { error: "unauthorized", detail: "unknown or revoked token" };
    const digest = createHmac("sha256", Buffer.from(row.salt)).update(token).digest();
    if (digest.length !== row.key_hash.length || !timingSafeEqual(digest, Buffer.from(row.key_hash)))
      return { error: "unauthorized", detail: "unknown or revoked token" }; // claude M2 n: ONE detail — "bad token" vs "unknown" was a prefix-existence oracle
    const kind = ((d.query("SELECT kind FROM agents WHERE id=?").get(row.agent_id) as any)?.kind ?? "agent") as "agent" | "human";
    return { value: { agentId: row.agent_id, scopes: normalizeScopes(csv(row.scopes)).split(",").filter(validScope) as Scope[], kind, tokenId: row.id } }; // finding 20: tokenId; legacy 8-name rows: unknown tokens are inert (M8)
  }

  /** Re-resolve a principal from the token ROW by id (§5: every request /
   *  every SSE tick / every cookie request). `live` is the SAME predicate
   *  tokenVerify applies minus the HMAC: not revoked AND agent_id not retired
   *  (B1-legacy one-way door) — callers must not re-derive it. */
  function tokenById(id: number): { agentId: string; scopes: Scope[]; kind: "agent" | "human"; revoked: boolean; retired: boolean; live: boolean } | null {
    const row = d.query("SELECT agent_id, scopes, revoked_at FROM tokens WHERE id=?").get(id) as any;
    if (!row) return null;
    const retired = d.query("SELECT 1 FROM agent_retired WHERE id=?").get(row.agent_id) !== null;
    const kind = ((d.query("SELECT kind FROM agents WHERE id=?").get(row.agent_id) as any)?.kind ?? "agent") as "agent" | "human";
    const revoked = row.revoked_at !== null;
    return { agentId: row.agent_id, scopes: normalizeScopes(csv(row.scopes)).split(",").filter(validScope) as Scope[], kind, revoked, retired, live: !revoked && !retired };
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

  /** returns true iff it wrote (the ≤1/min debounce passed) — the server
   *  piggybacks the presence refresh on the same debounce (claude M3 M7). */
  function tokenTouch(id: number): boolean {
    const row = d.query("SELECT last_used FROM tokens WHERE id=?").get(id) as any;
    if (!row) return false;
    const prev = Date.parse(row.last_used);
    if (Number.isFinite(prev) && seams.now().getTime() - prev < 60_000) return false;
    d.run("UPDATE tokens SET last_used=? WHERE id=?", [nowIso(), id]);
    return true;
  }

  // ---------- cursors (§6) ----------

  function epoch(): string {
    return (d.query("SELECT value FROM meta WHERE key='epoch'").get() as any).value;
  }
  // claude round-5 m1c: epoch reads on Res-returning paths must not throw raw.
  function epochSafe(): string | null {
    return (d.query("SELECT value FROM meta WHERE key='epoch'").get() as any)?.value ?? null;
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
  // claude M3 m2: consumer is caller-supplied (--consumer / cursor.set) —
  // grammar + byte cap, else one token can mint unbounded long cursor rows.
  // Cap is 128, not the 64 first proposed: the CLI's own namespaced keys reach
  // cli@<id32>.all#<dm~id32~id32~NNNN> = 114 B, and a 64 cap made
  // `watch --channel <long dm>` exit 2 on its own generated consumer.
  const CONSUMER_RE = /^[a-z0-9._#@~-]{1,128}$/;
  const CONSUMER_BAD: Res<never> = { error: "usage", detail: "consumer must match [a-z0-9._#@~-]{1,128}" };
  function cursorGet(agentId: string, consumer: string): Res<{ epoch: string; seq: number }> {
    if (!CONSUMER_RE.test(consumer)) return CONSUMER_BAD;
    const e = epochSafe();
    if (e === null) return { error: "internal", detail: "meta.epoch missing (corrupt DB)" };
    const row = cursorRaw(agentId, consumer);
    if (!row) return { value: { epoch: e, seq: 0 } };
    if (row.epoch !== e)
      // m2 (M2 card hygiene): ONE resync payload shape on the wire — always
      // include floor (rotateEpoch zeroes it; floor=0 is still explicit).
      return { error: "resync", detail: "cursor epoch stale (epoch rotated)", data: { resync: true, epoch: e, floor: gcFloor() } };
    return { value: { epoch: e, seq: row.seq } };
  }

  function cursorSet(agentId: string, consumer: string, ep: string, seq: number, force = false): Res<null> {
    if (!CONSUMER_RE.test(consumer)) return CONSUMER_BAD;
    if (!Number.isFinite(seq) || !/^[0-9a-f]{8,64}$/.test(ep))
      return { error: "usage", detail: "cursor must be <hex-epoch>.<int-seq>" };
    const e = epochSafe();
    if (e === null) return { error: "internal", detail: "meta.epoch missing (corrupt DB)" };
    if (ep !== e) return { error: "resync", detail: "epoch mismatch; resync required", data: { resync: true, epoch: e, floor: gcFloor() } };
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
    const e = epochSafe();
    if (e === null) return { error: "internal", detail: "meta.epoch missing (corrupt DB)" };
    if (m[1] !== e) return { error: "resync", detail: "epoch mismatch", data: { resync: true, epoch: e, floor: gcFloor() } };
    const seq = Number(m[2]);
    if (seq < gcFloor()) return { error: "resync", detail: "cursor below retention floor", data: { resync: true, epoch: e, floor: gcFloor() } };
    return { value: { ep: e, seq } };
  }

  function history(ctx: Ctx<M>, p: { channel?: string | null; limit?: number; since?: string }): Res<{ rows: MsgRow[]; hasMore: boolean; cursor: string }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    // claude M3 m3 + ruling (c): the gate is the UNFILTERED GLOBAL SNAPSHOT
    // only. A channel-filtered or since-paged view returns exactly what the
    // canSee row predicate allows — public rows are readable by every token
    // (§5), dm~ rows need membership or read:dm. This makes history{channel}
    // consistent with stream scope=channel: (both ungated). No 6th scope.
    // grok 3433 M-a: ONE normalization feeds BOTH the gate and the query. A
    // wrong-typed channel (false / 0 / true from a bare --channel) is a usage
    // error (post param-type precedent); "" names no channel ⇒ it IS the
    // unfiltered snapshot. The gate and the SQL must never disagree on it.
    if (p.channel !== undefined && p.channel !== null && typeof p.channel !== "string")
      return { error: "usage", detail: "history channel must be a string" };
    const chan = p.channel ? p.channel : null;
    const unfiltered = chan === null && !p.since;
    if (!isRootCtx(ctx) && unfiltered && !ctx.principal.scopes.includes("read:all"))
      return { error: "forbidden", detail: "unfiltered history snapshot requires read:all" };
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
      // claude round-5 m1: epoch read INSIDE the txn before COMMIT — a
      // cross-process rotateEpoch between COMMIT and epoch() would pair the
      // new epoch with the old hw (exactly the cross-epoch pairing epochs
      // exist to prevent; §6 says "same txn").
      const ep = epochSafe();
      if (ep === null) throw new Error("meta.epoch missing (corrupt DB)");
      const dm = hasScope(ctx, "read:dm") ? 1 : 0;
      if (!ascending) {
        // N2 (round 3): SNAPSHOT pages over MESSAGES (§6 "history returns
        // messages, not events") — events are retention-bounded and absent for
        // pre-events legacy rows. Cursor = events high-water read in the SAME
        // txn (§6), so the stream handoff has no hole; dedupe by msg_id.
        // G2 (grok B1 pin): canSee is the ROW PREDICATE here, in BOTH modes —
        // read:all alone does NOT satisfy canSee; DM omniview = read:all AND
        // read:dm. Predicate in WHERE, never a post-LIMIT filter.
        const visSql = `(m.channel NOT GLOB 'dm~*' OR EXISTS (SELECT 1 FROM channel_members cm WHERE cm.channel = m.channel AND cm.agent_id = ?) OR ? = 1)`;
        const q = chan
          ? d.query(`SELECT m.* FROM messages m WHERE m.channel=? AND ${visSql} ORDER BY m.created_at DESC, m.rowid DESC LIMIT ?`)
          : d.query(`SELECT m.* FROM messages m WHERE ${visSql} ORDER BY m.created_at DESC, m.rowid DESC LIMIT ?`);
        const got = (chan ? q.all(chan, ctx.principal.agentId, dm, limit + 1) : q.all(ctx.principal.agentId, dm, limit + 1)) as MsgRow[];
        const hw = Math.max((d.query("SELECT coalesce(max(seq),0) m FROM events").get() as any).m as number, gcFloor());
        d.exec("COMMIT");
        const rows = got.slice(0, limit).reverse();
        return { value: { rows, hasMore: got.length > limit, cursor: `${ep}.${hw}` } };
      }
      const dir = ascending ? "ASC" : "DESC";
      // G2 m-d: same predicate in the WHERE BEFORE LIMIT — a post-LIMIT JS
      // filter livelocks when a whole page is hidden (cursor never advances).
      const evs = (chan
        ? d.query(`SELECT e.seq,e.msg_id FROM events e JOIN messages m ON m.id=e.msg_id WHERE e.kind='msg' AND e.seq>? AND m.channel=? AND (m.channel NOT GLOB 'dm~*' OR EXISTS (SELECT 1 FROM channel_members cm WHERE cm.channel=m.channel AND cm.agent_id=?) OR ?=1) ORDER BY e.seq ${dir} LIMIT ?`)
        : d.query(`SELECT e.seq,e.msg_id FROM events e JOIN messages m ON m.id=e.msg_id WHERE e.kind='msg' AND e.seq>? AND (m.channel NOT GLOB 'dm~*' OR EXISTS (SELECT 1 FROM channel_members cm WHERE cm.channel=m.channel AND cm.agent_id=?) OR ?=1) ORDER BY e.seq ${dir} LIMIT ?`)) as any;
      const got = (chan ? evs.all(seqFrom, chan, ctx.principal.agentId, dm, limit + 1) : evs.all(seqFrom, ctx.principal.agentId, dm, limit + 1)) as { seq: number; msg_id: string }[];
      const hasMore = got.length > limit;
      const page = ascending ? got.slice(0, limit) : got.slice(0, limit).reverse();
      const rows = page.map((e) => d.query("SELECT * FROM messages WHERE id=?").get(e.msg_id) as MsgRow).filter(Boolean);
      const lastSeq = page.length ? page[page.length - 1].seq : seqFrom;
      d.exec("COMMIT");
      return { value: { rows, hasMore, cursor: `${ep}.${lastSeq}` } };
    } catch (e) {
      // M4 (grok round 3 nit): history must not throw out of the Res contract
      // (post/rename precedent) — a SQL failure is `internal`, same as everywhere.
      // claude round-5 m1: only roll back when a txn is actually open — a throw
      // AFTER COMMIT would otherwise make ROLLBACK itself throw ("no transaction
      // is active") and escape the Res contract.
      if (d.inTransaction) d.exec("ROLLBACK");
      return { error: "internal", detail: `history: ${String(e)}` };
    }
  }

  /** finding 9/11: the inboxWait SCAN lives here — server-reachable, fully
   *  asserted (read:all gate, epoch check, malformed cursor). The wait LOOP
   *  (backoff) is transport-side via seams.sleep. */
  function waitStep(ctx: Ctx<M>, p: { for?: string; consumer?: string; since?: string; noAll?: boolean }): Res<{ messages: MsgRow[]; cursor: string; done: boolean }> {
    const bad = ctxCheck(ctx); if (bad) return bad;
    const target = p.for ?? ctx.principal.agentId;
    if (target !== ctx.principal.agentId && !hasScope(ctx, "read:all"))
      return { error: "forbidden", detail: "inbox.wait for another agent requires read:all" };
    const consumer = p.consumer ?? "default";
    // claude M3-fold: same grammar on the third consumer entry point (the RFC
    // row says `consumer` matches it — inbox.wait must not accept what
    // cursor.set would then reject, stranding the at-least-once commit).
    if (!CONSUMER_RE.test(consumer)) return CONSUMER_BAD;
    const ep0 = epochSafe();
    if (ep0 === null) return { error: "internal", detail: "meta.epoch missing (corrupt DB)" };
    let ep = ep0; let seq: number;
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
    // F: membership resolves at TAIL time from the same-snapshot Map (a since=
    // resume replays against CURRENT membership — late-joiner semantics).
    const mem = membershipsOf(target);
    const messages: MsgRow[] = [];
    let cur = seq;
    for (const e of tailEvents(seq, 500)) {
      cur = e.seq;
      if (e.kind !== "msg" || !e.msg_id) continue;
      const m = d.query("SELECT * FROM messages WHERE id=?").get(e.msg_id) as MsgRow | null;
      // G2: SSE/stream canSee on EVERY event carrying a msg_id (server mode).
      // E1: noAll opts this consumer out of the @all broadcast arm (auditor
      // watch). The CALLER must namespace the consumer (cli.noall…) — a
      // different predicate must never share a cursor row with the default.
      if (m && m.sender !== target && canSeeChannel(ctx, m.channel) && recipientsMatch(m.recipients, target, role, mem, m.created_at, p.noAll ? { ignoreAll: true } : undefined)) messages.push(m);
    }
    return { value: { messages, cursor: `${ep}.${cur}`, done: messages.length > 0 } };
  }

  // high-water seq for the SSE hello frame (§3: no SQL above the core)
  function eventsHighWater(): number {
    return Math.max((d.query("SELECT coalesce(max(seq),0) m FROM events").get() as any).m as number, gcFloor());
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

  function preflight(): { badIds: string[]; badChannels: string[]; zeroMemberDms: string[]; roleCollisions: string[]; phantomMembers: string[]; resurrectedIds: string[] } {
    const badIds = (d.query("SELECT DISTINCT id FROM agents").all() as any[])
      .map((r) => r.id).filter((id: string) => id && !ID_RE.test(id));
    // G1: preflight uses the SAME widened write-gate helper as post() — else
    // every legitimate DM is flagged bad.
    const badChannels = (d.query("SELECT DISTINCT sender FROM messages").all() as any[])
      .map((r) => r.sender).filter((s: string) => s && !ID_RE.test(s))
      .concat((d.query("SELECT name FROM channels").all() as any[]).map((r) => r.name).filter((n: string) => n && !validChannelName(n)));
    // G5 C1: zero-member dm-shaped channels are the documented repair surface.
    const zeroMemberDms = (d.query("SELECT name FROM channels WHERE name GLOB 'dm~*'").all() as any[])
      .map((r) => r.name)
      .filter((n: string) => (d.query("SELECT count(*) c FROM channel_members WHERE channel=?").get(n) as any).c === 0);
    // §5: legacy roles colliding with ids or retired ids (write-path checks
    // don't rewrite legacy rows; preflight lists offenders).
    const roleCollisions = (d.query("SELECT id, role FROM agents WHERE role IS NOT NULL").all() as any[])
      .filter((r) => d.query("SELECT 1 FROM agents WHERE id=? AND id!=?").get(r.role, r.id) || d.query("SELECT 1 FROM agent_retired WHERE id=?").get(r.role))
      .map((r) => `${r.id}!role=${r.role}`);
    return { badIds, badChannels: [...new Set(badChannels)], zeroMemberDms, roleCollisions, phantomMembers: phantomMembersNow(),
      // B1-legacy: agents rows resurrected by the b922c1f touch() bug (repair surface).
      resurrectedIds: (d.query("SELECT a.id FROM agents a JOIN agent_retired r ON r.id = a.id").all() as any[]).map((r) => r.id) };
  }
  // n1 (claude nit): channel_members rows pointing at non-agents (decision A
  // allows a local DM to a not-yet-minted id) — listed so hosts can repair.
  // Computed PER CALL (a handle-open snapshot goes stale — same latch class as M4).
  const phantomMembersNow = () => (d.query("SELECT DISTINCT agent_id FROM channel_members").all() as any[])
    .map((r) => r.agent_id)
    .filter((id: string) => !d.query("SELECT 1 FROM agents WHERE id=?").get(id));

  // local watch helpers (finding 9: no SQL above the core)
  function allMessages(): MsgRow[] {
    return d.query("SELECT * FROM messages ORDER BY created_at ASC, rowid ASC").all() as MsgRow[];
  }
  function allMessageIds(): string[] {
    return (d.query("SELECT id FROM messages").all() as any[]).map((r) => r.id);
  }
  // point lookup for the SSE broadcaster (§3: no SQL above the core)
  function messageById(id: string): MsgRow | null {
    return (d.query("SELECT * FROM messages WHERE id=?").get(id) as MsgRow | null) ?? null;
  }

  // §7 /raw: resolve a mirror filename back to its authoritative message row.
  // (SQLite is the source of truth — the file is just the byte we serve.)
  // messages.file is stored home-relative ("messages/<channel>/<name>"), so
  // rebuild that exact shape — a basename match against another channel's
  // identically-named file can never slip through.
  function messageByFile(channel: string, file: string): MsgRow | null {
    return (d.query("SELECT * FROM messages WHERE channel=? AND file=?").get(channel, `messages/${channel}/${file}`) as MsgRow | null) ?? null;
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
    tokenCreate, tokenVerify, tokenById, tokenList, tokenRevoke, tokenTouch,
    cursorGet, cursorSet, history, waitStep, tailEvents, eventsHighWater, epoch, gcFloor, rotateEpoch, gc, preflight,
    allMessages, allMessageIds, messageById, messageByFile, ensureChannel,
    groupCreate, groupJoin, groupLeave, groupDelete, groupList, groupShow, channelCreate,
    canSeeChannel, membershipsOf, deliveredMsgIds, dmMembers, dmMembersFor, dmChannelForPair,
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
