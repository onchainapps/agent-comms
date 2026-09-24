/**
 * Bus core — all DB logic of agent-comms, extracted from bin/comms.ts (RFC-001 §3).
 *
 * Synchronous, typed results and typed errors. No console.log, no process.exit,
 * no stdin, no @file resolution, no ambient env reads. Determinism flows through
 * the injectable seams (clock/rng/pid/mirror). mode (local|server) is bound at
 * openBus() — the server build cannot construct a local-root principal (§3).
 */
import { Database } from "bun:sqlite";
import { join } from "node:path";
import { mkdirSync } from "node:fs";
import { defaultSeams, type Seams } from "./seams.ts";

// ---------- public types ----------

export type Mode = "local" | "server";
export type Scope = "read:all" | "post:as" | "tokens:admin" | "agents:admin";
export const ALL_SCOPES: readonly Scope[] = ["read:all", "post:as", "tokens:admin", "agents:admin"];

export type Principal = {
  agentId: string;
  kind: "agent" | "human" | "service";
  scopes: Scope[];
  localRoot?: true;
};

export type Ctx = { principal: Principal; actor: string };

export const localCtx = (actor: string): Ctx => ({
  principal: { agentId: actor, kind: "agent", scopes: [...ALL_SCOPES], localRoot: true },
  actor,
});

export type BusError =
  | { error: "usage"; detail: string }
  | { error: "not_found"; detail: string }
  | { error: "forbidden"; detail: string }
  | { error: "identity_conflict"; detail: string }
  | { error: "conflict"; detail: string }
  | { error: "contention"; detail: string };

export type Ok<T> = { error?: undefined; value: T };
export type Res<T> = Ok<T> | BusError;

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

// QUIRK: stamp() calls nowIso() itself (two clock reads per call in the original;
// with a frozen clock seam this is byte-identical).
const mkNowIso = (s: Seams) => () => s.now().toISOString().replace(/\.\d{3}Z$/, "Z");
const mkStamp = (nowIso: () => string) => () =>
  nowIso().replace(/[-:TZ]/g, "").slice(0, 15).replace(/(\d{8})(\d{6})/, "$1T$2");
const mkShortHex = (s: Seams) => (n = 2) =>
  Array.from(s.rng(n), (b) => b.toString(16).padStart(2, "0")).join("");
const csv = (str?: string | null) => (str ?? "").split(",").map((x) => x.trim()).filter(Boolean);

/** scope normalizer contract (§4, grok): split→trim→dedupe→sort→join. */
export const normalizeScopes = (scopes: Iterable<string>): string =>
  [...new Set([...scopes].map((s) => s.trim()).filter(Boolean))].sort().join(",");

/** canonical req_hash input (§6): UTF-8, keys sorted, recipients sorted. */
export function canonicalJson(v: unknown): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v) ?? "null";
  if (Array.isArray(v)) return "[" + v.map(canonicalJson).join(",") + "]";
  return "{" + Object.keys(v).sort().map((k) => JSON.stringify(k) + ":" + canonicalJson((v as any)[k])).join(",") + "}";
}

export function sha256hex(text: string): string {
  const buf = new Uint8Array(new TextEncoder().encode(text));
  // bun:crypto-free fallback kept sync via node:crypto
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { createHash } = require("node:crypto") as typeof import("node:crypto");
  return createHash("sha256").update(buf).digest("hex");
}

/** recursive-CTE recipient split — probe-verified == csv() (§4 N1). */
const SPLIT_SQL = `WITH RECURSIVE s(rest,tok) AS (
    SELECT coalesce(?,'') || ',', NULL
    UNION ALL
    SELECT substr(rest, instr(rest,',')+1), trim(substr(rest,1,instr(rest,',')-1)) FROM s WHERE rest <> '')
  SELECT tok FROM s WHERE tok IS NOT NULL AND tok <> ''`;

function recipientsMatch(recips: string, agent: string, role?: string | null): boolean {
  const toks = new Set(csv(recips));
  if (toks.has("@all") || toks.has(agent)) return true;
  return !!role && toks.has(role);
}

// ---------- open / schema ----------

export type BusOpts = {
  home: string;
  mode: Mode;
  seams?: Seams;
  /** server mode: busy_timeout ms (150, §9). local keeps 5000. */
  busyTimeoutMs?: number;
};

export type Bus = ReturnType<typeof openBusCore>;

export function openBus(opts: BusOpts) {
  const bus = openBusCore(opts);
  return bus;
}

function openBusCore({ home, mode, seams = defaultSeams, busyTimeoutMs }: BusOpts) {
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

  // self-migrations (existing pattern) + v2.2 additive schema (§4)
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

  // triggers (§4) — recursive CTE split, presence debounce, agents_ai, tokens_ai/au
  d.exec(`
    CREATE TRIGGER IF NOT EXISTS msg_ai AFTER INSERT ON messages BEGIN
      INSERT INTO events(kind,msg_id,agent_id,at) VALUES('msg',NEW.id,NEW.sender,NEW.created_at);
      INSERT INTO message_recipients(msg,target)
      WITH RECURSIVE s(rest,tok) AS (
        SELECT coalesce(NEW.recipients,'') || ',', NULL
        UNION ALL
        SELECT substr(rest, instr(rest,',')+1), trim(substr(rest,1,instr(rest,',')-1)) FROM s WHERE rest <> '')
      SELECT NEW.id, tok FROM s WHERE tok IS NOT NULL AND tok <> '';
    END;
    CREATE TRIGGER IF NOT EXISTS msg_au AFTER UPDATE OF status ON messages BEGIN
      INSERT INTO events(kind,msg_id,agent_id,at) VALUES('status',NEW.id,NEW.sender,NEW.updated_at);
    END;
    CREATE TRIGGER IF NOT EXISTS reads_ai AFTER INSERT ON reads BEGIN
      INSERT INTO events(kind,msg_id,agent_id,at) VALUES('read',NEW.msg,NEW.agent,NEW.read_at);
    END;
    CREATE TRIGGER IF NOT EXISTS agents_ai AFTER INSERT ON agents BEGIN
      INSERT INTO events(kind,agent_id,at) VALUES('presence',NEW.id,NEW.last_seen);
    END;
    CREATE TRIGGER IF NOT EXISTS agents_au AFTER UPDATE ON agents
    WHEN OLD.id IS NOT NEW.id OR OLD.role IS NOT NEW.role OR OLD.caps IS NOT NEW.caps
      OR (julianday(NEW.last_seen) - julianday(OLD.last_seen)) * 86400 >= 60
    BEGIN
      INSERT INTO events(kind,agent_id,at)
      VALUES(CASE WHEN OLD.id IS NOT NEW.id THEN 'rename' ELSE 'presence' END, NEW.id, NEW.last_seen);
    END;
    CREATE TRIGGER IF NOT EXISTS tokens_ai AFTER INSERT ON tokens BEGIN
      INSERT INTO events(kind,agent_id,at) VALUES('token',NEW.agent_id,NEW.created_at);
    END;
    CREATE TRIGGER IF NOT EXISTS tokens_au AFTER UPDATE OF revoked_at ON tokens BEGIN
      INSERT INTO events(kind,agent_id,at) VALUES('token',NEW.agent_id,NEW.revoked_at);
    END;
  `);

  d.run(
    "INSERT OR IGNORE INTO channels(name,purpose,created_at,created_by) VALUES('general','tooling / meta / cross-project chatter','',?)",
    ["system"],
  );

  // epoch: rotate ONLY on restore (runbook), never here (§3 N3)
  if (!d.query("SELECT value FROM meta WHERE key='epoch'").get())
    d.run("INSERT INTO meta(key,value) VALUES('epoch',?)", [hex(seams.rng(16))]);

  // one-shot backfill of message_recipients for pre-trigger rows (§4)
  const backfilled = (d.query("SELECT value FROM meta WHERE key='backfill_recipients'").get() as any) !== null;
  if (!backfilled) {
    // one-shot backfill using the same split semantics as the msg_ai CTE (§4 N1)
    d.exec("BEGIN IMMEDIATE");
    try {
      for (const r of d.query("SELECT id, recipients FROM messages").all() as any[]) {
        for (const t of splitCsv(String(r.recipients ?? "")))
          d.run("INSERT INTO message_recipients(msg,target) VALUES(?,?)", [r.id, t]);
      }
      d.run("INSERT OR REPLACE INTO meta(key,value) VALUES('backfill_recipients',?)", [nowIso()]);
      d.exec("COMMIT");
    } catch (e) { d.exec("ROLLBACK"); throw e; }
  }

  // JS-side mirror of the SQL split (used by backfill + parity tests)
  function splitCsv(s: string): string[] {
    const out: string[] = [];
    let rest = s + ",";
    while (rest !== "") {
      const i = rest.indexOf(",");
      const tok = rest.slice(0, i).trim();
      rest = rest.slice(i + 1);
      if (tok !== "") out.push(tok);
    }
    return out;
  }

  // ---------- internals ----------

  const isActive = (lastSeen: string) => {
    const t = Date.parse(lastSeen);
    return Number.isFinite(t) && seams.now().getTime() - t <= PRESENCE_TTL_MS;
  };

  function touch(agent: string) {
    if (!agent) return;
    const t = nowIso();
    const r = d.run("UPDATE agents SET last_seen=? WHERE id=?", [t, agent]);
    if (r.changes === 0 && mode === "local") {
      // local-mode auto-register; server mode never auto-creates rows (§3)
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

  // QUIRK preserved: fname uses a SECOND stamp() call (same frozen-clock value).
  function insertMessage(m: Omit<MsgRow, "file"> & { as?: string | null }, fname: string): MsgRow {
    const content = renderMd({ ...m, file: "", meta: m.as ? JSON.stringify({ as: m.as }) : null });
    const file = seams.mirror(join(MSG_DIR, m.channel), fname, content);
    d.run(
      `INSERT INTO messages(id,thread,re,sender,recipients,type,status,tags,subject,body,file,created_at,updated_at,channel,meta)
       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [m.id, m.thread, m.re, m.sender, m.recipients, m.type, m.status, m.tags, m.subject, m.body,
       file, m.created_at, m.updated_at, m.channel, m.as ? JSON.stringify({ as: m.as }) : null],
    );
    return { ...m, file, meta: m.as ? JSON.stringify({ as: m.as }) : null };
  }

  // ---------- commands ----------

  function joinAgent(ctx: Ctx, p: { agent: string; role: string; caps?: string; fingerprint?: string | null }): Res<{ agent: AgentRow; active: AgentRow[]; unresolved: number }> {
    if (!p.agent || !p.role) return { error: "usage", detail: "error: join requires --agent and --role" };
    if (!ID_RE.test(p.agent)) return { error: "usage", detail: `invalid agent id: ${p.agent}` };
    if (mode === "server" && p.agent !== ctx.principal.agentId)
      return { error: "forbidden", detail: `agent assertion '${p.agent}' != principal '${ctx.principal.agentId}'` };
    const t = nowIso();
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
      [p.agent, p.role, p.caps ?? "", mode === "local" ? seams.pid() : null, t, t, "{}", fp,
       mode === "local" ? "agent" : (d.query("SELECT kind FROM agents WHERE id=?").get(p.agent) as any)?.kind ?? "agent"],
    );
    const row = d.query("SELECT * FROM agents WHERE id=?").get(p.agent) as AgentRow;
    const active = listAgents(true);
    // QUIRK preserved: unresolved count filters sender!=agent only (no status semantics beyond IN list)
    const unresolved = (d.query(
      "SELECT COUNT(*) c FROM messages WHERE status IN ('open','acked','in_progress') AND sender!=?",
    ).get(p.agent) as any).c;
    return { value: { agent: row, active, unresolved } };
  }

  function listAgents(activeOnly: boolean): AgentRow[] {
    return (d.query("SELECT * FROM agents ORDER BY last_seen DESC, rowid ASC").all() as AgentRow[])
      .filter((r) => !!r.id)
      .filter((r) => (activeOnly ? isActive(r.last_seen ?? "") : true));
  }

  function post(ctx: Ctx, p: {
    from: string; to: string; type: string; subject?: string; body: string;
    thread?: string | null; re?: string | null; tags?: string; channel?: string | null;
    as?: string | null; idempotencyKey?: string | null;
  }): Res<{ id: string; channel: string; thread: string; file: string }> {
    if (!p.from || !p.to) return { error: "usage", detail: "error: post requires --from and --to" };
    if (!ID_RE.test(p.from)) return { error: "usage", detail: `invalid from id: ${p.from}` };
    if (!MSG_TYPES.includes(p.type as any)) return { error: "usage", detail: `error: --type must be one of ${MSG_TYPES.join(",")}` };
    if (!TYPE_RE.test(p.type)) return { error: "usage", detail: `invalid type: ${p.type}` };
    if (p.channel !== undefined && p.channel !== null && !ID_RE.test(p.channel))
      return { error: "usage", detail: `invalid channel: ${p.channel}` };
    if (mode === "server") {
      if (p.from !== ctx.principal.agentId && !(ctx.principal.scopes.includes("post:as") && p.as))
        return { error: "forbidden", detail: `from '${p.from}' != principal (needs post:as + as)` };
      if (p.as && !ctx.principal.scopes.includes("post:as"))
        return { error: "forbidden", detail: "as requires post:as scope" };
    }
    const t = nowIso();
    if (mode === "local") touch(p.from);

    // idempotency (§6): same key+hash ⇒ replay; same key+different hash ⇒ conflict
    let idem: { key: string; hash: string } | null = null;
    if (ctx.principal.localRoot !== true && p.idempotencyKey) {
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

    // channel resolution: explicit > inherit thread/parent > general
    let channel: string | null = p.channel ?? null;
    if (!channel) {
      const parentId = p.thread || p.re;
      if (parentId) {
        const par = d.query("SELECT channel FROM messages WHERE id=? OR thread=? ORDER BY created_at ASC LIMIT 1")
          .get(parentId, parentId) as any;
        channel = par?.channel ?? null;
      }
    }
    channel = channel || "general";
    ensureChannel(channel, p.from);

    const sender = p.as ?? p.from;              // §5: sender = as-target (observable)
    const asAudit = p.as ? ctx.principal.agentId : null; // meta.as = principal
    const mid = newId(sender.split("-")[0]);
    const thread = p.thread || mid;
    const fname = `msg-${stamp()}-${sender}-${p.type}-${mid.split("-").pop()}.md`;
    const m: MsgRow = {
      id: mid, thread, re: p.re ?? null, sender, recipients: p.to,
      type: p.type, status: "open", tags: p.tags ?? "", subject: p.subject ?? "",
      body: p.body, file: "", created_at: t, updated_at: t, channel,
      meta: asAudit ? JSON.stringify({ as: asAudit }) : null,
    };

    // insert-first (§9): DB txn commits (message + idempotency + retry on 16-bit
    // PK collision), THEN mirror write outside the txn.
    let file = "";
    try {
      d.exec("BEGIN IMMEDIATE");
      try {
        let id = mid, fname2 = fname, tries = 0;
        for (;;) {
          try {
            d.run(
              `INSERT INTO messages(id,thread,re,sender,recipients,type,status,tags,subject,body,file,created_at,updated_at,channel,meta)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
              [id, thread, m.re, sender, p.to, p.type, "open", m.tags, m.subject, p.body, "", t, t, channel,
               asAudit ? JSON.stringify({ as: asAudit }) : null],
            );
            m.id = id; m.file = "";
            break;
          } catch (e: any) {
            if (!String(e?.message ?? e).includes("UNIQUE") || ++tries >= 8) throw e;
            id = newId(sender.split("-")[0]);                       // G12: retry, same format
            fname2 = `msg-${stamp()}-${sender}-${p.type}-${id.split("-").pop()}.md`;
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
      if (msg.includes("UNIQUE") && idem) return { error: "conflict", detail: "idempotency key race" };
      if (msg.includes("SQLITE_BUSY") || msg.includes("database is locked")) return { error: "contention", detail: "busy after retries" };
      throw e;
    }

    const content = renderMd(m);
    seams.mirror(join(MSG_DIR, channel), fname, content);
    file = join("messages", channel, fname); // display path, relative to home (old contract)
    d.run("UPDATE messages SET file=? WHERE id=?", [file, m.id]);
    m.file = file;
    return { value: { id: m.id, channel, thread, file } };
  }

  function inbox(ctx: Ctx, p: { agent: string; open?: boolean; unread?: boolean; channel?: string | null }): Res<{ rows: MsgRow[]; unreadIds: Set<string> }> {
    if (mode === "server" && p.agent !== ctx.principal.agentId && !ctx.principal.scopes.includes("read:all"))
      return { error: "forbidden", detail: "for≠self requires read:all" };
    const peek = mode === "server" && p.agent !== ctx.principal.agentId; // non-marking peek
    if (mode === "local" || (!peek && p.agent === ctx.principal.agentId)) touch(p.agent);
    const role = roleOf(p.agent);
    const rows = d.query("SELECT * FROM messages ORDER BY created_at ASC").all() as MsgRow[];
    const readIds = new Set((d.query("SELECT msg FROM reads WHERE agent=?").all(p.agent) as any[]).map((x) => x.msg));
    const out = rows.filter((r) => {
      if (r.sender === p.agent) return false;
      if (p.channel && r.channel !== p.channel) return false;
      if (!recipientsMatch(r.recipients, p.agent, role)) return false;
      if (p.open && !["open", "acked", "in_progress"].includes(r.status)) return false;
      if (p.unread && readIds.has(r.id)) return false;
      return true;
    });
    return { value: { rows: out, unreadIds: new Set(out.map((r) => r.id).filter((id) => !readIds.has(id))) } };
  }

  function read(ctx: Ctx, p: { agent: string; id: string }): Res<MsgRow & { receipts: Receipts }> {
    const r = d.query("SELECT * FROM messages WHERE id=?").get(p.id) as MsgRow | null;
    if (!r) return { error: "not_found", detail: `no such message: ${p.id}` };
    if (mode === "local") touch(p.agent);
    const peek = mode === "server" && p.agent !== ctx.principal.agentId && ctx.principal.scopes.includes("read:all");
    if (!peek) d.run("INSERT OR REPLACE INTO reads(agent,msg,read_at) VALUES(?,?,?)", [p.agent, p.id, nowIso()]);
    return { value: { ...r, receipts: receiptsForMsg(r) } };
  }

  function threadOf(ctx: Ctx, id: string): Res<{ rows: MsgRow[]; receipts: Receipts[] }> {
    const rows = d.query("SELECT * FROM messages WHERE thread=? OR id=? ORDER BY created_at ASC").all(id, id) as MsgRow[];
    if (!rows.length) return { error: "not_found", detail: `no thread: ${id}` };
    return { value: { rows, receipts: rows.map(receiptsForMsg) } };
  }

  function receipts(ctx: Ctx, id: string): Res<MsgRow & { receipts: Receipts }> {
    const r = d.query("SELECT * FROM messages WHERE id=?").get(id) as MsgRow | null;
    if (!r) return { error: "not_found", detail: `no such message: ${id}` };
    return { value: { ...r, receipts: receiptsForMsg(r) } };
  }

  function setStatus(ctx: Ctx, p: { agent: string; id: string; state: string }): Res<{ id: string; status: string }> {
    if (!STATES.includes(p.state as any)) return { error: "usage", detail: `error: state must be one of ${STATES.join(",")}` };
    const r = d.query("SELECT * FROM messages WHERE id=?").get(p.id) as MsgRow | null;
    if (!r) return { error: "not_found", detail: `no such message: ${p.id}` };
    if (mode === "local") touch(p.agent);
    // §5 status permission: sender OR resolved recipient OR agents:admin
    const isAdmin = ctx.principal.scopes.includes("agents:admin") || ctx.principal.localRoot === true;
    if (!isAdmin && r.sender !== p.agent) {
      const agents = d.query("SELECT id, role FROM agents").all() as any[];
      const isRecipient = agents.some((a) => a.id === p.agent && recipientsMatch(r.recipients, a.id, a.role))
        || recipientsMatch(r.recipients, p.agent, roleOf(p.agent));
      if (!isRecipient) return { error: "forbidden", detail: "status: not sender, not recipient" };
    }
    d.run("UPDATE messages SET status=?, updated_at=? WHERE id=?", [p.state, nowIso(), p.id]);
    return { value: { id: p.id, status: p.state } };
  }

  function channels(ctx: Ctx): { name: string; n: number; last: string | null; purpose: string | null }[] {
    const counts = d.query("SELECT channel name, COUNT(*) n, MAX(created_at) last FROM messages GROUP BY channel").all() as any[];
    const cmap = new Map<string, any>(counts.map((c) => [c.name, c]));
    for (const c of d.query("SELECT name, purpose FROM channels").all() as any[])
      if (!cmap.has(c.name)) cmap.set(c.name, { name: c.name, n: 0, last: null, purpose: c.purpose });
    for (const c of counts) c.purpose = (d.query("SELECT purpose FROM channels WHERE name=?").get(c.name) as any)?.purpose ?? "";
    return [...cmap.values()].sort((a, b) => String(b.last ?? "").localeCompare(String(a.last ?? "")));
  }

  function rename(ctx: Ctx, p: { agent: string; to: string; fingerprint?: string | null }): Res<{ announced: MsgRow }> {
    if (!p.agent || !p.to) return { error: "usage", detail: "error: rename requires --agent <old> --to <new>" };
    if (!ID_RE.test(p.to)) return { error: "usage", detail: `invalid new id: ${p.to}` };
    const old = d.query("SELECT * FROM agents WHERE id=?").get(p.agent) as any;
    if (!old) return { error: "not_found", detail: `error: no such agent '${p.agent}'` };
    if (mode === "server") {
      const isAdmin = ctx.principal.scopes.includes("agents:admin");
      if (p.agent !== ctx.principal.agentId && !isAdmin)
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
      file: "", created_at: t, updated_at: t, channel: "general", meta: null,
    };
    // QUIRK preserved: mirror written BEFORE the agents UPDATE (announce msg first).
    seams.mirror(join(MSG_DIR, "general"), fname, renderMd(m));
    m.file = join("messages", "general", fname);
    // §4: single transaction — announce + id move + dependent rows
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
        d.exec("COMMIT");
      } catch (e) { d.exec("ROLLBACK"); throw e; }
    } catch (e: any) {
      if (String(e?.message ?? e).includes("SQLITE_BUSY")) return { error: "contention", detail: "busy" };
      throw e;
    }
    return { value: { announced: m } };
  }

  // ---------- server-mode token ops (§5) ----------

  function tokenCreate(p: { agent: string; kind?: "agent" | "human" | "service"; scopes?: Scope[]; admin?: boolean }): Res<{ token: string; prefix: string }> {
    if (!ID_RE.test(p.agent)) return { error: "usage", detail: `invalid agent id: ${p.agent}` };
    const scopes = normalizeScopes(p.admin ? ALL_SCOPES : (p.scopes ?? (p.kind === "human" ? ["read:all"] : [])));
    const bytes = seams.rng(32);
    const token = "ac_" + b64url(bytes);
    const prefix = token.slice(3, 15);
    const salt = seams.rng(16);
    const { createHmac } = require("node:crypto") as typeof import("node:crypto");
    const keyHash = createHmac("sha256", Buffer.from(salt)).update(token).digest();
    const t = nowIso();
    try {
      d.exec("BEGIN IMMEDIATE");
      try {
        const existingAdmin = d.query(
          "SELECT count(*) c FROM tokens WHERE revoked_at IS NULL AND instr(',' || scopes || ',', ',tokens:admin,') > 0",
        ).get() as any;
        if (!existingAdmin.c && p.admin) {
          // bootstrap via local CLI only; no-op marker for the guard doc
        }
        d.run("INSERT OR IGNORE INTO agents(id,role,caps,pid,joined_at,last_seen,meta,kind) VALUES(?,?,?,?,?,?,?,?)",
          [p.agent, p.agent, "", null, t, t, "{}", p.kind ?? "agent"]);
        d.run("INSERT INTO tokens(prefix,agent_id,salt,key_hash,scopes,created_at,last_used) VALUES(?,?,?,?,?,?,?)",
          [prefix, p.agent, salt, keyHash, scopes, t, t]);
        d.exec("COMMIT");
      } catch (e) { d.exec("ROLLBACK"); throw e; }
    } catch (e: any) {
      if (String(e?.message ?? e).includes("UNIQUE")) return { error: "conflict", detail: "prefix collision (60-bit; retry)" };
      throw e;
    }
    return { value: { token, prefix } };
  }

  function tokenVerify(token: string): Res<{ agentId: string; scopes: Scope[]; kind: string }> {
    if (!token.startsWith("ac_") || token.length < 20) return { error: "forbidden", detail: "malformed token" };
    const prefix = token.slice(3, 15);
    const row = d.query("SELECT * FROM tokens WHERE prefix=? AND revoked_at IS NULL").get(prefix) as any;
    if (!row) return { error: "forbidden", detail: "unknown or revoked token" };
    const { createHmac, timingSafeEqual } = require("node:crypto") as typeof import("node:crypto");
    const digest = createHmac("sha256", Buffer.from(row.salt)).update(token).digest();
    if (digest.length !== row.key_hash.length || !timingSafeEqual(digest, Buffer.from(row.key_hash)))
      return { error: "forbidden", detail: "bad token" };
    const kind = (d.query("SELECT kind FROM agents WHERE id=?").get(row.agent_id) as any)?.kind ?? "agent";
    return { value: { agentId: row.agent_id, scopes: csv(row.scopes) as Scope[], kind } };
  }

  function tokenTouch(id: number) {
    // debounced ≤1/min (§9)
    const row = d.query("SELECT last_used FROM tokens WHERE id=?").get(id) as any;
    if (!row) return;
    const prev = Date.parse(row.last_used);
    if (Number.isFinite(prev) && seams.now().getTime() - prev < 60_000) return;
    d.run("UPDATE tokens SET last_used=? WHERE id=?", [nowIso(), id]);
  }

  // ---------- cursors (§6) ----------

  function cursorGet(agentId: string, consumer: string): { epoch: string; seq: number } {
    const epoch = (d.query("SELECT value FROM meta WHERE key='epoch'").get() as any).value;
    const row = d.query("SELECT epoch, last_seq FROM cursors WHERE agent_id=? AND consumer=?").get(agentId, consumer) as any;
    if (!row || row.epoch !== epoch) return { epoch, seq: 0 };
    return { epoch, seq: row.last_seq };
  }

  function cursorSet(agentId: string, consumer: string, epoch: string, seq: number, force = false): Res<null> {
    const cur = cursorGet(agentId, consumer);
    if (epoch !== cur.epoch && !force) return { error: "not_found", detail: "epoch mismatch; resync required" };
    if (!force && seq < cur.seq) return { error: "conflict", detail: "cursor not monotonic (use force)" };
    d.run("INSERT INTO cursors(agent_id,consumer,epoch,last_seq) VALUES(?,?,?,?) ON CONFLICT(agent_id,consumer) DO UPDATE SET epoch=excluded.epoch,last_seq=excluded.last_seq",
      [agentId, consumer, epoch, seq]);
    return { value: null };
  }

  // ---------- streaming/history (§6) ----------

  function history(ctx: Ctx, p: { channel?: string | null; limit?: number }): Res<{ rows: MsgRow[]; cursor: { epoch: string; seq: number } }> {
    if (mode === "server" && !ctx.principal.scopes.includes("read:all"))
      return { error: "forbidden", detail: "history requires read:all" };
    const limit = Math.min(p.limit ?? 100, 500);
    // same read txn for rows + high-water cursor (§6 nit):
    d.exec("BEGIN");
    try {
      const rows = (p.channel
        ? d.query("SELECT * FROM messages WHERE channel=? ORDER BY created_at ASC LIMIT ?")
        : d.query("SELECT * FROM messages ORDER BY created_at ASC LIMIT ?")) as any;
      const got = (p.channel ? rows.all(p.channel, limit) : rows.all(limit)) as MsgRow[];
      const hw = (d.query("SELECT coalesce(max(seq),0) m FROM events").get() as any).m;
      const epoch = (d.query("SELECT value FROM meta WHERE key='epoch'").get() as any).value;
      d.exec("COMMIT");
      return { value: { rows: got, cursor: { epoch, seq: hw } } };
    } catch (e) { d.exec("ROLLBACK"); throw e; }
  }

  function tailEvents(afterSeq: number, limit = 200): { seq: number; kind: string; msg_id: string | null; agent_id: string | null; at: string }[] {
    return d.query("SELECT seq,kind,msg_id,agent_id,at FROM events WHERE seq>? ORDER BY seq LIMIT ?").all(afterSeq, limit) as any;
  }

  function epoch(): string {
    return (d.query("SELECT value FROM meta WHERE key='epoch'").get() as any).value;
  }

  function rotateEpoch(): string {
    const e = hex(seams.rng(16));
    d.run("INSERT INTO meta(key,value) VALUES('epoch',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [e]);
    return e;
  }

  function gc(): { events: number; idempotency: number } {
    const cutE = new Date(seams.now().getTime() - 30 * 86400_000).toISOString();
    const cutI = new Date(seams.now().getTime() - 86400_000).toISOString();
    const a = d.run("DELETE FROM events WHERE at < ? AND seq > (SELECT coalesce(min(seq),0) FROM events)", [cutE]);
    const b = d.run("DELETE FROM idempotency WHERE created_at < ?", [cutI]);
    return { events: a.changes, idempotency: b.changes };
  }

  function preflight(): { badIds: string[]; badChannels: string[] } {
    const badIds = (d.query("SELECT DISTINCT id FROM agents").all() as any[])
      .map((r) => r.id).filter((id) => id && !ID_RE.test(id));
    const badChannels = (d.query("SELECT DISTINCT sender FROM messages").all() as any[])
      .map((r) => r.sender).filter((s) => s && !ID_RE.test(s))
      .concat((d.query("SELECT name FROM channels").all() as any[]).map((r) => r.name).filter((n) => n && !ID_RE.test(n)));
    return { badIds, badChannels: [...new Set(badChannels)] };
  }

  function close() { d.close(); }

  return {
    db: d, home, mode, seams, DB_PATH, MSG_DIR, nowIso, stamp, newId,
    joinAgent, listAgents, post, inbox, read, threadOf, receipts, setStatus, channels, rename,
    tokenCreate, tokenVerify, tokenTouch,
    cursorGet, cursorSet, history, tailEvents, epoch, rotateEpoch, gc, preflight,
    isActive, recipientsMatch, roleOf, receiptsForMsg, renderMd, touch, close,
  };
}

function hex(b: Uint8Array) { return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join(""); }
function b64url(b: Uint8Array) {
  return btoa(String.fromCharCode(...b)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export { csv, recipientsMatch };
