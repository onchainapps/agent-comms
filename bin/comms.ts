#!/usr/bin/env bun
/**
 * comms — a join-able, serverless comms engine for cooperating agents.
 *
 * Backing store is a single SQLite database in WAL mode via bun:sqlite
 * (concurrency-safe, no daemon, no external deps). Any agent session on this
 * machine can `join`, `post`, `inbox`, and `watch`. Every message is also
 * mirrored to a human-readable Markdown file in the comms directory so it
 * interoperates with the existing prose handoffs and stays git-friendly.
 *
 * Rendezvous is purely the DB file on disk — agents "join" by pointing at the
 * same COMMS_HOME. No broker, no port, survives restarts.
 *
 * Usage:
 *   bun comms.ts join   --agent coord-1 --role coordinator [--caps pr-review]
 *   bun comms.ts who    [--all]
 *   bun comms.ts post   --from coord-1 --to research,lab --type ask \
 *                       --subject "..." [--thread ID] [--re ID] [--tags a,b] [--body -|text|@file]
 *   bun comms.ts inbox  --for coord-1 [--open] [--unread]
 *   bun comms.ts read   --for coord-1 --id MSGID
 *   bun comms.ts thread --id THREADID
 *   bun comms.ts ack|done --from coord-1 --id MSGID
 *   bun comms.ts status --from coord-1 --id MSGID --state blocked
 *   bun comms.ts watch  --for coord-1 [--interval 3] [--timeout 28800] [--once]
 *
 * Env: COMMS_HOME  project root holding .comms/ (DB) + messages/ (default: auto-detected)
 */
import { Database } from "bun:sqlite";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";

// Resolve the project root by walking up until we find a marker, so the CLI works
// whether invoked from bin/, a root shim, or elsewhere. Keeps the DB path stable.
function findRoot(start: string): string {
  let d = start;
  for (;;) {
    if (existsSync(join(d, "package.json")) || existsSync(join(d, ".comms"))) return d;
    const p = dirname(d);
    if (p === d) return start;
    d = p;
  }
}
const HOME = process.env.COMMS_HOME ?? findRoot(dirname(fileURLToPath(import.meta.url)));
const DB_PATH = join(HOME, ".comms", "comms.db");
const MSG_DIR = join(HOME, "messages"); // human-readable mirror of each message
const PRESENCE_TTL_MS = 15 * 60 * 1000; // active if seen within 15 min

const MSG_TYPES = new Set(["ask", "ack", "reply", "result", "status", "handoff", "note", "rfc", "announce"]);
const STATES = new Set(["open", "acked", "in_progress", "done", "blocked"]);

// ---------- helpers ----------
const nowIso = () => new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
const stamp = () => nowIso().replace(/[-:TZ]/g, "").slice(0, 15).replace(/(\d{8})(\d{6})/, "$1T$2");
const shortHex = (n = 2) => Array.from(crypto.getRandomValues(new Uint8Array(n)), b => b.toString(16).padStart(2, "0")).join("");
const newId = (prefix: string) => `${stamp()}-${prefix}-${shortHex(2)}`;
const csv = (s?: string | null) => (s ?? "").split(",").map(x => x.trim()).filter(Boolean);

function db(): Database {
  mkdirSync(join(HOME, ".comms"), { recursive: true });
  const d = new Database(DB_PATH, { create: true });
  d.exec("PRAGMA journal_mode = WAL");
  d.exec("PRAGMA busy_timeout = 5000");
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
  `);
  // self-migrate DBs that predate the channel column
  const cols = d.query("PRAGMA table_info(messages)").all() as any[];
  if (!cols.some(c => c.name === "channel"))
    d.exec("ALTER TABLE messages ADD COLUMN channel TEXT NOT NULL DEFAULT 'general'");
  d.exec("CREATE INDEX IF NOT EXISTS idx_msg_channel ON messages(channel)");
  // self-migrate DBs that predate the fingerprint column (identity enforcement)
  const acols = d.query("PRAGMA table_info(agents)").all() as any[];
  if (!acols.some(c => c.name === "fingerprint"))
    d.exec("ALTER TABLE agents ADD COLUMN fingerprint TEXT");
  // one runtime (fingerprint) => one id. Enforced at the DB layer too.
  d.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_fp ON agents(fingerprint) WHERE fingerprint IS NOT NULL");
  d.run("INSERT OR IGNORE INTO channels(name,purpose,created_at,created_by) VALUES('general','tooling / meta / cross-project chatter','',?)", ["system"]);
  return d;
}

function ensureChannel(d: Database, name: string, by: string) {
  d.run("INSERT OR IGNORE INTO channels(name,purpose,created_at,created_by) VALUES(?,?,?,?)", [name, "", nowIso(), by]);
}

// best-effort heartbeat; auto-registers unknown agents so post/watch just work
function touch(d: Database, agent: string) {
  if (!agent) return; // never register a null/blank agent
  const t = nowIso();
  const r = d.run("UPDATE agents SET last_seen=? WHERE id=?", [t, agent]);
  if (r.changes === 0) {
    d.run(
      "INSERT OR IGNORE INTO agents(id,role,caps,pid,joined_at,last_seen,meta) VALUES(?,?,?,?,?,?,?)",
      [agent, agent, "", process.pid, t, t, "{}"],
    );
  }
}

const isActive = (lastSeen: string) => {
  const t = Date.parse(lastSeen);
  return Number.isFinite(t) && Date.now() - t <= PRESENCE_TTL_MS;
};

function recipientsMatch(recips: string, agent: string, role?: string | null): boolean {
  const toks = new Set(csv(recips));
  if (toks.has("@all") || toks.has(agent)) return true;
  return !!role && toks.has(role);
}

type Row = Record<string, any>;
const roleOf = (d: Database, agent: string): string | null =>
  (d.query("SELECT role FROM agents WHERE id=?").get(agent) as Row | null)?.role ?? null;

// Per-message read receipts. Intended readers = registered agents (minus sender)
// the message is addressed to. Read = an explicit `read` row OR an inferred read
// (the agent posted a reply referencing this message).
function receiptsFor(d: Database, msg: Row) {
  const agents = d.query("SELECT id, role FROM agents WHERE id IS NOT NULL").all() as Row[];
  const intended = agents
    .filter(a => a.id !== msg.sender && recipientsMatch(msg.recipients, a.id, a.role))
    .map(a => a.id);
  const readMap = new Map<string, string | null>();
  for (const r of d.query("SELECT agent, read_at FROM reads WHERE msg=?").all(msg.id) as Row[])
    readMap.set(r.agent, r.read_at);
  for (const r of d.query("SELECT DISTINCT sender FROM messages WHERE re=?").all(msg.id) as Row[])
    if (!readMap.has(r.sender)) readMap.set(r.sender, null); // null = inferred via reply
  const readers = intended.filter(id => readMap.has(id)).map(id => ({ id, at: readMap.get(id) ?? null }));
  const unread = intended.filter(id => !readMap.has(id));
  return { intended, readers, unread };
}

function fmtReceipts(r: ReturnType<typeof receiptsFor>): string {
  if (!r.intended.length) return "receipts: (no registered recipients)";
  const read = r.readers.map(x => `${x.id}${x.at ? "✓" : "⤷"}`).join(" ") || "—";
  const unread = r.unread.length ? r.unread.join(" ") : "—";
  return `seen ${r.readers.length}/${r.intended.length}  ·  read: ${read}  ·  unread: ${unread}`;
}

// ---------- rendering ----------
function renderMd(m: Row): string {
  const fm = {
    id: m.id, channel: m.channel ?? "general", thread: m.thread, re: m.re, from: m.sender,
    to: csv(m.recipients), type: m.type, status: m.status,
    tags: csv(m.tags), created_at: m.created_at,
  };
  return `---\n${JSON.stringify(fm, null, 2)}\n---\n\n# ${m.subject || m.type}\n\n${m.body}\n`;
}

function fmtRow(r: Row, unread = false): string {
  const u = unread ? " *" : "  ";
  const subj = (r.subject ?? "").slice(0, 60);
  return `${u}${r.id.padEnd(24)} #${String(r.channel ?? "general").padEnd(12)} [${r.status.padEnd(11)}] ${r.type.padEnd(8)} ${String(r.sender).padStart(12)} -> ${String(r.recipients).padEnd(18)} ${subj}`;
}

function readBody(spec?: string): string {
  if (spec == null) return "";
  if (spec === "-") return readFileSync(0, "utf8");
  if (spec.startsWith("@")) return readFileSync(spec.slice(1), "utf8");
  return spec;
}

// ---------- commands ----------
function printWho(d: Database, activeOnly: boolean) {
  const rows = d.query("SELECT * FROM agents ORDER BY last_seen DESC").all() as Row[];
  console.log(activeOnly ? "active agents:" : "known agents:");
  let shown = 0;
  for (const r of rows) {
    if (!r.id) continue; // skip malformed rows defensively
    const act = isActive(r.last_seen);
    if (activeOnly && !act) continue;
    shown++;
    console.log(`  ${act ? "●" : "○"} ${String(r.id).padEnd(20)} role=${String(r.role ?? "-").padEnd(14)} caps=${(r.caps || "-").padEnd(24)} seen=${r.last_seen}`);
  }
  if (!shown) console.log("  (none)");
}

function cmdJoin(a: Args) {
  if (!a.agent || !a.role) { console.error("error: join requires --agent and --role"); process.exit(2); }
  const d = db();
  const t = nowIso();
  // One id per agent. A fingerprint identifies the runtime; pass --fingerprint
  // or export COMMS_FINGERPRINT (stable per runtime). Enforcement is active only
  // when a fingerprint is presented, so pre-fingerprint agents keep working.
  const fp = a.fingerprint ?? process.env.COMMS_FINGERPRINT ?? null;
  if (fp) {
    const byFp = d.query("SELECT id FROM agents WHERE fingerprint=?").get(fp) as Row | null;
    if (byFp && byFp.id !== a.agent) {
      console.error(
        `error: this runtime already joined as '${byFp.id}'. One id per agent.\n` +
        `  Reconnect as yourself:  --agent ${byFp.id}\n` +
        `  Or change your name (announces it to @all):  bun comms.ts rename --agent ${byFp.id} --to ${a.agent} --fingerprint <fp>`,
      );
      process.exit(3);
    }
    const byId = d.query("SELECT fingerprint FROM agents WHERE id=?").get(a.agent) as Row | null;
    if (byId && byId.fingerprint && byId.fingerprint !== fp) {
      console.error(`error: id '${a.agent}' is already claimed by another runtime. Pick a different id, or coordinate a rename.`);
      process.exit(3);
    }
  }
  d.run(
    `INSERT INTO agents(id,role,caps,pid,joined_at,last_seen,meta,fingerprint) VALUES(?,?,?,?,?,?,?,?)
     ON CONFLICT(id) DO UPDATE SET role=excluded.role, caps=excluded.caps, pid=excluded.pid, last_seen=excluded.last_seen,
       fingerprint=COALESCE(excluded.fingerprint, agents.fingerprint)`,
    [a.agent, a.role, a.caps ?? "", process.pid, t, t, "{}", fp],
  );
  console.log(`joined: ${a.agent} (role=${a.role}, caps=${a.caps || "-"}, ${fp ? `fp=${String(fp).slice(0, 8)}…` : "fp=UNSET — identity unprotected, set COMMS_FINGERPRINT"})`);
  printWho(d, true);
  const n = (d.query(
    "SELECT COUNT(*) c FROM messages WHERE status IN ('open','acked','in_progress') AND sender!=?",
  ).get(a.agent) as Row).c;
  console.log(`\n${n} unresolved message(s) in flight. Run: bun comms.ts inbox --for ${a.agent} --open`);
}

function cmdWho(a: Args) { printWho(db(), !a.all); }

// Write a message's human/git mirror into messages/<channel>/ and return the
// relative path for the (display-only) file column. New channels get a folder
// automatically. The bus itself is the DB — this file is just the mirror.
function writeMsgFile(channel: string, fname: string, content: string): string {
  const dir = join(MSG_DIR, channel);
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, fname), content);
  return join("messages", channel, fname);
}

// Change an agent's id. Announces the change to @all first, then migrates the
// registry row. Historical messages keep the old sender (accurate); only the
// live identity moves. Requires the owning fingerprint if one is set.
function cmdRename(a: Args) {
  if (!a.agent || !a.to) { console.error("error: rename requires --agent <old> --to <new>"); process.exit(2); }
  const d = db();
  const old = d.query("SELECT * FROM agents WHERE id=?").get(a.agent) as Row | null;
  if (!old) { console.error(`error: no such agent '${a.agent}'`); process.exit(1); }
  const fp = a.fingerprint ?? process.env.COMMS_FINGERPRINT ?? null;
  if (old.fingerprint && old.fingerprint !== fp) {
    console.error(`error: rename of '${a.agent}' must come from the same runtime (fingerprint mismatch).`); process.exit(3);
  }
  if (d.query("SELECT id FROM agents WHERE id=?").get(a.to)) {
    console.error(`error: id '${a.to}' already exists — pick a free name.`); process.exit(3);
  }
  const t = nowIso();
  ensureChannel(d, "general", a.to);
  const mid = newId(String(a.to).split("-")[0]);
  const fname = `msg-${stamp()}-${a.to}-announce-${mid.split("-").pop()}.md`;
  const m: Row = {
    id: mid, thread: mid, re: null, sender: a.agent, recipients: "@all",
    type: "announce", status: "open", tags: "identity,rename",
    subject: `Agent rename: ${a.agent} -> ${a.to}`,
    body: `Identity change: ${a.agent} is now ${a.to} (same runtime). Please update routing; historical messages keep sender=${a.agent}.`,
    file: "", created_at: t, updated_at: t, channel: "general",
  };
  m.file = writeMsgFile(m.channel, fname, renderMd(m));
  d.run(
    `INSERT INTO messages(id,thread,re,sender,recipients,type,status,tags,subject,body,file,created_at,updated_at,channel)
     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [m.id, m.thread, m.re, m.sender, m.recipients, m.type, m.status, m.tags, m.subject, m.body, m.file, m.created_at, m.updated_at, m.channel],
  );
  d.run("UPDATE agents SET id=?, last_seen=? WHERE id=?", [a.to, t, a.agent]);
  console.log(`renamed ${a.agent} -> ${a.to} (announced to @all). New activity uses ${a.to}; history keeps ${a.agent}.`);
}

function cmdPost(a: Args) {
  if (!a.sender || !a.to) { console.error("error: post requires --from and --to"); process.exit(2); }
  const d = db();
  touch(d, a.sender);
  if (!MSG_TYPES.has(a.type)) { console.error(`error: --type must be one of ${[...MSG_TYPES].sort()}`); process.exit(2); }
  const body = readBody(a.body);
  const mid = newId(a.sender.split("-")[0]);
  const thread = a.thread || mid;
  const t = nowIso();
  // channel: explicit --channel  >  inherit from thread/parent  >  general
  let channel: string | undefined = a.channel;
  if (!channel) {
    const parentId = a.thread || a.re;
    if (parentId) {
      const p = d.query("SELECT channel FROM messages WHERE id=? OR thread=? ORDER BY created_at ASC LIMIT 1").get(parentId, parentId) as Row | null;
      channel = p?.channel;
    }
  }
  channel = channel || "general";
  ensureChannel(d, channel, a.sender);
  const fname = `msg-${stamp()}-${a.sender}-${a.type}-${mid.split("-").pop()}.md`;
  const m: Row = {
    id: mid, thread, re: a.re ?? null, sender: a.sender, recipients: a.to,
    type: a.type, status: "open", tags: a.tags ?? "", subject: a.subject ?? "",
    body, file: "", created_at: t, updated_at: t, channel,
  };
  m.file = writeMsgFile(channel, fname, renderMd(m));
  d.run(
    `INSERT INTO messages(id,thread,re,sender,recipients,type,status,tags,subject,body,file,created_at,updated_at,channel)
     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    [m.id, m.thread, m.re, m.sender, m.recipients, m.type, m.status, m.tags, m.subject, m.body, m.file, m.created_at, m.updated_at, m.channel],
  );
  console.log(`posted ${mid}  [#${channel}]  thread=${thread}  -> ${a.to}  (${fname})`);
}

function cmdInbox(a: Args) {
  const d = db();
  touch(d, a.agent);
  const role = roleOf(d, a.agent);
  const rows = d.query("SELECT * FROM messages ORDER BY created_at ASC").all() as Row[];
  const readIds = new Set((d.query("SELECT msg FROM reads WHERE agent=?").all(a.agent) as Row[]).map(x => x.msg));
  let shown = 0;
  for (const r of rows) {
    if (r.sender === a.agent) continue;
    if (a.channel && r.channel !== a.channel) continue;
    if (!recipientsMatch(r.recipients, a.agent, role)) continue;
    if (a.open && !["open", "acked", "in_progress"].includes(r.status)) continue;
    const unread = !readIds.has(r.id);
    if (a.unread && !unread) continue;
    console.log(fmtRow(r, unread));
    shown++;
  }
  console.log(shown ? `\n${shown} message(s). '*' = unread. Read: bun comms.ts read --for ${a.agent} --id <ID>` : "(inbox empty for filter)");
}

function cmdRead(a: Args) {
  const d = db();
  touch(d, a.agent);
  const r = d.query("SELECT * FROM messages WHERE id=?").get(a.id) as Row | null;
  if (!r) { console.error(`no such message: ${a.id}`); process.exit(1); }
  d.run("INSERT OR REPLACE INTO reads(agent,msg,read_at) VALUES(?,?,?)", [a.agent, a.id, nowIso()]);
  console.log(`--- ${r.id} | thread ${r.thread} | ${r.type} | ${r.status}`);
  console.log(`from ${r.sender} -> ${r.recipients} | ${r.created_at}`);
  if (r.re) console.log(`re: ${r.re}`);
  if (r.tags) console.log(`tags: ${r.tags}`);
  console.log(`\n# ${r.subject}\n\n${r.body}\n\n(file: ${r.file})`);
  console.log(`\n${fmtReceipts(receiptsFor(d, r))}`);
}

function cmdReceipts(a: Args) {
  const d = db();
  const r = d.query("SELECT * FROM messages WHERE id=?").get(a.id) as Row | null;
  if (!r) { console.error(`no such message: ${a.id}`); process.exit(1); }
  console.log(`${r.id}  [${r.type}/${r.status}]  ${r.sender} -> ${r.recipients}`);
  console.log(`  ${r.subject}`);
  console.log(`  ${fmtReceipts(receiptsFor(d, r))}`);
  console.log(`  (✓ = opened via read · ⤷ = inferred from a reply)`);
}

function cmdChannels(_a: Args) {
  const d = db();
  const counts = d.query("SELECT channel name, COUNT(*) n, MAX(created_at) last FROM messages GROUP BY channel").all() as Row[];
  const cmap = new Map<string, Row>(counts.map(c => [c.name, c]));
  for (const c of d.query("SELECT name FROM channels").all() as Row[])
    if (!cmap.has(c.name)) cmap.set(c.name, { name: c.name, n: 0, last: null });
  const purposes = new Map((d.query("SELECT name, purpose FROM channels").all() as Row[]).map(c => [c.name, c.purpose]));
  const list = [...cmap.values()].sort((a, b) => String(b.last ?? "").localeCompare(String(a.last ?? "")));
  console.log("channels:");
  for (const c of list)
    console.log(`  #${String(c.name).padEnd(14)} ${String(c.n).padStart(4)} msgs  last=${c.last ?? "-"}  ${purposes.get(c.name) ?? ""}`);
  console.log(`\nfilter any view: --channel <name>  ·  post to one: post ... --channel <name> (or reply, which inherits)`);
}

function cmdThread(a: Args) {
  const d = db();
  const rows = d.query("SELECT * FROM messages WHERE thread=? OR id=? ORDER BY created_at ASC").all(a.id, a.id) as Row[];
  if (!rows.length) { console.error(`no thread: ${a.id}`); process.exit(1); }
  console.log(`thread ${a.id}  (${rows.length} message(s))`);
  for (const r of rows) {
    const rc = receiptsFor(d, r);
    console.log(`${fmtRow(r)}  seen ${rc.readers.length}/${rc.intended.length}`);
  }
}

function setStatus(agent: string, id: string, state: string) {
  const d = db();
  touch(d, agent);
  if (!STATES.has(state)) { console.error(`error: state must be one of ${[...STATES].sort()}`); process.exit(2); }
  const r = d.query("SELECT id FROM messages WHERE id=?").get(id) as Row | null;
  if (!r) { console.error(`no such message: ${id}`); process.exit(1); }
  d.run("UPDATE messages SET status=?, updated_at=? WHERE id=?", [state, nowIso(), id]);
  console.log(`${id} -> ${state}`);
}

async function cmdWatch(a: Args) {
  const d = db();
  const role = roleOf(d, a.agent);
  const ivSec = a.interval ?? 3;
  const capSec = a.timeout ?? 28800;
  const seen = new Set((d.query("SELECT id FROM messages").all() as Row[]).map(r => r.id));
  const scope = a.all ? (a.channel ? `all of #${a.channel}` : "ALL channels (firehose)") : (a.channel ? `#${a.channel} addressed to me` : "addressed to me");
  console.log(`watch: ${a.agent} (role=${role ?? "-"}) scope=${scope}; every ${ivSec}s; ${a.once ? "one-shot" : `cap ${capSec}s`}. baseline=${seen.size} msgs`);
  let stop = false;
  process.on("SIGTERM", () => { stop = true; });
  const interval = ivSec * 1000;
  const cap = capSec * 1000;
  const started = Date.now();
  while (!stop) {
    touch(d, a.agent);
    const rows = d.query("SELECT * FROM messages ORDER BY created_at ASC").all() as Row[];
    let hitForMe = false;
    for (const r of rows) {
      if (seen.has(r.id)) continue;
      seen.add(r.id);
      if (r.sender === a.agent) continue;
      if (a.channel && r.channel !== a.channel) continue;
      // --all surfaces every message in scope (a whole channel); default = only addressed to me
      if (a.all || recipientsMatch(r.recipients, a.agent, role)) { console.log(`NEW ${fmtRow(r)}`); hitForMe = true; }
    }
    if (a.once) break;
    if (hitForMe && a["exit-on-new"]) { console.log("watch: message for me; exiting"); break; }
    if (Date.now() - started >= cap) { console.log("watch: timeout cap reached; exiting"); break; }
    await Bun.sleep(interval);
  }
}

// ---------- arg parsing ----------
type Args = Record<string, any>;
function parse(argv: string[]): Args {
  const [cmd, ...rest] = argv;
  const a: Args = { cmd };
  for (let i = 0; i < rest.length; i++) {
    const t = rest[i];
    if (!t.startsWith("--")) continue;
    const key = t.slice(2) === "for" ? "agent" : t.slice(2) === "from" ? "sender" : t.slice(2);
    const next = rest[i + 1];
    if (next === undefined || next.startsWith("--")) { a[key] = true; }
    else { a[key] = next; i++; }
  }
  if (a.interval) a.interval = Number(a.interval);
  if (a.timeout) a.timeout = Number(a.timeout);
  return a;
}

const HELP = `comms — join-able serverless agent comms (bun:sqlite)
commands: join | rename | who | post | inbox | read | thread | receipts | channels | ack | done | status | watch
run 'bun comms.ts <cmd> --help-ish' — see header of this file for full usage.`;

async function main() {
  const a = parse(process.argv.slice(2));
  switch (a.cmd) {
    case "join": return cmdJoin(a);
    case "rename": return cmdRename(a);
    case "who": return cmdWho(a);
    case "post": return cmdPost(a);
    case "inbox": return cmdInbox(a);
    case "read": return cmdRead(a);
    case "thread": return cmdThread(a);
    case "receipts": return cmdReceipts(a);
    case "channels": return cmdChannels(a);
    case "ack": return setStatus(a.agent, a.id, "acked");
    case "done": return setStatus(a.agent, a.id, "done");
    case "status": return setStatus(a.agent, a.id, a.state);
    case "watch": return await cmdWatch(a);
    default:
      console.log(HELP);
      process.exit(a.cmd ? 2 : 0);
  }
}
await main();
