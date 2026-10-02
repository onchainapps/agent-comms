#!/usr/bin/env bun
/**
 * comms CLI — thin shell over the bus core (RFC-001 §3 piece 4).
 *
 * All DB logic lives in src/bus.ts. This file owns ONLY: argv parsing, env
 * (COMMS_HOME / COMMS_URL / COMMS_TOKEN / COMMS_FINGERPRINT / COMMS_TEST_SEAMS),
 * readBody (@file/stdin), rendering, exit codes, transport selection (§7),
 * and the watch loops. No SQL, no DB access.
 * Byte-identical local-mode behavior is enforced by tests/golden.test.ts.
 *
 * Env: COMMS_HOME        project root holding .comms/ (DB) + messages/ (default: auto-detected)
 *      COMMS_URL         server base URL ⇒ remote transport (§7 precedence:
 *                        COMMS_URL ⇒ remote; --local forces direct; else COMMS_HOME direct)
 *      COMMS_TOKEN       bearer token for remote mode (or --token)
 *      COMMS_FINGERPRINT stable per-runtime identity for join/rename guards (local mode;
 *                        server mode IGNORES fingerprint — token is identity, §5)
 *      COMMS_TEST_SEAMS  JSON {at,seed,pid} — frozen seams for the golden harness (test-only)
 */
import { dirname, join, basename } from "node:path";
import { fileURLToPath } from "node:url";
import { existsSync, readFileSync } from "node:fs";
import { openBus, localCtx, EXIT_CODES, type Bus, type MsgRow, type Res } from "../src/bus.ts";
import { testSeams, type Seams } from "../src/seams.ts";
import { RpcBus } from "../src/rpc-bus.ts";
import { REMOTE_METHODS } from "../src/cli-wire.ts";

function findRoot(start: string): string {
  let d = start;
  for (;;) {
    if (existsSync(join(d, "package.json")) || existsSync(join(d, ".comms"))) return d;
    const p = dirname(d);
    if (p === d) return start;
    d = p;
  }
}

// ---------- transport (§7) ----------
// The local DB is opened ONLY in local mode: the hosted single-writer rule
// means a remote CLI must not open the server's SQLite file (it would run its
// own migrations and race the server writer).
let HOME = "";
let localBus: Bus<"local"> | null = null;
function core(): Bus<"local"> {
  if (!localBus) {
    const seams: Seams | undefined = process.env.COMMS_TEST_SEAMS
      ? testSeams(JSON.parse(process.env.COMMS_TEST_SEAMS))
      : undefined;
    localBus = openBus({ home: HOME, mode: "local", seams });
  }
  return localBus;
}

let REMOTE: RpcBus | null = null;
let remoteToken = "";
let bannerPrinted = false;
let remoteIdentity = { id: "", scopes: "" };

/** One transport surface for every command. `actor` is the CLI's --agent/--from
 *  claim: in local mode it becomes the localCtx principal (host is root); in
 *  remote mode it travels inside params as a §5 ASSERTION and the server
 *  resolves the principal from the token row. */
type Call = (name: string, actor: string, p?: Record<string, unknown>) => Promise<Res<any>>;

const LOCAL_CALLS: Record<string, (b: Bus<"local">, actor: string, p: any) => Res<any>> = {
  joinAgent: (b, actor, p) => b.joinAgent(localCtx(actor), p),
  listAgents: (b, _a, p) => ({ value: b.listAgents(p.activeOnly) }),
  post: (b, actor, p) => b.post(localCtx(actor), p),
  inbox: (b, actor, p) => b.inbox(localCtx(actor), p),
  read: (b, actor, p) => b.read(localCtx(actor), p),
  threadOf: (b, actor, p) => b.threadOf(localCtx(actor), p.id),
  receipts: (b, actor, p) => b.receipts(localCtx(actor), p.id),
  setStatus: (b, actor, p) => b.setStatus(localCtx(actor), p),
  channels: (b, actor) => b.channels(localCtx(actor)),
  rename: (b, actor, p) => b.rename(localCtx(actor), p),
  history: (b, actor, p) => b.history(localCtx(actor), p),
  waitStep: (b, actor, p) => b.waitStep(localCtx(actor), p),
  cursorGet: (b, actor, p) => b.cursorGet(actor, p.consumer ?? "default"),
  cursorSet: (b, actor, p) => b.cursorSet(actor, p.consumer, epOf(p.cursor), seqOf(p.cursor), p.force),
  tokenCreate: (b, actor, p) => b.tokenCreate(localCtx(actor), p),
  tokenList: (b, actor) => b.tokenList(localCtx(actor)),
  tokenRevoke: (b, actor, p) => b.tokenRevoke(localCtx(actor), p),
  groupCreate: (b, actor, p) => b.groupCreate(localCtx(actor), p),
  groupJoin: (b, actor, p) => b.groupJoin(localCtx(actor), p),
  groupLeave: (b, actor, p) => b.groupLeave(localCtx(actor), p),
  groupDelete: (b, actor, p) => b.groupDelete(localCtx(actor), p),
  groupList: (b, actor) => b.groupList(localCtx(actor)),
  groupShow: (b, actor, p) => b.groupShow(localCtx(actor), p),
  dmMembers: (b, actor, p) => b.dmMembersFor(localCtx(actor), p.channel),
};
const CURSOR_RE = /^([0-9a-f]{8,64})\.(\d+)$/;
const epOf = (c: string) => { const m = CURSOR_RE.exec(String(c)); return m ? m[1] : String(c); };
const seqOf = (c: string) => { const m = CURSOR_RE.exec(String(c)); return m ? Number(m[2]) : NaN; };

// Remote wire map: src/cli-wire.ts (side-effect-free so the m1 no-32601 pin
// can import it; this file must keep an unconditional main()).

/** §7 exit contract: -32004/-32006 ⇒ backoff per Retry-After, then 1. The
 *  backoff loop lives HERE (transport policy), not in RpcBus (mechanism);
 *  the SAME params object is re-sent, so a post's auto idempotency key is
 *  generated once per logical post and reused across its own retries (§6). */
const RETRY_VARIANTS = new Set(["rate_limited", "contention"]);
/** retry window (ms) — env override exists for tests/impatient scripts only. */
const RETRY_WINDOW_MS = Number(process.env.COMMS_RETRY_WINDOW_MS ?? 30_000) || 30_000;
/** per-request deadline: a server that accepts TCP but never answers must
 *  not hang the CLI forever (fetch has no default timeout). */
const REQUEST_TIMEOUT_MS = Number(process.env.COMMS_RPC_TIMEOUT_MS ?? 10_000) || 10_000;
/** claude M3 M6: AMBIGUOUS transport failures (reset / timeout — the server
 *  may have committed; the flaky-LAN case §6's auto idempotency key exists
 *  FOR) are retried inside the same bounded window with the SAME params
 *  object, but only for calls whose replay is harmless: reads, cursor
 *  commits (monotonic; conflict handled by the caller), status (same value),
 *  join (UPDATE-only presence refresh) and post (same key ⇒ server replays
 *  the original result). NOT token.create (a second secret), rename,
 *  group.*. `refused` never reached the server ⇒ fail fast (exit 1). */
const RETRY_UNAVAILABLE = new Set([
  "joinAgent", "listAgents", "post", "inbox", "read", "threadOf", "receipts", "setStatus",
  "channels", "history", "waitStep", "cursorGet", "cursorSet", "tokenList", "groupList", "groupShow", "dmMembers",
]);
async function remoteCall(name: string, p: Record<string, unknown>): Promise<Res<any>> {
  const [method, params] = REMOTE_METHODS[name](p);
  const deadline = Date.now() + RETRY_WINDOW_MS; // bounded: flaky-LAN retry window, then exit 1
  let backoff = 250;
  for (;;) {
    let retryAfterMs = 1000;
    const r = await REMOTE!.call(method, params, remoteToken, (h) => {
      const a = h.get("x-comms-agent"); const s = h.get("x-comms-scopes");
      // claude M2-fold m1: never overwrite known scopes with "" (a pre-auth
      // or header-less response must not blank an already-resolved identity).
      if (a !== null) remoteIdentity = { id: a, scopes: s !== null ? s : remoteIdentity.scopes };
      const ra = Number(h.get("retry-after"));
      if (Number.isFinite(ra) && ra > 0) retryAfterMs = ra * 1000;
    });
    maybeBanner();
    if (!r.error || Date.now() >= deadline) return r;
    if (RETRY_VARIANTS.has(r.error)) { await Bun.sleep(Math.min(Math.max(retryAfterMs, 250), 5000)); continue; }
    if (r.error === "unavailable" && RETRY_UNAVAILABLE.has(name) && r.data?.transport !== "refused") { await Bun.sleep(backoff); backoff = Math.min(backoff * 2, 5000); continue; }
    return r;
  }
}

function maybeBanner() {
  if (bannerPrinted) return;
  bannerPrinted = true;
  if (REMOTE) console.error(`transport=remote:${process.env.COMMS_URL} as ${remoteIdentity.id || "?"}(${remoteIdentity.scopes})`);
}

let CALL: Call = async (name, actor, p = {}) => LOCAL_CALLS[name](core(), actor, p);

function setupTransport(a: Args) {
  HOME = process.env.COMMS_HOME ?? findRoot(dirname(fileURLToPath(import.meta.url)));
  const url = process.env.COMMS_URL;
  const forceLocal = !!a.local;
  if (url && !forceLocal) {
    REMOTE = new RpcBus(url.replace(/\/+$/, ""), undefined, REQUEST_TIMEOUT_MS);
    remoteToken = (typeof a.token === "string" ? a.token : "") || process.env.COMMS_TOKEN || "";
    CALL = async (name, _actor, p = {}) => remoteCall(name, p);
    // §7: banner on EVERY remote command (identity from the first response's
    // x-comms-* headers); join/who force-print even if a command would
    // otherwise end before any response was inspected.
  } else if (url) {
    // §7 ambiguity: COMMS_URL set but --local won — say so even without
    // COMMS_HOME (claude M3 m5: the user set COMMS_URL and it is ignored).
    console.error(`transport=local:${HOME} (COMMS_URL=${url} ignored by --local)`);
    bannerPrinted = true;
  }
}

// ---------- rendering (byte-exact from the pre-refactor CLI) ----------
function fmtRow(r: MsgRow, unread = false): string {
  const u = unread ? " *" : "  ";
  const subj = (r.subject ?? "").slice(0, 60);
  return `${u}${r.id.padEnd(24)} #${String(r.channel ?? "general").padEnd(12)} [${r.status.padEnd(11)}] ${r.type.padEnd(8)} ${String(r.sender).padStart(12)} -> ${String(r.recipients).padEnd(18)} ${subj}`;
}
function fmtReceipts(r: { intended: string[]; readers: { id: string; at: string | null }[]; unread: string[] }): string {
  if (!r.intended.length) return "receipts: (no registered recipients)";
  const read = r.readers.map((x) => `${x.id}${x.at ? "✓" : "⤷"}`).join(" ") || "—";
  const unread = r.unread.length ? r.unread.join(" ") : "—";
  return `seen ${r.readers.length}/${r.intended.length}  ·  read: ${read}  ·  unread: ${unread}`;
}
function readBody(spec?: string): string {
  if (spec == null) return "";
  if (spec === "-") return readFileSync(0, "utf8");
  if (spec.startsWith("@")) return readFileSync(spec.slice(1), "utf8");
  return spec;
}
async function printWho(activeOnly: boolean) {
  // claude M3 (ruling b): presence is judged by the SERVER clock — the same
  // core isActive() the local path uses, i.e. true parity. The client clock
  // is never consulted (skew would flip ●/○ and, worse, re-filter rows the
  // server already judged active). Active set = server's activeOnly answer;
  // `who --all` = all rows + that set. No wire change, one extra read.
  let rows: any[];
  let activeIds: Set<string> | null = null;
  if (REMOTE) {
    const act: any[] = unwrap(await CALL("listAgents", "", { activeOnly: true }));
    activeIds = new Set(act.map((r) => r.id));
    rows = activeOnly ? act : unwrap(await CALL("listAgents", "", { activeOnly: false }));
  } else {
    rows = core().listAgents(false); // legacy parity: fetch all, filter below (tie order pinned by golden)
  }
  console.log(activeOnly ? "active agents:" : "known agents:");
  let shown = 0;
  for (const r of rows) {
    const act = activeIds ? activeIds.has(r.id) : core().isActive(r.last_seen ?? "");
    if (activeOnly && !act) continue;
    shown++;
    console.log(`  ${act ? "●" : "○"} ${String(r.id).padEnd(20)} role=${String(r.role ?? "-").padEnd(14)} caps=${(r.caps || "-").padEnd(24)} seen=${r.last_seen}`);
  }
  if (!shown) console.log("  (none)");
}

/** Unwrap a core Res or exit via the core's §7 table — the shell maps, never invents (finding 19). */
function unwrap<T>(r: Res<T>): T {
  if (r.error) {
    console.error(r.detail);
    process.exit(EXIT_CODES[r.error]);
  }
  return r.value;
}

// ---------- commands ----------
async function cmdJoin(a: Args) {
  if (!a.agent || !a.role) { console.error("error: join requires --agent and --role"); process.exit(2); }
  const fp = a.fingerprint ?? process.env.COMMS_FINGERPRINT ?? null;
  // §7: remote join does not stamp the server's pid (UPDATE-only branch, §5)
  // and the server IGNORES fingerprint — the token row is the identity.
  const v = unwrap(await CALL("joinAgent", a.agent, { agent: a.agent, role: a.role, caps: a.caps, fingerprint: fp }));
  // F CLI: join --group <name> = create-if-missing + join self.
  if (a.group) unwrap(await CALL("groupJoin", a.agent, { name: a.group, agent: a.agent }));
  console.log(`joined: ${a.agent} (role=${a.role}, caps=${a.caps || "-"}${a.group ? `, group=${a.group}` : ""}, ${REMOTE ? "remote — identity from token" : fp ? `fp=${String(fp).slice(0, 8)}…` : "fp=UNSET — identity unprotected, set COMMS_FINGERPRINT"})`);
  await printWho(true);
  console.log(`\n${v.unresolved} unresolved message(s) in flight. Run: bun comms.ts inbox --for ${a.agent} --open`);
}

async function cmdPost(a: Args) {
  if (!a.sender || !a.to) { console.error("error: post requires --from and --to"); process.exit(2); }
  const body = readBody(a.body);
  const p: Record<string, unknown> = {
    from: a.sender, to: a.to, type: a.type, subject: a.subject, body,
    // --as: RFC §5 impersonation (requires post:as in server mode); legacy CLI
    // silently ignored the flag in local mode — core treats absent as no-op.
    ...(a.as ? { as: String(a.as) } : {}),
    thread: a.thread, re: a.re, tags: a.tags, channel: a.channel,
  };
  // §6/§7: remote CLI generates ONE idempotency key per logical post; the
  // backoff retry re-sends the same params object ⇒ same key ⇒ replay, not
  // duplicate. Local mode never sends keys (byte-parity: local has no retry).
  if (REMOTE) p.idempotencyKey = `cli:${a.sender}:${crypto.randomUUID()}`.slice(0, 128);
  const v = unwrap(await CALL("post", a.sender, p));
  // §7: `file` is server-relative in remote mode; use `read` (or GET /raw) for content.
  console.log(`posted ${v.id}  [#${v.channel}]  thread=${v.thread}  -> ${a.to}  (${basename(v.file)})`);
}

// M1.5 G1: dm sugar — channel + recipients resolve via the member PAIR.
async function cmdDm(a: Args) {
  if (!a.sender || !a.to) { console.error("error: dm requires --from and --to"); process.exit(2); }
  const body = readBody(a.body);
  const p: Record<string, unknown> = { from: a.sender, to: a.to, type: a.type ?? "note", subject: a.subject, body, dm: a.to };
  if (REMOTE) p.idempotencyKey = `cli:${a.sender}:${crypto.randomUUID()}`.slice(0, 128);
  const v = unwrap(await CALL("post", a.sender, p));
  console.log(`dm ${v.id}  [#${v.channel}]  thread=${v.thread}  -> ${a.to}  (${basename(v.file)})`);
}

// G4: dms --for <agent> — the agent's dm channels (member view), newest first.
async function cmdDms(a: Args) {
  const who = a.agent ?? a.for;
  if (!who) { console.error("error: dms requires --for"); process.exit(2); }
  const chans = unwrap(await CALL("channels", who, {})).filter((c: any) => String(c.name).startsWith("dm~"));
  const mine: any[] = [];
  for (const c of chans) {
    const members = unwrap(await CALL("dmMembers", who, { channel: c.name }));
    if (members.includes(who)) mine.push({ ...c, members });
  }
  for (const c of mine) console.log(`#${c.name}  ${c.n} msg(s)  last=${c.last ?? "?"}  members=${c.members.join(",")}`);
  console.log(mine.length ? `\n${mine.length} dm channel(s) for ${who}. Read one: bun comms.ts inbox --for ${who} --channel <name>` : `(no DM channels for ${who})`);
}

// M1.5 F: group verbs (self-organizing; delete needs agents:admin — local root is).
async function cmdGroup(a: Args) {
  const sub = a._pos?.[0];
  const name = a.group ?? a._pos?.[1];
  const who = a.agent ?? "";
  switch (sub) {
    case "create": {
      const v = unwrap(await CALL("groupCreate", who, { name, agent: a.agent }));
      console.log(v.created ? `group created: ${v.name}` : `group already exists: ${v.name}`);
      return;
    }
    case "join": {
      const v = unwrap(await CALL("groupJoin", who, { name, agent: a.agent }));
      console.log(`joined ${v.name} (${v.members.length} member(s): ${v.members.join(", ")})`);
      return;
    }
    case "leave": {
      const v = unwrap(await CALL("groupLeave", who, { name, agent: a.agent }));
      console.log(v.left ? `left ${v.name}` : `was not a member of ${v.name}`);
      return;
    }
    case "list": {
      const v = unwrap(await CALL("groupList", who, {}));
      if (!v.groups.length) { console.log("(no groups)"); return; }
      for (const g of v.groups)
        console.log(`  ${g.mine ? "*" : " "} ${String(g.name).padEnd(24)} ${String(g.members).padStart(3)} member(s)  by=${g.created_by} since=${g.created_at}`);
      return;
    }
    case "show": {
      const v = unwrap(await CALL("groupShow", who, { name }));
      console.log(`group ${v.name} (by ${v.created_by}, since ${v.created_at}):`);
      for (const m of v.members) console.log(`  ${m}`);
      return;
    }
    case "delete": {
      const v = unwrap(await CALL("groupDelete", who, { name }));
      console.log(`deleted group ${v.name} (undelivered backlog drops from members' inboxes — mailing-list semantics)`);
      return;
    }
    default:
      console.error("usage: group create|join|leave|list|show|delete [name] [--agent who] [--group name]");
      process.exit(2);
  }
}

async function cmdInbox(a: Args) {
  const v = unwrap(await CALL("inbox", a.agent, { agent: a.agent, open: a.open, unread: a.unread, channel: a.channel, ...(a["no-all"] ? { noAll: true } : {}) }));
  // claude M3 B2: core returns a Set, the wire (and the Session contract)
  // an array — one CALL surface must normalize ONE shape.
  const unread = new Set<string>(v.unreadIds);
  let shown = 0;
  for (const r of v.rows) { console.log(fmtRow(r, unread.has(r.id))); shown++; }
  console.log(shown ? `\n${shown} message(s). '*' = unread. Read: bun comms.ts read --for ${a.agent} --id <ID>` : "(inbox empty for filter)");
}

async function cmdRead(a: Args) {
  const r = unwrap(await CALL("read", a.agent, { agent: a.agent, id: a.id }));
  console.log(`--- ${r.id} | thread ${r.thread} | ${r.type} | ${r.status}`);
  console.log(`from ${r.sender} -> ${r.recipients} | ${r.created_at}`);
  if (r.re) console.log(`re: ${r.re}`);
  if (r.tags) console.log(`tags: ${r.tags}`);
  console.log(`\n# ${r.subject}\n\n${r.body}\n\n(file: ${r.file})`);
  console.log(`\n${fmtReceipts(r.receipts)}`);
}

async function cmdReceipts(a: Args) {
  const r = unwrap(await CALL("receipts", "", { id: a.id }));
  console.log(`${r.id}  [${r.type}/${r.status}]  ${r.sender} -> ${r.recipients}`);
  console.log(`  ${r.subject}`);
  console.log(`  ${fmtReceipts(r.receipts)}`);
  console.log(`  (✓ = opened via read · ⤷ = inferred from a reply)`);
}

async function cmdChannels() {
  const list = unwrap(await CALL("channels", "", {}));
  console.log("channels:");
  for (const c of list)
    console.log(`  #${String(c.name).padEnd(14)} ${String(c.n).padStart(4)} msgs  last=${c.last ?? "-"}  ${c.purpose ?? ""}`);
  console.log(`\nfilter any view: --channel <name>  ·  post to one: post ... --channel <name> (or reply, which inherits)`);
}

async function cmdThread(a: Args) {
  const v = unwrap(await CALL("threadOf", "", { id: a.id }));
  console.log(`thread ${a.id}  (${v.rows.length} message(s))`);
  v.rows.forEach((r: MsgRow, i: number) => console.log(`${fmtRow(r)}  seen ${v.receipts[i].readers.length}/${v.receipts[i].intended.length}`));
}

async function setStatus(agent: string, id: string, state: string) {
  const v = unwrap(await CALL("setStatus", agent, { agent, id, state }));
  console.log(`${v.id} -> ${v.status}`);
}

async function cmdRename(a: Args) {
  if (!a.agent || !a.to) { console.error("error: rename requires --agent <old> --to <new>"); process.exit(2); }
  const fp = a.fingerprint ?? process.env.COMMS_FINGERPRINT ?? null;
  unwrap(await CALL("rename", a.agent, { agent: a.agent, to: a.to, fingerprint: fp }));
  console.log(`renamed ${a.agent} -> ${a.to} (announced to @all). New activity uses ${a.to}; history keeps ${a.agent}.`);
}

// ---------- token subcommand (§5/§6; M3) ----------
async function cmdToken(a: Args) {
  const sub = a._pos?.[0];
  switch (sub) {
    case "create": {
      if (!a.agent) { console.error("error: token create requires --agent"); process.exit(2); }
      const scopes = a.scopes ? String(a.scopes).split(",").map((s) => s.trim()).filter(Boolean) : undefined;
      const v = unwrap(await CALL("tokenCreate", a.agent, {
        agent: a.agent, kind: a.kind, label: a.label, scopes, admin: !!a.admin, force: !!a.force,
      }));
      console.log(`token for ${v.agentId} (kind=${a.kind ?? "agent"}, scopes=${v.scopes}):`);
      console.log(`  ${v.token}`);
      console.log(`  prefix=${v.prefix} id=${v.id} — shown ONCE; store it now.`);
      if (!REMOTE) console.log("  (local bootstrap — the host is root; server mode requires tokens:admin)");
      return;
    }
    case "list": {
      const v = unwrap(await CALL("tokenList", a.agent ?? "", {}));
      if (!v.tokens.length) { console.log("(no tokens)"); return; }
      for (const t of v.tokens)
        console.log(`  #${String(t.id).padStart(3)} ${String(t.agentId).padEnd(16)} ${String(t.kind).padEnd(5)} prefix=${t.prefix} scopes=${t.scopes.join(",")}${t.revoked_at ? ` REVOKED@${t.revoked_at}` : ""} last=${t.last_used ?? "?"}`);
      return;
    }
    case "revoke": {
      const id = Number(a.id ?? a._pos?.[1]);
      if (!Number.isFinite(id)) { console.error("error: token revoke requires --id <tokenId>"); process.exit(2); }
      unwrap(await CALL("tokenRevoke", a.agent ?? "", { id }));
      console.log(`token #${id} revoked (one-way door: re-verify fails closed)`);
      return;
    }
    default:
      console.error("usage: token create --agent <id> [--kind agent|human] [--scopes a,b] [--admin] [--force] [--label L] | token list | token revoke --id <N>");
      process.exit(2);
  }
}

// ---------- watch ----------
async function cmdWatch(a: Args) {
  if (REMOTE) return cmdWatchRemote(a);
  const bus = core();
  const role = bus.roleOf(a.agent);
  // F: membership resolves at TAIL time — reload the incarnation Map every tick.
  let mem = bus.membershipsOf(a.agent);
  const ivSec = a.interval ?? 3;
  const capSec = a.timeout ?? 28800;
  const seen = new Set(bus.allMessageIds()); // core accessor — no SQL in the shell (finding 9)
  const scope = a.all ? (a.channel ? `all of #${a.channel}` : "ALL channels (firehose)") : a["no-all"] ? (a.channel ? `#${a.channel} addressed to me (no @all)` : "addressed to me (no @all)") : (a.channel ? `#${a.channel} addressed to me` : "addressed to me");
  console.log(`watch: ${a.agent} (role=${role ?? "-"}) scope=${scope}; every ${ivSec}s; ${a.once ? "one-shot" : `cap ${capSec}s`}. baseline=${seen.size} msgs`);
  let stop = false;
  process.on("SIGTERM", () => { stop = true; });
  const started = Date.now();
  while (!stop) {
    bus.touch(a.agent);
    mem = bus.membershipsOf(a.agent);
    let hitForMe = false;
    for (const r of bus.allMessages()) {
      if (seen.has(r.id)) continue;
      seen.add(r.id);
      if (r.sender === a.agent) continue;
      if (a.channel && r.channel !== a.channel) continue;
      // --all surfaces every message in scope (a whole channel); default = only addressed to me;
      // E1 --no-all drops the @all broadcast arm (auditor watch — announces must not
      // subscribe you to the firehose; role/group/id arms still deliver).
      if (a.all || bus.recipientsMatch(r.recipients, a.agent, role, mem, r.created_at, a["no-all"] ? { ignoreAll: true } : undefined)) { console.log(`NEW ${fmtRow(r)}`); hitForMe = true; }
    }
    if (a.once) break;
    if (hitForMe && a["exit-on-new"]) { console.log("watch: message for me; exiting"); break; }
    if (Date.now() - started >= capSec * 1000) { console.log("watch: timeout cap reached; exiting"); break; }
    await Bun.sleep(ivSec * 1000);
  }
}

/**
 * M3 cursor-backed watch (§6 Watch durability): persists position via
 * cursors(agent_id, consumer='cli') so back-to-back `watch --exit-on-new`
 * runs never skip what arrived between them. At-least-once: the cursor is
 * committed AFTER printing. A resync (epoch rotated / below gc floor) is
 * recovered by the §6 recovery commit — cursor.set to `<epoch>.<floor>`
 * with force (a foreign row must not block it) — then delivery resumes
 * from the floor, exactly where retained history starts.
 */
async function cmdWatchRemote(a: Args) {
  const ivSec = a.interval ?? 3;
  const capSec = a.timeout ?? 28800;
  // claude M3: resolve the PRINCIPAL first (cheap read; identity comes from
  // the token row via x-comms-agent) — the consumer key and the `for` param
  // both depend on whether --for names the principal or someone else.
  await CALL("cursorGet", a.agent, { consumer: "cli" });
  const me = remoteIdentity.id;
  const target: string = a.agent ?? me;
  const other = !!me && target !== me;
  // claude M3 M2/M3: consumer NAMESPACING. The cursor is one position in the
  // events space; a client-side filter (--channel) or a different target
  // (--for other, read:all peek) or a different predicate (--all) that
  // commits into the SAME row would permanently skip what the other
  // views haven't printed yet. Plain `watch --for <me>` keeps 'cli'.
  const consumer = typeof a.consumer === "string" ? a.consumer
    : `cli${other ? `@${target}` : ""}${a.all ? ".all" : ""}${a["no-all"] ? ".noall" : ""}${a.channel ? `#${a.channel}` : ""}`;
  // E1: the predicate variant MUST live in the consumer key — a noAll cursor
  // sharing 'cli' would skip @all rows the default view never printed (§6).
  const scope = a.all ? (a.channel ? `all of #${a.channel}` : "ALL channels (firehose)") : a["no-all"] ? (a.channel ? `#${a.channel} addressed to me (no @all)` : "addressed to me (no @all)") : (a.channel ? `#${a.channel} addressed to me` : "addressed to me");
  console.log(`watch: ${target} scope=${scope}; every ${ivSec}s; ${a.once ? "one-shot" : `cap ${capSec}s`} (remote, cursor consumer=${consumer})`);
  let stop = false;
  process.on("SIGTERM", () => { stop = true; });
  const started = Date.now();
  const recover = async (data?: Record<string, any>) => {
    const floor = Number(data?.floor ?? 0);
    unwrap(await CALL("cursorSet", a.agent, { consumer, cursor: `${data?.epoch}.${floor}`, force: true }));
    console.log(`watch: resync — recovery commit to ${data?.epoch}.${floor} (retained history resumes here)`);
  };
  /** commit AFTER printing (at-least-once). `conflict` = a concurrent watcher
   *  on the same consumer already committed further ⇒ benign (it printed
   *  those rows), never fatal. Returns false when a resync was recovered. */
  const commit = async (cursor: string): Promise<boolean> => {
    const r = await CALL("cursorSet", a.agent, { consumer, cursor });
    if (r.error === "resync") { await recover(r.data); return false; }
    if (r.error === "conflict") return true;
    unwrap(r);
    return true;
  };
  // our last known committed position (skip no-op commits when idle)
  const c0 = await CALL("cursorGet", a.agent, { consumer });
  let committed = c0.error ? "" : `${c0.value.epoch}.${c0.value.seq}`;
  while (!stop) {
    let hitForMe = false;
    if (a.all) {
      // Firehose: history since-mode ASC pages (events space — same cursor
      // space as waitStep). Requires read:all (§6); paging to hasMore=false
      // delivers every row exactly once. Channel filter is pushed SERVER-side
      // (history supports it) — no pulling the whole bus to print one channel.
      const cur = await CALL("cursorGet", a.agent, { consumer });
      if (cur.error === "resync") { await recover(cur.data); continue; }
      const v0 = unwrap(cur);
      let since = `${v0.epoch}.${v0.seq}`;
      let resynced = false;
      for (;;) {
        const hr = await CALL("history", a.agent, { since, limit: 1000, ...(a.channel ? { channel: a.channel } : {}) });
        // claude M3 M4: a fresh consumer ({epoch, 0}) on a GC'd server is
        // below the floor ⇒ history resyncs; that is the SAME §6 recovery.
        if (hr.error === "resync") { await recover(hr.data); resynced = true; break; }
        const h = unwrap(hr);
        for (const r of h.rows) {
          if (r.sender === target) continue;
          console.log(`NEW ${fmtRow(r)}`); hitForMe = true;
        }
        since = h.cursor;
        if (!h.hasMore) break;
      }
      if (resynced) continue;
      if (since !== `${v0.epoch}.${v0.seq}` && !(await commit(since))) continue;
    } else {
      // server defaults `since` from the stored (principal, consumer) cursor;
      // never auto-advances — we commit AFTER printing (at-least-once).
      // claude M3 B1: waitStep scans ≤500 EVENTS per step and returns the
      // scanned-to cursor even when nothing matched. Committing only when
      // messages.length>0 livelocked the watcher forever behind any 500
      // irrelevant events. Commit whenever the cursor ADVANCED, and drain
      // (no sleep) while it keeps advancing.
      let resynced = false;
      let last = committed;
      for (;;) {
        const w = await CALL("waitStep", a.agent, { consumer, ...(other ? { for: target } : {}), ...(a["no-all"] ? { noAll: true } : {}) });
        if (w.error === "resync") { await recover(w.data); committed = ""; resynced = true; break; }
        const v = unwrap(w);
        for (const m of v.messages) {
          if (a.channel && m.channel !== a.channel) continue;
          console.log(`NEW ${fmtRow(m)}`); hitForMe = true;
        }
        if (v.cursor === last) break; // caught up: nothing scanned past our position
        last = v.cursor;
        if (!(await commit(v.cursor))) { committed = ""; resynced = true; break; }
        committed = v.cursor;
      }
      if (resynced) continue;
    }
    if (a.once) break;
    if (hitForMe && a["exit-on-new"]) { console.log("watch: message for me; exiting"); break; }
    if (Date.now() - started >= capSec * 1000) { console.log("watch: timeout cap reached; exiting"); break; }
    await Bun.sleep(ivSec * 1000);
  }
}

// ---------- arg parsing ----------
type Args = Record<string, any>;
function parse(argv: string[]): Args {
  const [cmd, ...rest] = argv;
  const a: Args = { cmd, _pos: [] as string[] };
  for (let i = 0; i < rest.length; i++) {
    const t = rest[i];
    if (!t.startsWith("--")) { a._pos.push(t); continue; }
    const key = t.slice(2) === "for" ? "agent" : t.slice(2) === "from" ? "sender" : t.slice(2);
    const next = rest[i + 1];
    if (next === undefined || next.startsWith("--")) { a[key] = true; }
    else { a[key] = next; i++; }
  }
  if (a.interval) a.interval = Number(a.interval);
  if (a.timeout) a.timeout = Number(a.timeout);
  return a;
}

const HELP = `comms — join-able agent comms (local sqlite or hosted server)
commands: join | rename | who | post | dm | dms | inbox | read | thread | receipts | channels | group | token | ack | done | status | watch
transport: COMMS_URL+COMMS_TOKEN ⇒ remote · --local forces direct · else COMMS_HOME direct (§7)
group: group create|join|leave|list|show|delete <name> [--agent who] · post --to group:<name>
token: token create --agent <id> [--kind human] [--scopes a,b] [--admin] [--force] | token list | token revoke --id N
watch --all (remote) pages history since-cursors: public rows are visible to every token; dm~ rows need membership or read:dm (ruling c).
watch --no-all (E1) drops the @all broadcast arm: only id/role/group-addressed mail prints (auditor watch; consumer-namespaced cursor).
run 'bun comms.ts <cmd> --help-ish' — see header of this file for full usage.`;

async function main() {
  const a = parse(process.argv.slice(2));
  setupTransport(a);
  if (REMOTE && !remoteToken && a.cmd !== "help" && a.cmd !== undefined) {
    console.error("error: COMMS_URL set but no token — pass --token or set COMMS_TOKEN");
    process.exit(3);
  }
  switch (a.cmd) {
    case "join": return await cmdJoin(a);
    case "rename": return await cmdRename(a);
    case "who": return await printWho(!a.all);
    case "post": return await cmdPost(a);
    case "dm": return await cmdDm(a);
    case "dms": return await cmdDms(a);
    case "group": return await cmdGroup(a);
    case "token": return await cmdToken(a);
    case "inbox": return await cmdInbox(a);
    case "read": return await cmdRead(a);
    case "thread": return await cmdThread(a);
    case "receipts": return await cmdReceipts(a);
    case "channels": return await cmdChannels();
    case "ack": return await setStatus(a.agent, a.id, "acked");
    case "done": return await setStatus(a.agent, a.id, "done");
    case "status": return await setStatus(a.agent, a.id, a.state);
    case "watch": return await cmdWatch(a);
    case "history": {
      const v = unwrap(await CALL("history", a.agent ?? "", { channel: a.channel, since: a.since, limit: a.limit }));
      for (const r of v.rows) console.log(fmtRow(r));
      console.log(`\ncursor=${v.cursor} hasMore=${v.hasMore}`);
      return;
    }
    default:
      console.log(HELP);
      process.exit(a.cmd ? 2 : 0);
  }
}
// UNCONDITIONAL on purpose: the root ./comms.ts shim does `import "./bin/comms.ts"`,
// where import.meta.main is false — a guard here silently no-ops every shim
// command with exit 0 (AGENTS.md usage). Tests import the wire map from
// src/cli-wire.ts instead, never this file.
await main();
