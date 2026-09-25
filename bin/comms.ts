#!/usr/bin/env bun
/**
 * comms CLI — thin shell over the bus core (RFC-001 §3 piece 4).
 *
 * All DB logic lives in src/bus.ts. This file owns ONLY: argv parsing, env
 * (COMMS_HOME / COMMS_FINGERPRINT / COMMS_TEST_SEAMS), readBody (@file/stdin),
 * rendering, exit codes, and the watch polling loop. No SQL, no DB access.
 * Byte-identical local-mode behavior is enforced by tests/golden.test.ts.
 *
 * Env: COMMS_HOME        project root holding .comms/ (DB) + messages/ (default: auto-detected)
 *      COMMS_FINGERPRINT stable per-runtime identity for join/rename guards
 *      COMMS_TEST_SEAMS  JSON {at,seed,pid} — frozen seams for the golden harness (test-only)
 */
import { dirname, join, basename } from "node:path";
import { fileURLToPath } from "node:url";
import { existsSync, readFileSync } from "node:fs";
import { openBus, localCtx, EXIT_CODES, type MsgRow, type Res } from "../src/bus.ts";
import { testSeams, type Seams } from "../src/seams.ts";

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
const seams: Seams | undefined = process.env.COMMS_TEST_SEAMS
  ? testSeams(JSON.parse(process.env.COMMS_TEST_SEAMS))
  : undefined;
const bus = openBus({ home: HOME, mode: "local", seams });

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
function printWho(activeOnly: boolean) {
  const rows = bus.listAgents(false);
  console.log(activeOnly ? "active agents:" : "known agents:");
  let shown = 0;
  for (const r of rows) {
    const act = bus.isActive(r.last_seen ?? "");
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
function cmdJoin(a: Args) {
  if (!a.agent || !a.role) { console.error("error: join requires --agent and --role"); process.exit(2); }
  const fp = a.fingerprint ?? process.env.COMMS_FINGERPRINT ?? null;
  const v = unwrap(bus.joinAgent(localCtx(a.agent), { agent: a.agent, role: a.role, caps: a.caps, fingerprint: fp }));
  // F CLI: join --group <name> = create-if-missing + join self.
  if (a.group) unwrap(bus.groupJoin(localCtx(a.agent), { name: a.group, agent: a.agent }));
  console.log(`joined: ${a.agent} (role=${a.role}, caps=${a.caps || "-"}${a.group ? `, group=${a.group}` : ""}, ${fp ? `fp=${String(fp).slice(0, 8)}…` : "fp=UNSET — identity unprotected, set COMMS_FINGERPRINT"})`);
  printWho(true);
  console.log(`\n${v.unresolved} unresolved message(s) in flight. Run: bun comms.ts inbox --for ${a.agent} --open`);
}

function cmdPost(a: Args) {
  if (!a.sender || !a.to) { console.error("error: post requires --from and --to"); process.exit(2); }
  const body = readBody(a.body);
  const v = unwrap(bus.post(localCtx(a.sender), {
    from: a.sender, to: a.to, type: a.type, subject: a.subject, body,
    thread: a.thread, re: a.re, tags: a.tags, channel: a.channel,
  }));
  console.log(`posted ${v.id}  [#${v.channel}]  thread=${v.thread}  -> ${a.to}  (${basename(v.file)})`);
}

// M1.5 G1: dm sugar — channel + recipients resolve via the member PAIR.
function cmdDm(a: Args) {
  if (!a.sender || !a.to) { console.error("error: dm requires --from and --to"); process.exit(2); }
  const body = readBody(a.body);
  const v = unwrap(bus.post(localCtx(a.sender), {
    from: a.sender, to: a.to, type: a.type ?? "note", subject: a.subject, body, dm: a.to,
  }));
  console.log(`dm ${v.id}  [#${v.channel}]  thread=${v.thread}  -> ${a.to}  (${basename(v.file)})`);
}

// M1.5 F: group verbs (self-organizing; delete needs agents:admin — local root is).
function cmdGroup(a: Args) {
  const sub = a._pos?.[0];
  const name = a.group ?? a._pos?.[1];
  const who = a.agent ?? "";
  const ctx = localCtx(who);
  switch (sub) {
    case "create": {
      const v = unwrap(bus.groupCreate(ctx, { name, agent: a.agent }));
      console.log(v.created ? `group created: ${v.name}` : `group already exists: ${v.name}`);
      return;
    }
    case "join": {
      const v = unwrap(bus.groupJoin(ctx, { name, agent: a.agent }));
      console.log(`joined ${v.name} (${v.members.length} member(s): ${v.members.join(", ")})`);
      return;
    }
    case "leave": {
      const v = unwrap(bus.groupLeave(ctx, { name, agent: a.agent }));
      console.log(v.left ? `left ${v.name}` : `was not a member of ${v.name}`);
      return;
    }
    case "list": {
      const v = unwrap(bus.groupList(ctx));
      if (!v.groups.length) { console.log("(no groups)"); return; }
      for (const g of v.groups)
        console.log(`  ${g.mine ? "*" : " "} ${String(g.name).padEnd(24)} ${String(g.members).padStart(3)} member(s)  by=${g.created_by} since=${g.created_at}`);
      return;
    }
    case "show": {
      const v = unwrap(bus.groupShow(ctx, { name }));
      console.log(`group ${v.name} (by ${v.created_by}, since ${v.created_at}):`);
      for (const m of v.members) console.log(`  ${m}`);
      return;
    }
    case "delete": {
      const v = unwrap(bus.groupDelete(ctx, { name }));
      console.log(`deleted group ${v.name} (undelivered backlog drops from members' inboxes — mailing-list semantics)`);
      return;
    }
    default:
      console.error("usage: group create|join|leave|list|show|delete [name] [--agent who] [--group name]");
      process.exit(2);
  }
}

function cmdInbox(a: Args) {
  const v = unwrap(bus.inbox(localCtx(a.agent), { agent: a.agent, open: a.open, unread: a.unread, channel: a.channel }));
  let shown = 0;
  for (const r of v.rows) { console.log(fmtRow(r, v.unreadIds.has(r.id))); shown++; }
  console.log(shown ? `\n${shown} message(s). '*' = unread. Read: bun comms.ts read --for ${a.agent} --id <ID>` : "(inbox empty for filter)");
}

function cmdRead(a: Args) {
  const r = unwrap(bus.read(localCtx(a.agent), { agent: a.agent, id: a.id }));
  console.log(`--- ${r.id} | thread ${r.thread} | ${r.type} | ${r.status}`);
  console.log(`from ${r.sender} -> ${r.recipients} | ${r.created_at}`);
  if (r.re) console.log(`re: ${r.re}`);
  if (r.tags) console.log(`tags: ${r.tags}`);
  console.log(`\n# ${r.subject}\n\n${r.body}\n\n(file: ${r.file})`);
  console.log(`\n${fmtReceipts(r.receipts)}`);
}

function cmdReceipts(a: Args) {
  const r = unwrap(bus.receipts(localCtx(""), a.id));
  console.log(`${r.id}  [${r.type}/${r.status}]  ${r.sender} -> ${r.recipients}`);
  console.log(`  ${r.subject}`);
  console.log(`  ${fmtReceipts(r.receipts)}`);
  console.log(`  (✓ = opened via read · ⤷ = inferred from a reply)`);
}

function cmdChannels() {
  const list = unwrap(bus.channels(localCtx("")));
  console.log("channels:");
  for (const c of list)
    console.log(`  #${String(c.name).padEnd(14)} ${String(c.n).padStart(4)} msgs  last=${c.last ?? "-"}  ${c.purpose ?? ""}`);
  console.log(`\nfilter any view: --channel <name>  ·  post to one: post ... --channel <name> (or reply, which inherits)`);
}

function cmdThread(a: Args) {
  const v = unwrap(bus.threadOf(localCtx(""), a.id));
  console.log(`thread ${a.id}  (${v.rows.length} message(s))`);
  v.rows.forEach((r, i) => console.log(`${fmtRow(r)}  seen ${v.receipts[i].readers.length}/${v.receipts[i].intended.length}`));
}

function setStatus(agent: string, id: string, state: string) {
  const v = unwrap(bus.setStatus(localCtx(agent), { agent, id, state }));
  console.log(`${v.id} -> ${v.status}`);
}

function cmdRename(a: Args) {
  if (!a.agent || !a.to) { console.error("error: rename requires --agent <old> --to <new>"); process.exit(2); }
  const fp = a.fingerprint ?? process.env.COMMS_FINGERPRINT ?? null;
  unwrap(bus.rename(localCtx(a.agent), { agent: a.agent, to: a.to, fingerprint: fp }));
  console.log(`renamed ${a.agent} -> ${a.to} (announced to @all). New activity uses ${a.to}; history keeps ${a.agent}.`);
}

async function cmdWatch(a: Args) {
  const role = bus.roleOf(a.agent);
  // F: membership resolves at TAIL time — reload the incarnation Map every tick.
  let mem = bus.membershipsOf(a.agent);
  const ivSec = a.interval ?? 3;
  const capSec = a.timeout ?? 28800;
  const seen = new Set(bus.allMessageIds()); // core accessor — no SQL in the shell (finding 9)
  const scope = a.all ? (a.channel ? `all of #${a.channel}` : "ALL channels (firehose)") : (a.channel ? `#${a.channel} addressed to me` : "addressed to me");
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
      // --all surfaces every message in scope (a whole channel); default = only addressed to me
      if (a.all || bus.recipientsMatch(r.recipients, a.agent, role, mem, r.created_at)) { console.log(`NEW ${fmtRow(r)}`); hitForMe = true; }
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

const HELP = `comms — join-able serverless agent comms (bun:sqlite)
commands: join | rename | who | post | dm | inbox | read | thread | receipts | channels | group | ack | done | status | watch
group: group create|join|leave|list|show|delete <name> [--agent who] · post --to group:<name>
run 'bun comms.ts <cmd> --help-ish' — see header of this file for full usage.`;

async function main() {
  const a = parse(process.argv.slice(2));
  switch (a.cmd) {
    case "join": return cmdJoin(a);
    case "rename": return cmdRename(a);
    case "who": return printWho(!a.all);
    case "post": return cmdPost(a);
    case "dm": return cmdDm(a);
    case "group": return cmdGroup(a);
    case "inbox": return cmdInbox(a);
    case "read": return cmdRead(a);
    case "thread": return cmdThread(a);
    case "receipts": return cmdReceipts(a);
    case "channels": return cmdChannels();
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
