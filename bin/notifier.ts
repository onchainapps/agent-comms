#!/usr/bin/env bun
/**
 * bin/notifier.ts — RFC-002 delivery doorbells (N1, verdict-folded @comments 85+86).
 * Out-of-process push fan-out: consumes the bus through the SAME remote RPC surface
 * as the CLI (inbox.wait / cursor.set — at-least-once, cursor truth unchanged),
 * fires pointer-only hooks (http POST / exec argv), and never touches the server's
 * write path. RPC-only — never local-direct (§9; claude MAJOR-1).
 *
 * Design rules (RFC-002 §1 + verdicts):
 *  - Pointer-only: hooks receive {id, channel, thread, sender, to, type, subject,
 *    created_at} — NEVER body/file. Content stays pull under the recipient's token.
 *  - Advisory: a lost or duplicated doorbell costs latency only; the demoted cron
 *    sweep is the floor (claude MAJOR-2 — crons are NOT deleted, only relaxed).
 *  - ONE server-side predicate: per-sub inbox.wait with for=<recipient> (+ noAll per
 *    sub). No client-side addressed predicate (grok Q2 — no third recipientsMatch;
 *    SSE frames carry no recipients, scope=all fan-out is rejected).
 *  - Own-token default (claude Q3): a sub runs under the RECIPIENT's token — no
 *    read:all omniview, dm~ doorbells just work (canSee is the caller's ctx). The
 *    notifier touches ONLY cursorGet/cursorSet/inbox.wait (read + cursor ops).
 *  - inbox.wait is NOT a long-poll (grok): the loop polls ≥1s; wake latency ≈ one
 *    poll interval. Re-step immediately only when the cursor jumped a full page.
 *  - Head init (claude MAJOR-3): a fresh consumer drains WITHOUT firing (retained
 *    history is context, not news). Resync = force-commit to floor, scan to head
 *    WITHOUT replay, then exactly ONE {event:"comms.resync"} ring.
 *  - Coalesce (claude Q5): wakes are the unit, not POSTs. Leading edge fires the
 *    first ring of a quiet period; everything after coalesces into ONE trailing
 *    ring at window end (default 60 s ≥ typical run length). Per-sub wake bucket:
 *    3 burst, refill 1/2min — steady state equals the cron it relaxes.
 *  - At-least-once: the cursor advances ONLY after a hook success; a held burst
 *    (failed hook or empty bucket) never lets the commit pass un-hooked matches.
 *    Empty-burst advances commit anyway (livelock guard, claude M3 B1 lineage).
 *  - exec hook = static argv (NO shell), burst on stdin as one JSON line, counts
 *    via COMMS_DOORBELL_* env (grok: placeholders were never the design).
 *
 * usage: bun bin/notifier.ts [--config <path>] [--once]
 *   config default: $COMMS_NOTIFIER_CONFIG or ~/.config/agent-comms/notifier.json
 *   Run on the SINK host (gateway binds loopback there; exec hooks are local).
 *   { "url": "http://bus-host", "token": "ac_…",            // fallback token
 *     "subscriptions": [
 *       { "name": "don-grok-mail", "for": "don-grok", "token": "ac_…",  // recipient's own
 *         "hook": { "kind": "http", "url": "http://127.0.0.1:8642/webhooks/p/don-grok/bus",
 *                   "secret": "…" },
 *         "coalesceMs": 60000, "noAll": false, "channel": null, "dmDoorbells": true } |
 *       { "name": "wake-pi", "for": "pi",
 *         "hook": { "kind": "exec", "argv": ["tmux","send-keys","-t","pi","check the bus","Enter"] } } ] }
 */
import { RpcBus } from "../src/rpc-bus.ts";
import type { MsgRow } from "../src/bus.ts";
import type { Session } from "../src/bus-iface.ts";

type Pointer = { id: string; channel: string; thread: string; sender: string; to: string; type: string; subject: string; created_at: string };
type HttpHook = { kind: "http"; url: string; secret?: string };
type ExecHook = { kind: "exec"; argv: [string, ...string[]] };
export type Sub = {
  name: string; for: string; hook: HttpHook | ExecHook; token?: string;
  coalesceMs?: number; noAll?: boolean; channel?: string | null; dmDoorbells?: boolean;
};
export type Config = { url: string; token: string; subscriptions: Sub[] };

export const NAME_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/;     // consumer-safe: notify.<name>[.noall] ∈ CONSUMER_RE
export const DM_RE = /^dm~/;
const FLUSH_MAX_MSGS = 50;        // memory bound: force a ring regardless of bucket/window
const BACKOFF_MAX_MS = 30_000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** per-sub wake bucket (claude Q5): 3 burst, refill 1 per 2 min. */
export class WakeBucket {
  tokens: number;
  private last = Date.now();
  constructor(readonly burst = 3, readonly refillMs = 120_000) { this.tokens = burst; }
  refill(now = Date.now()): void {
    const add = Math.floor((now - this.last) / this.refillMs);
    if (add > 0) { this.tokens = Math.min(this.burst, this.tokens + add); this.last += add * this.refillMs; }
  }
  take(now = Date.now()): boolean { this.refill(now); if (this.tokens <= 0) return false; this.tokens--; return true; }
}

export function toPointer(m: MsgRow): Pointer {
  return { id: m.id, channel: m.channel, thread: m.thread, sender: m.sender, to: m.recipients, type: m.type, subject: m.subject, created_at: m.created_at };
}

export async function fireHook(hook: HttpHook | ExecHook, sub: string, payload: { event: string; subscription: string; messages: Pointer[] }): Promise<void> {
  if (hook.kind === "http") {
    const res = await fetch(hook.url, {
      method: "POST",
      headers: { "content-type": "application/json", "user-agent": "agent-comms-notifier/1", ...(hook.secret ? { "x-comms-secret": hook.secret } : {}) },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(10_000),
    });
    if (!res.ok) throw new Error(`http ${res.status}`);
  } else {
    // exec: argv directly (NO shell — config is operator-owned; message bytes
    // never reach argv, they ride stdin as one JSON line; grok pin 6).
    const proc = Bun.spawn(hook.argv, {
      stdin: "pipe", stdout: "ignore", stderr: "pipe",
      env: { ...process.env, COMMS_DOORBELL_COUNT: String(payload.messages.length), COMMS_DOORBELL_SUB: sub, COMMS_DOORBELL_EVENT: payload.event },
    });
    proc.stdin.write(JSON.stringify(payload.messages) + "\n");
    proc.stdin.end();
    const t = setTimeout(() => { try { proc.kill(); } catch { /* already dead */ } }, 15_000);
    const code = await proc.exited;
    clearTimeout(t);
    if (code !== 0) {
      const err = new TextDecoder().decode(await new Response(proc.stderr).arrayBuffer()).slice(0, 200);
      throw new Error(`exec exit ${code}: ${err}`);
    }
  }
}

/** One subscription loop. Returns on --once completion or throws on fatal. */
export async function runSub(session: Session, sub: Sub, opts: { once?: boolean; log?: (s: string) => void } = {}): Promise<void> {
  const once = opts.once ?? false;
  const log = opts.log ?? ((s: string) => console.log(`[${sub.name}] ${s}`));
  // E1 rule: the predicate variant MUST live in the consumer key — a noAll cursor
  // sharing the default row would skip rows the other view never rang for.
  const consumer = `notify.${sub.name}${sub.noAll ? ".noall" : ""}`;
  const coalesceMs = sub.coalesceMs ?? 60_000;
  const bucket = new WakeBucket();

  // stored position (fresh consumer ⇒ {epoch, 0} ⇒ head-init pass below)
  let committed = "";
  const c0 = await session.cursorGet({ consumer });
  if (!c0.error) committed = `${c0.value.epoch}.${c0.value.seq}`;
  let headInit = !committed || committed.endsWith(".0");

  let scan = committed;
  const burst = new Map<string, Pointer>();   // id-keyed: rescan is idempotent
  let leadingFired = false;                   // quiet-period leading edge already rung
  let windowStart = 0;                        // first message AFTER the leading ring
  let resyncPending = false;                  // one comms.resync ring after a head scan
  let backoff = 500;

  const recover = async (data?: Record<string, any>) => {
    const floor = Number(data?.floor ?? 0);
    const r = await session.cursorSet({ consumer, cursor: `${data?.epoch}.${floor}`, force: true });
    if (r.error) throw new Error(`resync commit failed: ${r.error}`);
    committed = `${data?.epoch}.${floor}`; scan = committed;
    burst.clear(); leadingFired = false; windowStart = 0;
    headInit = true; resyncPending = true;    // scan to head, no replay, ONE ring (MAJOR-3)
    log(`resync — committed to ${committed}; scanning to head (no replay)`);
  };

  const commitScan = async (): Promise<boolean> => {
    if (!scan || scan === committed) return true;
    const r = await session.cursorSet({ consumer, cursor: scan });
    if (r.error === "resync") { await recover(r.data); return false; }
    if (r.error === "conflict") return true;  // concurrent watcher on same consumer: benign
    if (r.error) throw new Error(`cursorSet: ${r.error} ${r.detail ?? ""}`);
    committed = scan;
    return true;
  };

  /** ring the hook and (on mail success) commit. force bypasses bucket+window. */
  const ring = async (event: "comms.mail" | "comms.resync", msgs: Pointer[], force: boolean): Promise<boolean> => {
    if (!force && event === "comms.mail" && !bucket.take()) return false; // wake budget spent: hold (at-least-once)
    try { await fireHook(sub.hook, sub.name, { event, subscription: sub.name, messages: msgs }); }
    catch (e) {
      log(`hook FAILED (${String(e).slice(0, 160)}) — cursor held at ${committed}; burst retried next drain`);
      return false;
    }
    if (event === "comms.mail") {
      const ok = await commitScan(); // resync here ⇒ burst already fired ⇒ dup, not loss
      log(`doorbell x${msgs.length} → ${sub.hook.kind}${msgs.length === 1 ? ` (${msgs[0].id})` : ""}`);
      burst.clear(); windowStart = 0;
      return ok;
    }
    log(`${event} ring delivered`);
    return true;
  };

  for (;;) {
    const w = await session.waitStep({ ...(sub.for ? { for: sub.for } : {}), consumer, since: scan || undefined, ...(sub.noAll ? { noAll: true } : {}) });
    if (w.error === "resync") { await recover(w.data); backoff = 500; continue; }
    if (w.error) {
      if (w.error === "unavailable" || w.error === "contention" || w.error === "rate_limited") {
        log(`transient ${w.error} — backoff ${backoff}ms`);
        await sleep(backoff); backoff = Math.min(backoff * 2, BACKOFF_MAX_MS);
        continue;
      }
      throw new Error(`inbox.wait: ${w.error} ${w.detail ?? ""}`);
    }
    backoff = 500;
    for (const m of w.value.messages) {
      if (sub.channel && m.channel !== sub.channel) continue;
      if (sub.dmDoorbells === false && DM_RE.test(m.channel)) continue;
      if (!burst.has(m.id)) {
        burst.set(m.id, toPointer(m));
        if (leadingFired && !windowStart) windowStart = Date.now(); // trailing window opens after a leading ring
      }
    }
    const advanced = w.value.cursor !== scan;
    const pageJump = advanced && Number(w.value.cursor.split(".")[1]) - Number(scan.split(".")[1] ?? 0) >= 500;
    scan = w.value.cursor;
    if (headInit) {
      // MAJOR-3: scan ALL pages to head discarding matches (>500 events is
      // normal on a lived-in bus); commit once, at head; then one resync ring.
      burst.clear();
      if (advanced) continue;
      headInit = false;
      if (!(await commitScan())) continue;
      log("head init: retained history skipped (subscription starts at head)");
      if (resyncPending) {
        resyncPending = false;
        if (!(await ring("comms.resync", [], true))) await sleep(1000); // one ring, never a replay storm
      }
      if (once) return;
    } else if (advanced) {
      // draining a backlog: ring at the memory bound only (grok: re-step on full pages)
      if (burst.size >= FLUSH_MAX_MSGS && !(await ring("comms.mail", [...burst.values()], true))) await sleep(1000);
      if (pageJump || burst.size < FLUSH_MAX_MSGS) continue; // no sleep while the cursor moves
    }

    if (once) { // cron-shaped run: drain-and-flush, bucket/window never hold back
      if (burst.size === 0) { if (!(await commitScan())) continue; return; }
      if (!(await ring("comms.mail", [...burst.values()], true))) await sleep(1000);
      return;
    }

    if (burst.size === 0) {
      if (!(await commitScan())) continue; // empty-burst advance: commit (livelock guard)
    } else if (!leadingFired) {
      if (await ring("comms.mail", [...burst.values()], false)) leadingFired = true; // leading edge
      else if (bucket.tokens <= 0) await sleep(Math.max(1000, coalesceMs));          // bucket-empty: hold
    } else if (windowStart && Date.now() - windowStart >= coalesceMs) {
      if (await ring("comms.mail", [...burst.values()], false)) leadingFired = false; // trailing coalesce
    }
    await sleep(1000); // idle cadence: one RPC/s per sub, zero LLM
  }
}

// ---------- main ----------
if (import.meta.main) {
  const argv = process.argv.slice(2);
  const once = argv.includes("--once");
  const cp = argv.indexOf("--config");
  const path = cp >= 0 ? argv[cp + 1] : process.env.COMMS_NOTIFIER_CONFIG || `${process.env.HOME}/.config/agent-comms/notifier.json`;
  let cfg: Config;
  try { cfg = await Bun.file(path).json(); }
  catch (e) { console.error(`notifier: cannot read config ${path}: ${String(e)}`); process.exit(2); }
  const subs = cfg.subscriptions ?? [];
  if (!cfg.url || !cfg.token || !subs.length) { console.error("notifier: config needs url, token, subscriptions[]"); process.exit(2); }
  for (const s of subs) {
    if (!NAME_RE.test(s.name)) { console.error(`notifier: bad subscription name '${s.name}' (^[a-z0-9][a-z0-9._-]{0,63}$)`); process.exit(2); }
    if (!s.for) { console.error(`notifier: subscription ${s.name} needs 'for' (delivery predicate target)`); process.exit(2); }
    if (!s.hook || (s.hook.kind !== "http" && s.hook.kind !== "exec")) { console.error(`notifier: subscription ${s.name} needs hook.kind http|exec`); process.exit(2); }
    if (s.hook.kind === "http" && !/^https?:\/\//.test(s.hook.url)) { console.error(`notifier: ${s.name} hook.url must be http(s)`); process.exit(2); }
    if (s.hook.kind === "exec" && (!Array.isArray(s.hook.argv) || s.hook.argv.length === 0 || s.hook.argv.some((x) => typeof x !== "string"))) { console.error(`notifier: ${s.name} hook.argv must be a non-empty string array`); process.exit(2); }
  }
  // own-token default (claude Q3): each sub runs under its recipient's token when
  // configured; cfg.token is the fallback (then for≠self needs read:all, server-gated).
  const bus = new RpcBus(cfg.url);
  const sessions = new Map<string, Session>();
  const sessionFor = (token: string) => {
    let s = sessions.get(token);
    if (!s) { s = bus.session({ token }); sessions.set(token, s); }
    return s;
  };
  let stop = false;
  process.on("SIGTERM", () => { stop = true; }); process.on("SIGINT", () => { stop = true; });
  console.log(`notifier: ${cfg.url}, ${subs.length} subscription(s)${once ? " (--once)" : ""}`);
  await Promise.all(subs.map(async (s) => {
    for (;;) {
      try { await runSub(sessionFor(s.token ?? cfg.token), s, { once }); return; }
      catch (e) {
        console.error(`[${s.name}] error: ${String(e).slice(0, 200)} — restart loop in 5s`);
        if (stop || once) return;
        await sleep(5000);
      }
    }
  }));
  if (!once) { while (!stop) await sleep(500); console.log("notifier: stopped"); }
}
