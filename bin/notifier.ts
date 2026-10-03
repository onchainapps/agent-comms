#!/usr/bin/env bun
/**
 * bin/notifier.ts — RFC-002 delivery doorbells (N1). Out-of-process push fan-out:
 * consumes the bus through the SAME remote RPC surface as the CLI (inbox.wait /
 * cursor.set — at-least-once, cursor truth unchanged), fires pointer-only hooks
 * (http POST / exec argv), and never touches the server's write path.
 *
 * Design rules (RFC-002 §1):
 *  - Pointer-only: hooks receive {id, channel, thread, sender, to, type, subject,
 *    created_at} — NEVER body/file. Content stays pull under the recipient's token.
 *  - Advisory: a lost or duplicated doorbell costs latency only; cursors remain
 *    the single delivery truth. Consumers dedupe by msg id.
 *  - dm~ lanes never doorbell (Q3 default; `dmDoorbells: true` opts in per sub).
 *  - At-least-once: the cursor advances ONLY after a hook success. A failed hook
 *    holds the cursor; a restart re-derives the same burst from the stored cursor
 *    (rescan is idempotent — burst dedupes by msg id).
 *  - Drain = cursor ADVANCES (the CLI watch loop's rule, claude M3 B1 lineage);
 *    `done` is not the drain signal.
 *  - Coalesce: one hook call per burst (default 5 s window; force-flush at 50
 *    msgs or 20 s age) so announce storms wake a sink once, not N times.
 *  - Fresh subscription baselines NOW (commit to the scan position without
 *    firing) — a first start never doorbells the retained history.
 *
 * usage: bun bin/notifier.ts [--config <path>] [--once]
 *   config default: $COMMS_NOTIFIER_CONFIG or ~/.config/agent-comms/notifier.json
 *   { "url": "http://host", "token": "ac_…",                 // read:all to peek others
 *     "subscriptions": [
 *       { "name": "don-grok-mail", "for": "don-grok",
 *         "hook": { "kind": "http", "url": "http://127.0.0.1:8642/webhooks/bus-don-grok",
 *                   "secret": "…" },
 *         "coalesceMs": 5000, "noAll": false, "channel": null, "dmDoorbells": false } |
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
  name: string; for: string; hook: HttpHook | ExecHook;
  coalesceMs?: number; noAll?: boolean; channel?: string | null; dmDoorbells?: boolean;
};
export type Config = { url: string; token: string; subscriptions: Sub[] };

export const NAME_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/;     // consumer-safe: notify.<name> ∈ CONSUMER_RE
export const DM_RE = /^dm~/;
const FLUSH_MAX_MSGS = 50;
const FLUSH_MAX_AGE_MS = 20_000;
const BACKOFF_MAX_MS = 30_000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function toPointer(m: MsgRow): Pointer {
  return { id: m.id, channel: m.channel, thread: m.thread, sender: m.sender, to: m.recipients, type: m.type, subject: m.subject, created_at: m.created_at };
}

export async function fireHook(hook: HttpHook | ExecHook, sub: string, burst: Pointer[]): Promise<void> {
  if (hook.kind === "http") {
    const res = await fetch(hook.url, {
      method: "POST",
      headers: { "content-type": "application/json", "user-agent": "agent-comms-notifier/1", ...(hook.secret ? { "x-comms-secret": hook.secret } : {}) },
      body: JSON.stringify({ event: "comms.mail", subscription: sub, messages: burst }),
      signal: AbortSignal.timeout(10_000),
    });
    if (!res.ok) throw new Error(`http ${res.status}`);
  } else {
    // exec: argv directly (NO shell — config is operator-owned 0600; message
    // bytes never reach argv, they ride stdin as one JSON line; grok Q4 rule).
    const proc = Bun.spawn(hook.argv, {
      stdin: "pipe", stdout: "ignore", stderr: "pipe",
      env: { ...process.env, COMMS_DOORBELL_COUNT: String(burst.length), COMMS_DOORBELL_SUB: sub },
    });
    proc.stdin.write(JSON.stringify(burst) + "\n");
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
  const consumer = `notify.${sub.name}`;
  const coalesceMs = sub.coalesceMs ?? 5000;

  // stored position (fresh consumer ⇒ {epoch, 0} ⇒ baseline pass below)
  let committed = "";
  const c0 = await session.cursorGet({ consumer });
  if (!c0.error) committed = `${c0.value.epoch}.${c0.value.seq}`;
  let baseline = !committed || committed.endsWith(".0");

  let scan = committed;             // scanned-to (may lead committed during a burst)
  const burst = new Map<string, Pointer>();  // id-keyed: rescan is idempotent
  let burstSince = 0;
  let backoff = 500;

  const recover = async (data?: Record<string, any>) => {
    const floor = Number(data?.floor ?? 0);
    const r = await session.cursorSet({ consumer, cursor: `${data?.epoch}.${floor}`, force: true });
    if (r.error) throw new Error(`resync commit failed: ${r.error}`);
    committed = `${data?.epoch}.${floor}`; scan = committed;
    burst.clear(); burstSince = 0;
    log(`resync — recovery commit to ${committed} (retained history resumes here)`);
  };

  const commitScan = async (): Promise<boolean> => {
    if (!scan || scan === committed) return true;
    const r = await session.cursorSet({ consumer, cursor: scan });
    if (r.error === "resync") { await recover(r.data); return false; }
    if (r.error === "conflict") return true; // concurrent watcher on same consumer: benign
    if (r.error) throw new Error(`cursorSet: ${r.error} ${r.detail ?? ""}`);
    committed = scan;
    return true;
  };

  const flush = async (): Promise<boolean> => {
    if (burst.size === 0) return commitScan();
    const msgs = [...burst.values()];
    try { await fireHook(sub.hook, sub.name, msgs); }
    catch (e) {
      log(`hook FAILED (${String(e).slice(0, 160)}) — cursor held at ${committed}; burst retried next drain`);
      return false; // at-least-once: nothing committed, burst kept
    }
    const ok = await commitScan(); // resync here ⇒ burst already fired ⇒ dup, not loss
    log(`doorbell x${msgs.length} → ${sub.hook.kind}${msgs.length === 1 ? ` (${msgs[0].id})` : ""}`);
    burst.clear(); burstSince = 0;
    return ok;
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
      if (!sub.dmDoorbells && DM_RE.test(m.channel)) continue;
      if (!burst.has(m.id)) { burst.set(m.id, toPointer(m)); if (!burstSince) burstSince = Date.now(); }
    }
    const advanced = w.value.cursor !== scan;
    scan = w.value.cursor;
    if (advanced) {
      if (!baseline && burst.size >= FLUSH_MAX_MSGS && !(await flush())) await sleep(1000);
      continue; // drain without sleeping while the cursor moves (M3 B1)
    }

    if (baseline) {
      // first start: history is context, not news — commit position, no hooks.
      baseline = false;
      const had = burst.size; burst.clear(); burstSince = 0;
      if (!(await commitScan())) continue;
      log(`baseline: ${had} retained msg(s) skipped (subscription starts now)`);
      if (once) return;
    } else if (once || burst.size >= FLUSH_MAX_MSGS || (burstSince && Date.now() - burstSince >= Math.min(coalesceMs, FLUSH_MAX_AGE_MS))) {
      // --once drains fully: the coalesce window never holds back a one-shot run.
      const ok = await flush();
      if (!ok && !once) await sleep(1000); // hook failure (daemon): retry flush next pass
      if (once) return;
    } else if (burst.size === 0) {
      if (!(await commitScan())) continue; // empty-burst advance: commit (livelock guard)
    }
    if (once) return;
    await sleep(1000); // idle cadence: one RPC/s, zero LLM
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
  const bus = new RpcBus(cfg.url);
  const session = bus.session({ token: cfg.token });
  let stop = false;
  process.on("SIGTERM", () => { stop = true; }); process.on("SIGINT", () => { stop = true; });
  console.log(`notifier: ${cfg.url}, ${subs.length} subscription(s)${once ? " (--once)" : ""}`);
  await Promise.all(subs.map(async (s) => {
    for (;;) {
      try { await runSub(session, s, { once }); return; }
      catch (e) {
        console.error(`[${s.name}] error: ${String(e).slice(0, 200)} — restart loop in 5s`);
        if (stop || once) return;
        await sleep(5000);
      }
    }
  }));
  if (!once) { while (!stop) await sleep(500); console.log("notifier: stopped"); }
}
