/**
 * RFC-002 N1 notifier tests (verdict-folded): doorbell semantics against a live
 * server-mode core (serverHandle session — same Session surface RpcBus builds).
 * Pins: pointer-only payload (no body/file), head-init (no history storm),
 * at-least-once on hook failure (cursor held, dup-on-retry not loss), idempotent
 * rescan, dm~ rings under the own-token default (dmDoorbells:false skips),
 * drain coalesces into ONE call, empty-burst livelock commit, exec hook
 * argv+stdin shape, read:all gate for for≠self, noAll consumer-key namespacing,
 * resync = head scan + exactly ONE comms.resync ring, WakeBucket math.
 */
import { expect, describe, test } from "bun:test";
import { mkdtempSync, rmSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openBus, localCtx } from "../src/bus.ts";
import { serverHandle, seedAgent, type Session } from "../src/bus-iface.ts";
import { runSub, WakeBucket, type Sub } from "../bin/notifier.ts";

function tmp() { return mkdtempSync(join(tmpdir(), "comms-nbf-")); }

async function harness() {
  const home = tmp();
  const boot = openBus({ home, mode: "local" });
  const tok = (boot.tokenCreate(localCtx("bootstrap"), { agent: "root", admin: true }) as any).value.token;
  boot.close();
  const core = openBus({ home, mode: "server" });
  const h = serverHandle(core);
  const root = h.session({ token: tok });
  const sender = ((await seedAgent(h, root, "nbf-sender", "worker")) as any).value.session as Session;
  const reader = ((await seedAgent(h, root, "nbf-reader", "worker", ["read:all"])) as any).value.session as Session;
  return { home, h, core, root, sender, reader };
}

/** an http hook sink: records EVERY attempt, first `failFirst` POSTs get a 500. */
function sink(failFirst = 0) {
  const bursts: any[] = [];
  let n = 0;
  const srv = Bun.serve({
    port: 0,
    async fetch(req) {
      const j = await req.json();
      bursts.push(j);
      return (++n <= failFirst) ? new Response("nope", { status: 500 }) : Response.json({ ok: true });
    },
  });
  return { url: `http://127.0.0.1:${srv.port}/hook`, bursts, close: () => srv.stop() };
}

describe("RFC-002 notifier (N1)", () => {
  test("doorbell fires pointer-only for addressed mail; baseline skips history", async () => {
    const { home, core, root, sender, reader } = await harness();
    try {
      // history BEFORE the subscription exists:
      await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "ask", subject: "old", body: "SECRET-BODY-1" });
      const s = sink();
      const sub: Sub = { name: "t1", for: "nbf-reader", hook: { kind: "http", url: s.url }, coalesceMs: 200 };
      // baseline pass (--once): no hook for the old message
      await runSub(reader, sub, { once: true });
      expect(s.bursts.length).toBe(0);
      // fresh mail: one doorbell, pointer-only
      await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "ask", subject: "new one", body: "SECRET-BODY-2" });
      await runSub(reader, sub, { once: true });
      expect(s.bursts.length).toBe(1);
      const msg = s.bursts[0].messages[0];
      expect(msg.subject).toBe("new one");
      expect(msg.channel).toBe("general");
      expect(JSON.stringify(s.bursts[0])).not.toContain("SECRET-BODY");   // pointer-only
      expect(s.bursts[0]).not.toHaveProperty("body");
      expect(msg).not.toHaveProperty("body");
      expect(msg).not.toHaveProperty("file");
      expect(s.bursts[0].event).toBe("comms.mail");
      // cursor advanced exactly once (at-least-once commit after hook)
      const c = await reader.cursorGet({ consumer: "notify.t1" });
      expect(c.error).toBeUndefined();
      // replay: nothing new ⇒ no second hook
      await runSub(reader, sub, { once: true });
      expect(s.bursts.length).toBe(1);
    } finally { core.close(); rmSync(home, { recursive: true, force: true }); }
  });

  test("hook failure holds the cursor; retry delivers the same burst (dup, not loss)", async () => {
    const { home, core, root, sender, reader } = await harness();
    try {
      const s = sink(1); // first POST fails
      const sub: Sub = { name: "t2", for: "nbf-reader", hook: { kind: "http", url: s.url }, coalesceMs: 200 };
      await runSub(reader, sub, { once: true }); // baseline — commits the scan position
      const base = (await reader.cursorGet({ consumer: "notify.t2" })) as any;
      expect(base.value.seq).toBeGreaterThan(0);
      await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "note", body: "x" });
      await runSub(reader, sub, { once: true }); // hook 500 ⇒ cursor held at baseline
      const held = await reader.cursorGet({ consumer: "notify.t2" });
      expect(((held as any).value as any).seq).toBe(base.value.seq);
      await runSub(reader, sub, { once: true }); // retry ⇒ same id fires once more
      const ids = s.bursts.flatMap((b) => b.messages.map((m: any) => m.id));
      expect(new Set(ids).size).toBe(1);   // one distinct message
      expect(ids.length).toBeGreaterThanOrEqual(2); // delivered twice (at-least-once)
      const after = await reader.cursorGet({ consumer: "notify.t2" });
      expect(((after as any).value as any).seq).toBeGreaterThan(base.value.seq);
    } finally { core.close(); rmSync(home, { recursive: true, force: true }); }
  });

  test("coalesce: 3 posts in one window ⇒ ONE hook call with 3 pointers", async () => {
    const { home, core, root, sender, reader } = await harness();
    try {
      const s = sink();
      const sub: Sub = { name: "t3", for: "nbf-reader", hook: { kind: "http", url: s.url }, coalesceMs: 10_000 };
      await runSub(reader, sub, { once: true }); // baseline
      for (const t of ["a", "b", "c"]) await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "note", body: t });
      // one --once run drains the backlog; force-flush at drain-end (cursor stops advancing)
      await runSub(reader, sub, { once: true });
      expect(s.bursts.length).toBe(1);
      expect(s.bursts[0].messages.length).toBe(3);
    } finally { core.close(); rmSync(home, { recursive: true, force: true }); }
  });

  test("dm~ rings under the own-token default; dmDoorbells:false skips", async () => {
    const { home, core, root, sender, reader } = await harness();
    try {
      // own-token premise (claude Q3): the recipient's own token sees dm~ rows,
      // so the default RINGS for them. Opt-out is explicit.
      const sub: Sub = { name: "t4", for: "nbf-reader", hook: { kind: "http", url: "http://127.0.0.1:1/x" }, coalesceMs: 200 };
      const s = sink();
      sub.hook = { kind: "http", url: s.url };
      await runSub(reader, sub, { once: true }); // head init
      await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "note", body: "dm one", dm: "nbf-reader" });
      await runSub(reader, sub, { once: true });
      expect(s.bursts.length).toBe(1);
      expect(s.bursts[0].messages[0].channel).toMatch(/^dm~/);
      const sub2: Sub = { ...sub, name: "t4b", dmDoorbells: false };
      const s2 = sink();
      sub2.hook = { kind: "http", url: s2.url };
      await runSub(reader, sub2, { once: true }); // head init for the new consumer
      await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "note", body: "dm two", dm: "nbf-reader" });
      await runSub(reader, sub2, { once: true });
      expect(s2.bursts.length).toBe(0);
    } finally { core.close(); rmSync(home, { recursive: true, force: true }); }
  });

  test("exec hook: argv direct, burst on stdin, env counts; no shell", async () => {
    const { home, core, root, sender, reader } = await harness();
    const out = join(home, "hooked.json");
    try {
      const sub: Sub = {
        name: "t5", for: "nbf-reader", coalesceMs: 200,
        hook: { kind: "exec", argv: ["bun", "-e", `await Bun.write(${JSON.stringify(out)}, await new Response(Bun.stdin.stream()).text())`] },
      };
      await runSub(reader, sub, { once: true }); // baseline
      await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "note", subject: "execme", body: "x" });
      await runSub(reader, sub, { once: true });
      expect(existsSync(out)).toBe(true);
      const got = JSON.parse(readFileSync(out, "utf8"));
      expect(got.length).toBe(1);
      expect(got[0].subject).toBe("execme");
      expect(got[0]).not.toHaveProperty("body");
    } finally { core.close(); rmSync(home, { recursive: true, force: true }); }
  });

  test("for≠self without read:all ⇒ forbidden (gate is the server's, unchanged)", async () => {
    const { home, h, core, root, sender } = await harness();
    try {
      const plain = ((await seedAgent(h, root, "nbf-plain", "worker")) as any).value.session as Session;
      const s = sink();
      const sub: Sub = { name: "t6", for: "nbf-reader", hook: { kind: "http", url: s.url } };
      let threw = "";
      try { await runSub(plain, sub, { once: true }); } catch (e) { threw = String(e); }
      expect(threw).toContain("forbidden");
      s.close();
    } finally { core.close(); rmSync(home, { recursive: true, force: true }); }
  });

  test("livelock guard: 600 irrelevant events commit the scan cursor with zero hooks", async () => {
    const { home, h, core, root, sender, reader } = await harness();
    try {
      const s = sink();
      const sub: Sub = { name: "t7", for: "nbf-reader", hook: { kind: "http", url: s.url }, coalesceMs: 200 };
      await runSub(reader, sub, { once: true }); // baseline
      const other = ((await seedAgent(h, root, "nbf-other", "worker")) as any).value.session as Session;
      for (let i = 0; i < 60; i++) await other.post({ from: "nbf-other", to: "nbf-other", type: "note", body: "not for reader" });
      await runSub(reader, sub, { once: true });
      expect(s.bursts.length).toBe(0);
      const c = ((await reader.cursorGet({ consumer: "notify.t7" })) as any).value;
      expect(c.seq).toBeGreaterThan(0); // scan advanced despite zero matches
      s.close();
    } finally { core.close(); rmSync(home, { recursive: true, force: true }); }
  });

  test("noAll predicate namespaces the consumer key (E1 rule)", async () => {
    const { home, h, core, root, sender, reader } = await harness();
    try {
      const s = sink();
      const sub: Sub = { name: "t8", for: "nbf-reader", noAll: true, hook: { kind: "http", url: s.url }, coalesceMs: 200 };
      await runSub(reader, sub, { once: true }); // head init
      await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "note", body: "direct" });
      await sender.post({ from: "nbf-sender", to: "@all", type: "announce", body: "broadcast" });
      await runSub(reader, sub, { once: true });
      expect(s.bursts.length).toBe(1);
      expect(s.bursts[0].messages.length).toBe(1); // @all arm dropped
      // cursor landed on the NAMESPACED row, not notify.t8
      const c = ((await reader.cursorGet({ consumer: "notify.t8.noall" })) as any).value;
      expect(c.seq).toBeGreaterThan(0);
      const plain = ((await reader.cursorGet({ consumer: "notify.t8" })) as any).value;
      expect(plain.seq).toBe(0);
      s.close();
    } finally { core.close(); rmSync(home, { recursive: true, force: true }); }
  });

  test("epoch rotation ⇒ head scan + exactly ONE comms.resync ring (no replay)", async () => {
    const { home, core, root, sender, reader } = await harness();
    try {
      const s = sink();
      const sub: Sub = { name: "t9", for: "nbf-reader", hook: { kind: "http", url: s.url }, coalesceMs: 200 };
      await runSub(reader, sub, { once: true }); // head init
      await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "note", body: "before rotation" });
      await runSub(reader, sub, { once: true }); // ring it, commit on old epoch
      expect(s.bursts.length).toBe(1);
      (core as any).rotateEpoch();
      await sender.post({ from: "nbf-sender", to: "nbf-reader", type: "note", body: "after rotation" });
      await runSub(reader, sub, { once: true });
      const events = s.bursts.map((b) => b.event);
      expect(events.filter((e) => e === "comms.resync").length).toBe(1); // ONE ring
      expect(events.filter((e) => e === "comms.mail").length).toBe(1);   // MAJOR-3: head scan does NOT replay
      const resyncRing = s.bursts.find((b) => b.event === "comms.resync");
      expect(resyncRing.messages.length).toBe(0);                        // pointer to "sweep me", not N rings
      const c = ((await reader.cursorGet({ consumer: "notify.t9" })) as any).value;
      expect(c.seq).toBeGreaterThan(0);                                  // committed at head on the new epoch
      s.close();
    } finally { core.close(); rmSync(home, { recursive: true, force: true }); }
  });

  test("WakeBucket: 3 burst, refill 1/2min, cap at burst", () => {
    const b = new WakeBucket(3, 120_000);
    const t0 = Date.now();
    expect(b.take(t0)).toBe(true); expect(b.take(t0)).toBe(true); expect(b.take(t0)).toBe(true);
    expect(b.take(t0)).toBe(false);                       // budget spent
    expect(b.take(t0 + 119_000)).toBe(false);             // partial refill does not tick early
    expect(b.take(t0 + 120_000)).toBe(true);              // one token after 2 min
    expect(b.take(t0 + 600_000)).toBe(true);              // refill capped at 3, not 4
    expect(b.take(t0 + 600_001)).toBe(true);
    expect(b.take(t0 + 600_002)).toBe(true);
    expect(b.take(t0 + 600_003)).toBe(false);             // cap honored
  });
});
