/**
 * Contract suite (RFC-001 §3): ONE suite, every Bus implementation.
 * M1 runs it against LocalBus; M2 adds RpcBus. Any divergence in results or
 * error variants between transports is a CI failure by construction.
 *
 * Server semantics: agent rows come from token.create (seedAgent), join is an
 * assertion, from==principal unless post:as+as.
 */
import { expect, describe, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LocalBus } from "../src/bus-iface.ts";
import type { BusInterface, Ctx } from "../src/bus.ts";

type Factory = () => Promise<{ bus: BusInterface; root: Ctx; cleanup: () => void }>;

const root: Ctx = {
  principal: { agentId: "c-admin", kind: "agent", scopes: ["read:all", "post:as", "tokens:admin", "agents:admin"] },
  actor: "c-admin",
};
const plain = (id: string): Ctx => ({ principal: { agentId: id, kind: "agent", scopes: [] }, actor: id });

export function contractSuite(name: string, make: Factory) {
  describe(`contract[${name}]`, () => {
    const withBus = async (fn: (b: BusInterface, r: Ctx) => Promise<void>) => {
      const { bus, root: rt, cleanup } = await make();
      try { await fn(bus, rt); } finally { await bus.close(); cleanup(); }
    };

    test("seedAgent + join assertion: re-join with foreign id rejected", async () => {
      await withBus(async (b, rt) => {
        const s = await b.seedAgent(rt, "c-one", "lab");
        expect(s.error).toBeUndefined();
        const ctx = (s as any).value as Ctx;
        // join is an assertion: same id ok (refresh), foreign id rejected
        expect((await b.joinAgent(ctx, { agent: "c-one", role: "lab2" })).error).toBeUndefined();
        expect((await b.joinAgent(ctx, { agent: "c-two", role: "x" })).error).toBe("forbidden");
        const agents = await b.listAgents(false);
        expect(agents.map((a) => a.id)).toContain("c-one");
      });
    });

    test("post → inbox → read marks read → receipts", async () => {
      await withBus(async (b, rt) => {
        const rc = (await b.seedAgent(rt, "rcv", "worker")) as any;
        const p = await b.post(rt, { from: "c-admin", to: "rcv", type: "ask", subject: "s", body: "b" });
        expect(p.error).toBeUndefined();
        const id = (p as any).value.id;
        const inb = await b.inbox(rc.value, { agent: "rcv" });
        expect((inb as any).value.rows.map((r: any) => r.id)).toContain(id);
        expect((inb as any).value.unreadIds).toContain(id);
        const rd = await b.read(rc.value, { agent: "rcv", id });
        expect((rd as any).value.subject).toBe("s");
        const inb2 = await b.inbox(rc.value, { agent: "rcv" });
        expect((inb2 as any).value.unreadIds).not.toContain(id);
        const rec = await b.receipts(rt, id);
        expect((rec as any).value.receipts.readers.map((x: any) => x.id)).toContain("rcv");
      });
    });

    test("reply inherits channel from parent; thread joins only via explicit thread (quirk)", async () => {
      await withBus(async (b, rt) => {
        const p = await b.post(rt, { from: "c-admin", to: "t-b", type: "ask", body: "q", channel: "gen-x" });
        const id = (p as any).value.id;
        const r = await b.post(rt, { from: "c-admin", to: "t-a", type: "reply", body: "a", re: id });
        expect((r as any).value.channel).toBe("gen-x"); // channel inherited from parent
        // QUIRK (preserved): thread = explicit --thread or own id; re alone does NOT join parent thread
        const th1 = await b.threadOf(rt, id);
        expect((th1 as any).value.rows.length).toBe(1);
        const r2 = await b.post(rt, { from: "c-admin", to: "t-a", type: "reply", body: "a2", re: id, thread: id });
        expect((r2 as any).value.channel).toBe("gen-x");
        const th2 = await b.threadOf(rt, id);
        expect((th2 as any).value.rows.length).toBe(2);
      });
    });

    test("status permission matrix: recipient-by-role ok, stranger no, admin yes", async () => {
      await withBus(async (b, rt) => {
        const w = (await b.seedAgent(rt, "s2", "worker")) as any;
        const x = (await b.seedAgent(rt, "s3", "other")) as any;
        const p = await b.post(rt, { from: "c-admin", to: "worker", type: "ask", body: "b" });
        const id = (p as any).value.id;
        expect((await b.setStatus(w.value, { agent: "s2", id, state: "acked" })).error).toBeUndefined();
        expect((await b.setStatus(x.value, { agent: "s3", id, state: "done" })).error).toBe("forbidden");
        expect((await b.setStatus(rt, { agent: "s3", id, state: "done" })).error).toBeUndefined();
      });
    });

    test("idempotency: replay same, conflict on drift", async () => {
      await withBus(async (b, rt) => {
        const p1 = await b.post(rt, { from: "c-admin", to: "x", type: "note", body: "b", idempotencyKey: "kk" });
        const p2 = await b.post(rt, { from: "c-admin", to: "x", type: "note", body: "b", idempotencyKey: "kk" });
        expect((p2 as any).value.id).toBe((p1 as any).value.id);
        expect((await b.post(rt, { from: "c-admin", to: "x", type: "note", body: "CHANGED", idempotencyKey: "kk" })).error).toBe("conflict");
      });
    });

    test("identity: from≠principal rejected; as requires post:as; sender=as-target", async () => {
      await withBus(async (b, rt) => {
        const nobody = plain("nobody");
        expect((await b.post(nobody, { from: "elsewho", to: "x", type: "note", body: "b" })).error).toBe("forbidden");
        expect((await b.post(nobody, { from: "nobody", as: "persona", to: "x", type: "note", body: "b" })).error).toBe("forbidden");
        const imp = (await b.seedAgent(rt, "imp", "bot", ["post:as"])) as any;
        const r = await b.post(imp.value, { from: "imp", as: "persona", to: "x", type: "note", body: "b" });
        expect(r.error).toBeUndefined();
        const m = (await b.receipts(rt, (r as any).value.id)) as any;
        expect(m.value.sender).toBe("persona");
      });
    });

    test("rename: transactional; history keeps old sender; self-rename ok; foreign needs agents:admin", async () => {
      await withBus(async (b, rt) => {
        const s = (await b.seedAgent(rt, "rn-a", "lab")) as any;
        const p = await b.post(s.value, { from: "rn-a", to: "y", type: "note", body: "b" });
        const id = (p as any).value.id;
        await b.read(s.value, { agent: "rn-a", id });
        // foreign rename without agents:admin (s has no scopes) → forbidden
        expect((await b.rename(s.value, { agent: "rn-a", to: "rn-x" })).error).toBeUndefined(); // self: allowed
        // now rn-x exists; a stranger cannot rename it
        const other = (await b.seedAgent(rt, "rn-c", "lab")) as any;
        expect((await b.rename(other.value, { agent: "rn-x", to: "rn-z" })).error).toBe("forbidden");
        // admin can
        expect((await b.rename(rt, { agent: "rn-x", to: "rn-b" })).error).toBeUndefined();
        expect(((await b.receipts(rt, id)) as any).value.sender).toBe("rn-a");
      });
    });

    test("inbox for≠self: needs read:all; with it = non-marking peek", async () => {
      await withBus(async (b, rt) => {
        const s = (await b.seedAgent(rt, "pk-1", "a")) as any;
        const p = await b.post(rt, { from: "c-admin", to: "pk-1", type: "note", body: "b" });
        const id = (p as any).value.id;
        expect((await b.inbox(s.value, { agent: "someone-else" })).error).toBe("forbidden");
        const peek = await b.inbox(rt, { agent: "pk-1" });
        expect((peek as any).value.rows.map((r: any) => r.id)).toContain(id);
        // peek did NOT mark read for pk-1
        const own = await b.inbox(s.value, { agent: "pk-1" });
        expect((own as any).value.unreadIds).toContain(id);
      });
    });

    test("history returns rows + epoch.seq cursor that advances", async () => {
      await withBus(async (b, rt) => {
        const h0 = await b.history(rt, {});
        expect((h0 as any).value.cursor.seq).toBeGreaterThanOrEqual(0);
        expect((h0 as any).value.cursor.epoch.length).toBeGreaterThan(8);
        await b.post(rt, { from: "c-admin", to: "z", type: "note", body: "b" });
        const h1 = await b.history(rt, {});
        expect((h1 as any).value.cursor.seq).toBeGreaterThan((h0 as any).value.cursor.seq);
      });
    });

    test("history without read:all rejected", async () => {
      await withBus(async (b, rt) => {
        const s = (await b.seedAgent(rt, "nh", "a")) as any;
        expect((await b.history(s.value, {})).error).toBe("forbidden");
      });
    });

    test("cursors: consumer isolation, at-least-once inboxWait, monotonic set", async () => {
      await withBus(async (b, rt) => {
        const s = (await b.seedAgent(rt, "cw-target", "t")) as any;
        await b.post(rt, { from: "c-admin", to: "t", type: "note", body: "one" });
        const w = await b.inboxWait(s.value, { for: "cw-target", consumer: "cli", timeout: 5 });
        expect((w as any).value.messages.length).toBe(1);
        const w2 = await b.inboxWait(s.value, { for: "cw-target", consumer: "cli", timeout: 1 });
        expect((w2 as any).value.messages.length).toBe(1); // not auto-advanced
        await b.cursorSet(s.value, { consumer: "cli", cursor: (w as any).value.cursor });
        const w3 = await b.inboxWait(s.value, { for: "cw-target", consumer: "cli", timeout: 1 });
        expect((w3 as any).value.messages.length).toBe(0);
        const w4 = await b.inboxWait(s.value, { for: "cw-target", consumer: "mcp", timeout: 1 });
        expect((w4 as any).value.messages.length).toBe(1); // other consumer unaffected
        const cur = (w as any).value.cursor as string;
        expect((await b.cursorSet(s.value, { consumer: "cli", cursor: cur.replace(/\d+$/, "0") })).error).toBe("conflict");
      });
    });

    test("usage/not_found errors identical shapes across transports", async () => {
      await withBus(async (b, rt) => {
        expect((await b.post(rt, { from: "c-admin", to: "x", type: "bogus type", body: "b" })).error).toBe("usage");
        expect((await b.post(rt, { from: "BAD ID", to: "x", type: "note", body: "b" })).error).toBe("usage");
        const p = await b.post(rt, { from: "c-admin", to: "x", type: "note", body: "b" });
        expect((await b.setStatus(rt, { agent: "c-admin", id: (p as any).value.id, state: "wat" })).error).toBe("usage");
        expect((await b.read(rt, { agent: "c-admin", id: "missing-1" })).error).toBe("not_found");
        expect((await b.threadOf(rt, "missing-1")).error).toBe("not_found");
      });
    });
  });
}

// M1: LocalBus participates now. M2 adds contractSuite("RpcBus", …).
contractSuite("LocalBus", async () => {
  const home = mkdtempSync(join(tmpdir(), "comms-contract-"));
  const bus = LocalBus.open({ home, mode: "server" });
  return { bus, root, cleanup: () => rmSync(home, { recursive: true, force: true }) };
});
