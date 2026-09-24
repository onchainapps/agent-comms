/**
 * Contract suite (RFC-001 §3): ONE suite, every Bus implementation.
 * M1 runs it against the local core driven with SERVER semantics through the
 * same handle the M2 server uses; M2 adds RpcBus over HTTP. Divergence in
 * results or error variants = CI failure by construction.
 *
 * Covers the §5 rules the reviewers proved were untested: read for≠self,
 * status agent-param spoof, token.create authz + subset rule + bootstrap guard,
 * fingerprint ignored server-side, `as` validation, waitStep for≠self,
 * epoch-mismatch resync, token identity following rename, no auto-register,
 * meta.as audit (finding list §"§5 contract coverage — missing tests for").
 */
import { expect, describe, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openBus, localCtx } from "../src/bus.ts";
import { serverHandle, seedAgent, type BusHandle, type Session } from "../src/bus-iface.ts";

type Factory = () => Promise<{ handle: BusHandle; root: Session; cleanup: () => void }>;

export function contractSuite(name: string, make: Factory) {
  describe(`contract[${name}]`, () => {
    const withBus = async (fn: (h: BusHandle, root: Session) => Promise<void>) => {
      const { handle, root, cleanup } = await make();
      try { await fn(handle, root); } finally { handle.close(); cleanup(); }
    };

    test("join assertion + client fingerprint ignored server-side (finding 12/P10)", async () => {
      await withBus(async (h, root) => {
        const s = await seedAgent(h, root, "c-one", "lab");
        expect(s.error).toBeUndefined();
        const sess = (s as any).value.session as Session;
        expect((await sess.joinAgent({ agent: "c-one", role: "lab2" })).error).toBeUndefined();
        expect((await sess.joinAgent({ agent: "c-two", role: "x" })).error).toBe("forbidden");
        // registering someone else's fingerprint must NOT lock the real owner
        // out (the DoS the reviewer proved): server ignores client fp entirely.
        expect((await sess.joinAgent({ agent: "c-one", role: "lab3", fingerprint: "someone-elses-fp" })).error).toBeUndefined();
        const others = await seedAgent(h, root, "bob", "b");
        const bob = (others as any).value.session as Session;
        expect((await bob.joinAgent({ agent: "bob", role: "b2", fingerprint: "someone-elses-fp" })).error).toBeUndefined();
        const agents = ((await root.listAgents(false)) as any).value.map((x: any) => x.id);
        expect(agents).toContain("c-one");
      });
    });

    test("post → inbox → read marks read → receipts", async () => {
      await withBus(async (h, root) => {
        const s = (await seedAgent(h, root, "rcv", "worker")) as any;
        const p = await root.post({ from: "root", to: "rcv", type: "ask", subject: "s", body: "b" });
        expect(p.error).toBeUndefined();
        const id = (p as any).value.id;
        const inb = await s.value.session.inbox({ agent: "rcv" });
        expect((inb as any).value.rows.map((r: any) => r.id)).toContain(id);
        const rd = await s.value.session.read({ agent: "rcv", id });
        expect((rd as any).value.subject).toBe("s");
        const rec = (await root.receipts(id)) as any;
        expect(rec.value.receipts.readers.map((x: any) => x.id)).toContain("rcv");
      });
    });

    test("reply inherits channel; thread joins only via explicit thread (quirk pinned)", async () => {
      await withBus(async (h, root) => {
        const p = await root.post({ from: "root", to: "t-b", type: "ask", body: "q", channel: "gen-x" });
        const id = (p as any).value.id;
        const r = await root.post({ from: "root", to: "t-a", type: "reply", body: "a", re: id });
        expect((r as any).value.channel).toBe("gen-x");
        expect((r as any).value.thread).not.toBe(id); // reply's thread = its own id
        expect(((await root.threadOf(id)) as any).value.rows.length).toBe(1); // QUIRK: re alone ≠ thread join
        await root.post({ from: "root", to: "t-a", type: "reply", body: "a2", re: id, thread: id });
        expect(((await root.threadOf(id)) as any).value.rows.length).toBe(2); // explicit thread joins
      });
    });

    test("status: principal wins over p.agent (finding B2/P1 spoof)", async () => {
      await withBus(async (h, root) => {
        const w = (await seedAgent(h, root, "s2", "worker")) as any;
        const x = (await seedAgent(h, root, "s3", "other")) as any;
        const p = await root.post({ from: "root", to: "worker", type: "ask", body: "b" });
        const id = (p as any).value.id;
        expect((await w.value.session.setStatus({ agent: "s2", id, state: "acked" })).error).toBeUndefined();
        // spoof: s3 passes agent:"s2" (a legit recipient) — server uses PRINCIPAL ⇒ forbidden
        expect((await x.value.session.setStatus({ agent: "s2", id, state: "done" })).error).toBe("forbidden");
        // root (read:all) may
        expect((await root.setStatus({ agent: "s3", id, state: "done" })).error).toBeUndefined();
      });
    });

    test("idempotency: replay same, conflict on drift", async () => {
      await withBus(async (h, root) => {
        const p1 = await root.post({ from: "root", to: "x", type: "note", body: "b", idempotencyKey: "kk" });
        const p2 = await root.post({ from: "root", to: "x", type: "note", body: "b", idempotencyKey: "kk" });
        expect((p2 as any).value.id).toBe((p1 as any).value.id);
        expect((await root.post({ from: "root", to: "x", type: "note", body: "CHANGED", idempotencyKey: "kk" })).error).toBe("conflict");
      });
    });

    test("identity: from≠principal rejected EVEN with post:as; as→sender=as-target, meta.as=principal", async () => {
      await withBus(async (h, root) => {
        const nobody = (await seedAgent(h, root, "nobody", "n")) as any;
        expect((await nobody.value.session.post({ from: "elsewho", to: "x", type: "note", body: "b" })).error).toBe("forbidden");
        expect((await nobody.value.session.post({ from: "nobody", as: "persona", to: "x", type: "note", body: "b" })).error).toBe("forbidden");
        const imp = (await seedAgent(h, root, "imp", "bot", ["post:as"])) as any;
        // finding B2: post:as governs `as`, NEVER relaxes the from assertion
        expect((await imp.value.session.post({ from: "somebody-else", as: "persona", to: "x", type: "note", body: "b" })).error).toBe("forbidden");
        const r = await imp.value.session.post({ from: "imp", as: "persona", to: "x", type: "note", body: "b" });
        expect(r.error).toBeUndefined();
        const m = (await root.receipts((r as any).value.id)) as any;
        expect(m.value.sender).toBe("persona");
        expect(JSON.parse(m.value.meta).as).toBe("imp"); // N4 audit
      });
    });

    test("`as` and channel validated before path join (finding B3 traversal)", async () => {
      await withBus(async (h, root) => {
        expect((await root.post({ from: "root", as: "../../../evil", to: "x", type: "note", body: "b" })).error).toBe("usage");
        expect((await root.post({ from: "root", to: "x", type: "note", body: "b", channel: "a/b" })).error).toBe("usage");
        expect((await root.post({ from: "root", to: "x", type: "note", body: "b", channel: ".." })).error).toBe("usage");
      });
    });

    test("token.create authz + subset rule + bootstrap guard ACTUALLY aborts (P8/P11)", async () => {
      await withBus(async (h, root) => {
        const plain = (await seedAgent(h, root, "pleb", "p")) as any;
        expect((await plain.value.session.tokenCreate({ agent: "x1", scopes: [] })).error).toBe("unauthorized");
        const scoped = await root.tokenCreate({ agent: "scoped", scopes: ["read:all"] });
        expect(scoped.error).toBeUndefined();
        const ss = h.session({ token: (scoped as any).value.token });
        expect((await ss.tokenCreate({ agent: "x2", scopes: ["read:all"] })).error).toBe("unauthorized"); // lacks tokens:admin
        // bootstrap guard: root minted admins above ⇒ second admin aborts w/o force
        expect((await root.tokenCreate({ agent: "don2", admin: true })).error).toBe("conflict");
        expect((await root.tokenCreate({ agent: "don2", admin: true, force: true })).error).toBeUndefined();
        // human mint (root holds tokens:admin:human via ALL_SCOPES) defaults read:all
        const hum = await root.tokenCreate({ agent: "bakon", kind: "human" });
        expect(hum.error).toBeUndefined();
        const hv = h.session({ token: (hum as any).value.token });
        expect((await hv.inbox({ agent: "scoped" })).error).toBeUndefined(); // read:all peek
      });
    });

    test("read for≠self: forbidden without read:all; NON-marking peek with it (finding B2/P2)", async () => {
      await withBus(async (h, root) => {
        const s = (await seedAgent(h, root, "pk-1", "a")) as any;
        const p = await root.post({ from: "root", to: "pk-1", type: "note", body: "b" });
        const id = (p as any).value.id;
        expect((await s.value.session.read({ agent: "someone-else", id })).error).toBe("forbidden");
        const peek = await root.read({ agent: "pk-1", id });
        expect(peek.error).toBeUndefined();
        expect(((await root.receipts(id)) as any).value.receipts.readers.map((x: any) => x.id)).not.toContain("pk-1"); // peek forged nothing
        await s.value.session.read({ agent: "pk-1", id });
        expect(((await root.receipts(id)) as any).value.receipts.readers.map((x: any) => x.id)).toContain("pk-1");
      });
    });

    test("rename: transactional; history keeps old sender; token identity follows; foreign needs agents:admin", async () => {
      await withBus(async (h, root) => {
        const s = (await seedAgent(h, root, "rn-a", "lab")) as any;
        const p = await s.value.session.post({ from: "rn-a", to: "y", type: "note", body: "b" });
        const id = (p as any).value.id;
        const other = (await seedAgent(h, root, "rn-c", "lab")) as any;
        expect((await other.value.session.rename({ agent: "rn-a", to: "rn-z" })).error).toBe("forbidden");
        expect((await s.value.session.rename({ agent: "rn-a", to: "rn-b" })).error).toBeUndefined(); // self-rename ok
        expect(((await root.receipts(id)) as any).value.sender).toBe("rn-a"); // history unchanged
        // SAME token now resolves to rn-b (rename rewrote tokens.agent_id)
        const sess2 = h.session({ token: s.value.token });
        expect(sess2.agentId).toBe("rn-b");
        expect((await sess2.post({ from: "rn-b", to: "y", type: "note", body: "b2" })).error).toBeUndefined();
      });
    });

    test("history: read:all gate, newest page + no-hole cursor, resync on stale epoch (finding 8/P9)", async () => {
      await withBus(async (h, root) => {
        const plain = (await seedAgent(h, root, "nh", "a")) as any;
        expect((await plain.value.session.history({})).error).toBe("forbidden");
        for (let i = 0; i < 5; i++) await root.post({ from: "root", to: "z", type: "note", body: `m${i}` });
        // The §6 handoff pattern: newest page → cursor = seq of the NEWEST
        // delivered row → stream resumes there ⇒ nothing between page and
        // stream is ever skipped (the old code returned oldest-N + high-water
        // cursor, which silently dropped the middle).
        const page = (await root.history({ limit: 2 })) as any;
        expect(page.value.rows.map((r: any) => r.body)).toEqual(["m3", "m4"]); // NEWEST page
        expect(page.value.hasMore).toBe(true);
        const tip = ((await root.history({})) as any).cursor as string;
        expect(page.cursor).toBe(tip); // cursor == newest delivered == stream resume point
        // since=<page cursor> delivers only what came AFTER (nothing here)
        const after = (await root.history({ since: page.cursor })) as any;
        expect(after.value.rows.length).toBe(0);
        await root.post({ from: "root", to: "z", type: "note", body: "m5" });
        const after2 = (await root.history({ since: page.cursor })) as any;
        expect(after2.value.rows.map((r: any) => r.body)).toEqual(["m5"]); // no hole, no dup
        const bad = await root.history({ since: "deadbeefdeadbeef.5" });
        expect(bad.error).toBe("resync");
        expect((bad as any).data.resync).toBe(true);
      });
    });

    test("cursors: consumer isolation, at-least-once waitStep, monotonic, epoch resync (findings 7/11)", async () => {
      await withBus(async (h, root) => {
        const s = (await seedAgent(h, root, "cw-target", "t")) as any;
        const sess = s.value.session as Session;
        await root.post({ from: "root", to: "t", type: "note", body: "one" });
        const w = (await sess.waitStep({ for: "cw-target", consumer: "cli" })) as any;
        expect(w.value.messages.length).toBe(1);
        expect(((await sess.waitStep({ for: "cw-target", consumer: "cli" })) as any).value.messages.length).toBe(1); // NOT auto-advanced
        expect((await sess.cursorSet({ consumer: "cli", cursor: w.value.cursor })).error).toBeUndefined();
        expect(((await sess.waitStep({ for: "cw-target", consumer: "cli" })) as any).value.messages.length).toBe(0);
        expect(((await sess.waitStep({ for: "cw-target", consumer: "mcp" })) as any).value.messages.length).toBe(1); // isolated
        expect((await sess.cursorSet({ consumer: "cli", cursor: w.value.cursor.replace(/\d+$/, "0") })).error).toBe("conflict");
        expect((await sess.cursorSet({ consumer: "cli", cursor: "not-a-cursor" })).error).toBe("usage");
        expect((await sess.cursorSet({ consumer: "cli", cursor: "deadbeefdeadbeef.99" })).error).toBe("resync");
        // waitStep for≠self requires read:all (finding 11/P3)
        expect((await sess.waitStep({ for: "root" })).error).toBe("forbidden");
        expect((await root.waitStep({ for: "cw-target" })).error).toBeUndefined();
      });
    });

    test("usage/not_found errors identical shapes across transports", async () => {
      await withBus(async (h, root) => {
        expect((await root.post({ from: "root", to: "x", type: "bogus type", body: "b" })).error).toBe("usage");
        expect((await root.post({ from: "BAD ID", to: "x", type: "note", body: "b" })).error).toBe("usage");
        const p = await root.post({ from: "root", to: "x", type: "note", body: "b" });
        expect((await root.setStatus({ agent: "root", id: (p as any).value.id, state: "wat" })).error).toBe("usage");
        expect((await root.read({ agent: "root", id: "missing-1" })).error).toBe("not_found");
        expect((await root.threadOf("missing-1")).error).toBe("not_found");
        // server mode: dangling re rejected (local leniency is a pinned divergence)
        expect((await root.post({ from: "root", to: "x", type: "reply", body: "b", re: "nope-9999" })).error).toBe("not_found");
      });
    });

    test("bad credential ⇒ unauthorized via resolve() (finding 10c)", async () => {
      await withBus(async (h) => {
        expect(h.resolve({ token: "ac_notarealtokenatall1234" }).error).toBe("unauthorized");
      });
    });

    test("local-root principal in server handle ⇒ internal (finding B1 runtime backstop)", async () => {
      await withBus(async (h) => {
        // simulate a transport that forgot to strip a forged localRoot flag
        const core = (h as any).raw;
        const forged = { principal: { agentId: "mallory", kind: "agent", scopes: [], localRoot: true }, actor: "mallory" } as any;
        expect(core.setStatus(forged, { agent: "mallory", id: "whatever", state: "done" }).error).toBe("internal");
        expect(core.post(forged, { from: "mallory", to: "x", type: "note", body: "b" }).error).toBe("internal");
      });
    });
  });
}

// M1 impl: the core in server mode, driven through the SAME handle the M2 HTTP
// server will use. Bootstrap mirrors §5: the first admin token is minted by a
// LOCAL-mode opener (bootstrap is local-only) before the server takes over.
contractSuite("CoreAsServer", async () => {
  const home = mkdtempSync(join(tmpdir(), "comms-contract-"));
  const bootBus = openBus({ home, mode: "local" });
  const boot = bootBus.tokenCreate(localCtx("bootstrap"), { agent: "root", admin: true });
  if (boot.error) throw new Error(boot.detail);
  bootBus.close();
  const core = openBus({ home, mode: "server" });
  const handle = serverHandle(core);
  const root = handle.session({ token: boot.value.token });
  return { handle, root, cleanup: () => rmSync(home, { recursive: true, force: true }) };
});
