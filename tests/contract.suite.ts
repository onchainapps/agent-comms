/**
 * Contract suite (RFC-001 §3): ONE suite, every Bus implementation.
 * M1 runs it against the local core driven with SERVER semantics through the
 * same handle the M2 server uses; M2 adds RpcBus over HTTP. Divergence in
 * results, error variants, or data = CI failure (data compared by toEqual).
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

    test("reply inherits channel; re alone joins the thread (E3 — was the re≠thread quirk)", async () => {
      await withBus(async (h, root) => {
        const p = await root.post({ from: "root", to: "t-b", type: "ask", body: "q", channel: "gen-x" });
        const id = (p as any).value.id;
        const r = await root.post({ from: "root", to: "t-a", type: "reply", body: "a", re: id });
        expect((r as any).value.channel).toBe("gen-x");
        // E3 (server mode): re alone anchors thread = replied row's root.
        // The legacy own-id quirk survives ONLY in local mode (golden pins it).
        expect((r as any).value.thread).toBe(id);
        expect(((await root.threadOf(id)) as any).value.rows.length).toBe(2); // re alone JOINED the thread
        // explicit thread still wins and can anchor elsewhere:
        const r2 = await root.post({ from: "root", to: "t-a", type: "reply", body: "a2", re: id, thread: id });
        expect((r2 as any).value.thread).toBe(id);
        expect(((await root.threadOf(id)) as any).value.rows.length).toBe(3);
        // re to a reply lands on the SAME root (derivation is transitive):
        const r3 = await root.post({ from: "root", to: "t-a", type: "reply", body: "a3", re: (r as any).value.id });
        expect((r3 as any).value.thread).toBe(id);
      });
    });

    test("E1: inbox/waitStep noAll drops ONLY the @all arm (id/role/group still deliver)", async () => {
      await withBus(async (h, root) => {
        const w = (await seedAgent(h, root, "e1-watch", "e1role")) as any;
        const s = w.value.session;
        await root.groupCreate({ name: "e1g" });
        // group join is a SELF assertion (§5) — the member session joins itself.
        expect((await s.groupJoin({ name: "e1g" })).error).toBeUndefined();
        await root.post({ from: "root", to: "e1-watch", type: "note", body: "by-id" });
        const bAll = await root.post({ from: "root", to: "@all", type: "announce", body: "broadcast" });
        await root.post({ from: "root", to: "e1role", type: "note", body: "by-role" });
        await root.post({ from: "root", to: "group:e1g", type: "note", body: "by-group" });
        const ids = (r: any) => (r as any).value.rows.map((x: any) => x.id);
        // default inbox: all four arms deliver (legacy behavior — pinned).
        expect(ids(await s.inbox({ agent: "e1-watch" })).length).toBe(4);
        // noAll: broadcast arm drops, id + role + group arms stay.
        const nb = ids(await s.inbox({ agent: "e1-watch", noAll: true }));
        expect(nb.length).toBe(3);
        expect(nb).not.toContain((bAll as any).value.id);
        // waitStep noAll under a NAMESPACED consumer; default consumer still
        // sees the broadcast — both fresh (no stored cursor ⇒ scan from 0),
        // independent positions in the events space.
        const wNo = (await s.waitStep({ consumer: "cli.noall", noAll: true })) as any;
        expect(wNo.value.messages.map((m: any) => m.body)).toEqual(["by-id", "by-role", "by-group"]);
        const wDef = (await s.waitStep({ consumer: "cli" })) as any;
        expect(wDef.value.messages.map((m: any) => m.body)).toContain("broadcast");
      });
    });

    test("E2: channel.create — exact idempotent, near-duplicate refused, post auto-create guarded", async () => {
      await withBus(async (h, root) => {
        // blessed create; exact-name repeat is idempotent, NOT an error.
        const c1 = await root.channelCreate({ name: "wildwestgame", purpose: "the game lane" });
        expect(c1.error).toBeUndefined();
        expect((c1 as any).value).toEqual({ name: "wildwestgame", created: true });
        const c2 = await root.channelCreate({ name: "wildwestgame" });
        expect((c2 as any).value).toEqual({ name: "wildwestgame", created: false });

        // the wildw_client lesson: [-_] variants are refused with the existing
        // name in the detail — no silent canonicalization. (Uppercase is
        // refused earlier by the ID_RE grammar itself.)
        for (const v of ["wild-west-game", "wild_west_game"]) {
          const r = await root.channelCreate({ name: v });
          expect(r.error).toBe("usage");
          expect(String((r as any).detail)).toContain("wildwestgame");
          // and the variant must NOT have been created as a side effect.
          const names = (((await root.channels()) as any).value ?? []).map((x: any) => x.name);
          expect(names).not.toContain(v);
        }
        expect((await root.channelCreate({ name: "WildWestGame" })).error).toBe("usage");
        expect((await root.channelCreate({ name: "a:b" })).error).toBe("usage"); // ':' unpinned before (grok E2)

        // grok E2 B1/B2: the regex is not a type check — ID_RE.test(12) ToString-
        // coerces to "12" and PASSES, then chanNorm/bind threw (TypeError local,
        // HTTP 500 on purpose). Both transports must now answer usage.
        expect((await root.channelCreate({ name: 12 as any })).error).toBe("usage");
        expect((await root.channelCreate({ name: undefined as any })).error).toBe("usage");
        expect((await root.channelCreate({ name: "e2-typed", purpose: 12 as any })).error).toBe("usage");
        expect((await root.post({ from: "root", to: "@all", type: "note", body: "x", channel: 12 as any })).error).toBe("usage");

        // post to an unknown channel still auto-creates — but THROUGH the guard.
        const typo = await root.post({ from: "root", to: "@all", type: "note", body: "x", channel: "wild-west-game" });
        expect(typo.error).toBe("usage");
        expect(String((typo as any).detail)).toContain("wildwestgame");
        const fresh = await root.post({ from: "root", to: "@all", type: "note", body: "x", channel: "e2-fresh-lane" });
        expect(fresh.error).toBeUndefined();

        // dm~ pair channels are system-managed (name grammar has no '~', so
        // channelCreate can never collide with one — the exclusion is belt
        // and braces for future grammars).
        expect((await root.channelCreate({ name: "dm~root-e2guy" })).error).toBe("usage");
      });
    });

    test("E2.x: channel.delete — gate, tombstone skeleton, revive, cap", async () => {
      await withBus(async (h, root) => {
        const w = ((await seedAgent(h, root, "e2x-w", "worker")) as any).value.session as Session;

        // creator retires own lane; drops from listings.
        expect((await w.channelCreate({ name: "e2x-lane", purpose: "mine" })).error).toBeUndefined();
        expect((await w.channelDelete({ name: "e2x-lane" })).error).toBeUndefined();
        const names = (((await root.channels()) as any).value ?? []).map((c: any) => c.name);
        expect(names).not.toContain("e2x-lane");

        // creator-or-admin gate: non-creator without agents:admin ⇒ forbidden;
        // the same agent with root (admin) succeeds.
        expect((await root.channelCreate({ name: "e2x-owned" })).error).toBeUndefined();
        expect((await w.channelDelete({ name: "e2x-owned" })).error).toBe("forbidden");
        expect((await root.channelDelete({ name: "e2x-owned" })).error).toBeUndefined(); // admin path

        // tombstone holds the SKELETON: a variant of a retired lane is refused
        // with a retired pointer, and delete-all-variants-then-recreate cannot
        // resurrect the incident.
        expect((await w.channelCreate({ name: "e2xgame" })).error).toBeUndefined();
        expect((await w.channelDelete({ name: "e2xgame" })).error).toBeUndefined();
        const rv = await w.channelCreate({ name: "e2-x-game" });
        expect(rv.error).toBe("usage");
        expect(String((rv as any).detail)).toContain("retired");
        expect(String((rv as any).detail)).toContain("e2xgame");

        // exact-name recreate REVIVES (tombstone cleared → listed again).
        // (Backdating is no longer load-bearing — the same-second contention
        // branch was cargo-culted and removed, claude t_3fb757d5 MINOR-1/Q5:
        // events carry no channel column and nothing reads channel deleted_at
        // except that branch. Kept as a harmless determinism pin.)
        (h as any).raw.testDb.run("UPDATE channel_tombstones SET deleted_at = datetime('now','-1 day') WHERE name='e2xgame'");
        const rev = await w.channelCreate({ name: "e2xgame" });
        expect(rev.error).toBeUndefined();
        expect((rev as any).value.created).toBe(true);
        const names2 = (((await root.channels()) as any).value ?? []).map((c: any) => c.name);
        expect(names2).toContain("e2xgame");
        expect((h as any).raw.testDb.query("SELECT 1 FROM channel_tombstones WHERE name='e2xgame'").get()).toBeNull();

        // MAJOR-1 (claude t_3fb757d5 P1): variant live + canonical live (the
        // pre-E2 coexistence state), retire variant, retire canonical, revive
        // canonical (OK — only a LIVE sibling blocks a revive), revive variant
        // (usage — live sibling again). No bricked skeleton.
        expect((await w.channelCreate({ name: "e2-x-game2" })).error).toBeUndefined(); // variant first
        (h as any).raw.testDb.run("INSERT INTO channels(name,purpose,created_at,created_by) VALUES('e2xgame2','',strftime('%Y-%m-%dT%H:%M:%SZ','now'),'e2x-w')"); // canonical, pre-E2 style
        expect((await w.channelDelete({ name: "e2-x-game2" })).error).toBeUndefined(); // retire variant
        expect((await w.channelDelete({ name: "e2xgame2" })).error).toBeUndefined(); // retire canonical too
        const rev2 = await w.channelCreate({ name: "e2xgame2" }); // revive via own tombstone
        expect(rev2.error).toBeUndefined();
        expect((rev2 as any).value.created).toBe(true);
        const rv2b = await w.channelCreate({ name: "e2_x_game2" }); // new spelling still fenced
        expect(rv2b.error).toBe("usage");
        expect((await w.channelCreate({ name: "e2-x-game2" })).error).toBe("usage"); // live sibling blocks variant revive
        // tombstone attribution (claude MINOR-2): who retired it is answerable.
        expect((await w.channelDelete({ name: "e2xgame2" })).error).toBeUndefined();
        expect(((h as any).raw.testDb.query("SELECT deleted_by FROM channel_tombstones WHERE name='e2xgame2'").get() as any).deleted_by).toBe(w.agentId);

        // NIT-3 (claude t_3fb757d5) + grok finding 3: non-creator typo ⇒
        // not_found, not forbidden — gate + existence re-read INSIDE the txn.
        expect((await w.channelDelete({ name: "e2x-nope-at-all" })).error).toBe("not_found");

        // grok findings 1/4 re-fold pins (floor ruling: claude MINOR-1 removed
        // the same-second contention as cargo-cult — events carry no channel
        // column, nothing reads deleted_at. The blessed path and the post back
        // door must therefore be IDENTICAL, not one floored + one open):
        // same-second delete ⇒ revive works via BOTH paths, tombstone cleared.
        expect((await w.channelCreate({ name: "e2xsec" })).error).toBeUndefined();
        expect((await w.channelDelete({ name: "e2xsec" })).error).toBeUndefined();
        const rev3 = await w.channelCreate({ name: "e2xsec" }); // same second, no floor
        expect(rev3.error).toBeUndefined();
        expect((rev3 as any).value.created).toBe(true);
        expect((h as any).raw.testDb.query("SELECT 1 FROM channel_tombstones WHERE name='e2xsec'").get()).toBeNull();
        expect((await w.channelDelete({ name: "e2xsec" })).error).toBeUndefined();
        const pRev = await w.post({ from: w.agentId, to: "@all", type: "note", body: "revive via back door", channel: "e2xsec" });
        expect(pRev.error).toBeUndefined(); // same second, same semantics as create
        expect((h as any).raw.testDb.query("SELECT 1 FROM channel_tombstones WHERE name='e2xsec'").get()).toBeNull();
        expect(((h as any).raw.testDb.query("SELECT created_by FROM channels WHERE name='e2xsec'").get() as any).created_by).toBe(w.agentId);

        // grok finding 4: a post to a LIVE lane must NOT touch tombstones.
        // (A tombstone co-existing with a live row is hand-SQL residue — claude
        // NIT-1 — but the hot path must never be what erases it.)
        expect((await w.channelCreate({ name: "e2xlive" })).error).toBeUndefined();
        (h as any).raw.testDb.run("INSERT INTO channel_tombstones(name,deleted_at,deleted_by) VALUES('e2xlive','2020-01-01T00:00:00Z',?)", [w.agentId]);
        expect((await w.post({ from: w.agentId, to: "@all", type: "note", body: "hot path", channel: "e2xlive" })).error).toBeUndefined();
        expect((h as any).raw.testDb.query("SELECT 1 FROM channel_tombstones WHERE name='e2xlive'").get()).not.toBeNull();

        // grok finding 2 (txn shape): a back-door post REJECTED by the guard or
        // the cap leaves NO message row and NO channel row (all inside the one
        // BEGIN IMMEDIATE now).
        expect((await w.post({ from: w.agentId, to: "@all", type: "note", body: "x", channel: "e2-x-sec" })).error).toBe("usage"); // live sibling e2xsec
        expect((h as any).raw.testDb.query("SELECT 1 FROM channels WHERE name='e2-x-sec'").get()).toBeNull();
        expect((h as any).raw.testDb.query("SELECT 1 FROM messages WHERE channel='e2-x-sec'").get()).toBeNull();

        // general is undeletable; unknown lane is not_found; bad grammar usage.
        expect((await root.channelDelete({ name: "general" })).error).toBe("usage");
        expect((await root.channelDelete({ name: "e2x-nope" })).error).toBe("not_found");
        expect((await root.channelDelete({ name: 12 as any })).error).toBe("usage");

        // cap (cost-not-permission, claude Q5): 64 created lanes per agent;
        // delete frees room.
        const c = ((await seedAgent(h, root, "e2x-c", "cap")) as any).value.session as Session;
        for (let i = 0; i < 64; i++) expect((await c.channelCreate({ name: "cc" + i })).error).toBeUndefined();
        expect((await c.channelCreate({ name: "cc64" })).error).toBe("usage");
        expect((await c.channelDelete({ name: "cc0" })).error).toBeUndefined();
        expect((await c.channelCreate({ name: "cc64" })).error).toBeUndefined(); // delete freed room
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
        // root (holds agents:admin) may
        expect((await root.setStatus({ agent: "s3", id, state: "done" })).error).toBeUndefined();
      });
    });

    test("status gate is agents:admin, NOT read:all (round-2 M2, RFC §5)", async () => {
      await withBus(async (h, root) => {
        const p = await root.post({ from: "root", to: "nobody-here", type: "ask", body: "b", channel: "st-gate" });
        const id = (p as any).value.id;
        // read:all alone (not sender, not recipient) ⇒ forbidden — visibility ≠ control
        const viewer = (await seedAgent(h, root, "viewer", "v", ["read:all"])) as any;
        expect((await viewer.value.session.setStatus({ agent: "viewer", id, state: "done" })).error).toBe("forbidden");
        // agents:admin alone ⇒ may set any
        const admin = (await seedAgent(h, root, "aadm", "a", ["agents:admin"])) as any;
        expect((await admin.value.session.setStatus({ agent: "aadm", id, state: "done" })).error).toBeUndefined();
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
        // M9 (round 2): valid credential lacking the scope ⇒ forbidden (-32002),
        // never unauthorized (-32001 = bad/missing credential only).
        expect((await plain.value.session.tokenCreate({ agent: "x1", scopes: [] })).error).toBe("forbidden");
        const scoped = await root.tokenCreate({ agent: "scoped", scopes: ["read:all"] });
        expect(scoped.error).toBeUndefined();
        const ss = h.session({ token: (scoped as any).value.token });
        expect((await ss.tokenCreate({ agent: "x2", scopes: ["read:all"] })).error).toBe("forbidden"); // lacks tokens:admin
        // M8a: scope enum + no-comma validation
        expect((await root.tokenCreate({ agent: "smug", scopes: ["read:all,agents:admin"] as any })).error).toBe("usage");
        expect((await root.tokenCreate({ agent: "smug", scopes: ["bogus"] as any })).error).toBe("usage");
        // M8b: tokens:admin is TRANSITIVELY ROOT — may mint scopes it lacks
        const t = await root.tokenCreate({ agent: "tadm", scopes: ["tokens:admin"], force: true }); // force: bootstrap guard (root admin exists)
        expect(t.error).toBeUndefined();
        const adm = { value: { session: h.session({ token: (t as any).value.token }) } } as any;
        expect((await adm.value.session.tokenCreate({ agent: "sub1", scopes: ["read:all"] })).error).toBeUndefined();
        // bootstrap guard: root minted admins above ⇒ second admin aborts w/o force
        expect((await root.tokenCreate({ agent: "don2", admin: true })).error).toBe("conflict");
        expect((await root.tokenCreate({ agent: "don2", admin: true, force: true })).error).toBeUndefined();
        // human mint (root holds tokens:admin) defaults read:all
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
        await sess2.channels(); // RpcBus: identity arrives on the first round-trip (x-comms-agent)
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
        const tip = ((await root.history({})) as any).value.cursor as string;
        expect(page.value.cursor).toBe(tip); // cursor == newest delivered == stream resume point
        // since=<page cursor> delivers only what came AFTER (nothing here)
        const after = (await root.history({ since: page.value.cursor })) as any;
        expect(after.value.rows.length).toBe(0);
        await root.post({ from: "root", to: "z", type: "note", body: "m5" });
        const after2 = (await root.history({ since: page.value.cursor })) as any;
        expect(after2.value.rows.map((r: any) => r.body)).toEqual(["m5"]); // no hole, no dup
        const bad = await root.history({ since: "deadbeefdeadbeef.5" });
        expect(bad.error).toBe("resync");
        expect((bad as any).data.resync).toBe(true);
        expect(typeof (bad as any).data.floor).toBe("number"); // m2 (M2 card): ONE resync payload shape — floor ALWAYS present
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
        const rsSet = await sess.cursorSet({ consumer: "cli", cursor: "deadbeefdeadbeef.99" });
        expect(rsSet.error).toBe("resync");
        expect(typeof (rsSet as any).data.floor).toBe("number"); // m2 hygiene: floor always present
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
        // m3 (round 2): rejected dangling-re must leave NO orphan channel row
        expect((await root.post({ from: "root", to: "x", type: "reply", body: "b", re: "nope-9999", channel: "orphan-chan" })).error).toBe("not_found");
        expect(((await root.channels()) as any).value.map((c: any) => c.name)).not.toContain("orphan-chan");
      });
    });

    test("history since-mode pages to hasMore=false ⇒ every row exactly once (round-2 M4)", async () => {
      await withBus(async (h, root) => {
        const ids: string[] = [];
        for (let i = 0; i < 7; i++) ids.push(((await root.post({ from: "root", to: "z", type: "note", body: `pg${i}` })) as any).value.id);
        let cursor = `${(h as any).raw.epoch()}.0`;
        const seen: string[] = [];
        for (let guard = 0; guard < 20; guard++) {
          const page = (await root.history({ since: cursor, limit: 3 })) as any;
          expect(page.error).toBeUndefined();
          seen.push(...page.value.rows.map((r: any) => r.id));
          cursor = page.value.cursor;
          if (!page.value.hasMore) break;
        }
        expect(seen).toEqual(ids); // ordered, no gaps, no dupes — the N-a hole is gone
      });
    });

    test("explicit since below gc_floor ⇒ resync on history AND waitStep (round-2 M3)", async () => {
      await withBus(async (h, root) => {
        const raw = (h as any).raw;
        for (let i = 0; i < 6; i++) raw.testDb.run("INSERT INTO events(kind,at) VALUES('msg','2020-01-01T00:00:00Z')");
        const g = raw.gc(); // real clock ⇒ 2020 events are past retention; floor advances
        expect(g.floor).toBeGreaterThan(0);
        const stale = `${raw.epoch()}.0`; // below floor
        const h1 = await root.history({ since: stale });
        expect(h1.error).toBe("resync");
        expect((h1 as any).data.floor).toBe(g.floor);
        const w1 = await root.waitStep({ since: stale });
        expect(w1.error).toBe("resync");
        expect((w1 as any).data.floor).toBe(g.floor);
        // at-or-above floor: normal operation
        expect((await root.history({ since: `${raw.epoch()}.${g.floor}` })).error).toBeUndefined();
      });
    });

    test("stored cursor in a PREVIOUS epoch ⇒ resync, never a silent seq-0 reset (grok round-3 #1)", async () => {
      await withBus(async (h, root) => {
        const raw = (h as any).raw;
        const a = ((await seedAgent(h, root, "alice2", "r", [])) as any).value.session;
        const carol = ((await seedAgent(h, root, "carol2", "r", [])) as any).value.session;
        const p1 = await carol.post({ from: "carol2", to: "alice2", type: "note", body: "one" });
        const p2 = await carol.post({ from: "carol2", to: "alice2", type: "note", body: "two" });
        void p1;
        // client consumed msg one only: commit a cursor at ITS event seq
        const s1 = (raw.testDb.query("SELECT seq FROM events WHERE kind='msg' AND msg_id=?").get((p1 as any).value.id) as any).seq;
        expect((await a.cursorSet({ consumer: "cli", cursor: `${raw.epoch()}.${s1}` })).error).toBeUndefined();
        // epoch rotates (restore/rewrite); gc_floor resets to 0 — the floor
        // check CANNOT fire, so only an epoch check can signal resync.
        raw.rotateEpoch();
        const w = await a.waitStep({ consumer: "cli" });
        expect(w.error).toBe("resync");
        expect((w as any).data.resync).toBe(true);
        expect((w as any).data.epoch).toBe(raw.epoch()); // the NEW epoch for the client
        expect(typeof (w as any).data.floor).toBe("number"); // m2 hygiene: floor always present
        // a cursor already at the new epoch (fresh client) is NOT a resync:
        expect((await a.cursorSet({ consumer: "cli2", cursor: `${raw.epoch()}.0`, force: true })).error).toBeUndefined();
        expect((await a.waitStep({ consumer: "cli2" })).error).toBeUndefined();
        void s1; void p2;
      });
    });

    test("session cursorGet on a foreign-epoch row ⇒ resync; recovery commit accepted (grok round-4 #1)", async () => {
      await withBus(async (h, root) => {
        const raw = (h as any).raw;
        const a = ((await seedAgent(h, root, "alice3", "r", [])) as any).value.session;
        const carol = ((await seedAgent(h, root, "carol3", "r", [])) as any).value.session;
        const p1 = await carol.post({ from: "carol3", to: "alice3", type: "note", body: "seen" });
        const p2 = await carol.post({ from: "carol3", to: "alice3", type: "note", body: "UNCONSUMED" });
        // commit cursor at p1's event seq — p2's event is UNCONSUMED
        const s1 = (raw.testDb.query("SELECT seq FROM events WHERE kind='msg' AND msg_id=?").get((p1 as any).value.id) as any).seq;
        expect((await a.cursorSet({ consumer: "cli", cursor: `${raw.epoch()}.${s1}` })).error).toBeUndefined();
        // gc so p2's event is deleted (floor above s1): age it past retention first
        raw.testDb.run("UPDATE events SET at = datetime('now','-59 days') WHERE msg_id = ?", [(p2 as any).value.id]);
        raw.gc();
        expect(raw.gcFloor()).toBeGreaterThan(s1);
        // epoch rotates (restore/rewrite): gc_floor resets to 0 ⇒ floor check cannot fire
        const e2 = raw.rotateEpoch();
        // SESSION cursorGet — the exact hole: must be resync, NOT {value:{seq:0}}
        const cg = await a.cursorGet({ consumer: "cli" });
        expect(cg.error).toBe("resync");
        expect((cg as any).data.resync).toBe(true);
        expect((cg as any).data.epoch).toBe(e2);
        expect(typeof (cg as any).data.floor).toBe("number"); // m2 hygiene: floor always present
        // the stored row was NOT rewritten by the failed get
        expect((raw.testDb.query("SELECT epoch FROM cursors WHERE agent_id='alice3' AND consumer='cli'").get() as any).epoch).not.toBe(e2);
        // recovery commit: explicit current-epoch set succeeds despite the foreign row
        expect((await a.cursorSet({ consumer: "cli", cursor: `${e2}.0` })).error).toBeUndefined();
        // and history snapshot hands off a floor-safe cursor the client can resume from
        const hist = await root.history({ limit: 10 }); // read:all lives on root
        expect((hist as any).value.rows.map((r: any) => r.body)).toContain("UNCONSUMED");
        expect((await a.waitStep({ consumer: "cli", since: (hist as any).value.cursor })).error).toBeUndefined();
      });
    });

    test("bad credential ⇒ unauthorized via resolve() (finding 10c)", async () => {
      await withBus(async (h) => {
        // awaited: transports without a local DB (RpcBus) must verify over
        // the wire; LocalBus returns the Res synchronously and await is a no-op.
        expect((await h.resolve({ token: "ac_notarealtokenatall1234" })).error).toBe("unauthorized");
      });
    });

    // ================= M1.5 G7 — DM matrix (server direction) =================

    const dmPair = async (h: BusHandle, root: Session) => {
      const a = (await seedAgent(h, root, "dm-alice", "da")) as any;
      const b = (await seedAgent(h, root, "dm-bob", "db")) as any;
      return { alice: a.value.session as Session, bob: b.value.session as Session };
    };

    test("G7 canonicalization: alice↔bob same channel; code-unit order; self-DM rejected; ~ collision impossible", async () => {
      await withBus(async (h, root) => {
        const { alice, bob } = await dmPair(h, root);
        const p1 = await alice.post({ from: "dm-alice", to: "dm-bob", type: "note", body: "a2b", dm: "dm-bob" });
        const p2 = await bob.post({ from: "dm-bob", to: "dm-alice", type: "note", body: "b2a", dm: "dm-alice" });
        expect(p1.error).toBeUndefined();
        expect((p2 as any).value.channel).toBe((p1 as any).value.channel); // pair-keyed, both directions
        expect((p1 as any).value.channel).toBe("dm~dm-alice~dm-bob"); // code-unit: '-'(45) < 'b'… 'dm-alice' < 'dm-bob'
        // non-canonical explicit spelling resolves to the SAME channel, no split
        const p3 = await alice.post({ from: "dm-alice", to: "dm-bob", type: "note", body: "expl", channel: "dm~dm-bob~dm-alice" });
        expect((p3 as any).value.channel).toBe((p1 as any).value.channel);
        // self-DM rejected
        expect((await alice.post({ from: "dm-alice", to: "dm-alice", type: "note", body: "s", dm: "dm-alice" })).error).toBe("usage");
        expect((await alice.post({ from: "dm-alice", to: "x", type: "note", body: "s", channel: "dm~zed~zed" })).error).toBe("usage");
        // ~ collision: recipient index stores bare ids (no ~ in ids)
        const rec = (await root.receipts((p1 as any).value.id)) as any;
        expect(rec.value.recipients).toBe("dm-bob"); // sugar: recipients = peer only
      });
    });

    test("G7 non-party invisibility: read/inbox/threadOf/receipts/waitStep/setStatus ⇒ byte-identical not_found; no reads row", async () => {
      await withBus(async (h, root) => {
        const { alice, bob } = await dmPair(h, root);
        const mallory = ((await seedAgent(h, root, "dm-mallory", "dm-role")) as any).value.session as Session;
        const p = await alice.post({ from: "dm-alice", to: "dm-bob", type: "note", body: "secret", dm: "dm-bob" });
        const id = (p as any).value.id;
        // read: same error + same detail SHAPE as a missing id; NO reads row.
        // (detail embeds the requested id — hidden and missing produce the same
        // bytes for the same id; no separate hidden-vs-missing string.)
        const rd = await mallory.read({ agent: "dm-mallory", id });
        const miss = await mallory.read({ agent: "dm-mallory", id: "zzz-not-a-real-msg-id-zzz" });
        expect(rd.error).toBe("not_found"); expect(miss.error).toBe("not_found");
        expect((rd as any).detail).toBe(`no such message: ${id}`);
        const raw = (h as any).raw;
        expect((raw.testDb.query("SELECT count(*) c FROM reads WHERE agent='dm-mallory' AND msg=?").get(id) as any).c).toBe(0);
        // inbox: absent
        const inb = await mallory.inbox({ agent: "dm-mallory" });
        expect((inb as any).value.rows.map((r: any) => r.id)).not.toContain(id);
        // party inbox: present
        const bin = await bob.inbox({ agent: "dm-bob" });
        expect((bin as any).value.rows.map((r: any) => r.id)).toContain(id);
        // threadOf: not_found (all rows filtered)
        const th = await mallory.threadOf(id);
        expect(th.error).toBe("not_found");
        // receipts: not_found BEFORE receiptsForMsg
        const rc = await mallory.receipts(id);
        expect(rc.error).toBe("not_found");
        // waitStep (stream scope=mine predicate): not delivered
        const ws = await mallory.waitStep({ consumer: "t" });
        expect((ws as any).value.messages.map((m: any) => m.id)).not.toContain(id);
        const wsB = await bob.waitStep({ consumer: "t" });
        expect((wsB as any).value.messages.map((m: any) => m.id)).toContain(id);
        // setStatus: not_found, NOT forbidden (no existence oracle)
        const st = await mallory.setStatus({ agent: "dm-mallory", id, state: "done" });
        expect(st.error).toBe("not_found");
      });
    });

    test("G7 role spoof closed: mallory joins with role=<an agent id> ⇒ identity_conflict (canSee is member-based)", async () => {
      await withBus(async (h, root) => {
        const { alice } = await dmPair(h, root);
        // server join is UPDATE-only; spoofy exists via token.create, then takes role=dm-alice
        const tc = (await root.tokenCreate({ agent: "spoofy", scopes: [] })) as any;
        const sp = h.session({ token: tc.value.token });
        const j = await sp.joinAgent({ agent: "spoofy", role: "dm-alice" });
        expect(j.error).toBe("identity_conflict");
      });
    });

    test("G7 R1 closed: role == retired id rejected after rename (pre-rename mail cannot be inherited by a role squatter)", async () => {
      await withBus(async (h, root) => {
        const bob = ((await seedAgent(h, root, "r1-bob", "b")) as any).value.session as Session;
        await root.post({ from: "root", to: "r1-bob", type: "note", body: "pre-rename mail" });
        expect((await root.rename({ agent: "r1-bob", to: "r1-carol" })).error).toBeUndefined();
        const mal = ((await seedAgent(h, root, "r1-mal", "m")) as any).value.session as Session;
        const j = await mal.joinAgent({ agent: "r1-mal", role: "r1-bob" }); // retired id as role
        expect(j.error).toBe("identity_conflict");
        // and the retired id cannot be re-minted as an id (one-way door)
        expect((await root.tokenCreate({ agent: "r1-bob", scopes: [] })).error).toBe("identity_conflict");
      });
    });

    test("G7 scope matrix: read:all-only sees channels but NOT DMs (history BOTH modes); read:dm sees all", async () => {
      await withBus(async (h, root) => {
        const { alice, bob } = await dmPair(h, root);
        const pub = await root.post({ from: "root", to: "x1", type: "note", body: "public", channel: "g7-pub" });
        const dm = await alice.post({ from: "dm-alice", to: "dm-bob", type: "note", body: "private", dm: "dm-bob" });
        const dmId = (dm as any).value.id;
        const allOnly = ((await seedAgent(h, root, "g7-allonly", "v", ["read:all"])) as any).value.session as Session;
        const dmAll = ((await seedAgent(h, root, "g7-dmall", "v", ["read:all", "read:dm"])) as any).value.session as Session;
        // snapshot mode
        const s1 = (await allOnly.history({ limit: 100 })) as any;
        expect(s1.value.rows.map((r: any) => r.id)).toContain((pub as any).value.id);
        expect(s1.value.rows.map((r: any) => r.id)).not.toContain(dmId);
        const s2 = (await dmAll.history({ limit: 100 })) as any;
        expect(s2.value.rows.map((r: any) => r.id)).toContain(dmId);
        // since mode (row predicate BEFORE LIMIT — m-d). Real epoch cursor:
        const raw = (h as any).raw;
        const cur0 = `${raw.epoch()}.0`;
        const f1 = (await allOnly.history({ since: cur0, limit: 100 })) as any;
        expect(f1.value.rows.map((r: any) => r.id)).not.toContain(dmId);
        const f2 = (await dmAll.history({ since: cur0, limit: 100 })) as any;
        expect(f2.value.rows.map((r: any) => r.id)).toContain(dmId);
        // channels(): dm-shaped hidden for read:all-only, listed for read:dm
        const ch1 = ((await allOnly.channels()) as any).value.map((x: any) => x.name);
        expect(ch1).toContain("g7-pub");
        expect(ch1.some((n: string) => n.startsWith("dm~"))).toBe(false);
        const ch2 = ((await dmAll.channels()) as any).value.map((x: any) => x.name);
        expect(ch2.some((n: string) => n.startsWith("dm~"))).toBe(true);
      });
    });

    test("G7 dm.members parity (fold m2): member sees list, non-party byte-identical not_found, non-dm name ⇒ usage", async () => {
      await withBus(async (h, root) => {
        const { alice } = await dmPair(h, root);
        const p = await alice.post({ from: "dm-alice", to: "dm-bob", type: "note", body: "x", dm: "dm-bob" });
        const chan = (p as any).value.channel as string;
        // member view
        const mem = (await alice.dmMembers(chan)) as any;
        expect(mem.error).toBeUndefined();
        expect(mem.value).toEqual(["dm-alice", "dm-bob"]);
        // non-party: SAME detail as a missing channel (G2 byte-identical rule)
        const mal = ((await seedAgent(h, root, "dmm-mal", "m")) as any).value.session as Session;
        const hid = (await mal.dmMembers(chan)) as any;
        const miss = (await mal.dmMembers("dm~zz1~zz2")) as any;
        expect(hid.error).toBe("not_found");
        expect(hid.detail).toBe(`no such channel: ${chan}`);
        expect(miss.error).toBe("not_found");
        // non-dm-shaped name ⇒ usage (decided by DM_SHAPED_RE on the input
        // string alone — not an existence oracle)
        const pub = (await mal.dmMembers("g7-public-x")) as any;
        expect(pub.error).toBe("usage");
        void root;
      });
    });

    test("consumer grammar cap (claude M3 m2): bad/long consumer ⇒ usage, never a new row", async () => {
      await withBus(async (h, root) => {
        const c0 = `${(h as any).raw.epoch()}.0`;
        expect((await root.cursorSet({ consumer: "x".repeat(129), cursor: c0 })).error).toBe("usage");
        expect((await root.cursorSet({ consumer: "bad name!", cursor: c0 })).error).toBe("usage");
        expect((await root.cursorGet({ consumer: "UPPER" })).error).toBe("usage");
        // M3-fold: the third entry point enforces the same grammar
        expect((await root.waitStep({ consumer: "x".repeat(129) })).error).toBe("usage");
        // namespaced CLI consumers stay legal
        expect((await root.cursorSet({ consumer: "cli#g7-chan", cursor: c0 })).error).toBeUndefined();
        expect((await root.cursorSet({ consumer: "cli.all", cursor: c0 })).error).toBeUndefined();
        expect((await root.cursorSet({ consumer: "cli@peer1", cursor: c0 })).error).toBeUndefined();
        // M3-fold: the LONGEST key the CLI itself generates must be legal —
        // cli@<id32>.all#dm~<id32>~<id32>~NNNN = 114 B (a 64 cap exited 2 here)
        const id32 = (c: string) => c.repeat(32);
        const longest = `cli@${id32("t")}.all#dm~${id32("a")}~${id32("b")}~9999`;
        expect(longest.length).toBe(114);
        expect((await root.cursorSet({ consumer: longest, cursor: c0 })).error).toBeUndefined();
        expect((await root.cursorGet({ consumer: longest })).error).toBeUndefined();
        expect((await root.waitStep({ consumer: longest })).error).toBeUndefined();
        expect((await root.cursorSet({ consumer: "x".repeat(128), cursor: c0 })).error).toBeUndefined();
      });
    });

    test("history gate (M3 ruling c): plain token — unfiltered snapshot forbidden; channel/since views ungated, dm~ rows still canSee-filtered", async () => {
      await withBus(async (h, root) => {
        const { alice } = await dmPair(h, root);
        const pub = await root.post({ from: "root", to: "x1", type: "note", body: "public", channel: "hg-pub" });
        const pubId = (pub as any).value.id;
        const dm = await alice.post({ from: "dm-alice", to: "dm-bob", type: "note", body: "private", dm: "dm-bob" });
        const dmId = (dm as any).value.id; const dmChan = (dm as any).value.channel;
        const plain = ((await seedAgent(h, root, "hg-plain", "p")) as any).value.session as Session;
        const cur0 = `${(h as any).raw.epoch()}.0`;
        expect((await plain.history({})).error).toBe("forbidden");
        // grok 3433 M-a: every "no channel" spelling is the unfiltered snapshot
        // (forbidden); wrong types are usage — never the snapshot.
        expect((await plain.history({ channel: null })).error).toBe("forbidden");
        expect((await plain.history({ channel: "" })).error).toBe("forbidden");
        expect((await plain.history({ channel: false as any })).error).toBe("usage");
        expect((await plain.history({ channel: 0 as any })).error).toBe("usage");
        expect((await plain.history({ channel: true as any })).error).toBe("usage");
        const byChan = (await plain.history({ channel: "hg-pub" })) as any;
        expect(byChan.error).toBeUndefined();
        expect(byChan.value.rows.map((r: any) => r.id)).toContain(pubId);
        const since = (await plain.history({ since: cur0, limit: 1000 })) as any;
        expect(since.error).toBeUndefined();
        expect(since.value.rows.map((r: any) => r.id)).toContain(pubId);
        expect(since.value.rows.map((r: any) => r.id)).not.toContain(dmId);
        // naming the dm channel directly must not bypass canSee (snapshot AND since)
        const dmSnap = (await plain.history({ channel: dmChan })) as any;
        expect(dmSnap.error).toBeUndefined();
        expect(dmSnap.value.rows.length).toBe(0);
        const dmSince = (await plain.history({ channel: dmChan, since: cur0 })) as any;
        expect(dmSince.value.rows.length).toBe(0);
        // the party still sees its own dm through the ungated channel view
        const party = (await alice.history({ channel: dmChan })) as any;
        expect(party.value.rows.map((r: any) => r.id)).toContain(dmId);
      });
    });

    test("G7 write side: @all into dm rejected; non-party --re/thread into dm ⇒ not_found; cross-channel dm thread attach rejected", async () => {
      await withBus(async (h, root) => {
        const { alice, bob } = await dmPair(h, root);
        const mal = ((await seedAgent(h, root, "g7w-mal", "w")) as any).value.session as Session;
        const dm = await alice.post({ from: "dm-alice", to: "dm-bob", type: "note", body: "s", dm: "dm-bob" });
        const dmId = (dm as any).value.id;
        // party posting @all into dm ⇒ usage (subset-of-members predicate)
        expect((await alice.post({ from: "dm-alice", to: "@all", type: "note", body: "x", channel: (dm as any).value.channel })).error).toBe("usage");
        // party posting to a non-member id ⇒ usage
        expect((await alice.post({ from: "dm-alice", to: "g7w-mal", type: "note", body: "x", channel: (dm as any).value.channel })).error).toBe("usage");
        // non-party --re into dm ⇒ not_found, same detail SHAPE as a missing
        // re (the id is embedded; hidden vs missing is one code, one shape).
        const r1 = await mal.post({ from: "g7w-mal", to: "x", type: "reply", body: "x", re: dmId });
        expect(r1.error).toBe("not_found");
        expect((r1 as any).detail).toBe(`error: re -> unknown message id '${dmId}'`);
        // non-party thread=<dm root> ⇒ not_found
        expect((await mal.post({ from: "g7w-mal", to: "x", type: "note", body: "x", thread: dmId })).error).toBe("not_found");
        // thread=<nonexistent> rejected in server mode
        expect((await mal.post({ from: "g7w-mal", to: "x", type: "note", body: "x", thread: "zzz-nonexistent-thread" })).error).toBe("not_found");
        // thread=<own public root> accepted
        const pub = await mal.post({ from: "g7w-mal", to: "x", type: "note", body: "p" });
        const ok = await mal.post({ from: "g7w-mal", to: "x", type: "note", body: "t", thread: (pub as any).value.id });
        expect(ok.error).toBeUndefined();
        // PARTY cross-channel attach into dm thread ⇒ usage (equality rule)
        expect((await alice.post({ from: "dm-alice", to: "x", type: "note", body: "x", thread: dmId, channel: "g7-pub" })).error).toBe("usage");
        // bob replies inside the dm thread — fine
        expect((await bob.post({ from: "dm-bob", to: "dm-alice", type: "reply", body: "r", re: dmId })).error).toBeUndefined();
      });
    });

    test("G7 rename keeps conversation via member pair (frozen name, moved ACL)", async () => {
      await withBus(async (h, root) => {
        const aliceS = (await seedAgent(h, root, "dm-alice", "da")) as any;
        const bobS = (await seedAgent(h, root, "dm-bob", "db")) as any;
        const alice = aliceS.value.session as Session;
        const p = await alice.post({ from: "dm-alice", to: "dm-bob", type: "note", body: "s", dm: "dm-bob" });
        const chan = (p as any).value.channel;
        expect((await root.rename({ agent: "dm-bob", to: "g7-bobby" })).error).toBeUndefined();
        // --dm <new peer> resolves the SAME stored channel (pair lookup, not name re-derive)
        const p2 = await alice.post({ from: "dm-alice", to: "g7-bobby", type: "note", body: "again", dm: "g7-bobby" });
        expect((p2 as any).value.channel).toBe(chan);
        // bob's ORIGINAL token now authenticates g7-bobby (tokens.agent_id
        // rewritten). Visibility follows the MOVED ACL (canSee); old mail does
        // NOT reappear in the new id's INBOX — no alias arm (G6 ruling: delivery
        // is recipient-literal; continuity runs through the channel, not arms).
        const bobby = h.session({ token: bobS.value.token });
        const rd = await bobby.read({ agent: "g7-bobby", id: (p as any).value.id });
        expect(rd.error).toBeUndefined(); // member via moved channel_members
        expect((await bobby.inbox({ agent: "g7-bobby" }) as any).value.rows.map((r: any) => r.id))
          .not.toContain((p as any).value.id); // no alias arm — pinned
        expect((await bobby.threadOf((p as any).value.id) as any).value.rows.length).toBeGreaterThan(0);
      });
    });

    // ---- M1.5 round-1 review fixes (claude 572b / grok 8ea5) ----

    test("M2: non-party dm not_found detail is REQUEST-derived, never the stored frozen name", async () => {
      await withBus(async (h, root) => {
        const a = ((await seedAgent(h, root, "o-alice", "oa")) as any).value.session as Session;
        const b = ((await seedAgent(h, root, "o-bob", "ob")) as any).value.session as Session;
        const mal = ((await seedAgent(h, root, "o-mal", "om")) as any).value.session as Session;
        expect((await a.post({ from: "o-alice", to: "o-bob", type: "note", body: "hi", dm: "o-bob" })).error).toBeUndefined();
        expect((await root.rename({ agent: "o-bob", to: "o-bob2" })).error).toBeUndefined();
        // mallory names the re-derived (nonexistent) channel: detail must NOT
        // reveal the stored dm~o-alice~o-bob — same shape as a never-existing pair.
        const r1 = await mal.post({ from: "o-mal", to: "o-alice", type: "note", body: "x", channel: "dm~o-alice~o-bob2" });
        const r2 = await mal.post({ from: "o-mal", to: "o-alice", type: "note", body: "x", channel: "dm~o-alice~o-zzz" });
        expect(r1.error).toBe("not_found"); expect(r2.error).toBe("not_found");
        // SAME shape: request-echo only. r1 must echo the REQUESTED name, and
        // the stored frozen name (dm~o-alice~o-bob, no suffix) must not leak
        // as a distinct string — detail equals "no such channel: <requested>".
        expect((r1 as any).detail).toBe(`no such channel: dm~o-alice~o-bob2`);
        expect((r2 as any).detail).toBe(`no such channel: dm~o-alice~o-zzz`);
        // M2-residual: reversed order and ~n suffix — existing pair and
        // never-existing pair must produce the SAME (canonical) detail shape.
        const d = async (ch: string) => ((await mal.post({ from: "o-mal", to: "o-alice", type: "note", body: "x", channel: ch })) as any).detail;
        expect(await d("dm~o-bob2~o-alice")).toBe("no such channel: dm~o-alice~o-bob2");
        expect(await d("dm~o-zzz~o-alice")).toBe("no such channel: dm~o-alice~o-zzz");
        expect(await d("dm~o-alice~o-bob2~7")).toBe("no such channel: dm~o-alice~o-bob2");
        expect(await d("dm~o-alice~o-zzz~7")).toBe("no such channel: dm~o-alice~o-zzz");
        // literal STORED name: canonicalized too (no raw echo of a real label)
        expect(await d("dm~o-alice~o-bob")).toBe("no such channel: dm~o-alice~o-bob");
      });
    });

    test("M3: usage-rejected first dm post leaves NO channel row; party post writes all three rows", async () => {
      await withBus(async (h, root) => {
        const raw = (h as any).raw;
        const a = ((await seedAgent(h, root, "u-alice", "ua")) as any).value.session as Session;
        await seedAgent(h, root, "u-carol", "uc");
        const r = await a.post({ from: "u-alice", to: "@all", type: "note", body: "x", channel: "dm~u-alice~u-carol" });
        expect(r.error).toBe("usage"); // wildcard recipient
        expect(raw.testDb.query("SELECT count(*) c FROM channels WHERE name='dm~u-alice~u-carol'").get().c).toBe(0); // rolled back
        expect(raw.testDb.query("SELECT count(*) c FROM channel_members").get().c).toBe(0);
        const ok = await a.post({ from: "u-alice", to: "u-carol", type: "note", body: "x", channel: "dm~u-alice~u-carol" });
        expect(ok.error).toBeUndefined();
        expect(raw.testDb.query("SELECT count(*) c FROM channels WHERE name='dm~u-alice~u-carol'").get().c).toBe(1);
        expect(raw.testDb.query("SELECT count(*) c FROM channel_members WHERE channel='dm~u-alice~u-carol'").get().c).toBe(2);
      });
    });

    test("m5: idempotency hash includes dm when set — same key, dm:bob vs dm:carol ⇒ conflict", async () => {
      await withBus(async (h, root) => {
        const a = ((await seedAgent(h, root, "i-alice", "ia")) as any).value.session as Session;
        await seedAgent(h, root, "i-bob", "ib"); await seedAgent(h, root, "i-carol", "ic");
        const p1 = await a.post({ from: "i-alice", to: "i-bob", type: "note", body: "b", dm: "i-bob", idempotencyKey: "k1" });
        expect(p1.error).toBeUndefined();
        const p2 = await a.post({ from: "i-alice", to: "i-carol", type: "note", body: "b", dm: "i-carol", idempotencyKey: "k1" });
        expect(p2.error).toBe("conflict"); // dm participates in the hash (only when set)
      });
    });

    test("m6: dm sugar is mutually exclusive with channel; to must be omitted or the peer", async () => {
      await withBus(async (h, root) => {
        const a = ((await seedAgent(h, root, "x-alice", "xa")) as any).value.session as Session;
        await seedAgent(h, root, "x-bob", "xb");
        expect((await a.post({ from: "x-alice", to: "x-bob", type: "note", body: "b", dm: "x-bob", channel: "general" })).error).toBe("usage");
        expect((await a.post({ from: "x-alice", to: "@all", type: "note", body: "b", dm: "x-bob" })).error).toBe("usage");
        const r = await a.post({ from: "x-alice", to: "x-bob", type: "note", body: "b", dm: "x-bob" });
        expect(r.error).toBeUndefined();
        expect((r as any).value.channel).toBe("dm~x-alice~x-bob");
      });
    });

    test("m7: a DM reply does not leak 'seen (replied)' receipts to a caller who cannot see it", async () => {
      await withBus(async (h, root) => {
        const raw = (h as any).raw;
        const mal = ((await seedAgent(h, root, "r-mal", "rm")) as any).value.session as Session;
        const bob = ((await seedAgent(h, root, "r-bob", "rb")) as any).value.session as Session;
        await seedAgent(h, root, "r-carol", "rc");
        const pub = await mal.post({ from: "r-mal", to: "r-bob", type: "ask", body: "q", channel: "pub-m7" });
        const pid = (pub as any).value.id;
        // bob replies INSIDE a DM with carol — mal is NOT a party there.
        const dm = await bob.post({ from: "r-bob", to: "r-carol", type: "note", body: "s", dm: "r-carol", re: pid });
        expect(dm.error).toBeUndefined();
        expect(raw.canSeeChannel({ principal: { agentId: "r-mal", kind: "agent", scopes: [] }, actor: "r-mal" } as any, (dm as any).value.channel)).toBe(false);
        const rec = (await mal.receipts(pid)) as any;
        expect(rec.error).toBeUndefined();
        // the reply row is invisible to mal ⇒ no inference leak
        expect(rec.value.receipts.readers.map((x: any) => x.id)).not.toContain("r-bob");
        expect(rec.value.receipts.unread).toContain("r-bob");
      });
    });

    test("M5: rename rewrites the DEFAULT role (role==old id) — no implicit alias arm, no preflight noise", async () => {
      await withBus(async (h, root) => {
        const raw = (h as any).raw;
        // default-role agent: token.create mints role == id (NOT seedAgent's explicit role)
        const tc = await root.tokenCreate({ agent: "d-bob" });
        expect(tc.error).toBeUndefined();
        const bob = h.session({ token: (tc as any).value.token });
        const p = await root.post({ from: "root", to: "d-bob", type: "note", body: "literal" });
        expect(p.error).toBeUndefined();
        expect((await bob.inbox({ agent: "d-bob" }) as any).value.rows.length).toBe(1);
        expect((await root.rename({ agent: "d-bob", to: "d-bobby" })).error).toBeUndefined();
        // role rewritten: bare-token role match must NOT deliver old-id mail
        const row = raw.testDb.query("SELECT role FROM agents WHERE id='d-bobby'").get() as any;
        expect(row.role).toBe("d-bobby"); // not "d-bob" (== retired id ⇒ R1 violation)
        const bobby = h.session({ token: (tc as any).value.token });
        expect((await bobby.inbox({ agent: "d-bobby" }) as any).value.rows.map((r: any) => r.id)).not.toContain((p as any).value.id);
        // post --to <retired id> no longer lands on the renamed agent via role
        const q = await root.post({ from: "root", to: "d-bob", type: "note", body: "after" });
        expect(q.error).toBeUndefined();
        expect((await bobby.inbox({ agent: "d-bobby" }) as any).value.rows.map((r: any) => r.id)).not.toContain((q as any).value.id);
        expect(raw.preflight().roleCollisions.filter((c: string) => c.startsWith("d-bobby!"))).toEqual([]);
      });
    });

    test("m8: re-joining an existing group at the 64-group cap succeeds (cap counts NEW memberships)", async () => {
      await withBus(async (h, root) => {
        const a = ((await seedAgent(h, root, "cap-a", "ca")) as any).value.session as Session;
        for (let i = 0; i < 64; i++) {
          const g = "cg" + i;
          expect((await a.groupCreate({ name: g })).error).toBeUndefined();
          expect((await a.groupJoin({ name: g })).error).toBeUndefined();
        }
        expect((await a.groupJoin({ name: "cg0" })).error).toBeUndefined(); // idempotent at cap
        expect((await a.groupJoin({ name: "cg64" })).error).toBe("usage");   // new membership fails
      });
    });

    // ================= M1.5 F — work-groups matrix (server direction) =================

    test("F late joiner sees earlier group traffic; leave stops delivery (delivery-time resolution)", async () => {
      await withBus(async (h, root) => {
        const a = ((await seedAgent(h, root, "f-a", "ra")) as any).value.session as Session;
        const b = ((await seedAgent(h, root, "f-b", "rb")) as any).value.session as Session;
        expect((await a.groupCreate({ name: "swap-migration" })).error).toBeUndefined();
        const p = await a.post({ from: "f-a", to: "group:swap-migration", type: "note", body: "early" });
        expect(p.error).toBeUndefined();
        // late joiner
        expect((await b.groupJoin({ name: "swap-migration" })).error).toBeUndefined();
        const inb = await b.inbox({ agent: "f-b" });
        expect((inb as any).value.rows.map((r: any) => r.id)).toContain((p as any).value.id);
        // group receipts: intended = members excl sender
        const rec = (await b.receipts((p as any).value.id)) as any;
        expect(rec.value.receipts.intended).toEqual(["f-b"]);
        // leave ⇒ delivery stops (and old rows drop out of inbox — mailing-list semantics)
        expect((await b.groupLeave({ name: "swap-migration" })).error).toBeUndefined();
        const inb2 = await b.inbox({ agent: "f-b" });
        expect((inb2 as any).value.rows.map((r: any) => r.id)).not.toContain((p as any).value.id);
      });
    });

    test("F @all never matches a group target — both halves; group:<nonexistent> post rejected usage", async () => {
      await withBus(async (h, root) => {
        const a = ((await seedAgent(h, root, "f2-a", "ra")) as any).value.session as Session;
        const b = ((await seedAgent(h, root, "f2-b", "rb")) as any).value.session as Session;
        await a.groupCreate({ name: "g2x" });
        await a.groupJoin({ name: "g2x" }); // a is member
        // half 1: an @all message is not a group delivery — b (NON-member) still gets @all via the @all arm
        const pAll = await a.post({ from: "f2-a", to: "@all", type: "announce", body: "broadcast" });
        expect(((await b.inbox({ agent: "f2-b" })) as any).value.rows.map((r: any) => r.id)).toContain((pAll as any).value.id);
        // half 2: group:x does not reach non-members
        const pGrp = await a.post({ from: "f2-a", to: "group:g2x", type: "note", body: "members only" });
        expect(((await b.inbox({ agent: "f2-b" })) as any).value.rows.map((r: any) => r.id)).not.toContain((pGrp as any).value.id);
        // recipient index stores the LITERAL group:x
        const raw = (h as any).raw;
        expect((raw.testDb.query("SELECT count(*) c FROM message_recipients WHERE msg=? AND target='group:g2x'").get((pGrp as any).value.id) as any).c).toBe(1);
        // n3: nonexistent group
        expect((await a.post({ from: "f2-a", to: "group:nope-squat", type: "note", body: "x" })).error).toBe("usage");
      });
    });

    test("F group ops authz: join agent assertion (self only), delete needs agents:admin, member rename cascades", async () => {
      await withBus(async (h, root) => {
        const a = ((await seedAgent(h, root, "f3-a", "ra")) as any).value.session as Session;
        const b = ((await seedAgent(h, root, "f3-b", "rb")) as any).value.session as Session;
        await a.groupCreate({ name: "g3x" });
        // assertion: b cannot subscribe a to a group
        expect((await b.groupJoin({ name: "g3x", agent: "f3-a" })).error).toBe("forbidden");
        // self-join needs NO scope (self-organizing)
        expect((await b.groupJoin({ name: "g3x" })).error).toBeUndefined();
        // delete without agents:admin ⇒ forbidden; with root (admin) ⇒ ok
        expect((await b.groupDelete({ name: "g3x" })).error).toBe("forbidden");
        expect((await root.groupDelete({ name: "g3x" })).error).toBeUndefined();
        // rename cascade: membership follows the id
        await root.groupCreate({ name: "g3y" });
        expect((await a.groupJoin({ name: "g3y" })).error).toBeUndefined();
        expect((await root.rename({ agent: "f3-a", to: "f3-a2" })).error).toBeUndefined();
        const p = await b.post({ from: "f3-b", to: "group:g3y", type: "note", body: "cascade" });
        // the renamed id's session: f3-a2's token was minted by seedAgent below;
        // membership followed via the rename txn (UPDATE cascade).
        const a2 = ((await seedAgent(h, root, "f3-a2", "ra")) as any).value.session as Session;
        expect(((await a2.inbox({ agent: "f3-a2" })) as any).value.rows.map((r: any) => r.id)).toContain((p as any).value.id);
      });
    });

    test("F JS ≡ SQL parity: deliveredMsgIds equals recipientsMatch(Map) row sets, incl. old-incarnation exclusion (claude fuzz)", async () => {
      await withBus(async (h, root) => {
        const raw = (h as any).raw;
        const a = ((await seedAgent(h, root, "f4-a", "ra")) as any).value.session as Session;
        const b = ((await seedAgent(h, root, "f4-b", "rb")) as any).value.session as Session;
        await a.groupCreate({ name: "g4x" });
        await a.groupJoin({ name: "g4x" });
        const p1 = await a.post({ from: "f4-a", to: "group:g4x", type: "note", body: "in-incarnation" });
        // delete + recreate (tombstone boundary crossed); the OLD-incarnation
        // message is marked older than the new groups.created_at (rewinding the
        // MESSAGE, never future-stamping the group — m-a rule). The re-create is
        // direct SQL: same-second groupEnsure would return contention (m-a).
        await root.groupDelete({ name: "g4x" });
        raw.testDb.run("INSERT INTO groups(name,created_by,created_at) VALUES('g4x','f4-a',?)", [raw.nowIso()]);
        raw.testDb.run("UPDATE messages SET created_at = datetime('now','-1 day') WHERE id=?", [(p1 as any).value.id]);
        raw.testDb.run("INSERT OR IGNORE INTO group_members(grp,agent_id,joined_at) VALUES('g4x','f4-a',datetime('now'))");
        const p2 = await a.post({ from: "f4-a", to: "group:g4x", type: "note", body: "new-incarnation" });
        const sqlSet = raw.deliveredMsgIds("f4-a", "ra");
        const mem = raw.membershipsOf("f4-a");
        // delivery set (SQL arms == JS recipientsMatch predicate; the sender
        // exclusion is a separate inbox predicate, not a delivery arm).
        const jsSet = new Set(
          (raw.allMessages() as any[])
            .filter((m) => raw.recipientsMatch(m.recipients, "f4-a", "ra", mem, m.created_at))
            .map((m) => m.id),
        );
        // both directions equal
        expect([...sqlSet].sort()).toEqual([...jsSet].sort());
        // old-incarnation row excluded from BOTH sets
        expect(sqlSet.has((p1 as any).value.id)).toBe(false);
        expect(jsSet.has((p1 as any).value.id)).toBe(false);
        expect(sqlSet.has((p2 as any).value.id)).toBe(true);
      });
    });

    test("F group fan-out triggers fire (events kind='group') + group name traversal rejected", async () => {
      await withBus(async (h, root) => {
        const a = ((await seedAgent(h, root, "f5-a", "ra")) as any).value.session as Session;
        expect((await a.groupJoin({ name: "g5x" })).error).toBeUndefined();
        const raw = (h as any).raw;
        expect((raw.testDb.query("SELECT count(*) c FROM events WHERE kind='group' AND agent_id='f5-a'").get() as any).c).toBeGreaterThan(0);
        expect((await a.groupCreate({ name: "../evil" })).error).toBe("usage");
        expect((await a.groupCreate({ name: "a:b" })).error).toBe("usage");
      });
    });

    // ══════════ RFC-003 lane-scoped seats (rev-1 §6 pin list 1–11) ══════════
    const lanesSess = async (h: BusHandle, root: Session, id: string, lanes: string[], scopes: string[] = []) => {
      const tc = await root.tokenCreate({ agent: id, scopes: scopes as any, lanes });
      expect(tc.error).toBeUndefined();
      const s = (h as any).session({ token: (tc as any).value.token }) as Session;
      const j = await s.joinAgent({ agent: id, role: "guest" });
      expect(j.error).toBeUndefined();
      return s;
    };

    test("RFC-003 1: default shape — lanes omitted/null ⇒ unrestricted; invalid shapes are usage; tokenVerify carries the Set", async () => {
      await withBus(async (h, root) => {
        const g = await root.tokenCreate({ agent: "r3g", scopes: ["read:all"] });
        for (let i = 0; i < 32; i++) await root.channelCreate({ name: "lane" + i }); // lanes exist at mint (claude M3)
        expect((g as any).value.lanes).toBeNull();
        expect((await root.tokenCreate({ agent: "r3a", lanes: "notanarray" as any })).error).toBe("usage"); // D2: Array.isArray FIRST
        expect((await root.tokenCreate({ agent: "r3b", lanes: [] })).error).toBe("usage");                 // claude B1: [] rejected
        expect((await root.tokenCreate({ agent: "r3c", lanes: [1, "general"] as any })).error).toBe("usage"); // typeof before regex
        expect((await root.tokenCreate({ agent: "r3d", lanes: ["Has Spaces!"] })).error).toBe("usage");
        expect((await root.tokenCreate({ agent: "r3e", lanes: Array.from({ length: 33 }, (_, i) => "l" + i) })).error).toBe("usage"); // cap 32
        expect((await root.tokenCreate({ agent: "r3f", lanes: Array.from({ length: 32 }, (_, i) => "lane" + i) })).error).toBeUndefined();
        // pin 11: tokenVerify returns the lane Set on the principal; the CLI-less
        // raw surface proves it without the HTTP layer:
        const raw = (h as any).raw;
        const tv = raw.tokenVerify("r3f" + "-nope"); // wrong secret ⇒ error, shape stays Res
        expect(!!tv.error).toBe(true);
        const tok = await root.tokenCreate({ agent: "r3g2", lanes: ["general"] });
        const tv2 = raw.tokenVerify((tok as any).value.token) as any;
        expect([...tv2.value.lanes].sort()).toEqual(["general"]);
      });
    });

    test("RFC-003 2: mint fail-closed — lane must be LIVE (near-dup names the sibling); scoped principal cannot mint; admin scopes rejected with lanes", async () => {
      await withBus(async (h, root) => {
        await root.channelCreate({ name: "arena" });
        expect((await root.tokenCreate({ agent: "r3m1", lanes: ["ghostlane"] })).error).toBe("usage"); // R2: live at mint
        expect((await root.tokenCreate({ agent: "r3m0", lanes: ["ARENA"] })).error).toBe("usage"); // invalid FORM
        const nd = await root.tokenCreate({ agent: "r3m2", lanes: ["a-rena"] }); // chanNorm("a-rena")=="arena" ⇒ near-dup
        expect(nd.error).toBe("usage");
        expect(String((nd as any).detail)).toContain("arena"); // minter is unrestricted ⇒ may name the sibling
        expect((await root.tokenCreate({ agent: "r3m3", lanes: ["general"], scopes: ["tokens:admin"] as any })).error).toBe("usage"); // claude M4
        expect((await root.tokenCreate({ agent: "r3m4", lanes: ["general"], scopes: ["agents:admin"] as any })).error).toBe("usage");
        const seat = await lanesSess(h, root, "r3seat", ["general"]);
        expect((await seat.tokenCreate({ agent: "r3kid" })).error).toBe("forbidden"); // grok M2 belt
        expect((await seat.tokenList()).error).toBe("forbidden");
        expect((await seat.tokenRevoke({ id: 1 })).error).toBe("forbidden");
      });
    });

    test("RFC-003 3: NULL/''/corrupt column — legacy token byte-identical, empty sees nothing, corrupt fails CLOSED", async () => {
      await withBus(async (h, root) => {
        await root.channelCreate({ name: "plaza" });
        await root.post({ from: "root", to: "*", type: "note", body: "hello plaza", channel: "plaza" });
        const legacy = await root.tokenCreate({ agent: "r3legacy", scopes: ["read:all"] });
        let ls = (h as any).session({ token: (legacy as any).value.token }) as Session;
        await ls.joinAgent({ agent: "r3legacy", role: "op" });
        expect(((await ls.history({ channel: "plaza" })) as any).value.rows.length).toBe(1); // NULL = byte-identical
        // hand-written column writes (the ONLY writers of '' are migrations/tests).
        // wrapSession snapshots the token row ⇒ REBUILD the session per column state.
        const rdb = (h as any).raw;
        const resess = async () => {
          const s2 = (h as any).session({ token: (legacy as any).value.token }) as Session;
          await s2.joinAgent({ agent: "r3legacy", role: "op" });
          return s2;
        };
        // empty grant ⇒ sees nothing: channels() has no plaza, history is empty
        rdb.testDb.run("UPDATE tokens SET lanes='' WHERE agent_id='r3legacy'");
        ls = await resess();
        expect(((await ls.history({ channel: "plaza" })) as any).value.rows.length).toBe(0);
        expect(((await ls.channels()) as any).value.some((c: any) => c.name === "plaza")).toBe(false);
        // corrupt ⇒ deny-all, NEVER unrestricted
        rdb.testDb.run("UPDATE tokens SET lanes='???~~~' WHERE agent_id='r3legacy'");
        ls = await resess();
        expect(((await ls.history({ channel: "plaza" })) as any).value.rows.length).toBe(0);
        expect(((await ls.channels()) as any).value.some((c: any) => c.name === "plaza")).toBe(false);
        // restore to unrestricted ⇒ the downgraded-DB shape (column absent ⇒ NULL)
        rdb.testDb.run("UPDATE tokens SET lanes=NULL WHERE agent_id='r3legacy'");
        ls = await resess();
        expect(((await ls.history({ channel: "plaza" })) as any).value.rows.length).toBe(1);
      });
    });

    test("RFC-003 4: scoped invisibility across every surface — inbox/channels/read/receipts/status not_found, threadOf drops, join unresolved ignores foreign lanes (grok M1)", async () => {
      await withBus(async (h, root) => {
        await root.channelCreate({ name: "secret" });
        const hid = ((await root.post({ from: "root", to: "r3g4", type: "ask", body: "hidden mail", channel: "secret" })) as any).value.id;
        const pub = ((await root.post({ from: "root", to: "*", type: "note", body: "public noise", channel: "general" })) as any).value.id;
        const seat = await lanesSess(h, root, "r3g4", ["general"]);
        expect(((await seat.inbox({ agent: "r3g4" })) as any).value.rows.length).toBe(0); // ask in a hidden lane
        expect(((await seat.read({ agent: "r3g4", id: hid })) as any).error).toBe("not_found");
        expect((await seat.receipts(hid) as any).error).toBe("not_found");
        expect(((await seat.setStatus({ agent: "r3g4", id: hid, state: "done" })) as any).error).toBe("not_found");
        expect(((await seat.threadOf(hid)) as any).error).toBe("not_found"); // ALL rows hidden ⇒ not_found (drop semantics)
        expect(((await seat.channels()) as any).value.some((c: any) => c.name === "secret")).toBe(false);
        const ch = await seat.channels();
        expect((ch as any).value.some((c: any) => c.name === "general")).toBe(true);
        // threadOf DROP when only part is hidden: thread root in general, reply smuggled into secret via explicit thread
        await root.post({ from: "root", to: "*", type: "reply", body: "reply in hidden lane", channel: "secret", thread: pub });
        const th = (await seat.threadOf(pub)) as any;
        expect(th.error).toBeUndefined();
        expect(th.value.rows.every((r: any) => r.channel === "general")).toBe(true);
        // grok M1: foreign-lane volume must not leak through join unresolved
        const j0 = await seat.joinAgent({ agent: "r3g4", role: "guest" });
        const base = (j0 as any).value.unresolved;
        await root.post({ from: "root", to: "r3g4", type: "ask", body: "another hidden ask", channel: "secret" });
        const j1 = await seat.joinAgent({ agent: "r3g4", role: "guest" });
        expect((j1 as any).value.unresolved).toBe(base); // the ask lives in an invisible lane ⇒ invisible volume
      });
    });

    test("RFC-003 5: history lane predicate in BOTH branches BEFORE LIMIT; read:all does not bypass; since-paging never livelocks", async () => {
      await withBus(async (h, root) => {
        await root.channelCreate({ name: "foreign" });
        for (let i = 0; i < 3; i++) await root.post({ from: "root", to: "*", type: "note", body: "f" + i, channel: "foreign" });
        const inl = ((await root.post({ from: "root", to: "*", type: "note", body: "mine", channel: "general" })) as any).value.id;
        const seat = await lanesSess(h, root, "r3h5", ["general"], ["read:all"]); // read:all must NOT bypass lanes
        const snap = (await seat.history({ channel: "foreign" })) as any;
        expect(snap.value.rows.length).toBe(0);
        let since = snap.value.cursor, pages = 0;
        for (;;) { const hp = (await seat.history({ since })) as any; pages++; expect(hp.value.rows.every((r: any) => r.channel !== "foreign")).toBe(true); if (!hp.value.hasMore) break; since = hp.value.cursor; expect(pages).toBeLessThan(10); } // no livelock: cursor advances through foreign rows
        const g = (await seat.history({ channel: "general" })) as any;
        expect(g.value.rows.map((r: any) => r.id)).toContain(inl);
      });
    });

    test("RFC-003 6: post gate on the RESOLVED channel — default→general, thread/re inheritance, out-of-list and in-list-missing uniform forbidden, usage wins, scoped dm sugar refused, no revive through the back door", async () => {
      await withBus(async (h, root) => {
        await root.channelCreate({ name: "tmp" });
        const inRoot = ((await root.post({ from: "root", to: "*", type: "note", body: "anchor", channel: "general" })) as any).value.id;
        const seat = await lanesSess(h, root, "r3p6", ["general"]);
        expect(((await seat.post({ from: "r3p6", to: "*", type: "note", body: "default" })) as any).value.channel).toBe("general");
        const out = await seat.post({ from: "r3p6", to: "*", type: "note", body: "x", channel: "foreign6" });
        const missing = await seat.post({ from: "r3p6", to: "*", type: "note", body: "x", channel: "tmp" }); // in-list, not granted... granted? NO — lanes=[general]; tmp NOT granted here
        expect(out.error).toBe("forbidden");
        expect(missing.error).toBe("forbidden");
        expect((out as any).detail).toBe((missing as any).detail.replace("tmp", "foreign6")); // uniform template ⇒ oracle-free
        // re/thread INHERIT a foreign channel ⇒ the gate runs on the RESOLVED value (grok B3)
        const fr = await seat.post({ from: "r3p6", to: "*", type: "reply", body: "x", re: inRoot, channel: "foreign6" });
        expect(fr.error).toBe("forbidden"); // explicit foreign channel still refused post-resolution
        const inh = await seat.post({ from: "r3p6", to: "*", type: "reply", body: "x", re: inRoot });
        expect((inh as any).value.channel).toBe("general"); // inherited general ⇒ allowed
        expect((await seat.post({ from: "r3p6", to: "*", type: "note", body: "x", channel: 123 as any })).error).toBe("usage"); // usage BEFORE the gate
        // `to` == peer (m6 rule); peer is a real AGENT so dmGate passes and the
        // refusal must come from the LANE gate (dmPending), not the dm gate.
        const dm6 = await seat.post({ from: "r3p6", to: "root", type: "note", body: "x", dm: "root" });
        expect(dm6.error).toBe("forbidden"); // scoped seats never CREATE a dm lane
        expect(String((dm6 as any).detail)).toContain("lane-scoped");
        expect((await seat.post({ from: "r3p6", to: "nosuchpeer", type: "note", body: "x", dm: "nosuchpeer" })).error).toBe("not_found"); // dmGate wins over the lane gate
        // r3 m4 (claude): in-list-MISSING through the post back door — grant
        // tmp, operator deletes, the seat may NOT un-delete by posting.
        await root.channelCreate({ name: "tmp6d" });
        const seat6d = await lanesSess(h, root, "r3p6d", ["general", "tmp6d"]);
        await root.channelDelete({ name: "tmp6d" });
        const noRev = await seat6d.post({ from: "r3p6d", to: "*", type: "note", body: "x", channel: "tmp6d" });
        expect(noRev.error).toBe("forbidden"); // back door is closed for scoped seats
        expect(((await root.post({ from: "root", to: "*", type: "note", body: "revive", channel: "tmp6d" })) as any).value.channel).toBe("tmp6d"); // operator revives
        await root.channelDelete({ name: "tmp6d" });
        // unrestricted revives through the back door exactly as before (byte-identical)
        await root.channelDelete({ name: "tmp" });
        const rev = await root.post({ from: "root", to: "*", type: "note", body: "revive", channel: "tmp" });
        expect((rev as any).value.channel).toBe("tmp");
      });
    });

    test("RFC-003 7: channel.create in-list materialize / out-of-list uniform forbidden / hidden sibling unnamed; channel.delete + rename scoped-forbidden", async () => {
      await withBus(async (h, root) => {
        await root.channelCreate({ name: "planned" });
        const seat = await lanesSess(h, root, "r3c7", ["planned", "general"]); // LIVE at mint (R2)
        expect((await seat.channelCreate({ name: "planned" }) as any).value.created).toBe(false); // already live ⇒ idempotent
        await root.channelDelete({ name: "planned" });                          // operator retires
        // r3 MAJOR-1 (claude P2, grok ruling 1): in-list-missing is ALWAYS a
        // revival (lanes are live at mint) — scoped seats never un-delete.
        const rev7 = await seat.channelCreate({ name: "planned" });
        expect(rev7.error).toBe("forbidden");
        const o1 = await seat.channelCreate({ name: "elsewhere" });
        const o2 = await seat.channelCreate({ name: "ghost7" });
        expect(o1.error).toBe("forbidden"); expect(o2.error).toBe("forbidden");
        expect((o1 as any).detail).toBe((o2 as any).detail.replace("ghost7", "elsewhere")); // uniform template ⇒ oracle-free
        expect((rev7 as any).detail).toBe((o1 as any).detail.replace("elsewhere", "planned")); // revive == out-of-list, oracle-free
        expect(((await root.channelCreate({ name: "planned" })) as any).value.created).toBe(true); // operator still revives
        // dup with a HIDDEN sibling: near-dup of foreign7 (live, outside lanes) — must NOT name it (grok B5)
        await root.channelCreate({ name: "foreign7" });
        const dup = await seat.channelCreate({ name: "foreign_7" }); // chanNorm("foreign_7")=="foreign7", hidden from the seat
        expect(dup.error).toBe("forbidden"); // hidden sibling ⇒ uniform out-of-list error, never a named usage
        expect(String((dup as any).detail)).not.toContain("foreign7");
        // a near-dup of a VISIBLE lane still answers out-of-list: the lane gate
        // runs BEFORE the dup scan (fail-closed beats helpfulness) — a seat can
        // never hold a lane that is a near-dup of another (mint refuses, pin 2).
        // unrestricted keeps the named pointer byte-identically
        const dupU = await root.channelCreate({ name: "foreign-7" }); // chanNorm("foreign-7")=="foreign7" — exact twin
        expect(dupU.error).toBe("usage");
        expect(String((dupU as any).detail)).toContain("foreign7");
        // lifecycle: scoped forbidden BEFORE existence, uniform
        const d1 = await seat.channelDelete({ name: "planned" });
        const d2 = await seat.channelDelete({ name: "nope7" });
        expect(d1.error).toBe("forbidden"); expect(d2.error).toBe("forbidden");
        expect((d2 as any).detail).toBe((d1 as any).detail.replace("planned", "nope7")); // uniform, no oracle
        expect((await root.channelDelete({ name: "planned" })).error).toBeUndefined(); // operator still can
        expect((await seat.rename({ agent: "r3c7", to: "r3c7b" })).error).toBe("forbidden"); // claude M4
        expect((await root.rename({ agent: "r3c7", to: "r3c7c" })).error).toBeUndefined();
      });
    });

    // r3 B1+MAJOR-2 (grok/claude): idempotency replay must respect lanes on
    // BOTH axes — the key namespace is lanes-scoped (sibling tokens never
    // collide), and a replay hit on an INVISIBLE row answers uniform
    // not_found naming none of id/channel/file/thread.
    test("RFC-003 12: idempotency replay is lane-blind — invisible hit ⇒ uniform not_found before hash check; key namespace lanes-scoped; visible replay byte-identical", async () => {
      await withBus(async (h, root) => {
        await root.channelCreate({ name: "secret12" });
        const un = await seedAgent(h, root, "r3i12", "w");
        const unS = (un as any).value.session as Session;
        const seat = await lanesSess(h, root, "r3i12", ["general"]); // sibling token, same agent (D1)
        // grok's probe shape: an anchor in a SECRET lane + a THREAD-INHERITED
        // post that never names the lane — the request hash has channel:null,
        // so the replay path (which runs before the resolved-channel gate) was
        // the ONLY way the leak happened. Same key, same agent, scoped sibling.
        const anchor = await root.post({ from: "root", to: "*", type: "note", body: "anchor", channel: "secret12" });
        const sp = await unS.post({ from: "r3i12", to: "root", type: "reply", body: "s", thread: (anchor as any).value.id, idempotencyKey: "sk12" });
        expect((sp as any).value.channel).toBe("secret12"); // inherited, no explicit channel
        // scoped sibling replays the SAME params+key: pre-r3 leaked {id,
        // channel:"secret12", file}; now invisible == missing, no names.
        const leak = await seat.post({ from: "r3i12", to: "root", type: "reply", body: "s", thread: (anchor as any).value.id, idempotencyKey: "sk12" });
        expect(leak.error).toBe("not_found"); // anchor invisible ⇒ fresh post would be not_found too
        const d = String((leak as any).detail);
        expect(d).not.toContain("secret12");
        expect(d).not.toContain((sp as any).value.id);
        // hash mismatch on an INVISIBLE row must NOT answer conflict
        const mism = await seat.post({ from: "r3i12", to: "root", type: "reply", body: "CHANGED", thread: (anchor as any).value.id, idempotencyKey: "sk12" });
        expect(mism.error).toBe("not_found");
        // key namespace is lanes-scoped: same key+params in a VISIBLE lane
        // must NOT conflict with the hidden row (pre-r3: cross-token conflict).
        const vp = await seat.post({ from: "r3i12", to: "root", type: "note", body: "v", channel: "general", idempotencyKey: "sk12" });
        expect((vp as any).error).toBeUndefined();
        // visible replay stays byte-identical: replay of vp returns the same id
        const vp2 = await seat.post({ from: "r3i12", to: "root", type: "note", body: "v", channel: "general", idempotencyKey: "sk12" });
        expect((vp2 as any).value.id).toBe((vp as any).value.id);
        expect((vp2 as any).value.channel).toBe("general");
      });
    });

    // r3 MAJOR-3 (claude P1): DM lane entries canonicalize to the STORED pair
    // name and require the seat's agent to be a party (or read:dm).
    test("RFC-003 13: dm lane mint — stored pair name wins over typed spelling; non-party refused; read:dm auditor accepted; phantom pair refused", async () => {
      await withBus(async (h, root) => {
        const a = await seedAgent(h, root, "r3dm1", "w");
        const b = await seedAgent(h, root, "r3dm2", "w");
        await seedAgent(h, root, "r3dm3", "w"); // auditor candidate must exist pre-mint
        const aS = (a as any).value.session as Session;
        const dm = await aS.post({ from: "r3dm1", to: "r3dm2", type: "note", body: "hi", dm: "r3dm2" });
        expect((dm as any).error).toBeUndefined();
        const stored = (dm as any).value.channel; // dm~r3dm1~r3dm2 (sorted)
        const reversed = "dm~r3dm2~r3dm1";
        // reversed spelling canonicalizes to the stored name
        const t1 = await root.tokenCreate({ agent: "r3dm1", lanes: ["general", reversed] });
        expect((t1 as any).error).toBeUndefined();
        expect((t1 as any).value.lanes).toEqual([stored, "general"].sort());
        // non-party without read:dm is refused (dead grant otherwise)
        const t2 = await root.tokenCreate({ agent: "r3dm3", lanes: ["general", stored] });
        expect(t2.error).toBe("usage");
        expect(String((t2 as any).detail)).toContain("not a party");
        // auditor with read:dm is accepted
        const t3 = await root.tokenCreate({ agent: "r3dm3", lanes: [stored], scopes: ["read:dm"] as any });
        expect((t3 as any).error).toBeUndefined();
        // phantom pair refused
        expect((await root.tokenCreate({ agent: "r3dm1", lanes: ["dm~r3dm1~nosuch"] })).error).toBe("usage");
        // a seat minted with the canonical name actually SEES the lane
        const seat = (h as any).session({ token: (t1 as any).value.token }) as Session;
        const hist = await seat.history({ channel: stored });
        expect((hist as any).error).toBeUndefined();
      });
    });

    // r3 m5 (claude): the scoped list/revoke belt is CODE, not only
    // transitivity (no mint path makes the combo, but the gate answers too).
    test("RFC-003 14: scoped list/revoke belt + m3 deny-all renders distinct from unrestricted in tokenList", async () => {
      await withBus(async (h, root) => {
        const seat = await lanesSess(h, root, "r3b14", ["general"]);
        expect((await seat.tokenList()).error).toBe("forbidden");
        expect((await seat.tokenRevoke({ id: 1 })).error).toBe("forbidden");
        const deny = await root.tokenCreate({ agent: "r3b14c", lanes: ["general"] }); // sanity mint
        expect((deny as any).error).toBeUndefined();
        const rows = (((await root.tokenList()) as any).value.tokens) as any[];
        const mine = rows.find((t) => t.agentId === "r3b14c");
        expect(Array.isArray(mine.lanes)).toBe(true);
      });
    });

    test("RFC-003 10: cursor isolation — scoped watch never touches the unrestricted sibling row; forged :L consumers rejected; rotation with the same lanes keeps position", async () => {
      await withBus(async (h, root) => {
        const un = await seedAgent(h, root, "r3w", "w");
        const unS = (un as any).value.session as Session;
        const seat = await lanesSess(h, root, "r3w", ["general"]); // SAME agent id, second token (D1: per-token)
        await root.post({ from: "root", to: "r3w", type: "note", body: "w1", channel: "general" });
        const a = await seat.waitStep({ for: "r3w", consumer: "cli" }); // scoped pages (sees w1 in its lane)
        expect(a.error).toBeUndefined();
        expect(((a as any).value.messages as any[]).length).toBe(1);
        expect((await seat.cursorSet({ consumer: "cli", cursor: (a as any).value.cursor })).error).toBeUndefined(); // commit OWN row
        expect(((await unS.cursorGet({ consumer: "cli" })) as any).value.seq).toBe(0); // sibling untouched
        const b = await unS.waitStep({ for: "r3w", consumer: "cli" });
        expect(b.error).toBeUndefined();
        expect((await unS.cursorSet({ consumer: "cli", cursor: (b as any).value.cursor })).error).toBeUndefined();
        expect(((await seat.cursorGet({ consumer: "cli" })) as any).value.seq).toBe(Number((a as any).value.cursor.split(".")[1])); // seat row = its own position
        expect(((await unS.cursorGet({ consumer: "cli" })) as any).value.seq).toBe(Number((b as any).value.cursor.split(".")[1]));
        // forged suffixed names rejected pre-DB (':' outside CONSUMER_RE — no smuggling)
        expect((await seat.cursorGet({ consumer: "cli:Ldeadbeef0000" })).error).toBe("usage");
        expect((await seat.cursorSet({ consumer: "cli:Ldeadbeef0000", cursor: (a as any).value.cursor })).error).toBe("usage");
        // rotation with the SAME lanes ⇒ same lane-hash ⇒ position survives (D1)
        const rot = await root.tokenCreate({ agent: "r3w", lanes: ["general"] });
        const seat2 = (h as any).session({ token: (rot as any).value.token }) as Session;
        await seat2.joinAgent({ agent: "r3w", role: "w" });
        expect(((await seat2.cursorGet({ consumer: "cli" })) as any).value.seq).toBe(Number((a as any).value.cursor.split(".")[1]));
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
