import { expect, test, describe } from "bun:test";
import { mkdtempSync, rmSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { openBus, localCtx, serverCtx, csv, normalizeScopes, canonicalJson, sha256hex, RPC_CODES, EXIT_CODES } from "../src/bus.ts";
import { testSeams } from "../src/seams.ts";

const tmp = () => mkdtempSync(join(tmpdir(), "comms-test-"));
const srv = (home: string) => openBus({ home, mode: "server", seams: testSeams({}) });
const loc = (home: string) => openBus({ home, mode: "local", seams: testSeams({}) });

describe("N1: recipient split — recursive CTE == csv(), no aborts on legacy data", () => {
  test("migration backfill succeeds and index equals csv() for hostile recipients", () => {
    const home = tmp();
    mkdirSync(join(home, ".comms"), { recursive: true });
    const d0 = new Database(join(home, ".comms", "comms.db"), { create: true });
    d0.exec(`
      CREATE TABLE agents(id TEXT PRIMARY KEY NOT NULL, role TEXT, caps TEXT, pid INTEGER, joined_at TEXT, last_seen TEXT, meta TEXT);
      CREATE TABLE messages(id TEXT PRIMARY KEY, thread TEXT, re TEXT, sender TEXT, recipients TEXT, type TEXT,
        status TEXT, tags TEXT, subject TEXT, body TEXT, file TEXT, created_at TEXT, updated_at TEXT, channel TEXT NOT NULL DEFAULT 'general');
      CREATE TABLE reads(agent TEXT, msg TEXT, read_at TEXT, PRIMARY KEY(agent, msg));
      CREATE TABLE channels(name TEXT PRIMARY KEY NOT NULL, purpose TEXT, created_at TEXT, created_by TEXT);
    `);
    // finding 5: tab/newline/NBSP cases — SQLite trim strips only U+0020; jstrim
    // must give JS-trim parity so backfill==trigger==csv() BY CONSTRUCTION.
    const hostile = ['a"b', "a\\b", "don,", "", "a, b ,c", "x,,y", "@all", "a,\tb", "a,\nb", "a,\u00a0b"];
    hostile.forEach((recips, i) => {
      d0.run(
        "INSERT INTO messages(id,thread,sender,recipients,type,status,subject,body,file,created_at,updated_at,channel) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        [`m${i}`, `m${i}`, "sys", recips, "note", "done", "s", "b", "f", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z", "general"],
      );
    });
    d0.close();

    const bus = loc(home);
    for (let i = 0; i < hostile.length; i++) {
      const idx = (bus.testDb.query("SELECT target FROM message_recipients WHERE msg=? ORDER BY target").all(`m${i}`) as any[])
        .map((r) => r.target);
      expect([...new Set(idx)]).toEqual([...new Set(csv(hostile[i]))].sort());
    }
    // trigger path parity for NEW inserts too
    const r = bus.post(localCtx("sys"), { from: "sys", to: 'we"ird, ok ,', type: "note", body: "x" });
    expect(r.error).toBeUndefined();
    const idx = (bus.testDb.query("SELECT target FROM message_recipients WHERE msg=?").all((r as any).value.id) as any[])
      .map((x) => x.target);
    expect([...new Set(idx)]).toEqual([...new Set(csv('we"ird, ok ,'))].sort());
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("empty recipients produce zero index rows (no target='' drift)", () => {
    const home = tmp();
    const bus = loc(home);
    const r = bus.post(localCtx("sys"), { from: "sys", to: ",", type: "note", body: "x" });
    expect((r as any).error).toBeUndefined();
    const rows = bus.testDb.query("SELECT COUNT(*) c FROM message_recipients WHERE msg=?").get((r as any).value.id) as any;
    expect(rows.c).toBe(0);
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("fan-out events (§3′/§4)", () => {
  test("post fires msg_ai; status fires msg_au; read fires reads_ai; join fires agents_ai", () => {
    const home = tmp();
    const bus = loc(home);
    bus.joinAgent(localCtx("a1"), { agent: "a1", role: "lab" });
    const p = bus.post(localCtx("a1"), { from: "a1", to: "a2", type: "ask", body: "hi" });
    const mid = (p as any).value.id;
    bus.setStatus(localCtx("a1"), { agent: "a1", id: mid, state: "done" });
    bus.read(localCtx("a2"), { agent: "a2", id: mid });
    const kinds = (bus.testDb.query("SELECT kind FROM events ORDER BY seq").all() as any[]).map((r) => r.kind);
    expect(kinds).toContain("msg");
    expect(kinds).toContain("status");
    expect(kinds).toContain("read");
    expect(kinds).toContain("presence");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("agents_au debounce: touch within 60s emits nothing; role change always emits", () => {
    const home = tmp();
    const bus = loc(home);
    bus.joinAgent(localCtx("a1"), { agent: "a1", role: "lab" });
    const before = (bus.testDb.query("SELECT count(*) c FROM events WHERE kind='presence'").get() as any).c;
    bus.touch("a1"); bus.touch("a1"); bus.touch("a1"); // same frozen clock ⇒ no events
    expect((bus.testDb.query("SELECT count(*) c FROM events WHERE kind='presence'").get() as any).c).toBe(before);
    bus.joinAgent(localCtx("a1"), { agent: "a1", role: "lead" }); // role change ⇒ emits
    expect((bus.testDb.query("SELECT count(*) c FROM events WHERE kind='presence'").get() as any).c).toBe(before + 1);
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("finding 6 (P7): NULL timestamps from stale/manual writers do NOT abort", () => {
    const home = tmp();
    const bus = loc(home);
    // agents INSERT without last_seen
    expect(() => bus.testDb.run("INSERT INTO agents(id,role) VALUES('n1','r')")).not.toThrow();
    // messages INSERT without created_at
    expect(() => bus.testDb.run("INSERT INTO messages(id,thread,sender,recipients,type,status,body,updated_at) VALUES('x1','x1','s','a','note','open','b','2026-01-01T00:00:00Z')")).not.toThrow();
    // revoke then UN-revoke (revoked_at back to NULL)
    const t = bus.tokenCreate(localCtx("s"), { agent: "n1", scopes: ["read:all"] });
    expect(t.error).toBeUndefined();
    expect(bus.tokenRevoke(localCtx("s"), { id: (t as any).value.id }).error).toBeUndefined();
    expect(() => bus.testDb.run("UPDATE tokens SET revoked_at=NULL WHERE id=?", [(t as any).value.id])).not.toThrow();
    // the un-revoked token verifies again
    expect(bus.tokenVerify((t as any).value.token).error).toBeUndefined();
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("tokens (§4/§5)", () => {
  test("create→verify roundtrip; bad token rejected; revoke rejects; tokenId returned", () => {
    const home = tmp();
    const bus = loc(home); // bootstrap path = local-mode opener (§5)
    const root = localCtx("root");
    const r = bus.tokenCreate(root, { agent: "bot1", scopes: ["read:all"] });
    expect(r.error).toBeUndefined();
    const { token } = (r as any).value;
    expect(token.startsWith("ac_")).toBe(true);
    const v = bus.tokenVerify(token);
    expect((v as any).value.agentId).toBe("bot1");
    expect((v as any).value.scopes).toEqual(["read:all"]);
    expect((v as any).value.tokenId).toBeGreaterThan(0); // finding 20
    expect(bus.tokenVerify(token + "x").error).toBe("unauthorized");
    expect(bus.tokenRevoke(root, { id: (r as any).value.id }).error).toBeUndefined();
    expect(bus.tokenVerify(token).error).toBe("unauthorized");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("admin:true mints the full 4-name NORMALIZED scope set (§4/§5); human defaults read:all", () => {
    const home = tmp();
    const bus = loc(home); // bootstrap = local-mode opener (§5)
    const root = localCtx("root");
    const a = bus.tokenCreate(root, { agent: "don", admin: true, force: true });
    const v1 = bus.tokenVerify((a as any).value.token);
    expect([...(v1 as any).value.scopes].sort()).toEqual(["agents:admin", "post:as", "read:all", "tokens:admin"]);
    const norm = (v1 as any).value.scopes.join(",");
    expect(norm).toBe([...(v1 as any).value.scopes].sort().join(",")); // normalized sorted CSV
    const h = bus.tokenCreate(root, { agent: "bakon", kind: "human" });
    const v2 = bus.tokenVerify((h as any).value.token);
    expect((v2 as any).value.scopes).toEqual(["read:all"]);
    expect((v2 as any).value.kind).toBe("human");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("finding P8: bootstrap guard ABORTS a second admin without force; force passes", () => {
    const home = tmp();
    const bus = loc(home); // bootstrap = local-mode opener (§5); guard applies to ALL openers
    const root = localCtx("root");
    const a1 = bus.tokenCreate(root, { agent: "don", admin: true });
    expect(a1.error).toBeUndefined();
    const a2 = bus.tokenCreate(root, { agent: "don2", admin: true });
    expect(a2.error).toBe("conflict");
    const a3 = bus.tokenCreate(root, { agent: "don2", admin: true, force: true });
    expect(a3.error).toBeUndefined();
    const c = (bus.testDb.query(
      "SELECT count(*) c FROM tokens WHERE revoked_at IS NULL AND instr(',' || scopes || ',', ',tokens:admin,') > 0",
    ).get() as any).c;
    expect(c).toBe(2);
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("server principal WITHOUT tokens:admin cannot mint (P11)", () => {
    const home = tmp();
    const bus = srv(home);
    const ctx = serverCtx("pleb", []);
    bus.testDb.run("INSERT INTO agents(id,role,last_seen) VALUES('pleb','p','2026-01-01T00:00:00Z')");
    expect(bus.tokenCreate(ctx, { agent: "x", scopes: [] }).error).toBe("forbidden"); // M9: valid cred lacking scope ⇒ -32002
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("normalization + canonical hash (§4/§6)", () => {
  test("normalizeScopes: trim, dedupe, sort", () => {
    expect(normalizeScopes(["tokens:admin", " read:all ", "read:all"])).toBe("read:all,tokens:admin");
  });
  test("canonicalJson sorts keys, stable hash", () => {
    const a = canonicalJson({ b: 1, a: [3, { z: 1, y: null }] });
    expect(a).toBe('{"a":[3,{"y":null,"z":1}],"b":1}');
    expect(sha256hex(a)).toBe(sha256hex(canonicalJson({ b: 1, a: [3, { y: null, z: 1 }] })));
  });
});

describe("idempotency (§6)", () => {
  test("same key+same params replays original id; changed subject ⇒ conflict", () => {
    const home = tmp();
    const bus = srv(home);
    // bootstrap via a LOCAL opener (§5: bootstrap is local-only), then the
    // server-mode principal uses its own token scopes
    const bootHome = bus; // same DB; mint through the server with an admin ctx
    bootHome.tokenCreate(serverCtx("cli1", ["tokens:admin"]), { agent: "cli1", admin: true });
    const ctx = serverCtx("cli1", ["read:all", "post:as", "tokens:admin", "agents:admin"]);
    const p1 = bus.post(ctx, { from: "cli1", to: "x", type: "note", body: "b", subject: "s", idempotencyKey: "k1" });
    expect(p1.error).toBeUndefined();
    const p2 = bus.post(ctx, { from: "cli1", to: "x", type: "note", body: "b", subject: "s", idempotencyKey: "k1" });
    expect((p2 as any).value.id).toBe((p1 as any).value.id);
    const p3 = bus.post(ctx, { from: "cli1", to: "x", type: "note", body: "b", subject: "DIFFERENT", idempotencyKey: "k1" });
    expect(p3.error).toBe("conflict");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("cursors (§6)", () => {
  test("per-consumer isolation; monotonic unless force; epoch mismatch ⇒ resync error with data", () => {
    const home = tmp();
    const bus = srv(home);
    expect((bus.cursorGet("a", "cli") as any).value.seq).toBe(0);
    bus.cursorSet("a", "cli", bus.epoch(), 10);
    bus.cursorSet("a", "mcp", bus.epoch(), 3);
    expect((bus.cursorGet("a", "cli") as any).value.seq).toBe(10);
    expect((bus.cursorGet("a", "mcp") as any).value.seq).toBe(3);
    expect(bus.cursorSet("a", "cli", bus.epoch(), 5).error).toBe("conflict");
    expect(bus.cursorSet("a", "cli", bus.epoch(), 5, true).error).toBeUndefined();
    const e2 = bus.rotateEpoch();
    // M4 (grok round 4): stored foreign-epoch row ⇒ resync, NOT a seq-0 collapse
    const cg = bus.cursorGet("a", "cli");
    expect(cg.error).toBe("resync");
    expect((cg as any).data.resync).toBe(true);
    expect((cg as any).data.epoch).toBe(e2);
    // recovery commit: explicit current-epoch set is NOT blocked by the foreign row
    expect(bus.cursorSet("a", "cli", e2, 0).error).toBeUndefined();
    const r = bus.cursorSet("a", "cli", "deadbeefdead", 7);
    expect(r.error).toBe("resync"); // N3: typed resync, not not_found
    expect((r as any).data.resync).toBe(true);
    expect((r as any).data.epoch).toBe(e2);
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("finding 7 (B3): gc advances a REAL floor so a cursor in the hole RESYNCS", () => {
    const home = tmp();
    const bus = srv(home);
    // 10 old events (at=2025) via direct inserts
    for (let i = 0; i < 10; i++)
      bus.testDb.run("INSERT INTO events(kind,at) VALUES('msg','2025-01-01T00:00:00Z')");
    const minSeq = (bus.testDb.query("SELECT min(seq) m FROM events").get() as any).m;
    bus.cursorSet("a", "cli", bus.epoch(), minSeq + 3); // cursor inside the hole
    const g = bus.gc(); // frozen clock 2026 ⇒ all 2025 events are past retention
    expect(g.events).toBeGreaterThanOrEqual(9);
    expect(g.floor).toBeGreaterThanOrEqual(minSeq + 9);
    const r = bus.cursorSet("a", "cli", bus.epoch(), minSeq + 5);
    expect(r.error).toBe("resync"); // NOT silently accepted
    expect((r as any).data.floor).toBe(g.floor);
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("history handoff (§6 nit + finding 8)", () => {
  test("newest page, cursor=last delivered, no hole on handoff", () => {
    const home = tmp();
    const bus = srv(home);
    const ctx = serverCtx("don", ["read:all"]);
    bus.testDb.run("INSERT INTO agents(id,role,last_seen) VALUES('don','d','2026-01-01T00:00:00Z')");
    const h0 = bus.history(ctx, {});
    for (let i = 0; i < 5; i++) bus.post(ctx, { from: "don", to: "other", type: "note", body: `m${i}` });
    const page = bus.history(ctx, { limit: 2 }) as any;
    expect(page.value.rows.map((r: any) => r.body)).toEqual(["m3", "m4"]); // NEWEST page
    expect(page.value.hasMore).toBe(true);
    const rest = bus.history(ctx, { since: page.value.cursor }) as any;
    expect(rest.value.rows.map((r: any) => r.body)).toEqual([]); // cursor = last DELIVERED (m4) ⇒ no dup
    const mid = bus.history(ctx, { since: "deadbeefdeadbeef.1" }) as any; // foreign epoch ⇒ resync
    expect(mid.error).toBe("resync");
    void h0;
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("history requires read:all in server mode", () => {
    const home = tmp();
    const bus = srv(home);
    expect(bus.history(serverCtx("x", []), {}).error).toBe("forbidden");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("history internal-Res catch is live and rollback-guarded (claude round-5 m1)", () => {
    const home = tmp();
    const bus = srv(home);
    const ctx = serverCtx("don", ["read:all"]);
    bus.testDb.run("INSERT INTO agents(id,role,last_seen) VALUES('don','d','2026-01-01T00:00:00Z')");
    bus.post(ctx, { from: "don", to: "other", type: "note", body: "x" });
    // Force the catch: drop meta.epoch so the in-txn epoch() read throws (claude Q4).
    // Mutation M-d (rethrow instead of internal Res) must fail HERE, and the
    // guarded ROLLBACK must not itself throw ("no transaction is active").
    bus.testDb.run("DELETE FROM meta WHERE key='epoch'");
    const snap = bus.history(ctx, {}) as any;
    expect(snap.error).toBe("internal");
    expect(String(snap.detail)).toContain("history:");
    const since = bus.history(ctx, { since: "deadbeefdeadbeef.0" }) as any; // parseCursor epoch read must stay in Res, not raw-throw
    expect(since.error).toBe("internal");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("local-mode quirks pinned (finding 14 / §10)", () => {
  test("rejected post (bad type / bad channel) STILL registers the sender (touch-before-validate)", () => {
    const home = tmp();
    const bus = loc(home);
    expect(bus.post(localCtx("late"), { from: "late", to: "x", type: "bogus", body: "b" }).error).toBe("usage");
    expect((bus.testDb.query("SELECT count(*) c FROM agents WHERE id='late'").get() as any).c).toBe(1);
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
  test("dangling --re inserts silently in LOCAL mode (legacy leniency)", () => {
    const home = tmp();
    const bus = loc(home);
    const r = bus.post(localCtx("s1"), { from: "s1", to: "x", type: "reply", body: "b", re: "nope-9999" });
    expect(r.error).toBeUndefined();
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
  test("inbox does NOT mark reads (legacy); mark:true opts in", () => {
    const home = tmp();
    const bus = loc(home);
    bus.joinAgent(localCtx("a1"), { agent: "a1", role: "lab" });
    bus.joinAgent(localCtx("a2"), { agent: "a2", role: "worker" });
    bus.post(localCtx("a1"), { from: "a1", to: "a2", type: "ask", body: "hi" });
    const v = bus.inbox(localCtx("a2"), { agent: "a2" }) as any;
    expect(v.value.rows.length).toBe(1);
    expect((bus.testDb.query("SELECT count(*) c FROM reads WHERE agent='a2'").get() as any).c).toBe(0);
    bus.inbox(localCtx("a2"), { agent: "a2", mark: true });
    expect((bus.testDb.query("SELECT count(*) c FROM reads WHERE agent='a2'").get() as any).c).toBe(1);
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("identifier validation (§9)", () => {
  test("traversal attempts rejected on channel, from, and as (finding B3)", () => {
    const home = tmp();
    const bus = loc(home);
    expect(bus.post(localCtx("sys"), { from: "sys", to: "x", type: "note", body: "b", channel: "../../home/comms/.ssh" }).error).toBe("usage");
    expect(bus.post(localCtx("../evil"), { from: "../evil", to: "x", type: "note", body: "b" }).error).toBe("usage");
    expect(bus.post(localCtx("sys"), { from: "sys", to: "x", type: "note;rm", body: "b" }).error).toBe("usage");
    expect(bus.post(localCtx("sys"), { from: "sys", as: "../../../evil", to: "x", type: "note", body: "b" }).error).toBe("usage");
    // nothing escaped: only sys + late registered agents exist under messages/
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("rename (§4 transactional)", () => {
  test("rewrites reads/tokens/cursors/idempotency/events in one txn; history keeps old sender", () => {
    const home = tmp();
    const bus = srv(home);
    bus.testDb.run("INSERT INTO agents(id,role,last_seen) VALUES('old','lab','2026-01-01T00:00:00Z')");
    const ctx = serverCtx("old", ["post:as", "read:all", "tokens:admin", "agents:admin"]);
    const p = bus.post(ctx, { from: "old", to: "other", type: "note", body: "b" });
    const mid = (p as any).value.id;
    bus.testDb.run("INSERT INTO reads(agent,msg,read_at) VALUES('old',?,'now')", [mid]);
    bus.tokenCreate(serverCtx("old", ["tokens:admin", "read:all"]), { agent: "old", scopes: ["read:all"] });
    bus.cursorSet("old", "cli", bus.epoch(), 5);
    const r = bus.rename(ctx, { agent: "old", to: "new" });
    expect(r.error).toBeUndefined();
    expect((bus.testDb.query("SELECT count(*) c FROM agents WHERE id='new'").get() as any).c).toBe(1);
    expect((bus.testDb.query("SELECT count(*) c FROM reads WHERE agent='new'").get() as any).c).toBe(1);
    expect((bus.testDb.query("SELECT count(*) c FROM tokens WHERE agent_id='new'").get() as any).c).toBe(1);
    expect((bus.testDb.query("SELECT count(*) c FROM cursors WHERE agent_id='new'").get() as any).c).toBe(1);
    expect((bus.testDb.query("SELECT count(*) c FROM idempotency WHERE agent_id='new'").get() as any).c).toBe(0);
    expect((bus.testDb.query("SELECT sender FROM messages WHERE id=?").get(mid) as any).sender).toBe("old");
    const kinds = (bus.testDb.query("SELECT kind FROM events").all() as any[]).map((x) => x.kind);
    expect(kinds).toContain("rename");
    // M7 (round 2): events are point-in-time audit — the OLD id survives in
    // prior events (the rename event itself carries the new id via agents_au).
    expect((bus.testDb.query("SELECT count(*) c FROM events WHERE agent_id='old'").get() as any).c).toBeGreaterThan(0);
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("finding 16: rename announce is INSIDE the txn — rollback leaves no orphan .md", () => {
    const home = tmp();
    const files = new Map<string, string>();
    const bus = openBus({ home, mode: "local", seams: testSeams({ files }) });
    bus.testDb.run("INSERT INTO agents(id,role,last_seen) VALUES('ra','lab','2026-01-01T00:00:00Z')");
    bus.testDb.run("INSERT INTO agents(id,role,last_seen) VALUES('rb','lab','2026-01-01T00:00:00Z')"); // target exists ⇒ conflict
    const r = bus.rename(localCtx("ra"), { agent: "ra", to: "rb" });
    expect(r.error).toBe("identity_conflict");
    expect([...files.keys()].filter((k) => k.includes("announce")).length).toBe(0); // mirror untouched
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("round-3 pins (m2 quirk, M5 forced collision, M6 trigger upgrade)", () => {
  test("m2: local inbox/read touch-before-validate registers unvalidated ids (legacy parity)", () => {
    const home = tmp();
    const bus = loc(home);
    // legacy de4ed3b quirk: inbox --for "BAD ID" succeeds AND writes the row
    expect(bus.inbox(localCtx("BAD ID"), { agent: "BAD ID" }).error).toBeUndefined();
    const ids = (bus.testDb.query("SELECT id FROM agents").all() as any[]).map((r) => r.id);
    expect(ids).toContain("BAD ID");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });

  test("M5: forced PK collision ⇒ derived thread follows the NEW id; explicit thread survives", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "local", seams: testSeams({ seed: 7 }) });
    const ctx = localCtx("sys");
    const first = bus.post(ctx, { from: "sys", to: "x", type: "note", body: "one" });
    const firstId = (first as any).value.id;
    // force a collision: seed resets per-bus, so a fresh bus mints the same id
    bus.close();
    const bus2 = openBus({ home, mode: "local", seams: testSeams({ seed: 7 }) });
    const second = bus2.post(ctx, { from: "sys", to: "x", type: "note", body: "two" });
    expect(second.error).toBeUndefined();
    const v = (second as any).value;
    expect(v.id).not.toBe(firstId); // retried to a fresh id
    expect(v.thread).toBe(v.id); // M5: derived thread = NEW id, never the victim's
    expect((bus2.testDb.query("SELECT thread FROM messages WHERE id=?").get(v.id) as any).thread).toBe(v.id);
    // explicit thread is never retargeted by the retry
    const pinned = bus2.post(ctx, { from: "sys", to: "x", type: "note", body: "three", thread: "manual-thread" });
    expect((pinned as any).value.thread).toBe("manual-thread");
    bus2.close(); rmSync(home, { recursive: true, force: true });
  });

  test("M6: reopening a DB with old-generation triggers upgrades them + rebuilds recipients", () => {
    const home = tmp();
    const bus = loc(home);
    // n2 (round 3): replace ALL SEVEN triggers with their old-generation text
    // (23aa4cb DDL: space-trim only, NO coalesce) so the NULL-last_seen
    // assertion below is not vacuous.
    bus.testDb.exec(`
      DROP TRIGGER msg_ai; DROP TRIGGER msg_au; DROP TRIGGER reads_ai;
      DROP TRIGGER agents_ai; DROP TRIGGER agents_au; DROP TRIGGER tokens_ai; DROP TRIGGER tokens_au;
      CREATE TRIGGER msg_ai AFTER INSERT ON messages BEGIN
        INSERT INTO events(kind,msg_id,agent_id,at) VALUES('msg',NEW.id,NEW.sender,NEW.created_at);
        INSERT INTO message_recipients(msg,target)
        WITH RECURSIVE s(rest,tok) AS (
          SELECT coalesce(NEW.recipients,'') || ',', NULL
          UNION ALL
          SELECT substr(rest, instr(rest,',')+1), trim(substr(rest,1,instr(rest,',')-1)) FROM s WHERE rest <> '')
        SELECT NEW.id, tok FROM s WHERE tok IS NOT NULL AND tok <> '';
      END;
      CREATE TRIGGER msg_au AFTER UPDATE OF status ON messages BEGIN
        INSERT INTO events(kind,msg_id,agent_id,at) VALUES('status',NEW.id,NEW.sender,NEW.updated_at);
      END;
      CREATE TRIGGER reads_ai AFTER INSERT ON reads BEGIN
        INSERT INTO events(kind,msg_id,agent_id,at) VALUES('read',NEW.msg,NEW.agent,NEW.read_at);
      END;
      CREATE TRIGGER agents_ai AFTER INSERT ON agents BEGIN
        INSERT INTO events(kind,agent_id,at) VALUES('presence',NEW.id,NEW.last_seen);
      END;
      CREATE TRIGGER agents_au AFTER UPDATE ON agents
      WHEN OLD.id IS NOT NEW.id OR OLD.role IS NOT NEW.role OR OLD.caps IS NOT NEW.caps
        OR (julianday(NEW.last_seen) - julianday(OLD.last_seen)) * 86400 >= 60
      BEGIN
        INSERT INTO events(kind,agent_id,at)
        VALUES(CASE WHEN OLD.id IS NOT NEW.id THEN 'rename' ELSE 'presence' END, NEW.id, NEW.last_seen);
      END;
      CREATE TRIGGER tokens_ai AFTER INSERT ON tokens BEGIN
        INSERT INTO events(kind,agent_id,at) VALUES('token',NEW.agent_id,NEW.created_at);
      END;
      CREATE TRIGGER tokens_au AFTER UPDATE OF revoked_at ON tokens BEGIN
        INSERT INTO events(kind,agent_id,at) VALUES('token',NEW.agent_id,NEW.revoked_at);
      END;`);
    bus.testDb.exec("DELETE FROM meta WHERE key='schema_version'");
    // a row written by the OLD trigger: tab-padded recipient indexed WITH the tab
    bus.testDb.run("INSERT INTO messages(id,thread,sender,recipients,type,status,body,created_at,channel) VALUES('m-old','m-old','sys','a,\tb','note','open','b','2026-01-01T00:00:00Z','general')");
    bus.testDb.run("INSERT INTO message_recipients(msg,target) VALUES('m-old','\\tb')");
    bus.close();
    // reopen with current code ⇒ version bump drops+recreates triggers, rebuilds index
    const bus2 = loc(home);
    const sql = (bus2.testDb.query("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='msg_ai'").get() as any).sql;
    expect(sql).toContain("char(9)"); // WSET present ⇒ new generation
    expect(sql).toContain("coalesce");
    const targets = (bus2.testDb.query("SELECT target FROM message_recipients WHERE msg='m-old' ORDER BY target").all() as any[]).map((r) => r.target);
    expect(targets).toEqual(["a", "b"]); // rebuilt via WSET trim: "\tb" → "b" — matches csv() parity
    // n2: agents_ai was ALSO old-gen (no coalesce) ⇒ NULL last_seen only survives
    // because the migration actually replaced it:
    bus2.testDb.run("INSERT INTO agents(id,role,joined_at,last_seen) VALUES('nulld','r','2026-01-01T00:00:00Z',NULL)");
    bus2.close(); rmSync(home, { recursive: true, force: true });
  });

  test("R1: migration is forward-only — a newer stored version keeps its triggers", () => {
    const home = tmp();
    const bus = loc(home);
    // simulate a DB bumped to v99 by a NEWER binary with distinctive triggers
    bus.testDb.exec("UPDATE meta SET value='99' WHERE key='schema_version'");
    bus.testDb.exec("DROP TRIGGER msg_ai; CREATE TRIGGER msg_ai AFTER INSERT ON messages BEGIN SELECT 1; END;");
    bus.close();
    const bus2 = loc(home); // must NOT downgrade v99 triggers
    const sql = (bus2.testDb.query("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='msg_ai'").get() as any).sql;
    expect(sql).toContain("SELECT 1"); // untouched
    expect((bus2.testDb.query("SELECT value FROM meta WHERE key='schema_version'").get() as any).value).toBe("99");
    bus2.close(); rmSync(home, { recursive: true, force: true });
  });

  test("R1: DROP+CREATE+rebuild+version share ONE txn — no trigger-less window (race probe)", async () => {
    // Adapted from don-claude's zz_m6race.ts: a child process posts in a tight
    // loop while the parent repeatedly forces version bumps + reopens. Every
    // committed message must end up with its msg event — under the old
    // two-txn migration, ~1% permanently lost theirs.
    const home = tmp();
    openBus({ home, mode: "local" }).close();
    const child = Bun.spawn(["bun", import.meta.path.replace("bus.test.ts", "m6writer.ts"), home, "3000"], { stdout: "ignore", stderr: "ignore" });
    await Bun.sleep(200);
    const stopAt = Date.now() + 2500;
    for (let i = 0; Date.now() < stopAt; i++) {
      try {
        const b = openBus({ home, mode: "local" });
        b.testDb.run("UPDATE meta SET value='1' WHERE key='schema_version'"); // force next-bump
        b.close();
        openBus({ home, mode: "local" }).close(); // runs the atomic upgrade
      } catch { /* SQLITE_BUSY under load is fine — the writer or a retry wins */ }
    }
    await child.exited;
    const b = loc(home);
    const noEvent = (b.testDb.query("SELECT count(*) c FROM messages m WHERE NOT EXISTS (SELECT 1 FROM events e WHERE e.kind='msg' AND e.msg_id=m.id)").get() as any).c;
    const noRcpt = (b.testDb.query("SELECT count(*) c FROM messages m WHERE NOT EXISTS (SELECT 1 FROM message_recipients r WHERE r.msg=m.id)").get() as any).c;
    const posted = (b.testDb.query("SELECT count(*) c FROM messages").get() as any).c;
    expect(posted).toBeGreaterThan(20); // the probe actually did work
    expect(noEvent).toBe(0); // zero lost events — the window is gone
    expect(noRcpt).toBe(0);
    b.close(); rmSync(home, { recursive: true, force: true });
  }, 30_000);

  test("R2: history snapshot reads MESSAGES, not events — legacy + gc'd rows stay visible", () => {
    const home = tmp();
    const bus = loc(home);
    const ctx = localCtx("don");
    for (let i = 0; i < 5; i++) bus.post(ctx, { from: "don", to: "alice", type: "note", body: `m${i}` });
    // legacy row WITHOUT any event (pre-events era):
    bus.testDb.run("INSERT INTO messages(id,thread,sender,recipients,type,status,body,created_at,channel) VALUES('m-legacy','m-legacy','don','alice','note','open','OLD','2024-01-01T00:00:00Z','general')");
    // age ALL msg events out of retention and gc ⇒ events table empties
    bus.testDb.run("UPDATE events SET at='2020-01-01T00:00:00Z' WHERE kind='msg'");
    bus.gc();
    const page = bus.history(ctx, { limit: 50 }) as any;
    const bodies = page.value.rows.map((r: any) => r.body);
    expect(bodies).toContain("OLD"); // legacy message visible
    expect(bodies.filter((b: string) => /^m\d$/.test(b))).toHaveLength(5); // gc'd-event messages visible
    // cursor = events high-water in the same txn ⇒ stream handoff has no hole.
    // round-4 (don-claude P1): the high-water is max(max(seq), gc_floor) — after
    // gc the surviving max(seq) can sit BELOW the floor (here: presence seq 1,
    // floor 7), and a sub-floor cursor resyncs at every since-entry point.
    const hw = Math.max((bus.testDb.query("SELECT coalesce(max(seq),0) m FROM events").get() as any).m, bus.gcFloor());
    expect(page.value.cursor).toBe(`${bus.epoch()}.${hw}`);
    // the handoff cursor must be ACCEPTED by every since-bearing entry point:
    expect((bus.history(ctx, { since: page.value.cursor }) as any).error).toBeUndefined();
    expect((bus.waitStep(ctx, { since: page.value.cursor }) as any).error).toBeUndefined();
    const [ep, sq] = page.value.cursor.split(".");
    expect(bus.cursorSet("don", "cli", ep, Number(sq)).error).toBeUndefined();
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("mapping tables (§7)", () => {
  test("RPC/exit codes match the RFC table exactly", () => {
    expect(RPC_CODES.forbidden).toBe(-32002);
    expect(RPC_CODES.unauthorized).toBe(-32001);
    expect(RPC_CODES.not_found).toBe(-32003);
    expect(RPC_CODES.rate_limited).toBe(-32004);
    expect(RPC_CODES.conflict).toBe(-32005);
    expect(RPC_CODES.unavailable).toBe(-32006);
    expect(RPC_CODES.usage).toBe(-32602);
    expect(RPC_CODES.internal).toBe(-32603);
    expect(EXIT_CODES.usage).toBe(2);
    expect(EXIT_CODES.identity_conflict).toBe(3);
    expect(EXIT_CODES.forbidden).toBe(3);
  });
});
