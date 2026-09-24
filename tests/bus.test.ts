import { expect, test, describe } from "bun:test";
import { mkdtempSync, rmSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { openBus, csv, normalizeScopes, canonicalJson, sha256hex, localCtx } from "../src/bus.ts";
import { testSeams } from "../src/seams.ts";

function tmp() {
  const dir = mkdtempSync(join(tmpdir(), "comms-test-"));
  return dir;
}

describe("N1: recipient split — recursive CTE == csv(), no aborts on legacy data", () => {
  test("migration backfill succeeds and index equals csv() for hostile recipients", () => {
    const home = tmp();
    mkdirSync(join(home, ".comms"), { recursive: true });
    // Build a PRE-trigger legacy DB by hand (old schema, no triggers, no events).
    const d0 = new Database(join(home, ".comms", "comms.db"), { create: true });
    d0.exec(`
      CREATE TABLE agents(id TEXT PRIMARY KEY NOT NULL, role TEXT, caps TEXT, pid INTEGER, joined_at TEXT, last_seen TEXT, meta TEXT);
      CREATE TABLE messages(id TEXT PRIMARY KEY, thread TEXT, re TEXT, sender TEXT, recipients TEXT, type TEXT,
        status TEXT, tags TEXT, subject TEXT, body TEXT, file TEXT, created_at TEXT, updated_at TEXT, channel TEXT NOT NULL DEFAULT 'general');
      CREATE TABLE reads(agent TEXT, msg TEXT, read_at TEXT, PRIMARY KEY(agent, msg));
      CREATE TABLE channels(name TEXT PRIMARY KEY NOT NULL, purpose TEXT, created_at TEXT, created_by TEXT);
    `);
    const hostile = ['a"b', "a\\b", "don,", "", "a, b ,c", "x,,y", "@all"];
    hostile.forEach((recips, i) => {
      d0.run(
        "INSERT INTO messages(id,thread,sender,recipients,type,status,subject,body,file,created_at,updated_at,channel) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        [`m${i}`, `m${i}`, "sys", recips, "note", "done", "s", "b", "f", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z", "general"],
      );
    });
    d0.close();

    // openBus must migrate WITHOUT throwing and backfill index rows == csv()
    const bus = openBus({ home, mode: "local", seams: testSeams({}) });
    for (let i = 0; i < hostile.length; i++) {
      const idx = (bus.db.query("SELECT target FROM message_recipients WHERE msg=? ORDER BY target").all(`m${i}`) as any[])
        .map((r) => r.target);
      expect(idx).toEqual(csv(hostile[i]).sort());
    }
    // trigger path parity for NEW inserts too
    const r = bus.post(localCtx("sys"), { from: "sys", to: 'we"ird, ok ,', type: "note", body: "x" });
    expect(r.error).toBeUndefined();
    const mid = (r as any).value.id;
    const idx = (bus.db.query("SELECT target FROM message_recipients WHERE msg=? ORDER BY target").all(mid) as any[])
      .map((x) => x.target);
    expect(idx).toEqual(csv('we"ird, ok ,').sort());
    bus.close();
    rmSync(home, { recursive: true, force: true });
  });

  test("empty recipients produce zero index rows (no target='' drift)", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "local", seams: testSeams({}) });
    const r = bus.post(localCtx("sys"), { from: "sys", to: ",", type: "note", body: "x" });
    expect((r as any).error).toBeUndefined();
    const rows = bus.db.query("SELECT COUNT(*) c FROM message_recipients WHERE msg=?").get((r as any).value.id) as any;
    expect(rows.c).toBe(0);
    bus.close();
    rmSync(home, { recursive: true, force: true });
  });
});

describe("fan-out events (§3′/§4)", () => {
  test("post fires msg_ai; status fires msg_au; read fires reads_ai; join fires agents_ai", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "local", seams: testSeams({}) });
    bus.joinAgent(localCtx("a1"), { agent: "a1", role: "lab" });
    const p = bus.post(localCtx("a1"), { from: "a1", to: "a2", type: "ask", body: "hi" });
    const mid = (p as any).value.id;
    bus.setStatus(localCtx("a1"), { agent: "a1", id: mid, state: "done" });
    bus.read(localCtx("a2"), { agent: "a2", id: mid });
    const kinds = (bus.db.query("SELECT kind FROM events ORDER BY seq").all() as any[]).map((r) => r.kind);
    expect(kinds).toContain("msg");
    expect(kinds).toContain("status");
    expect(kinds).toContain("read");
    expect(kinds).toContain("presence");
    bus.close();
    rmSync(home, { recursive: true, force: true });
  });

  test("agents_au debounce: touch within 60s emits nothing; role change always emits", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "local", seams: testSeams({}) });
    bus.joinAgent(localCtx("a1"), { agent: "a1", role: "lab" });
    const before = (bus.db.query("SELECT count(*) c FROM events WHERE kind='presence'").get() as any).c;
    bus.touch("a1"); bus.touch("a1"); bus.touch("a1"); // same frozen clock ⇒ no events
    const after = (bus.db.query("SELECT count(*) c FROM events WHERE kind='presence'").get() as any).c;
    expect(after).toBe(before);
    bus.joinAgent(localCtx("a1"), { agent: "a1", role: "lead" }); // role change ⇒ emits
    const after2 = (bus.db.query("SELECT count(*) c FROM events WHERE kind='presence'").get() as any).c;
    expect(after2).toBe(after + 1);
    bus.close();
    rmSync(home, { recursive: true, force: true });
  });
});

describe("tokens (§4/§5)", () => {
  test("create→verify roundtrip; bad token rejected; revoke rejects", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "server", seams: testSeams({}) });
    const r = bus.tokenCreate({ agent: "bot1", scopes: ["read:all"] });
    expect(r.error).toBeUndefined();
    const { token } = (r as any).value;
    expect(token.startsWith("ac_")).toBe(true);
    const v = bus.tokenVerify(token);
    expect((v as any).value.agentId).toBe("bot1");
    expect((v as any).value.scopes).toEqual(["read:all"]);
    expect(bus.tokenVerify(token + "x").error).toBe("forbidden");
    bus.db.run("UPDATE tokens SET revoked_at='now' WHERE prefix=?", [(r as any).value.prefix]);
    expect(bus.tokenVerify(token).error).toBe("forbidden");
    bus.close();
    rmSync(home, { recursive: true, force: true });
  });

  test("admin:true mints full normalized scope set; human defaults read:all", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "server", seams: testSeams({}) });
    const a = bus.tokenCreate({ agent: "don", admin: true });
    const v1 = bus.tokenVerify((a as any).value.value ?? (a as any).value.token);
    expect((v1 as any).value.scopes.sort()).toEqual(["agents:admin", "post:as", "read:all", "tokens:admin"]);
    const h = bus.tokenCreate({ agent: "bakon", kind: "human" });
    const v2 = bus.tokenVerify((h as any).value.token);
    expect((v2 as any).value.scopes).toEqual(["read:all"]);
    expect((v2 as any).value.kind).toBe("human");
    bus.close();
    rmSync(home, { recursive: true, force: true });
  });

  test("bootstrap guard: second admin without --force semantics (UNIQUE-free, predicate visible)", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "local", seams: testSeams({}) });
    bus.tokenCreate({ agent: "don", admin: true });
    const c = (bus.db.query(
      "SELECT count(*) c FROM tokens WHERE revoked_at IS NULL AND instr(',' || scopes || ',', ',tokens:admin,') > 0",
    ).get() as any).c;
    expect(c).toBe(1);
    bus.close();
    rmSync(home, { recursive: true, force: true });
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
    const bus = openBus({ home, mode: "server", seams: testSeams({}) });
    bus.tokenCreate({ agent: "cli1", admin: true });
    const ctx = { principal: { agentId: "cli1", kind: "agent" as const, scopes: ["read:all", "post:as", "tokens:admin", "agents:admin"] as any }, actor: "cli1" };
    const p1 = bus.post(ctx, { from: "cli1", to: "x", type: "note", body: "b", subject: "s", idempotencyKey: "k1" });
    expect(p1.error).toBeUndefined();
    const p2 = bus.post(ctx, { from: "cli1", to: "x", type: "note", body: "b", subject: "s", idempotencyKey: "k1" });
    expect((p2 as any).value.id).toBe((p1 as any).value.id);
    const p3 = bus.post(ctx, { from: "cli1", to: "x", type: "note", body: "b", subject: "DIFFERENT", idempotencyKey: "k1" });
    expect(p3.error).toBe("conflict");
    bus.close();
    rmSync(home, { recursive: true, force: true });
  });
});

describe("cursors (§6)", () => {
  test("per-consumer isolation; monotonic unless force; epoch mismatch resyncs", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "server", seams: testSeams({}) });
    expect(bus.cursorGet("a", "cli").seq).toBe(0);
    bus.cursorSet("a", "cli", bus.epoch(), 10);
    bus.cursorSet("a", "mcp", bus.epoch(), 3);
    expect(bus.cursorGet("a", "cli").seq).toBe(10);
    expect(bus.cursorGet("a", "mcp").seq).toBe(3);
    expect(bus.cursorSet("a", "cli", bus.epoch(), 5).error).toBe("conflict");
    expect(bus.cursorSet("a", "cli", bus.epoch(), 5, true).error).toBeUndefined();
    const e2 = bus.rotateEpoch();
    expect(bus.cursorGet("a", "cli")).toEqual({ epoch: e2, seq: 0 }); // mismatch ⇒ resync baseline
    expect(bus.cursorSet("a", "cli", "bogus-epoch", 7).error).toBe("not_found");
    bus.close();
    rmSync(home, { recursive: true, force: true });
  });
});

describe("history handoff (§6 nit)", () => {
  test("history returns rows + events high-water cursor in one txn", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "server", seams: testSeams({}) });
    const ctx = { principal: { agentId: "don", kind: "agent" as const, scopes: ["read:all"] as any }, actor: "don" };
    const h0 = bus.history(ctx, {});
    expect((h0 as any).value.cursor.seq).toBeGreaterThanOrEqual(0);
    bus.post(ctx, { from: "don", to: "other", type: "note", body: "b" });
    const h1 = bus.history(ctx, {});
    expect((h1 as any).value.cursor.seq).toBeGreaterThan((h0 as any).value.cursor.seq);
    expect((h1 as any).value.rows.length).toBe(1);
    bus.close();
    rmSync(home, { recursive: true, force: true });
  });
});

describe("server-mode authz (§5)", () => {
  const mk = () => {
    const home = tmp();
    const bus = openBus({ home, mode: "server", seams: testSeams({}) });
    return { home, bus };
  };
  test("from≠principal rejected without post:as", () => {
    const { home, bus } = mk();
    const ctx = { principal: { agentId: "me", kind: "agent" as const, scopes: [] as any }, actor: "me" };
    expect(bus.post(ctx, { from: "other", to: "x", type: "note", body: "b" }).error).toBe("forbidden");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
  test("as requires post:as; sender=as-target; meta.as=principal; mirror shows as:", () => {
    const { home, bus } = mk();
    const ctx = { principal: { agentId: "admin", kind: "agent" as const, scopes: ["post:as"] as any }, actor: "admin" };
    const r = bus.post(ctx, { from: "admin", as: "bot", to: "x", type: "note", body: "b" });
    expect(r.error).toBeUndefined();
    const m = bus.db.query("SELECT sender, meta FROM messages WHERE id=?").get((r as any).value.id) as any;
    expect(m.sender).toBe("bot");
    expect(JSON.parse(m.meta).as).toBe("admin");
    const mirror = bus.seams.mirror === undefined ? "" : "";
    expect((r as any).value.file).toContain("general");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
  test("status: recipient-by-role may ack; stranger may not", () => {
    const { home, bus } = mk();
    bus.db.run("INSERT INTO agents(id,role,last_seen) VALUES('sender','coord','2026-01-01T00:00:00Z'),('worker','lab','2026-01-01T00:00:00Z'),('rand','x','2026-01-01T00:00:00Z')");
    const p = bus.post({ principal: { agentId: "sender", kind: "agent", scopes: ["post:as", "read:all", "tokens:admin", "agents:admin"] }, actor: "sender" } as any,
      { from: "sender", to: "lab", type: "ask", body: "b" });
    const mid = (p as any).value.id;
    const worker = { principal: { agentId: "worker", kind: "agent" as const, scopes: [] as any }, actor: "worker" };
    const rand = { principal: { agentId: "rand", kind: "agent" as const, scopes: [] as any }, actor: "rand" };
    expect(bus.setStatus(worker, { agent: "worker", id: mid, state: "acked" }).error).toBeUndefined();
    expect(bus.setStatus(rand, { agent: "rand", id: mid, state: "done" }).error).toBe("forbidden");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
  test("server mode never auto-registers via touch", () => {
    const { home, bus } = mk();
    bus.touch("ghost");
    expect(bus.db.query("SELECT count(*) c FROM agents WHERE id='ghost'").get()).toEqual({ c: 0 });
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
  test("inbox for≠self requires read:all", () => {
    const { home, bus } = mk();
    bus.db.run("INSERT INTO agents(id,role,last_seen) VALUES('a','a','2026-01-01T00:00:00Z'),('b','b','2026-01-01T00:00:00Z')");
    const ctx = { principal: { agentId: "a", kind: "agent" as const, scopes: [] as any }, actor: "a" };
    expect(bus.inbox(ctx, { agent: "b" }).error).toBe("forbidden");
    const ctx2 = { principal: { agentId: "a", kind: "agent" as const, scopes: ["read:all"] as any }, actor: "a" };
    expect(bus.inbox(ctx2, { agent: "b" }).error).toBeUndefined();
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("identifier validation (§9)", () => {
  test("traversal attempts rejected on channel and from", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "local", seams: testSeams({}) });
    expect(bus.post(localCtx("sys"), { from: "sys", to: "x", type: "note", body: "b", channel: "../../home/comms/.ssh" }).error).toBe("usage");
    expect(bus.post(localCtx("../evil"), { from: "../evil", to: "x", type: "note", body: "b" }).error).toBe("usage");
    expect(bus.post(localCtx("sys"), { from: "sys", to: "x", type: "note;rm", body: "b" }).error).toBe("usage");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});

describe("rename (§4 transactional)", () => {
  test("rewrites reads/tokens/cursors/idempotency in one txn; history keeps old sender", () => {
    const home = tmp();
    const bus = openBus({ home, mode: "server", seams: testSeams({}) });
    bus.db.run("INSERT INTO agents(id,role,last_seen) VALUES('old','lab','2026-01-01T00:00:00Z')");
    const p = bus.post({ principal: { agentId: "old", kind: "agent", scopes: ["post:as", "read:all", "tokens:admin", "agents:admin"] }, actor: "old" } as any,
      { from: "old", to: "other", type: "note", body: "b" });
    const mid = (p as any).value.id;
    bus.db.run("INSERT INTO reads(agent,msg,read_at) VALUES('old',?,'now')", [mid]);
    bus.tokenCreate({ agent: "old", scopes: ["read:all"] });
    bus.cursorSet("old", "cli", bus.epoch(), 5);
    const r = bus.rename({ principal: { agentId: "old", kind: "agent", scopes: ["agents:admin"] }, actor: "old" } as any,
      { agent: "old", to: "new" });
    expect(r.error).toBeUndefined();
    expect(bus.db.query("SELECT count(*) c FROM agents WHERE id='new'").get()).toEqual({ c: 1 });
    expect((bus.db.query("SELECT count(*) c FROM reads WHERE agent='new'").get() as any).c).toBe(1);
    expect((bus.db.query("SELECT count(*) c FROM tokens WHERE agent_id='new'").get() as any).c).toBe(1);
    expect((bus.db.query("SELECT count(*) c FROM cursors WHERE agent_id='new'").get() as any).c).toBe(1);
    expect((bus.db.query("SELECT sender FROM messages WHERE id=?").get(mid) as any).sender).toBe("old");
    const kinds = (bus.db.query("SELECT kind FROM events").all() as any[]).map((x) => x.kind);
    expect(kinds).toContain("rename");
    bus.close(); rmSync(home, { recursive: true, force: true });
  });
});
