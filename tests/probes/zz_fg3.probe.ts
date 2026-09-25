// Probe vs RFC-001 App F+G v3 (@8b34091). Real src/bus.ts where executable, SQLite
// reference model of the v3 SQL/rules elsewhere (G is not implemented yet).
//   REPO=/media/bakon/data/Dev/bakons/agent-comms bun run zz_fg3.probe.ts
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
const REPO = process.env.REPO ?? "/media/bakon/data/Dev/bakons/agent-comms";
const { openBus, localCtx } = await import(join(REPO, "src/bus.ts"));
const { testSeams } = await import(join(REPO, "src/seams.ts"));
const ok = (b: boolean) => (b ? "PASS" : "FAIL");

// ---------------------------------------------------------------- B1 history canSee
{
  const d = new Database(":memory:");
  d.exec(`CREATE TABLE messages(id TEXT PRIMARY KEY, sender TEXT, created_at TEXT, channel TEXT NOT NULL);
  CREATE INDEX idx_msg_created ON messages(created_at);
  CREATE INDEX idx_msg_channel ON messages(channel);
  CREATE TABLE channel_members(channel TEXT, agent_id TEXT, PRIMARY KEY(channel, agent_id));
  CREATE INDEX cm_agent ON channel_members(agent_id, channel);
  INSERT INTO channel_members VALUES('dm~alice~bob','alice'),('dm~alice~bob','bob');`);
  const ins = d.prepare("INSERT INTO messages VALUES(?,?,?,?)");
  for (let i = 0; i < 20000; i++) {
    const ch = i % 10 === 0 ? "dm~alice~bob" : i % 997 === 0 ? "dm~naked~zero" : "general";
    ins.run("m" + i, "x", new Date(Date.UTC(2026, 0, 1) + i * 1000).toISOString().replace(/\.\d{3}Z$/, "Z"), ch);
  }
  d.exec("ANALYZE");
  const SNAP = `SELECT * FROM messages m WHERE m.channel NOT GLOB 'dm~*'
     OR EXISTS (SELECT 1 FROM channel_members cm WHERE cm.channel = m.channel AND cm.agent_id = ?)
     OR ? = 1 ORDER BY m.created_at DESC, rowid DESC LIMIT ?`;
  const q = d.query(SNAP);
  const dmRows = (who: string, dm: number) => (q.all(who, dm, 100000) as any[]).filter((r) => r.channel.startsWith("dm~"));
  const allOnly = dmRows("carol", 0), member = dmRows("alice", 0), omni = dmRows("carol", 1);
  console.log("B1 read:all-only human sees 0 dm rows:", ok(allOnly.length === 0), allOnly.length);
  console.log("B1 member alice sees own dm, not naked:", ok(member.length === 2000 && member.every((r) => r.channel === "dm~alice~bob")), member.length);
  const naked = Array.from({ length: 20000 }, (_, i) => i).filter((i) => i % 10 !== 0 && i % 997 === 0).length;
  const memberNaked = member.filter((r) => r.channel === "dm~naked~zero").length;
  console.log("B1 read:all+read:dm sees all incl zero-member:", ok(omni.length === 2000 + naked), omni.length, "(naked rows", naked + ")");
  console.log("B1 zero-member channel invisible to a member-less non-scope caller:", ok(memberNaked === 0 && dmRows("bob", 0).every((r) => r.channel === "dm~alice~bob")));
  console.log("B1 snapshot plan:", (d.query("EXPLAIN QUERY PLAN " + SNAP).all("carol", 0, 50) as any[]).map((r) => r.detail).join(" | "));
}

// ---------------------------------------------------------------- B2 Map parity + n5 +1s
{
  const d = new Database(":memory:");
  d.exec(`CREATE TABLE messages(id TEXT PRIMARY KEY, created_at TEXT);
  CREATE TABLE message_recipients(msg TEXT, target TEXT);
  CREATE INDEX msg_rec_idx ON message_recipients(target, msg);
  CREATE TABLE groups(name TEXT PRIMARY KEY, created_by TEXT, created_at TEXT NOT NULL);
  CREATE TABLE group_members(grp TEXT, agent_id TEXT, joined_at TEXT, PRIMARY KEY(grp, agent_id));
  CREATE INDEX gm_agent ON group_members(agent_id, grp);
  CREATE TABLE group_tombstones(name TEXT PRIMARY KEY, deleted_at TEXT);`);
  const iso = (s: number) => new Date(Date.UTC(2026, 8, 25, 5) + s * 1000).toISOString().replace(/\.\d{3}Z$/, "Z");
  let clock = 0, n = 0;
  const now = () => iso(clock);
  const post = (target: string) => { const id = "m" + n++; d.run("INSERT INTO messages VALUES(?,?)", [id, now()]); d.run("INSERT INTO message_recipients VALUES(?,?)", [id, target]); return id; };
  const create = (g: string) => {
    const tb = d.query("SELECT deleted_at FROM group_tombstones WHERE name=?").get(g) as any;
    const plus1 = tb ? iso((Date.parse(tb.deleted_at) - Date.UTC(2026, 8, 25, 5)) / 1000 + 1) : null;
    const at = plus1 && plus1 > now() ? plus1 : now();              // max(nowIso, deleted_at+1s)
    d.run("INSERT INTO groups VALUES(?,?,?)", [g, "x", at]);
  };
  const del = (g: string) => { d.run("DELETE FROM group_members WHERE grp=?", [g]); d.run("DELETE FROM groups WHERE name=?", [g]); d.run("INSERT OR REPLACE INTO group_tombstones VALUES(?,?)", [g, now()]); };
  const join_ = (g: string, a: string) => d.run("INSERT OR IGNORE INTO group_members VALUES(?,?,?)", [g, a, now()]);
  const ARM = `SELECT r.msg FROM group_members gm JOIN groups g ON g.name=gm.grp
    JOIN message_recipients r ON r.target=('group:'||gm.grp)
    JOIN messages m ON m.id=r.msg AND m.created_at >= g.created_at WHERE gm.agent_id=?`;
  const sqlSet = (a: string) => new Set((d.query(ARM).all(a) as any[]).map((r) => r.msg));
  const jsSet = (a: string) => {
    const mem = new Map<string, string>((d.query("SELECT g.name, g.created_at FROM group_members gm JOIN groups g ON g.name=gm.grp WHERE gm.agent_id=?").all(a) as any[]).map((r) => [r.name, r.created_at]));
    const out = new Set<string>();
    for (const r of d.query("SELECT r.msg, r.target, m.created_at FROM message_recipients r JOIN messages m ON m.id=r.msg").all() as any[]) {
      if (!r.target.startsWith("group:")) continue;
      const g = r.target.slice(6);
      if (mem.has(g) && r.created_at >= mem.get(g)!) out.add(r.msg);
    }
    return out;
  };
  // fuzz: random create/delete/join/post/tick, compare JS Map arm vs SQL arm
  let seed = 7; const rnd = (k: number) => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) % k);
  let mism = 0;
  for (let step = 0; step < 4000; step++) {
    const g = "g" + rnd(3), a = "a" + rnd(3), op = rnd(10);
    const exists = !!d.query("SELECT 1 FROM groups WHERE name=?").get(g);
    if (op === 0 && !exists) create(g); else if (op === 1 && exists) del(g);
    else if (op < 4 && exists) join_(g, a); else if (op < 8) post("group:" + g); else clock += rnd(2);
    for (const x of ["a0", "a1", "a2"]) { const s = sqlSet(x), j = jsSet(x); if (s.size !== j.size || [...s].some((m) => !j.has(m))) mism++; }
  }
  console.log("B2 JS Map arm == SQL arm over 4000 random ops x 3 agents:", ok(mism === 0), "mismatches", mism);

  // n5 edge: same-second delete+recreate, then the NEW incarnation posts in that second
  clock = 10000; d.exec("DELETE FROM groups; DELETE FROM group_members; DELETE FROM group_tombstones;");
  create("inc"); join_("inc", "old"); const oldMsg = post("group:inc");
  del("inc"); create("inc"); join_("inc", "new"); const newMsg = post("group:inc");  // all within second 10000
  const got = sqlSet("new");
  console.log("n5 old-incarnation same-second row NOT inherited:", ok(!got.has(oldMsg)));
  console.log("n5 NEW incarnation's own same-second post delivered:", ok(got.has(newMsg)),
    "(created_at", (d.query("SELECT created_at FROM groups WHERE name='inc'").get() as any).created_at, "msg", now() + ")");
}

// ---------------------------------------------------------------- T2 local-mode minting path (real code)
{
  const home = mkdtempSync(join(tmpdir(), "zz-fg3-"));
  const bus = openBus({ home, mode: "local", seams: testSeams({}) });
  bus.joinAgent(localCtx("alice"), { agent: "alice", role: "a" });
  bus.joinAgent(localCtx("mallory"), { agent: "mallory", role: "zed" });   // role first (no agent zed yet)
  const z = bus.joinAgent(localCtx("zed"), { agent: "zed", role: "z" });    // LOCAL join mints id zed
  console.log("T2-local join mints id 'zed' while mallory holds role=zed:", z.error ?? "accepted");
  const p = bus.post(localCtx("alice"), { from: "alice", to: "zed", type: "note", body: "for zed only" });
  const mb = bus.inbox(localCtx("mallory"), { agent: "mallory" });
  console.log("T2-local mallory inbox has alice->zed:", mb.value.rows.some((r: any) => r.id === p.value.id));
  // retired-id re-mint via local join (agent_retired is only checked by token.create/rename in v3 text)
  bus.joinAgent(localCtx("bob"), { agent: "bob", role: "b" });
  const q = bus.post(localCtx("alice"), { from: "alice", to: "bob", type: "note", body: "for old bob" });
  bus.rename(localCtx("bob"), { agent: "bob", to: "carol" });
  const rb = bus.joinAgent(localCtx("bob"), { agent: "bob", role: "b2" });  // re-mint the renamed-away id
  const ib = bus.inbox(localCtx("bob"), { agent: "bob" });
  console.log("G6-local re-minted 'bob' via local join:", rb.error ?? "accepted", "| inherits old bob mail:", ib.value.rows.some((r: any) => r.id === q.value.id));
  bus.close?.(); rmSync(home, { recursive: true, force: true });
}

// ---------------------------------------------------------------- T2 TOCTOU across two connections
{
  const dir = mkdtempSync(join(tmpdir(), "zz-fg3t-")), f = join(dir, "b.db");
  const c1 = new Database(f), c2 = new Database(f);
  c1.exec("PRAGMA journal_mode=WAL; CREATE TABLE agents(id TEXT PRIMARY KEY, role TEXT);INSERT INTO agents VALUES('mallory','m');");
  // naive check-then-write (separate autocommit statements), interleaved
  const joinChk = !c1.query("SELECT 1 FROM agents WHERE id='zed'").get();        // join: no agent zed
  const mintChk = !c2.query("SELECT 1 FROM agents WHERE role='zed'").get();      // token.create: no role zed
  if (joinChk) c1.run("UPDATE agents SET role='zed' WHERE id='mallory'");
  if (mintChk) c2.run("INSERT INTO agents VALUES('zed','z')");
  const bothNaive = (c1.query("SELECT count(*) c FROM agents WHERE id='zed' OR role='zed'").get() as any).c;
  console.log("T2-TOCTOU naive check-then-write: both succeed (collision rows=2):", bothNaive === 2 ? "REPRODUCES" : "no");
  // guarded single-statement forms
  c1.exec("DELETE FROM agents WHERE id='zed'; UPDATE agents SET role='m' WHERE id='mallory';");
  const r1 = c1.run("UPDATE agents SET role='zed' WHERE id='mallory' AND NOT EXISTS(SELECT 1 FROM agents WHERE id='zed')");
  const r2 = c2.run("INSERT INTO agents(id,role) SELECT 'zed','z' WHERE NOT EXISTS(SELECT 1 FROM agents WHERE role='zed')");
  console.log("T2-TOCTOU guarded single statements: join changes", r1.changes, "mint changes", r2.changes, ok(r1.changes + r2.changes === 1));
  c1.close(); c2.close(); rmSync(dir, { recursive: true, force: true });
}

// ---------------------------------------------------------------- G6 v3 rename txn model
{
  const d = new Database(":memory:");
  d.exec(`CREATE TABLE message_recipients(msg TEXT NOT NULL, target TEXT NOT NULL);
  CREATE UNIQUE INDEX mr_uq ON message_recipients(msg,target);
  INSERT INTO message_recipients VALUES('m1','bob'),('m1','carol');
  CREATE TABLE channel_members(channel TEXT, agent_id TEXT, PRIMARY KEY(channel, agent_id));
  INSERT INTO channel_members VALUES('dm~alice~bob','alice'),('dm~alice~bob','bob');
  CREATE TABLE agent_retired(id TEXT PRIMARY KEY, renamed_to TEXT NOT NULL, at TEXT NOT NULL);
  CREATE INDEX agent_retired_new ON agent_retired(renamed_to, id);`);
  const ren = (o: string, nw: string) => {
    if (d.query("SELECT 1 FROM agent_retired WHERE id=?").get(nw)) return "identity_conflict";
    d.exec("BEGIN IMMEDIATE");
    d.run("UPDATE OR IGNORE channel_members SET agent_id=? WHERE agent_id=?", [nw, o]);
    d.run("DELETE FROM channel_members WHERE agent_id=?", [o]);
    d.run("UPDATE agent_retired SET renamed_to=? WHERE renamed_to=?", [nw, o]);
    d.run("INSERT INTO agent_retired VALUES(?,?,?)", [o, nw, "t"]);
    d.exec("COMMIT"); return "ok";
  };
  let r: string; try { r = ren("bob", "dave"); } catch (e: any) { r = "THROW " + e.message; }
  console.log("G6 v3 rename with m1 carrying bob+carol (no recipient rewrite):", r, ok(r === "ok"));
  ren("dave", "erin");
  console.log("G6 chain:", JSON.stringify(d.query("SELECT id, renamed_to FROM agent_retired ORDER BY id").all()));
  console.log("G6 members moved:", JSON.stringify(d.query("SELECT agent_id FROM channel_members ORDER BY agent_id").all().map((x: any) => x.agent_id)));
  console.log("G6 rename back erin->bob:", ren("erin", "bob"), "(undo impossible by design — say so)");
}
