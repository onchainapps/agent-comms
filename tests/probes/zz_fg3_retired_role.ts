// Retired-id-as-role: after rename bob->carol, v3 blocks re-minting id 'bob' (agent_retired,
// token.create/rename) and blocks role == <existing agent id>. 'bob' is no longer an agent id,
// so a role=bob join passes the §5 check as written. Real code @8b34091 (server mode).
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
const REPO = process.env.REPO ?? "/media/bakon/data/Dev/bakons/agent-comms";
const { openBus, serverCtx } = await import(join(REPO, "src/bus.ts"));
const { testSeams } = await import(join(REPO, "src/seams.ts"));
const home = mkdtempSync(join(tmpdir(), "zz-ret-"));
const bus = openBus({ home, mode: "server", seams: testSeams({}) });
for (const [id, role] of [["alice", "a"], ["bob", "b"], ["mallory", "m"]])
  bus.testDb.run("INSERT INTO agents(id,role,last_seen) VALUES(?,?,'2026-01-01T00:00:00Z')", [id, role]);
const p = bus.post(serverCtx("alice", []), { from: "alice", to: "bob", type: "note", body: "old bob backlog" });
if (p.error) { console.log("post error:", p.error, p.detail); process.exit(1); }
const rn = bus.rename(serverCtx("bob"), { agent: "bob", to: "carol" });
console.log("rename bob->carol:", rn.error ?? "ok");
const isAgent = !!bus.testDb.query("SELECT 1 FROM agents WHERE id='bob'").get();
console.log("'bob' is still an agents.id (would the v3 §5 role==id check fire?):", isAgent);
const j = bus.joinAgent(serverCtx("mallory"), { agent: "mallory", role: "bob" });
console.log("mallory join role=bob:", j.error ?? "accepted");
const ib = bus.inbox(serverCtx("mallory"), { agent: "mallory" });
console.log("mallory inbox has old alice->bob:", ib.value.rows.some((r: any) => r.id === p.value.id));
const st = bus.setStatus(serverCtx("mallory"), { agent: "mallory", id: p.value.id, state: "done" });
console.log("mallory setStatus done on it:", st.error ?? "ok");
rmSync(home, { recursive: true, force: true });
