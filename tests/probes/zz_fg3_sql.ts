import { Database } from "bun:sqlite";
const d = new Database(":memory:");
d.exec(`CREATE TABLE channels(name TEXT PRIMARY KEY, created_at TEXT);
CREATE TABLE channel_members(channel TEXT, agent_id TEXT, PRIMARY KEY(channel, agent_id));
CREATE INDEX cm_agent ON channel_members(agent_id, channel);
CREATE TABLE message_recipients(msg TEXT NOT NULL, target TEXT NOT NULL);
CREATE INDEX msg_rec_idx ON message_recipients(target, msg);
INSERT INTO message_recipients VALUES('m1','me'),('m2','lab'),('m3','@all');
INSERT INTO channel_members VALUES('general','alice'),('general','bob'),('general','carol'),
 ('dm~alice~bob','alice'),('dm~alice~bob','bob'),('dm~alice~bob~2','alice'),('dm~alice~bob~2','bob'),
 ('dm~alice~carol','alice'),('dm~alice~carol','carol'),('dm~alice~bob~3','alice'),('dm~alice~bob~3','bob'),('dm~alice~bob~3','carol');
INSERT INTO channels VALUES('dm~alice~bob','2026-01-01T00:00:00Z'),('dm~alice~bob~2','2026-03-01T00:00:00Z'),('dm~alice~bob~3','2026-04-01T00:00:00Z');`);
const N1 = `SELECT cm.channel FROM channel_members cm INDEXED BY cm_agent
WHERE cm.agent_id IN (?, ?) AND cm.channel GLOB 'dm~*'
GROUP BY cm.channel
HAVING count(*) = 2
   AND (SELECT count(*) FROM channel_members x WHERE x.channel = cm.channel) = 2`;
console.log("n1 matches:", JSON.stringify(d.query(N1).all("alice", "bob")));
console.log("n1 plan:", (d.query("EXPLAIN QUERY PLAN " + N1).all("alice", "bob") as any[]).map((r) => r.detail).join(" | "));
const pick = `SELECT c.name FROM channels c WHERE c.name IN (${N1}) ORDER BY c.created_at DESC LIMIT 1`;
console.log("n1 tie-break newest:", JSON.stringify(d.query(pick).all("alice", "bob")));
// bind claims
const named = "SELECT msg FROM message_recipients WHERE target = :agent UNION ALL SELECT msg FROM message_recipients WHERE target='@all'";
for (const [k, v] of [["{agent}", { agent: "me" }], ["{$agent}", { $agent: "me" }], ["{':agent'}", { ":agent": "me" }]] as const) {
  let out: any; try { out = (d.query(named).all(v as any) as any[]).map((r) => r.msg); } catch (e: any) { out = "THROW " + e.message; }
  console.log(`bind :agent from ${k}:`, JSON.stringify(out));
}
const POS = `SELECT msg FROM message_recipients WHERE target = ?
UNION ALL SELECT msg FROM message_recipients WHERE target = ? AND ? IS NOT NULL
UNION ALL SELECT msg FROM message_recipients WHERE target = '@all'`;
console.log("positional role=lab:", JSON.stringify((d.query(POS).all("me", "lab", "lab") as any[]).map((r) => r.msg)));
console.log("positional role=NULL:", JSON.stringify((d.query(POS).all("me", null, null) as any[]).map((r) => r.msg)));
console.log("positional plan:", (d.query("EXPLAIN QUERY PLAN " + POS).all("me", null, null) as any[]).map((r) => r.detail).join(" | "));
