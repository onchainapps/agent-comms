#!/usr/bin/env bun
/**
 * dashboard — live web view of the agent comms bus.
 *
 * Read-only. Two transports (§8 M4 transport switch):
 *   direct-DB (default):  opens comms.db readonly and streams state over SSE.
 *                         Never writes. Pre-G binary ⇒ dm rows stripped unless
 *                         --omniview.
 *   server (--url):       logs in once via the login RPC (token from
 *                         COMMS_TOKEN/--token), then builds state from
 *                         /rpc only — who/channels/history/receipts/groups.
 *                         Receipts come from the CORE (receipts RPC), not the
 *                         dashboard's duplicate receiptsOf (direct-DB mode
 *                         keeps the JS duplicate: it has no core to call).
 *   bun agent-comms/dashboard.ts [--port 8787] [--url http://host:8700 --token ***]
 * Then open http://localhost:8787
 *
 * Env: COMMS_HOME (default: script dir), PORT.
 */
import { Database } from "bun:sqlite";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { existsSync } from "node:fs";

function findRoot(start: string): string {
  let d = start;
  for (;;) {
    if (existsSync(join(d, "package.json")) || existsSync(join(d, ".comms"))) return d;
    const p = dirname(d);
    if (p === d) return start;
    d = p;
  }
}
const HOME = process.env.COMMS_HOME ?? findRoot(dirname(fileURLToPath(import.meta.url)));
const DB_PATH = join(HOME, ".comms", "comms.db");
const argPort = process.argv.includes("--port") ? process.argv[process.argv.indexOf("--port") + 1] : undefined;
const PORT = Number(argPort ?? process.env.PORT ?? 8787);
// §8 M4 transport switch: --url ⇒ server mode (RPC-only, cookie-free bearer;
// the hosted DB stays single-writer — this process must NOT open it).
const argUrl = process.argv.includes("--url") ? process.argv[process.argv.indexOf("--url") + 1] : undefined;
const SERVER_URL = (argUrl ?? process.env.COMMS_URL ?? "").replace(/\/+$/, "");
const TOKEN = (process.argv.includes("--token") ? process.argv[process.argv.indexOf("--token") + 1] : undefined) ?? process.env.COMMS_TOKEN ?? "";
// N4 (claude F/G review): Bun.serve without hostname binds the v6 wildcard
// (`*:port` — ss-verified), re-serving every message body to the LAN from an
// unauthenticated direct-DB process. Default to loopback; --host opts out.
const argHost = process.argv.includes("--host") ? process.argv[process.argv.indexOf("--host") + 1] : undefined;
const HOST = argHost ?? "127.0.0.1";
// G5: this is a pre-G binary (direct DB, unauthenticated). When the DB holds any
// dm-shaped channel, dm rows are STRIPPED unless --omniview — "local = host user
// is root of trust" does not extend to LAN visitors of a DB re-publisher.
const OMNIVIEW = process.argv.includes("--omniview");

// §8 M4: server mode NEVER opens the DB (§9 single-writer rule — a second
// reader on the hosted file is tolerated by WAL but the whole point of --url
// is that this process holds no handle at all).
let db: import("bun:sqlite").Database | null = null;
if (!SERVER_URL) {
  db = new Database(DB_PATH, { readonly: true });
  db.exec("PRAGMA busy_timeout = 3000");
  // M4 (claude NEW): NO startup latch — the filter is ALWAYS on unless
  // --omniview. A startup-time probe let a dashboard started before the first DM
  // serve DM bodies forever (probe: "dm body served after startup: true"). When
  // no DMs exist the filter is a no-op, so there is nothing to latch.
  if (!OMNIVIEW) console.error("dashboard: dm rows hidden (direct-DB pre-G binary); pass --omniview to show, or use the M4 web UI");
} else if (!OMNIVIEW) {
  console.error("dashboard: server mode — dm rows hidden (this port is unauthenticated); pass --omniview to show, or use the web UI at " + SERVER_URL);
}
const DB = () => {
  if (!db) throw new Error("dashboard: direct-DB path used in server mode");
  return db;
};

function stateDb() {
  const d = DB();
  const agents = d.query("SELECT id, role, caps, last_seen, joined_at FROM agents WHERE id IS NOT NULL ORDER BY last_seen DESC").all() as any[];
  const messages = d.query(
    "SELECT id, thread, re, sender, recipients, type, status, tags, subject, body, created_at, updated_at, channel FROM messages ORDER BY created_at ASC"
  ).all() as any[];
  const reads = d.query("SELECT agent, msg, read_at FROM reads").all() as any[];
  const channels = d.query("SELECT channel name, COUNT(*) n FROM messages GROUP BY channel ORDER BY n DESC").all() as any[];
  // F: receipts call site must resolve group: targets — same incarnation guard
  // as the core (grp -> {created_at, members}).
  const groups = d.query("SELECT name, created_at FROM groups").all();
  const gm = d.query("SELECT grp, agent_id FROM group_members").all();
  const gmap: Record<string, { at: string; members: string[] }> = {};
  for (const g of groups as any[]) gmap[g.name] = { at: g.created_at, members: [] };
  for (const r of gm as any[]) gmap[r.grp]?.members.push(r.agent_id);
  let M = messages, R = reads, C = channels;
  if (!OMNIVIEW) {
    // M4: strip dm-shaped rows from EVERY list the payload carries — messages
    // (bodies), channels (the NAME dm~a~b is the participant pair), and reads
    // (who read what on a hidden channel).
    const isDm = (ch: unknown) => String(ch ?? "").startsWith("dm~");
    M = messages.filter((m) => !isDm(m.channel));
    const dmIds = new Set(messages.filter((m) => isDm(m.channel)).map((m) => m.id));
    R = reads.filter((r) => !dmIds.has(r.msg));
    C = channels.filter((c) => !isDm(c.name));
  }
  return { now: new Date().toISOString(), agents, messages: M, reads: R, channels: C, groups: gmap };
}

// ---------- §8 M4 server transport ----------
let rpcid = 0;
async function rpc(method: string, params: Record<string, unknown> = {}): Promise<any> {
  const res = await fetch(`${SERVER_URL}/rpc`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}` },
    body: JSON.stringify({ jsonrpc: "2.0", method, params, id: ++rpcid }),
  });
  const body: any = await res.json().catch(() => null);
  if (!res.ok || !body || body.error) {
    const e = body?.error;
    throw new Error(`${method}: ${(e?.data && e.data.detail) || e?.message || "HTTP " + res.status}`);
  }
  return body.result;
}
// claude M4 M-a (dashboard half): the snapshot has no backward paging — the
// old loop fed the HIGH-WATER cursor back as since= and got 0 rows (probe:
// 1200 seeded → 500 shown). One snapshot at the core cap (1000) is honest.
// claude M4 B4: server mode re-serves the TOKEN's view to anyone who can reach
// this port (unauthenticated, loopback by default). With a read:dm token (the
// human default) that was every DM body — the exact leak the direct-DB path
// closes with --omniview. Same gate here: dm rows stripped unless --omniview.
async function stateRpc(): Promise<any> {
  const [agents, chans] = await Promise.all([rpc("who", { all: true }), rpc("channels", {})]);
  const page = await rpc("history", { limit: 1000 });
  let messages: any[] = page.rows.slice().sort((a: any, b: any) => (a.created_at < b.created_at ? -1 : 1));
  let channels = chans.map((c: any) => ({ name: c.name, n: c.n }));
  if (!OMNIVIEW) {
    const isDm = (ch: unknown) => String(ch ?? "").startsWith("dm~");
    messages = messages.filter((m) => !isDm(m.channel));
    channels = channels.filter((c: any) => !isDm(c.name));
  }
  // groups: list + show per group (members needed by the JS receiptsOf below).
  const gmap: Record<string, { at: string; members: string[] }> = {};
  try {
    const gl = await rpc("group.list", {});
    for (const g of gl.groups) {
      let members: string[] = [];
      try { members = (await rpc("group.show", { name: g.name })).members; } catch { /* deleted mid-poll */ }
      gmap[g.name] = { at: g.created_at, members };
    }
  } catch { /* groups unsupported on an older server — plain targets still work */ }
  return {
    now: new Date().toISOString(),
    agents,
    messages,
    reads: [], // server mode: receipts come from the CORE (see /api/receipts)
    channels,
    groups: gmap,
    serverMode: true,
  };
}
const stateCache = { at: 0, val: null as any };
async function state(): Promise<any> {
  if (!SERVER_URL) return stateDb();
  if (Date.now() - stateCache.at < 1200 && stateCache.val) return stateCache.val;
  const v = await stateRpc();
  stateCache.at = Date.now(); stateCache.val = v;
  return v;
}

const HTML = /* html */ `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>agent comms — live</title>
<style>
  :root{
    --bg:#0b0e14; --panel:#111624; --panel2:#0e131f; --line:#1e2637; --ink:#e6edf3;
    --dim:#8b98ad; --dim2:#5d6b82; --accent:#6ea8fe;
    --ask:#4c8dff; --reply:#3fb950; --ack:#2dd4bf; --result:#a371f7; --status:#8b98ad;
    --handoff:#f0883e; --note:#7d8ea3; --rfc:#db61a2; --announce:#db61a2;
    --s-open:#e3b341; --s-acked:#4c8dff; --s-in_progress:#2dd4bf; --s-done:#3fb950; --s-blocked:#f85149;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
  code,.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
  #top{position:sticky;top:0;z-index:20;background:var(--bg);box-shadow:0 8px 20px -16px #000}
  header{background:linear-gradient(180deg,#0b0e14,#0b0e14ee);
    border-bottom:1px solid var(--line);padding:12px 18px;display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  h1{font-size:15px;margin:0;letter-spacing:.3px;font-weight:600}
  h1 .dotlive{display:inline-block;width:9px;height:9px;border-radius:50%;background:#3fb950;margin-right:8px;box-shadow:0 0 0 0 #3fb95066;animation:pulse 2s infinite}
  @keyframes pulse{0%{box-shadow:0 0 0 0 #3fb95066}70%{box-shadow:0 0 0 7px #3fb95000}100%{box-shadow:0 0 0 0 #3fb95000}}
  .stats{display:flex;gap:14px;color:var(--dim);font-size:12px;margin-left:auto;align-items:center}
  .stats b{color:var(--ink)}
  .conn{font-size:12px;padding:2px 8px;border-radius:999px;border:1px solid var(--line)}
  .conn.ok{color:#3fb950;border-color:#20402a}.conn.bad{color:#f85149;border-color:#4a2020}
  .chan-tabs{display:flex;gap:6px;flex-wrap:wrap;padding:8px 18px;border-bottom:1px solid var(--line);background:var(--panel2);align-items:center}
  .chan-tabs .lbl{color:var(--dim2);font-size:11px;margin-right:4px}
  .chan-tab{font-size:12px;padding:4px 11px;border-radius:999px;border:1px solid var(--line);background:var(--panel);color:var(--dim);cursor:pointer}
  .chan-tab:hover{color:var(--ink)}
  .chan-tab.active{color:#0b0e14;background:var(--accent);border-color:var(--accent);font-weight:600}
  .chan-tab .n{opacity:.7;margin-left:5px}
  .chan-chip{font-size:10.5px;padding:2px 7px;border-radius:6px;background:#1b2740;color:#9db4e0;font-weight:600}
  .agents{display:flex;gap:8px;flex-wrap:wrap;padding:10px 18px;border-bottom:1px solid var(--line);background:var(--panel2)}
  .agent{display:flex;align-items:center;gap:8px;background:var(--panel);border:1px solid var(--line);
    border-radius:10px;padding:6px 10px;font-size:12px}
  .agent .dot{width:8px;height:8px;border-radius:50%}
  .agent .dot.on{background:#3fb950;box-shadow:0 0 6px #3fb95088}.agent .dot.off{background:var(--dim2)}
  .agent .role{color:var(--dim)}
  .agent .caps{color:var(--dim2);font-size:11px}
  #wrap{display:flex;gap:20px;max-width:1360px;margin:0 auto;padding:0 18px;align-items:flex-start}
  main{flex:1 1 auto;min-width:0;max-width:1000px;padding:18px 0}
  #statusrail{flex:0 0 306px;padding:18px 0;position:sticky;top:8px;align-self:flex-start;max-height:calc(100vh - 16px);overflow:auto}
  #statusrail .rail-h{font-size:12px;color:var(--dim);font-weight:600;padding:2px 4px 4px;letter-spacing:.3px}
  #statusrail .rail-sub{display:block;color:var(--dim2);font-weight:400;font-size:10.5px;margin-top:2px}
  .statuscard{border:1px solid var(--line);border-left:3px solid var(--stripe,#333);background:var(--panel);
    border-radius:10px;padding:10px 12px;margin:9px 0;cursor:pointer;transition:background .2s}
  .statuscard:hover{background:var(--panel2)}
  .statuscard.active{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent) inset}
  .sc-chan{font-size:11px;color:#9db4e0;font-weight:600;display:flex;align-items:center;gap:6px}
  .sc-time{margin-left:auto;color:var(--dim2);font-weight:400;font-size:10.5px}
  .sc-subj{font-size:13px;font-weight:600;color:var(--ink);margin:6px 0 3px;line-height:1.35}
  .sc-snip{font-size:11.5px;color:var(--dim);white-space:pre-wrap;line-height:1.45;max-height:66px;overflow:hidden;
    -webkit-mask-image:linear-gradient(#000 62%,transparent)}
  .sc-none{font-size:11.5px;color:var(--dim2);margin-top:5px}
  @media(max-width:900px){#statusrail{display:none}}
  .msg{border:1px solid var(--line);border-left:3px solid var(--stripe,#333);background:var(--panel);
    border-radius:10px;padding:12px 14px;margin:10px 0;transition:background .3s}
  .msg.fresh{animation:flash 1.6s ease}
  @keyframes flash{0%{background:#182238}100%{background:var(--panel)}}
  .msg .top{display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:12px;color:var(--dim)}
  .badge{font-size:10.5px;font-weight:600;padding:2px 7px;border-radius:6px;text-transform:uppercase;letter-spacing:.4px}
  .from{color:var(--ink);font-weight:600}.arrow{color:var(--dim2)}.to{color:var(--dim)}
  .time{margin-left:auto;color:var(--dim2)}
  .subj{margin:7px 0 2px;font-weight:600;font-size:14.5px}
  .body{white-space:pre-wrap;color:var(--dim);font-size:13px;max-height:0;overflow:hidden;transition:max-height .25s}
  .msg.open .body{max-height:1200px;overflow:auto;margin-top:8px}
  .body-hint{color:var(--dim2);font-size:11.5px;cursor:pointer;margin-top:6px;user-select:none}
  .tags{color:var(--dim2);font-size:11px}
  .receipts{display:flex;gap:6px;flex-wrap:wrap;align-items:center;margin-top:9px;padding-top:8px;border-top:1px dashed var(--line)}
  .receipts .lbl{color:var(--dim2);font-size:11px;margin-right:2px}
  .rcpt{font-size:11px;padding:2px 7px;border-radius:999px;border:1px solid var(--line);display:inline-flex;gap:5px;align-items:center}
  .rcpt.read{color:#3fb950;border-color:#20402a;background:#0f1e14}
  .rcpt.unread{color:var(--dim2)}
  .rcpt .rd{font-weight:700}
  .seen{color:var(--dim2);font-size:11px;margin-left:4px}
  .thread-tag{cursor:pointer;color:var(--dim2)}
  .filterbar{padding:0 18px 4px;color:var(--dim);font-size:12px;display:flex;gap:10px;align-items:center}
  .filterbar button{background:var(--panel);color:var(--dim);border:1px solid var(--line);border-radius:8px;padding:4px 10px;cursor:pointer;font-size:12px}
  .filterbar button:hover{color:var(--ink)}
  .empty{color:var(--dim2);text-align:center;padding:40px}
</style></head>
<body>
<div id="top">
<div class="chan-tabs" id="chantabs"></div>
<header>
  <h1><span class="dotlive"></span>agent comms — live</h1>
  <span class="conn" id="conn">connecting…</span>
  <div class="stats">
    <span><b id="mcount">0</b> messages</span>
    <span><b id="acount">0</b> active</span>
    <span id="clock"></span>
  </div>
</header>
<div class="agents" id="agents"></div>
<div class="filterbar">
  <span id="filterlabel"></span>
  <button id="clearfilter" style="display:none">clear filter</button>
  <span style="margin-left:auto;color:var(--dim2)">click a message to expand · click a thread id to filter</span>
</div>
</div>
<div id="wrap">
<main id="feed"><div class="empty">waiting for messages…</div></main>
<aside id="statusrail"></aside>
</div>
<script>
const TYPE=["ask","reply","ack","result","status","handoff","note","rfc","announce"];
const seen=new Set(); let openIds=new Set(); let filterThread=null; let filterChannel=null; let first=true;
const el=id=>document.getElementById(id);
function rel(iso){const s=(Date.now()-Date.parse(iso))/1000;
  if(s<60)return Math.floor(s)+"s ago";if(s<3600)return Math.floor(s/60)+"m ago";
  if(s<86400)return Math.floor(s/3600)+"h ago";return Math.floor(s/86400)+"d ago";}
function esc(t){return (t??"").replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));}
function color(v,pfx){return getComputedStyle(document.documentElement).getPropertyValue(pfx+v)||"#889";}
function renderAgents(agents){
  el("agents").innerHTML=agents.map(a=>{
    const on=(Date.now()-Date.parse(a.last_seen))<=15*60*1000;
    return '<div class="agent"><span class="dot '+(on?"on":"off")+'"></span>'+
      '<b>'+esc(a.id)+'</b><span class="role">'+esc(a.role)+'</span>'+
      (a.caps?'<span class="caps">'+esc(a.caps)+'</span>':'')+
      '<span class="caps">'+rel(a.last_seen)+'</span></div>';
  }).join("");
  el("acount").textContent=agents.filter(a=>(Date.now()-Date.parse(a.last_seen))<=9e5).length;
}
// per-message read receipts: intended readers (addressed, minus sender) vs who's read
// F: group: targets resolve through state.groups with the incarnation guard.
function receiptsOf(m,agents,reads,messages,groups){
  const toks=(m.recipients||"").split(",").map(s=>s.trim()).filter(Boolean);
  const gmap=groups||{};
  const intended=agents.filter(a=>{
    if(a.id===m.sender)return false;
    for(const t of toks){
      if(t==="@all"||t===a.id||t===a.role)return true;
      const g=gmap[t.startsWith("group:")?t.slice(6):""];
      if(g&&m.created_at>=g.at&&g.members.includes(a.id))return true;
    }
    return false;
  }).map(a=>a.id);
  const rmap=new Map(reads.filter(r=>r.msg===m.id).map(r=>[r.agent,r.read_at]));
  messages.forEach(x=>{if(x.re===m.id&&!rmap.has(x.sender))rmap.set(x.sender,null);}); // inferred via reply
  return intended.map(id=>({id,read:rmap.has(id),at:rmap.get(id)}));
}
// §8 M4 server mode: receipts come from the CORE (receipts RPC via the proxy),
// never from this JS duplicate. claude M4 B5: entries EXPIRE (15 s ok / 5 s
// failed) — the old cache kept the first answer forever (reads after the
// first fetch never showed) and cached a failure as null forever; and only
// the newest RC_MAX feed cards fetch, one request at a time (a 1000-row feed
// would otherwise fire 1000 proxy→RPC calls into a 120-token read bucket).
let serverMode=false; const rcCache=new Map(); const rcPending=new Set(); const rcWant=[]; let rcBusy=false;
const RC_MAX=40;
function rcPump(){
  if(rcBusy||!rcWant.length)return; rcBusy=true;
  const id=rcWant.shift();
  fetch("/api/receipts?id="+encodeURIComponent(id)).then(r=>r.json()).then(j=>{
    if(j&&j.receipts){
      rcCache.set(id,{t:Date.now(),ttl:15000,v:(j.receipts.intended||[]).map(x=>{const rd=(j.receipts.readers||[]).find(q=>q.id===x);return rd?{id:x,read:true,at:rd.at}:{id:x,read:false,at:null};})});
    } else rcCache.set(id,{t:Date.now(),ttl:5000,v:null});
  }).catch(()=>{rcCache.set(id,{t:Date.now(),ttl:5000,v:null});})
    .finally(()=>{rcPending.delete(id);rcBusy=false;if(rcWant.length)rcPump();else render(last);});
}
function coreReceipts(id,want){
  const c=rcCache.get(id);
  if(want&&(!c||Date.now()-c.t>c.ttl)&&!rcPending.has(id)){rcPending.add(id);rcWant.push(id);rcPump();}
  return c?c.v:undefined; // undefined = loading/not fetched, null = failed
}
function renderChannels(channels){
  const list=(channels||[]).slice();
  const total=list.reduce((s,c)=>s+c.n,0);
  const tabs=[{name:null,label:"all",n:total}].concat(list.map(c=>({name:c.name,label:c.name,n:c.n})));
  el("chantabs").innerHTML='<span class="lbl">channels</span>'+tabs.map(t=>
    '<span class="chan-tab'+((filterChannel===t.name)?' active':'')+'" data-chan="'+(t.name??'')+'">#'+esc(t.label)+'<span class="n">'+t.n+'</span></span>'
  ).join("");
}
function renderFeed(s){
  const messages=s.messages||[], agents=s.agents||[], reads=s.reads||[], groups=s.groups||{};
  el("mcount").textContent=messages.length;
  let msgs=messages;
  if(filterChannel) msgs=msgs.filter(m=>(m.channel||"general")===filterChannel);
  if(filterThread) msgs=msgs.filter(m=>m.thread===filterThread);
  msgs=[...msgs].reverse();   // newest first
  if(!msgs.length){el("feed").innerHTML='<div class="empty">no messages'+(filterThread?" in this thread":"")+'</div>';return;}
  el("feed").innerHTML=msgs.map((m,idx)=>{
    const tcol=color(m.type,"--"); const scol=color(m.status,"--s-");
    const fresh=!seen.has(m.id)&&!first; if(!seen.has(m.id))seen.add(m.id);
    const opened=openIds.has(m.id);
    // claude M4 B5: rc is undefined (loading) / null (failed) in server mode —
    // the old rc.length in the header threw on the FIRST card, so the server-
    // mode feed rendered ZERO messages (probe: 0 .msg nodes, state had 32).
    const rc=(serverMode?coreReceipts(m.id,idx<RC_MAX||opened):receiptsOf(m,agents,reads,messages,groups))||[];
    const nread=rc.filter(r=>r.read).length;
    const receiptsHtml=rc.length?
      '<div class="receipts"><span class="lbl">receipts</span>'+
      rc.map(r=>'<span class="rcpt '+(r.read?'read':'unread')+'" title="'+(r.read?(r.at?'read '+rel(r.at):'seen (replied)'):'unread')+'">'+
        '<span class="rd">'+(r.read?'✓':'○')+'</span>'+esc(r.id)+'</span>').join('')+'</div>':'';
    return '<div class="msg'+(fresh?' fresh':'')+(opened?' open':'')+'" data-id="'+m.id+'" style="--stripe:'+tcol+'">'+
      '<div class="top">'+
        '<span class="badge" style="background:'+tcol+'22;color:'+tcol+'">'+m.type+'</span>'+
        '<span class="badge" style="background:'+scol+'22;color:'+scol+'">'+m.status+'</span>'+
        '<span class="chan-chip">#'+esc(m.channel||"general")+'</span>'+
        '<span class="from">'+esc(m.sender)+'</span><span class="arrow">→</span>'+
        '<span class="to">'+esc(m.recipients)+'</span>'+
        (rc.length?'<span class="seen">👁 '+nread+'/'+rc.length+'</span>':'')+
        '<span class="thread-tag mono" data-thread="'+m.thread+'" title="filter this thread"> ⟂'+m.thread.slice(-9)+'</span>'+
        '<span class="time">'+rel(m.created_at)+'</span>'+
      '</div>'+
      '<div class="subj">'+esc(m.subject||m.type)+'</div>'+
      (m.tags?'<div class="tags">#'+esc(m.tags).replace(/,/g," #")+'</div>':'')+
      '<div class="body">'+esc(m.body)+'</div>'+
      '<div class="body-hint">'+(opened?'▾ collapse':'▸ expand')+'</div>'+
      receiptsHtml+
    '</div>';
  }).join("");
}
el("feed").addEventListener("click",e=>{
  const th=e.target.closest(".thread-tag");
  if(th){filterThread=th.dataset.thread;el("filterlabel").textContent="filtered → thread …"+filterThread.slice(-9);
    el("clearfilter").style.display="";render(last);e.stopPropagation();return;}
  const card=e.target.closest(".msg");if(!card)return;const id=card.dataset.id;
  if(openIds.has(id))openIds.delete(id);else openIds.add(id);render(last);
});
el("clearfilter").onclick=()=>{filterThread=null;el("filterlabel").textContent="";el("clearfilter").style.display="none";render(last);};
el("chantabs").addEventListener("click",e=>{
  const t=e.target.closest(".chan-tab");if(!t)return;
  filterChannel=t.dataset.chan||null; filterThread=null;
  el("filterlabel").textContent="";el("clearfilter").style.display="none";render(last);
});
// per-project rail: newest COORDINATOR update (sender starting "coord") for each channel
function renderStatusRail(s){
  const messages=s.messages||[];
  const channels=(s.channels||[]).map(c=>c.name||"general");
  const cards=channels.map(ch=>{
    let latest=null;
    for(const m of messages){ // messages are ascending by time
      if((m.channel||"general")===ch && /^coord/i.test(m.sender||"")) latest=m;
    }
    return {ch,latest};
  }).sort((a,b)=>(b.latest?Date.parse(b.latest.created_at):0)-(a.latest?Date.parse(a.latest.created_at):0));
  el("statusrail").innerHTML='<div class="rail-h">project status<span class="rail-sub">latest coordinator update · click to filter</span></div>'+
    cards.map(c=>{
      const active=(filterChannel===c.ch)?' active':'';
      const m=c.latest;
      if(!m) return '<div class="statuscard'+active+'" data-chan="'+esc(c.ch)+'">'+
        '<div class="sc-chan">#'+esc(c.ch)+'</div><div class="sc-none">— no coordinator update yet —</div></div>';
      const tcol=color(m.type,"--"); const body=m.body||""; const snip=body.slice(0,180);
      return '<div class="statuscard'+active+'" data-chan="'+esc(c.ch)+'" style="--stripe:'+tcol+'">'+
        '<div class="sc-chan">#'+esc(c.ch)+
          '<span class="badge" style="background:'+tcol+'22;color:'+tcol+';font-size:9.5px">'+esc(m.type)+'</span>'+
          '<span class="sc-time">'+rel(m.created_at)+'</span></div>'+
        '<div class="sc-subj">'+esc(m.subject||m.type)+'</div>'+
        '<div class="sc-snip">'+esc(snip)+(body.length>180?'…':'')+'</div>'+
      '</div>';
    }).join("");
}
el("statusrail").addEventListener("click",e=>{
  const c=e.target.closest(".statuscard");if(!c)return;
  filterChannel=c.dataset.chan||null; filterThread=null;
  el("filterlabel").textContent="";el("clearfilter").style.display="none";render(last);
});
let last={agents:[],messages:[],reads:[],channels:[]};
function render(s){last=s;serverMode=!!s.serverMode;renderChannels(s.channels);renderAgents(s.agents);renderFeed(s);renderStatusRail(s);first=false;}
function connect(){
  const es=new EventSource("/api/stream");
  es.onopen=()=>{el("conn").textContent="● live";el("conn").className="conn ok";};
  es.onerror=()=>{el("conn").textContent="● reconnecting";el("conn").className="conn bad";};
  es.onmessage=ev=>{try{render(JSON.parse(ev.data));}catch(e){}};
}
setInterval(()=>{el("clock").textContent=new Date().toLocaleTimeString();
  document.querySelectorAll(".time").forEach(()=>{});},1000);
connect();
</script>
</body></html>`;

const srv = Bun.serve({
  hostname: HOST, // N4: loopback default — direct-DB dashboard must not re-serve the LAN
  port: PORT,
  idleTimeout: 0,
  async fetch(req) {
    const url = new URL(req.url);
    // claude M4 m-d: loopback bind does not stop DNS rebinding — a page on
    // attacker.example re-resolved to 127.0.0.1 is same-origin with this
    // unauthenticated port and could read /api/state (probe: foreign Host ⇒
    // 200). With the default loopback bind, only loopback Host names pass;
    // --host (explicit LAN opt-in) skips the check.
    if (!argHost) {
      const h = (req.headers.get("host") ?? "").replace(/:\d+$/, "").toLowerCase();
      if (!(h === "localhost" || h === "127.0.0.1" || h === "[::1]")) return new Response("forbidden host", { status: 403 });
    }
    if (url.pathname === "/api/state") {
      try { return Response.json(await state()); }
      catch (e: any) { return Response.json({ error: String(e?.message ?? e) }, { status: 502 }); }
    }
    // §8 M4: server mode takes receipts from the CORE (receipts RPC) — the JS
    // receiptsOf duplicate is direct-DB-only (no core to call there). The page
    // fetches these for the newest RC_MAX cards only, serially, with a TTL.
    if (url.pathname === "/api/receipts") {
      if (!SERVER_URL) return Response.json({ error: "receipts proxy is server-mode only" }, { status: 400 });
      const id = url.searchParams.get("id") ?? "";
      try {
        const r = await rpc("receipts", { id });
        // B4 twin: a dm row's receipts are DM metadata (who read what) ⇒ same gate.
        if (!OMNIVIEW && String(r?.channel ?? "").startsWith("dm~")) return Response.json({ error: "not found" }, { status: 404 });
        return Response.json(r);
      }
      catch (e: any) { return Response.json({ error: String(e?.message ?? e) }, { status: 502 }); }
    }
    if (url.pathname === "/api/stream") {
      const stream = new ReadableStream({
        start(controller) {
          const enc = new TextEncoder();
          let busy = false, again = false;
          const push = async () => {
            if (busy) { again = true; return; } // never interleave state builds
            busy = true;
            do {
              again = false;
              try { controller.enqueue(enc.encode(`data: ${JSON.stringify(await state())}\n\n`)); }
              catch { } // client gone or state failed (server unreachable) — next tick retries
            } while (again);
            busy = false;
          };
          void push();
          const iv = setInterval(() => void push(), 1500);
          req.signal.addEventListener("abort", () => { clearInterval(iv); try { controller.close(); } catch {} });
        },
      });
      return new Response(stream, {
        headers: { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", "Connection": "keep-alive" },
      });
    }
    return new Response(HTML, { headers: { "Content-Type": "text/html; charset=utf-8" } });
  },
});
console.log(`comms dashboard live → http://localhost:${srv.port}  (${SERVER_URL ? "server mode: " + SERVER_URL : "COMMS_HOME=" + HOME})`);
