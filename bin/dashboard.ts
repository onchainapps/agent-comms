#!/usr/bin/env bun
/**
 * dashboard — live web view of the agent comms bus.
 *
 * Read-only: opens comms.db and streams state over SSE. Never writes.
 *   bun agent-comms/dashboard.ts [--port 8787]
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
// N4 (claude F/G review): Bun.serve without hostname binds the v6 wildcard
// (`*:port` — ss-verified), re-serving every message body to the LAN from an
// unauthenticated direct-DB process. Default to loopback; --host opts out.
const argHost = process.argv.includes("--host") ? process.argv[process.argv.indexOf("--host") + 1] : undefined;
const HOST = argHost ?? "127.0.0.1";
// G5: this is a pre-G binary (direct DB, unauthenticated). When the DB holds any
// dm-shaped channel, dm rows are STRIPPED unless --omniview — "local = host user
// is root of trust" does not extend to LAN visitors of a DB re-publisher.
const OMNIVIEW = process.argv.includes("--omniview");

const db = new Database(DB_PATH, { readonly: true });
db.exec("PRAGMA busy_timeout = 3000");
const dmStrip = !OMNIVIEW && !!db.query("SELECT 1 FROM channels WHERE name GLOB 'dm~*' LIMIT 1").get();
if (dmStrip) console.error("dashboard: dm-shaped channels present — dm rows hidden (direct-DB pre-G binary); pass --omniview to show, or use the M4 web UI");

function state() {
  const agents = db.query("SELECT id, role, caps, last_seen, joined_at FROM agents WHERE id IS NOT NULL ORDER BY last_seen DESC").all();
  const messages = db.query(
    "SELECT id, thread, re, sender, recipients, type, status, tags, subject, body, created_at, updated_at, channel FROM messages ORDER BY created_at ASC"
  ).all();
  const reads = db.query("SELECT agent, msg, read_at FROM reads").all();
  const channels = db.query("SELECT channel name, COUNT(*) n FROM messages GROUP BY channel ORDER BY n DESC").all();
  // F: receipts call site must resolve group: targets — same incarnation guard
  // as the core (grp -> {created_at, members}).
  const groups = db.query("SELECT name, created_at FROM groups").all();
  const gm = db.query("SELECT grp, agent_id FROM group_members").all();
  const gmap: Record<string, { at: string; members: string[] }> = {};
  for (const g of groups as any[]) gmap[g.name] = { at: g.created_at, members: [] };
  for (const r of gm as any[]) gmap[r.grp]?.members.push(r.agent_id);
  return { now: new Date().toISOString(), agents, messages: dmStrip ? (messages as any[]).filter((m) => !String(m.channel).startsWith("dm~")) : messages, reads, channels, groups: gmap };
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
  el("feed").innerHTML=msgs.map(m=>{
    const tcol=color(m.type,"--"); const scol=color(m.status,"--s-");
    const fresh=!seen.has(m.id)&&!first; if(!seen.has(m.id))seen.add(m.id);
    const opened=openIds.has(m.id);
    const rc=receiptsOf(m,agents,reads,messages,groups); const nread=rc.filter(r=>r.read).length;
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
function render(s){last=s;renderChannels(s.channels);renderAgents(s.agents);renderFeed(s);renderStatusRail(s);first=false;}
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

Bun.serve({
  hostname: HOST, // N4: loopback default — direct-DB dashboard must not re-serve the LAN
  port: PORT,
  idleTimeout: 0,
  fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/api/state") return Response.json(state());
    if (url.pathname === "/api/stream") {
      const stream = new ReadableStream({
        start(controller) {
          const enc = new TextEncoder();
          const push = () => { try { controller.enqueue(enc.encode(`data: ${JSON.stringify(state())}\n\n`)); } catch {} };
          push();
          const iv = setInterval(push, 1500);
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
console.log(`comms dashboard live → http://localhost:${PORT}  (COMMS_HOME=${HOME})`);
