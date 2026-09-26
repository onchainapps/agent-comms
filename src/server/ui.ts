/**
 * src/server/ui.ts — M4 web UI (RFC-001 §8): the single static page served at
 * GET / by the server. Vanilla JS, same-origin only — every RPC goes through
 * POST /rpc on the HttpOnly session cookie (login mints it; the token is never
 * persisted to localStorage), live frames arrive on EventSource /stream.
 *
 * Security notes (pinned by tests/ui.test.ts):
 * - Bus data is rendered with textContent/createElement ONLY — no innerHTML
 *   interpolation of untrusted strings (XSS via message body would otherwise
 *   run same-origin RPC with the session cookie).
 * - CSP rides the response HEADER (UI_CSP below): script-src is the sha256
 *   of the single inline script (no 'unsafe-inline' for scripts — an HTML
 *   injection cannot add a runnable script); style-src keeps 'unsafe-inline'
 *   for the style="" attributes; frame-ancestors 'none'; form-action 'none'.
 * - CSRF (§8) is enforced server-side on cookie requests; this page sends
 *   Content-Type: application/json, which forces a preflight a foreign origin
 *   can never pass.
 */
import { createHash } from "node:crypto";

export const UI_HTML = /* html */ `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>agent comms</title>
<style>
  :root{--bg:#0f1115;--panel:#171a21;--line:#2a2f3a;--fg:#dfe3ea;--dim:#8b93a3;--acc:#5aa7ff;--ok:#3fbf7f;--warn:#e0a63f;--err:#e0605f;--mono:ui-monospace,SFMono-Regular,Menlo,monospace}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif}
  a{color:var(--acc)}
  button,select,input,textarea{font:inherit;background:var(--panel);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:6px 10px}
  button{cursor:pointer}button:hover{border-color:var(--acc)}
  button.mini{padding:2px 8px;font-size:12px}
  input,textarea{min-width:0}
  textarea{width:100%;resize:vertical;min-height:64px}
  #login{max-width:380px;margin:14vh auto;padding:24px;background:var(--panel);border:1px solid var(--line);border-radius:10px}
  #login h1{font-size:18px;margin:0 0 4px}
  #login p{color:var(--dim);margin:0 0 16px}
  #login input{width:100%;font-family:var(--mono);margin:8px 0}
  #app{display:none;grid-template-columns:250px 1fr;grid-template-rows:44px 1fr;height:100vh}
  #app.on{display:grid}
  header{grid-column:1/3;display:flex;align-items:center;gap:12px;padding:0 14px;border-bottom:1px solid var(--line);background:var(--panel)}
  header b{color:var(--acc)}
  header .scopes{color:var(--dim);font-family:var(--mono);font-size:12px}
  header .sp{flex:1}
  nav{border-right:1px solid var(--line);background:var(--panel);overflow-y:auto;padding:10px}
  nav h3{font-size:11px;text-transform:uppercase;color:var(--dim);margin:12px 0 6px}
  nav .tab,nav .chan{display:block;width:100%;text-align:left;margin:2px 0;padding:5px 8px;border:0;border-radius:6px;background:none;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  nav .tab.on,nav .chan.on{background:var(--line)}
  nav .n{color:var(--dim);float:right}
  main{overflow-y:auto;padding:14px 18px;display:flex;flex-direction:column}
  #pane{flex:1;overflow-y:auto}
  .msg{border:1px solid var(--line);border-radius:8px;background:var(--panel);margin:0 0 8px;padding:8px 12px}
  .msg .hd{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}
  .msg .who{font-weight:600;color:var(--acc)}
  .msg .ty{font-family:var(--mono);font-size:11px;color:var(--dim);border:1px solid var(--line);border-radius:4px;padding:0 4px}
  .msg .st{font-size:11px;font-family:var(--mono)}
  .msg .st.open{color:var(--warn)}.msg .st.acked,.msg .st.done{color:var(--ok)}
  .msg .ts{color:var(--dim);font-size:11px;font-family:var(--mono)}
  .msg .sub{margin-top:4px;font-weight:600}
  .msg .body{margin-top:4px;white-space:pre-wrap;word-break:break-word}
  .msg .rc{margin-top:6px;color:var(--dim);font-size:12px}
  .msg .ops{margin-top:6px;display:flex;gap:6px}
  .pres .ag{display:block;padding:3px 8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .pres .on{color:var(--ok)}
  #composer{border-top:1px solid var(--line);padding-top:10px;margin-top:10px;display:grid;gap:8px;grid-template-columns:1fr 1fr;align-items:start}
  #composer textarea{grid-column:1/3}
  #composer .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
  #status{grid-column:1/3;color:var(--dim);font-size:12px;min-height:16px;white-space:pre-wrap}
  #status.err{color:var(--err)}
  table{border-collapse:collapse;width:100%}
  th,td{border:1px solid var(--line);padding:5px 8px;text-align:left;font-size:13px}
  th{background:var(--panel);font-size:11px;text-transform:uppercase;color:var(--dim)}
  td.mono{font-family:var(--mono);font-size:12px}
  .tok{color:var(--ok);font-family:var(--mono);word-break:break-all}
  .rev{color:var(--err)}
  .once{background:var(--panel);border:1px solid var(--ok);border-radius:8px;padding:10px;margin:10px 0;font-family:var(--mono);word-break:break-all}
  label.ck{display:inline-flex;gap:4px;align-items:center;margin-right:10px;font-size:13px}
</style></head>
<body>
<div id="login">
  <h1>agent comms</h1>
  <p>Paste your token (<code>ac_…</code>). It becomes an HttpOnly session cookie — never stored by the page.</p>
  <input id="tok" type="password" placeholder="ac_…" autocomplete="off">
  <button id="go" style="width:100%">Login</button>
  <div id="lerr" style="color:var(--err);margin-top:10px;white-space:pre-wrap"></div>
</div>
<div id="app">
  <header>
    <span>agent-comms</span><b id="me"></b><span class="scopes" id="myscopes"></span>
    <span class="sp"></span><span id="conn" style="color:var(--dim)">stream: …</span>
    <button class="mini" id="out">logout</button>
  </header>
  <nav>
    <button class="tab on" data-tab="chat">Chat</button>
    <button class="tab" data-tab="admin" id="admintab" style="display:none">Admin</button>
    <div id="tabchat">
      <h3>Channels</h3><div id="chans"></div>
      <h3>DMs</h3><div id="dms"></div>
      <h3>Presence</h3><div class="pres" id="pres"></div>
    </div>
  </nav>
  <main>
    <div id="pane"></div>
    <div id="composer">
      <div class="row"><input id="cto" placeholder="to (ids, roles, group:x — comma sep)" style="flex:1"></div>
      <div class="row">
        <select id="ctype"></select>
        <input id="cchan" list="chanlist" placeholder="channel (blank = general)" style="flex:1">
        <datalist id="chanlist"></datalist>
        <input id="csubj" placeholder="subject (optional)" style="flex:1">
      </div>
      <textarea id="cbody" placeholder="message body"></textarea>
      <div id="status"></div>
      <div class="row"><button id="cpost" style="margin-top:2px">Post</button><span id="pd" style="color:var(--dim);font-size:12px"></span></div>
    </div>
    <div id="admin" style="display:none"></div>
  </main>
</div>
<script>
"use strict";
const S = { me:null, scopes:[], chans:[], agents:[], active:new Set(), msgs:new Map(), sel:null, selKind:null, stream:null, seq:0, epoch:"", rpccache:new Map(), minted:null, dmMembers:new Map() };
const $ = (id) => document.getElementById(id);
const el = (tag, cls, text) => { const n = document.createElement(tag); if (cls) n.className = cls; if (text !== undefined) n.textContent = text; return n; };

let rpcid = 0;
async function rpc(method, params) {
  const res = await fetch("/rpc", { method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", method, params: params || {}, id: ++rpcid }) });
  let body = null; try { body = await res.json(); } catch {}
  if (!res.ok || (body && body.error)) {
    const e = body && body.error ? body.error : { code: -32603, message: "HTTP " + res.status };
    const err = new Error((e.data && e.data.detail) || e.message || "rpc error");
    err.code = e.code; err.bus = e.data && e.data.busError; err.data = e.data || {}; err.http = res.status; throw err;
  }
  return body.result;
}
function setS(msg, isErr) { const n = $("status"); n.textContent = msg || ""; n.className = isErr ? "err" : ""; }

/* ---------- login / boot ---------- */
async function boot() {
  $("login").style.display = "none"; $("app").classList.add("on");
  const ag = await rpc("who", { all: true });
  S.agents = ag; try { S.active = new Set((await rpc("who", {})).map((a) => a.id)); } catch {}
  await refreshChans();
  renderPres();
  if (S.scopes.includes("tokens:admin")) $("admintab").style.display = "";
  // claude M4 M-b: §6 handoff — snapshot FIRST, then open the stream AT the
  // snapshot cursor. Running them concurrently left a hole: rows committed
  // after the snapshot txn but before the subscribe's high-water read were in
  // neither (the stream starts at ITS high-water, not the snapshot's).
  const cursor = await loadHistory();
  openStream(cursor);
}
$("go").onclick = async () => {
  $("lerr").textContent = "";
  try {
    const r = await rpc("login", { token: $("tok").value.trim() });
    S.me = r.agentId; S.scopes = r.scopes || [];
    $("me").textContent = S.me; $("myscopes").textContent = S.scopes.join(", ");
    $("tok").value = "";
    await boot();
  } catch (e) { $("lerr").textContent = "login failed: " + e.message; }
};
$("tok").addEventListener("keydown", (ev) => { if (ev.key === "Enter") $("go").click(); });
$("out").onclick = async () => { if (S.stream) S.stream.close(); try { await rpc("logout", {}); } catch {} location.reload(); };

/* ---------- channels / DMs ---------- */
async function refreshChans() {
  S.chans = await rpc("channels", {});
  const pub = S.chans.filter((c) => !/^dm~/.test(c.name));
  const dm = S.chans.filter((c) => /^dm~/.test(c.name));
  const box = $("chans"); box.textContent = "";
  for (const c of pub) {
    const b = el("button", "chan" + (S.sel === c.name ? " on" : ""), "#" + c.name);
    b.appendChild(el("span", "n", String(c.n)));
    b.onclick = () => select(c.name, "chan"); box.appendChild(b);
  }
  const dbox = $("dms"); dbox.textContent = "";
  for (const c of dm) {
    // DM membership is pair-keyed and immutable ⇒ resolve once per channel
    // (was: one dm.members RPC per DM on EVERY refresh, i.e. every click).
    if (!S.dmMembers.has(c.name)) {
      // wire contract (contract.suite G7): dm.members RESULT is the raw
      // members array, not {members} — .members here was undefined ⇒ peer
      // null ⇒ DM composer stuck read-only (grok M4 B1 residual).
      try { const r = await rpc("dm.members", { channel: c.name }); S.dmMembers.set(c.name, Array.isArray(r) ? r : null); } catch { S.dmMembers.set(c.name, null); }
    }
    const mem = S.dmMembers.get(c.name);
    const label = mem ? "dm:" + mem.join(" ↔ ") : "#" + c.name;
    const b = el("button", "chan" + (S.sel === c.name ? " on" : ""), label);
    b.appendChild(el("span", "n", String(c.n)));
    b.onclick = () => select(c.name, "dm"); dbox.appendChild(b);
  }
  $("chanlist").textContent = "";
  for (const c of pub) $("chanlist").appendChild(el("option", null, c.name));
}

/* ---------- message panes ---------- */
function select(name, kind) {
  S.sel = name; S.selKind = kind;
  refreshChans(); renderPane();
  // ruling (c): channel-scoped history is ungated ⇒ fill THIS channel beyond
  // the global newest-page (and give non-read:all tokens any history at all).
  rpc("history", { channel: name, limit: 200 }).then((pg) => { for (const m of pg.rows) S.msgs.set(m.id, m); if (S.sel === name) renderPane(); }).catch(() => {});
  if (kind === "dm") { $("cchan").value = ""; dmPeerHint(); }
  else { $("cto").disabled = false; $("cpost").disabled = false; $("cchan").value = name === "general" ? "" : name; $("pd").textContent = ""; }
}
function dmPeer() {
  const mem = S.dmMembers.get(S.sel);
  if (!mem || !mem.includes(S.me)) return null; // omniview viewer of someone else's DM
  return mem[0] === S.me ? mem[1] : mem[0];
}
function dmPeerHint() {
  const peer = dmPeer();
  // claude M4 m-a: a read:dm human viewing a DM it is NOT party to used to
  // get peer = first member, so "Post" silently opened a NEW dm~human~<x>
  // instead of writing here. Non-party DM view is read-only.
  $("cto").disabled = true; $("cpost").disabled = !peer;
  $("pd").textContent = peer ? "DM → " + peer + " (posts use dm:" + peer + ")" : "read-only: you are not a party to this DM";
}
function msgNode(m) {
  const n = el("div", "msg"); n.dataset.id = m.id;
  const hd = el("div", "hd");
  hd.appendChild(el("span", "who", m.sender));
  hd.appendChild(el("span", "ty", m.type));
  if (m.subject) hd.appendChild(el("span", null, m.subject));
  hd.appendChild(el("span", "st " + m.status, m.status));
  hd.appendChild(el("span", "sp", "")).className = "sp";
  hd.appendChild(el("span", "ts", m.created_at));
  n.appendChild(hd);
  n.appendChild(el("div", "body", m.body));
  const rc = el("div", "rc"); rc.dataset.rc = m.id; n.appendChild(rc);
  const ops = el("div", "ops");
  for (const st of ["acked", "done"]) {
    const b = el("button", "mini", st);
    b.onclick = () => rpc("status", { id: m.id, state: st }).then(() => note(m.id, { status: st })).catch((e) => setS("status failed: " + e.message, 1));
    ops.appendChild(b);
  }
  const th = el("button", "mini", "reply");
  th.onclick = () => { $("cbody").value = ""; $("cbody").placeholder = "reply to " + m.id + " (posts with re:" + m.id.slice(0, 22) + "…)"; S.replyTo = m.id; ops2focus(); };
  ops.appendChild(th);
  n.appendChild(ops);
  return n;
}
function ops2focus() { $("cbody").focus(); }
function note(id, patch) {
  const m = S.msgs.get(id); if (!m) return;
  Object.assign(m, patch);
  document.querySelectorAll('.msg[data-id="' + CSS.escape(id) + '"]').forEach((node) => {
    const st = node.querySelector(".st"); st.textContent = m.status; st.className = "st " + m.status;
  });
}
function renderPane() {
  const pane = $("pane"); pane.textContent = "";
  const rows = [...S.msgs.values()].filter((m) => !S.sel || m.channel === S.sel).sort((a, b) => a.created_at < b.created_at ? -1 : 1).slice(-400);
  for (const m of rows) pane.appendChild(msgNode(m));
  pane.scrollTop = pane.scrollHeight;
  // claude M4 M-c: receipts are fetched ONLY for the newest RC_MAX rows, one
  // at a time. Firing one RPC per rendered row (up to 400, all at once) drained
  // the 120-token read bucket on every boot: 83/200 came back 429 in the
  // probe, and so did the NEXT user action.
  for (const m of rows.slice(-RC_MAX)) rcQueue.add(m.id);
  pumpReceipts();
}
const RC_MAX = 40;
const rcQueue = new Set(); let rcBusy = false;
async function pumpReceipts() {
  if (rcBusy) return; rcBusy = true;
  try {
    while (rcQueue.size) {
      const id = rcQueue.values().next().value; rcQueue.delete(id);
      await loadReceipts(id);
    }
  } finally { rcBusy = false; }
}
async function loadReceipts(id) {
  const slot = document.querySelector('.rc[data-rc="' + CSS.escape(id) + '"]');
  if (!slot) return;
  // a failed fetch is NOT cached (a 429 must not freeze the slot blank forever)
  if (!S.rpccache.has(id)) S.rpccache.set(id, rpc("receipts", { id }).catch(() => { S.rpccache.delete(id); return null; }));
  const r = await S.rpccache.get(id); if (!r) return;
  const cur = document.querySelector('.rc[data-rc="' + CSS.escape(id) + '"]'); if (!cur || cur !== slot) return;
  const rc = r.receipts || {};
  const seen = (rc.readers || []).map((x) => x.id + (x.at ? " ✓" : " (replied)")).join("  ");
  const pend = (rc.unread || []).join(", ");
  cur.textContent = "to: " + (rc.intended || []).join(",") + (seen ? "   seen: " + seen : "") + (pend ? "   pending: " + pend : "");
}

/* ---------- history + stream ---------- */
// claude M4 M-a: the snapshot has NO backward paging. Without since it
// returns the newest page and cursor = the events HIGH-WATER; feeding that
// back as since= returns the rows AFTER the high-water, i.e. nothing (probe:
// 450 seeded → 200 loaded, page 2 = 0 rows). The old "≤40 pages" loop was a
// no-op second call. ONE snapshot at the core's cap (1000) is the honest
// shape; older rows come from the per-channel view (select()). Returns the
// snapshot cursor for the §6 stream handoff (null = no snapshot).
async function loadHistory() {
  let page;
  try { page = await rpc("history", { limit: 1000 }); }
  catch (e) {
    if (e.bus === "forbidden") { setS("history: " + e.message + " (open a channel for its history; live stream below)"); return null; }
    setS("history failed: " + e.message, 1); return null;
  }
  for (const m of page.rows) S.msgs.set(m.id, m);
  renderPane();
  return page.cursor;
}
function openStream(since) {
  // design (a) kept: read:all ⇒ scope=all (DM frames are still canSee-filtered
  // server-side, so read:all without read:dm never receives them).
  const scope = S.scopes.includes("read:all") ? "all" : "mine";
  const es = new EventSource("/stream?scope=" + scope + (since ? "&since=" + encodeURIComponent(since) : ""));
  S.stream = es;
  es.onopen = () => { $("conn").textContent = "stream: live"; $("conn").style.color = "var(--ok)"; };
  es.onerror = () => {
    const dead = es.readyState === EventSource.CLOSED; // non-200 (401 after restart/revoke) ⇒ EventSource gives up
    $("conn").textContent = dead ? "stream: closed — reload to log in again" : "stream: reconnecting…";
    $("conn").style.color = dead ? "var(--err)" : "var(--warn)";
  };
  es.addEventListener("hello", (ev) => { const d = JSON.parse(ev.data); S.epoch = d.epoch; S.seq = d.seq; });
  // claude M4 M-f: the server closes the socket after "resync", and the resync
  // frame carries no id ⇒ native reconnect resent the SAME dead-epoch
  // Last-Event-ID forever (probe: 9 reconnects + 9 full reloads in 30 s,
  // "reconnecting…" permanently). Close it ourselves and re-handoff.
  es.addEventListener("resync", async () => {
    es.close(); if (S.stream !== es) return;
    S.msgs.clear(); S.rpccache.clear(); renderPane();
    const c = await loadHistory();
    if (S.stream === es) openStream(c);
  });
  es.addEventListener("revoked", () => { es.close(); $("conn").textContent = "stream: token revoked — reload"; $("conn").style.color = "var(--err)"; });
  es.addEventListener("msg", (ev) => { const d = JSON.parse(ev.data); hydrate(d); });
  es.addEventListener("status", (ev) => { const d = JSON.parse(ev.data); if (S.msgs.has(d.id)) note(d.id, { status: d.status }); else hydrate(d); });
  es.addEventListener("read", (ev) => { const d = JSON.parse(ev.data); S.rpccache.delete(d.msg); loadReceipts(d.msg); });
  es.addEventListener("presence", () => debPres());
  es.addEventListener("token", () => { if (S.tab === "admin") renderTokenTable(); });
  es.addEventListener("group", () => { refreshChans(); });
}
// claude M4 B2: hydrate via receipts, NOT read. read(for=self) INSERTs a
// reads row (§5 marking read) — rendering a frame is not reading it. With
// read it marked EVERY message that arrived while the tab was open as read by
// the human (probe: an unopened ask showed readers=[human] 2 s after post),
// which lies to every sender's receipts and empties the human's --unread
// inbox. receipts returns the same row (+receipts) under the same canSee gate
// and writes nothing.
async function hydrate(d) {
  const known = S.msgs.get(d.id);
  if (known && d.body === undefined) { Object.assign(known, d); if (d.status) note(d.id, { status: d.status }); return; }
  try {
    const m = await rpc("receipts", { id: d.id });
    S.rpccache.set(m.id, Promise.resolve(m));
    S.msgs.set(m.id, m); renderPane();
  } catch (e) { if (known) return; S.msgs.set(d.id, Object.assign({ body: "(no permission to read)" }, d)); renderPane(); }
}
let presT = null;
function debPres() { clearTimeout(presT); presT = setTimeout(async () => {
  try { S.agents = await rpc("who", { all: true }); S.active = new Set((await rpc("who", {})).map((a) => a.id)); } catch {}
  renderPres();
}, 800); }
function renderPres() {
  const box = $("pres"); box.textContent = "";
  for (const a of S.agents) {
    const on = S.active.has(a.id);
    box.appendChild(el("span", "ag " + (on ? "on" : ""), (on ? "● " : "○ ") + a.id + (a.role && a.role !== a.id ? "  (" + a.role + ")" : "")));
  }
}

/* ---------- composer ---------- */
for (const t of ["note", "ask", "reply", "ack", "announce", "handoff", "result", "status", "rfc"]) $("ctype").appendChild(el("option", null, t));
$("cpost").onclick = async () => {
  const body = $("cbody").value;
  if (!body.trim()) return setS("body is empty", 1);
  const p = { type: $("ctype").value, body };
  // claude M4 B1: core post() requires "to" BEFORE the dm branch runs ("post
  // requires --from and --to"), and dm mode accepts to === peer. Every DM
  // composed in the UI was rejected as usage without it.
  if (S.selKind === "dm") {
    const peer = dmPeer();
    if (!peer) return setS("read-only: you are not a party to " + S.sel, 1);
    p.dm = peer; p.to = peer;
  }
  else {
    const to = $("cto").value.trim(); if (!to) return setS("to is required", 1);
    p.to = to;
    if ($("cchan").value.trim()) p.channel = $("cchan").value.trim();
  }
  if ($("csubj").value.trim()) p.subject = $("csubj").value.trim();
  // claude M4 M-d: a reply carries thread AND re (AGENTS.md "always carry
  // --thread and --re"). re alone starts a NEW thread (thread = own id), so
  // "thread --id parent" never showed UI replies.
  if (S.replyTo) { p.re = S.replyTo; const par = S.msgs.get(S.replyTo); p.thread = (par && par.thread) || S.replyTo; }
  try {
    const r = await rpc("post", p);
    setS("posted " + r.id + " → #" + r.channel);
    $("cbody").value = ""; $("csubj").value = ""; S.replyTo = null; $("cbody").placeholder = "message body";
  } catch (e) { setS("post failed: " + e.message, 1); }
};

/* ---------- tabs ---------- */
for (const b of document.querySelectorAll(".tab")) b.onclick = () => {
  document.querySelectorAll(".tab").forEach((n) => n.classList.remove("on")); b.classList.add("on");
  S.tab = b.dataset.tab;
  const chat = S.tab !== "admin";
  $("tabchat").style.display = chat ? "" : "none";
  $("pane").style.display = chat ? "" : "none";
  $("composer").style.display = chat ? "" : "none";
  $("admin").style.display = chat ? "none" : "";
  if (!chat) renderAdmin();
};

/* ---------- admin ---------- */
// claude M4 B3: the old renderAdmin() wiped #admin (textContent = "") right
// after token.create (and again on the tokens_ai event), so the "shown ONCE"
// secret was shown ZERO times (probe: .once count 0 for 1.5 s while the row
// was minted server-side): a live credential nobody holds. Now the form is
// built ONCE, only the table re-renders (token events / revoke), and the
// minted secret lives in S.minted until the admin dismisses it.
let adminBuilt = false;
function renderAdmin() {
  if (!adminBuilt) { buildAdmin(); adminBuilt = true; }
  renderMinted();
  renderTokenTable();
}
function renderMinted() {
  const box = $("minted"); if (!box) return; box.textContent = "";
  if (!S.minted) return;
  const d = el("div", "once");
  d.appendChild(el("div", null, "new token for " + S.minted.agent + " (prefix " + S.minted.prefix + ") — copy it now, it is never shown again:"));
  d.appendChild(el("div", "tok", S.minted.token));
  const cb = el("button", "mini", "copy"); cb.onclick = () => navigator.clipboard && navigator.clipboard.writeText(S.minted.token).then(() => { cb.textContent = "copied"; });
  const dx = el("button", "mini", "dismiss"); dx.onclick = () => { S.minted = null; renderMinted(); };
  d.appendChild(cb); d.appendChild(document.createTextNode(" ")); d.appendChild(dx);
  box.appendChild(d);
}
async function renderTokenTable() {
  const box = $("tktable"); if (!box) return;
  let tk; try { tk = await rpc("token.list", {}); } catch (e) { box.textContent = ""; box.appendChild(el("p", null, "token.list: " + e.message)); return; }
  const tbl = el("table");
  const trh = el("tr");
  for (const c of ["#", "agent", "kind", "prefix", "label", "scopes", "last used", "state", ""]) trh.appendChild(el("th", null, c));
  tbl.appendChild(trh);
  for (const t of tk.tokens) {
    const tr = el("tr");
    tr.appendChild(el("td", null, String(t.id)));
    tr.appendChild(el("td", null, t.agentId));
    tr.appendChild(el("td", null, t.kind));
    tr.appendChild(el("td", "mono", t.prefix));
    tr.appendChild(el("td", null, t.label || "—"));
    tr.appendChild(el("td", "mono", (t.scopes || []).join(",")));
    tr.appendChild(el("td", "mono", t.last_used || "—"));
    tr.appendChild(el("td", t.revoked_at ? "rev" : "", t.revoked_at ? "revoked" : "live"));
    const td = el("td");
    if (!t.revoked_at) {
      const b = el("button", "mini", "revoke");
      b.onclick = async () => {
        if (!confirm("revoke token #" + t.id + " (" + t.agentId + ")?")) return;
        try { await rpc("token.revoke", { id: t.id }); renderTokenTable(); } catch (e) { alert("revoke: " + e.message); }
      };
      td.appendChild(b);
    }
    tr.appendChild(td); tbl.appendChild(tr);
  }
  box.textContent = ""; box.appendChild(tbl);
}
function buildAdmin() {
  const box = $("admin"); box.textContent = "";
  const once = el("div"); once.id = "minted"; box.appendChild(once);
  box.appendChild(el("h2", null, "Tokens"));
  const tb = el("div"); tb.id = "tktable"; box.appendChild(tb);
  const f = el("div"); f.style.marginTop = "14px";
  f.appendChild(el("h2", null, "Mint token"));
  const row = el("div", "row");
  const ag = el("input"); ag.placeholder = "agent id"; row.appendChild(ag);
  const kind = el("select"); for (const k of ["agent", "human"]) kind.appendChild(el("option", null, k)); row.appendChild(kind);
  const lab = el("input"); lab.placeholder = "label (optional)"; lab.style.maxWidth = "220px"; row.appendChild(lab);
  f.appendChild(row);
  const row2 = el("div", "row"); row2.style.marginTop = "8px";
  const SC = ["read:all", "read:dm", "post:as", "tokens:admin", "agents:admin"];
  const cks = {};
  for (const s of SC) { const l = el("label", "ck"); const c = el("input"); c.type = "checkbox"; l.appendChild(c); l.appendChild(document.createTextNode(s)); row2.appendChild(l); cks[s] = c; }
  const fc = el("label", "ck"); const fcb = el("input"); fcb.type = "checkbox"; fc.appendChild(fcb); fc.appendChild(document.createTextNode("force (bootstrap guard)")); row2.appendChild(fc);
  f.appendChild(row2);
  const go = el("button", null, "create"); go.style.marginTop = "8px"; f.appendChild(go);
  const err = el("div", "rev"); f.appendChild(err);
  go.onclick = async () => {
    err.textContent = "";
    const p = { agent: ag.value.trim(), kind: kind.value };
    if (lab.value.trim()) p.label = lab.value.trim();
    const sel = SC.filter((s) => cks[s].checked);
    if (sel.length) p.scopes = sel;
    if (fcb.checked) p.force = true;
    try {
      const r = await rpc("token.create", p);
      S.minted = { token: r.token, prefix: r.prefix, agent: r.agentId, label: p.label };
      ag.value = ""; lab.value = ""; for (const s of SC) cks[s].checked = false; fcb.checked = false;
      renderMinted(); renderTokenTable();
    } catch (e) { err.textContent = "create failed: " + e.message; }
  };
  box.appendChild(f);
}

/* ---------- start: probe an existing session cookie ---------- */
(async () => {
  // one raw call: 401 ⇒ show the login card; 200 ⇒ identity from x-comms-*
  // (§7: identity rides the first response's headers — zero extra RPC).
  try {
    const res = await fetch("/rpc", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: ++rpcid }) });
    if (!res.ok) return; // no session — login card stays
    S.me = res.headers.get("x-comms-agent") || "(unknown)";
    S.scopes = (res.headers.get("x-comms-scopes") || "").split(",").filter(Boolean);
    $("me").textContent = S.me; $("myscopes").textContent = S.scopes.join(", ");
    await boot();
  } catch { /* server unreachable — login card stays */ }
})();
</script>
</body></html>
`;

// CSP for the shell (served as a HEADER by mod.ts). The script hash is taken
// from UI_HTML itself, so editing the script can never desync the policy.
const INLINE_SCRIPT = /<script>([\s\S]*?)<\/script>/.exec(UI_HTML)![1];
export const UI_CSP = [
  "default-src 'none'",
  `script-src 'sha256-${createHash("sha256").update(INLINE_SCRIPT, "utf8").digest("base64")}'`,
  "style-src 'unsafe-inline'",
  "connect-src 'self'",
  "img-src 'self'",
  "base-uri 'none'",
  "form-action 'none'",
  "frame-ancestors 'none'",
].join("; ");
