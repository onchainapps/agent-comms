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
 * - CSP default-src 'self'; 'unsafe-inline' for the single-file app.
 * - CSRF (§8) is enforced server-side on cookie requests; this page sends
 *   Content-Type: application/json, which forces a preflight a foreign origin
 *   can never pass.
 */
export const UI_HTML = /* html */ `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'">
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
const S = { me:null, scopes:[], chans:[], agents:[], active:new Set(), msgs:new Map(), sel:null, selKind:null, stream:null, seq:0, epoch:"", rpccache:new Map() };
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
  loadHistory();
  openStream();
  if (S.scopes.includes("tokens:admin")) $("admintab").style.display = "";
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
$("out").onclick = async () => { try { await rpc("logout", {}); } catch {} location.reload(); };

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
    let label = "#" + c.name;
    try { label = "dm:" + (await rpc("dm.members", { channel: c.name })).members.join(" ↔ "); } catch {}
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
  document.querySelectorAll("#chans .chan,#dms .chan").forEach((n) => n.classList.remove("on"));
  refreshChans(); renderPane();
  if (kind === "dm") { $("cto").disabled = true; $("cchan").value = ""; dmPeerHint(); }
  else { $("cto").disabled = false; $("cchan").value = name === "general" ? "" : name; }
}
function dmPeerHint() {
  const m = S.sel.match(/^dm~([^~]+)~([^~]+)/);
  const peer = m && (m[1] === S.me ? m[2] : m[1]);
  $("pd").textContent = peer ? "DM → " + peer + " (posts use dm:" + peer + ")" : "";
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
  for (const m of rows) { pane.appendChild(msgNode(m)); loadReceipts(m.id); }
  pane.scrollTop = pane.scrollHeight;
}
async function loadReceipts(id) {
  const slot = document.querySelector('.rc[data-rc="' + CSS.escape(id) + '"]');
  if (!slot) return;
  if (!S.rpccache.has(id)) S.rpccache.set(id, rpc("receipts", { id }).catch(() => null));
  const r = await S.rpccache.get(id); if (!r) return;
  const cur = document.querySelector('.rc[data-rc="' + CSS.escape(id) + '"]'); if (!cur || cur !== slot) return;
  const rc = r.receipts || {};
  const seen = (rc.readers || []).map((x) => x.id + (x.at ? " ✓" : " (replied)")).join("  ");
  const pend = (rc.unread || []).join(", ");
  cur.textContent = "to: " + (rc.intended || []).join(",") + (seen ? "   seen: " + seen : "") + (pend ? "   pending: " + pend : "");
}

/* ---------- history + stream ---------- */
async function loadHistory() {
  let cursor, pages = 0;
  for (;;) {
    let page;
    try { page = await rpc("history", cursor ? { since: cursor, limit: 200 } : { limit: 200 }); }
    catch (e) {
      if (e.bus === "resync" && e.data.epoch && !cursor) { cursor = e.data.epoch + "." + e.data.floor; continue; }
      if (e.bus === "forbidden") { setS("history: " + e.message + " (showing live stream only)"); return; }
      setS("history failed: " + e.message, 1); return;
    }
    for (const m of page.rows) S.msgs.set(m.id, m);
    pages++;
    if (!page.hasMore || pages > 40) break;
    cursor = page.cursor;
  }
  renderPane();
}
function openStream() {
  const scope = S.scopes.includes("read:all") && S.scopes.includes("read:dm") ? "all" : S.scopes.includes("read:all") ? "all" : "mine";
  const es = new EventSource("/stream?scope=" + scope);
  S.stream = es;
  es.onopen = () => { $("conn").textContent = "stream: live"; $("conn").style.color = "var(--ok)"; };
  es.onerror = () => { $("conn").textContent = "stream: reconnecting…"; $("conn").style.color = "var(--warn)"; };
  es.addEventListener("hello", (ev) => { const d = JSON.parse(ev.data); S.epoch = d.epoch; S.seq = d.seq; });
  es.addEventListener("resync", () => { S.msgs.clear(); renderPane(); loadHistory(); });
  es.addEventListener("msg", (ev) => { const d = JSON.parse(ev.data); hydrate(d); });
  es.addEventListener("status", (ev) => { const d = JSON.parse(ev.data); if (S.msgs.has(d.id)) note(d.id, { status: d.status }); else hydrate(d); });
  es.addEventListener("read", (ev) => { const d = JSON.parse(ev.data); S.rpccache.delete(d.msg); loadReceipts(d.msg); });
  es.addEventListener("presence", () => debPres());
  es.addEventListener("token", () => { if (S.tab === "admin") renderAdmin(); });
  es.addEventListener("group", () => { refreshChans(); });
}
async function hydrate(d) {
  const known = S.msgs.get(d.id);
  if (known && d.body === undefined) { Object.assign(known, d); return; }
  try {
    const m = await rpc("read", { for: S.me, id: d.id });
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
  if (S.selKind === "dm") { const m = S.sel.match(/^dm~([^~]+)~([^~]+)/); p.dm = m && (m[1] === S.me ? m[2] : m[1]); }
  else {
    const to = $("cto").value.trim(); if (!to) return setS("to is required", 1);
    p.to = to;
    if ($("cchan").value.trim()) p.channel = $("cchan").value.trim();
  }
  if ($("csubj").value.trim()) p.subject = $("csubj").value.trim();
  if (S.replyTo) p.re = S.replyTo;
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
async function renderAdmin() {
  const box = $("admin"); box.textContent = "";
  const h = el("h2", null, "Tokens"); box.appendChild(h);
  let tk; try { tk = await rpc("token.list", {}); } catch (e) { box.appendChild(el("p", null, "token.list: " + e.message)); return; }
  const tbl = el("table");
  const trh = el("tr");
  for (const c of ["#", "agent", "kind", "prefix", "scopes", "last used", "state", ""]) trh.appendChild(el("th", null, c));
  tbl.appendChild(trh);
  for (const t of tk.tokens) {
    const tr = el("tr");
    tr.appendChild(el("td", null, String(t.id)));
    tr.appendChild(el("td", null, t.agentId));
    tr.appendChild(el("td", null, t.kind));
    tr.appendChild(el("td", "mono", t.prefix));
    tr.appendChild(el("td", "mono", (t.scopes || []).join(",")));
    tr.appendChild(el("td", "mono", t.last_used || "—"));
    tr.appendChild(el("td", t.revoked_at ? "rev" : "", t.revoked_at ? "revoked" : "live"));
    const td = el("td");
    if (!t.revoked_at) {
      const b = el("button", "mini", "revoke");
      b.onclick = async () => { try { await rpc("token.revoke", { id: t.id }); renderAdmin(); } catch (e) { alert("revoke: " + e.message); } };
      td.appendChild(b);
    }
    tr.appendChild(td); tbl.appendChild(tr);
  }
  box.appendChild(tbl);
  const f = el("div"); f.style.marginTop = "14px";
  f.appendChild(el("h2", null, "Mint token"));
  const row = el("div", "row");
  const ag = el("input"); ag.placeholder = "agent id"; row.appendChild(ag);
  const kind = el("select"); for (const k of ["agent", "human"]) kind.appendChild(el("option", null, k)); row.appendChild(kind);
  f.appendChild(row);
  const row2 = el("div", "row"); row2.style.marginTop = "8px";
  const SC = ["read:all", "read:dm", "post:as", "tokens:admin", "agents:admin"];
  const cks = {};
  for (const s of SC) { const l = el("label", "ck"); const c = el("input"); c.type = "checkbox"; l.appendChild(c); l.appendChild(document.createTextNode(s)); row2.appendChild(l); cks[s] = c; }
  const fc = el("label", "ck"); const fcb = el("input"); fcb.type = "checkbox"; fc.appendChild(fcb); fc.appendChild(document.createTextNode("force (bootstrap guard)")); row2.appendChild(fc);
  f.appendChild(row2);
  const go = el("button", null, "create"); go.style.marginTop = "8px"; f.appendChild(go);
  const out = el("div"); f.appendChild(out);
  go.onclick = async () => {
    out.textContent = "";
    const p = { agent: ag.value.trim(), kind: kind.value };
    const sel = SC.filter((s) => cks[s].checked);
    if (sel.length) p.scopes = sel;
    if (fcb.checked) p.force = true;
    try {
      const r = await rpc("token.create", p);
      const d = el("div", "once"); d.textContent = r.token + "   (prefix " + r.prefix + " — shown ONCE)";
      out.appendChild(d);
      const cb = el("button", "mini", "copy"); cb.onclick = () => navigator.clipboard && navigator.clipboard.writeText(r.token);
      out.appendChild(cb);
      renderAdmin();
    } catch (e) { out.appendChild(el("div", "rev", "create failed: " + e.message)); }
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
