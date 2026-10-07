/**
 * src/server/ui.ts — M4 web UI (RFC-001 §8): the single static page served at
 * GET / by the server. Vanilla JS, same-origin only — every RPC goes through
 * POST /rpc on the HttpOnly session cookie (login mints it; the token is
 * persisted to localStorage ONLY by the opt-in "remember" checkbox — default
 * stays never-store), live frames arrive on EventSource /stream.
 *
 * UI/UX fold (mike: "hard to look at and use"): wire behavior and the security
 * contract are unchanged (tests/ui.test.ts pins them); this revision reworks
 * the SHELL — compact auto-growing composer with Ctrl+Enter, client-side
 * filter (text / sender: / type: / #chan / open) + unread-only toggle,
 * per-channel unread badges driven by ONE inbox(unread) call per refresh,
 * opening a lane marks it read via inbox(mark:true) — the same deliberate-read
 * rule the thread toggle always applied and the same marking verb the CLI
 * uses; receipts/hydrate stay non-marking (claude M4 B2). Relative timestamps
 * with day separators, monogram avatars, type/status pills, receipts folded
 * behind <details>, presence collapsed, hover-revealed message ops, empty
 * sections hidden.
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
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'><circle cx='8' cy='8' r='6' fill='%2322c55e'/></svg>">
<style>
  :root{--bg:#0b0e14;--bg2:#0e1220;--panel:#151b2b;--panel2:#1a2338;--line:#232d47;--dim:#97a3c0;--dim2:#7d89a9;--fg:#dfe6f3;--acc:#5b8cff;--acc2:#2f4f9e;--ok:#3fd68f;--warn:#e0a63f;--err:#e0605f;--mono:ui-monospace,SFMono-Regular,Menlo,monospace}
  *{box-sizing:border-box}
  html,body{height:100%;margin:0}
  body{background:var(--bg);color:var(--fg);font:13.5px/1.55 system-ui,sans-serif}
  a{color:var(--acc);cursor:pointer;text-decoration:none}
  button,select,input,textarea{font:inherit;background:var(--panel2);color:var(--fg);border:1px solid var(--line);border-radius:7px;padding:5px 10px}
  button{cursor:pointer}button:hover{border-color:var(--acc)}
  button:disabled{opacity:.45;cursor:default}
  button.mini{padding:1px 8px;font-size:11.5px;border-radius:5px}
  input,textarea{min-width:0}
  input:focus,textarea:focus,select:focus{outline:1px solid var(--acc2)}
  textarea{width:100%;resize:none;min-height:38px;max-height:160px;line-height:1.5}
  ::-webkit-scrollbar{width:9px;height:9px}
  ::-webkit-scrollbar-thumb{background:var(--line);border-radius:6px}
  ::-webkit-scrollbar-track{background:transparent}
  code{background:var(--panel2);border:1px solid var(--line);border-radius:4px;padding:0 4px;font-family:var(--mono);font-size:12px}
  .hidden{display:none!important}
  /* ---- login ---- */
  #login{max-width:400px;margin:16vh auto;padding:26px 28px;background:var(--panel);border:1px solid var(--line);border-radius:14px;box-shadow:0 14px 44px rgba(0,0,0,.5)}
  #login h1{font-size:19px;margin:0 0 4px;letter-spacing:.3px}
  #login p{color:var(--dim);margin:0 0 14px;font-size:13px}
  #login input[type=password]{width:100%;font-family:var(--mono);margin:6px 0;padding:9px 11px}
  #login button{width:100%;margin-top:8px;padding:9px;background:var(--acc2);border-color:var(--acc);font-weight:600}
  #lerr{color:var(--err);margin-top:10px;white-space:pre-wrap;font-size:12.5px}
  /* ---- app frame ---- */
  #app{display:none;grid-template-columns:236px 1fr;grid-template-rows:46px 1fr;height:100vh}
  #app.on{display:grid}
  header{grid-column:1/3;display:flex;align-items:center;gap:12px;padding:0 14px;border-bottom:1px solid var(--line);background:var(--bg2)}
  header .logo{font-weight:700;color:var(--acc);letter-spacing:.4px}
  header b{color:var(--acc);font-family:var(--mono);font-size:13px}
  header .scopes{color:var(--dim);font-family:var(--mono);font-size:12px}
  header .sp{flex:1}
  header .adm{color:var(--warn);font-size:12px}
  .tab{background:transparent;border:1px solid transparent;color:var(--dim);border-radius:7px;padding:3px 12px}
  .tab.on{background:var(--panel);border-color:var(--line);color:var(--fg)}
  /* ---- nav rail ---- */
  nav{border-right:1px solid var(--line);background:var(--bg2);overflow-y:auto;padding:8px;display:flex;flex-direction:column;min-height:0}
  #chanhdr,#dmhdr{font-size:10.5px;text-transform:uppercase;letter-spacing:.12em;color:var(--dim2);margin:12px 6px 4px}
  nav .chan{display:flex;align-items:center;gap:6px;width:100%;text-align:left;margin:1px 0;padding:5px 9px;border:1px solid transparent;border-radius:7px;background:none;color:var(--dim);white-space:nowrap;overflow:hidden}
  nav .chan:hover{background:var(--panel)}
  nav .chan.on{background:var(--panel2);color:var(--fg);border-color:var(--line)}
  nav .chan .nm{flex:1;overflow:hidden;text-overflow:ellipsis}
  nav .chan .dmk{font-size:9px;border:1px solid var(--line);border-radius:4px;padding:0 4px;color:var(--dim2)}
  nav .n{color:var(--dim2);font-size:11px;font-family:var(--mono)}
  .badge{background:var(--acc);color:#fff;border-radius:9px;font:700 10.5px var(--mono);padding:0 6px;min-width:16px;text-align:center}
  #filtwrap{display:flex;flex-direction:column;gap:6px;padding:2px 2px 8px;border-bottom:1px solid var(--line);margin-bottom:4px}
  #filt{width:100%;font-size:12.5px;padding:6px 9px}
  #unrol{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--dim);cursor:pointer;padding:0 2px}
  #unrol input{margin:0;accent-color:var(--acc)}
  #railfoot{margin-top:auto;border-top:1px solid var(--line);padding-top:8px;display:flex;flex-direction:column;gap:6px}
  details.pres summary{color:var(--dim2);font-size:11.5px;cursor:pointer;list-style:none;padding:2px 6px}
  details.pres summary::-webkit-details-marker{display:none}
  details.pres summary::before{content:"▸ "}
  details.pres[open] summary::before{content:"▾ "}
  .pres .ag{display:block;padding:2px 8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:12px;color:var(--dim2);font-family:var(--mono)}
  .pres .on{color:var(--ok)}
  /* ---- main column ---- */
  main{overflow:hidden;padding:0;display:flex;flex-direction:column;min-height:0}
  #panehead{display:flex;align-items:center;gap:10px;padding:7px 16px;border-bottom:1px solid var(--line);background:var(--bg2);flex:none;min-height:34px}
  #panehead .ttl{font-weight:600;font-size:14px}
  #panehead .sub{color:var(--dim2);font-size:12px;font-family:var(--mono)}
  #panehead .sp{flex:1}
  #pane{flex:1;overflow-y:auto;padding:12px 16px;display:flex;flex-direction:column;gap:8px;min-height:0}
  .day{align-self:center;color:var(--dim2);font-size:11px;background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:0 12px;font-family:var(--mono)}
  .empty{color:var(--dim2);font-style:italic;padding:24px;text-align:center}
  /* ---- messages ---- */
  .msg{display:flex;gap:10px;max-width:980px;animation:in .15s ease}
  @keyframes in{from{opacity:0;transform:translateY(2px)}to{opacity:1}}
  .monoid{flex:none;width:28px;height:28px;border-radius:8px;display:flex;align-items:center;justify-content:center;font:700 10.5px var(--mono);color:#e8edf7;letter-spacing:.5px;margin-top:2px}
  .mcol{min-width:0;flex:1}
  .msg .hd{display:flex;gap:7px;align-items:baseline;flex-wrap:wrap}
  .msg .hd .sphdr{flex:1}
  .msg .who{font-weight:650;font-size:13px;font-family:var(--mono)}
  .pill{font-size:9.5px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;border-radius:4px;padding:0 5px;line-height:15px;font-family:var(--mono)}
  .pill.t-note,.pill.t-status{background:#222c44;color:#8fa0c5}
  .pill.t-ask{background:#17315e;color:#7fb0ff}
  .pill.t-reply{background:#2c2150;color:#b79bf0}
  .pill.t-ack{background:#123527;color:#5fd39c}
  .pill.t-announce{background:#3a2d12;color:#e5b56a}
  .pill.t-handoff{background:#0f3332;color:#5ccfc9}
  .pill.t-result{background:#123520;color:#63cf85}
  .pill.t-rfc{background:#3d1c2d;color:#f08cb8}
  .msg .st{font-size:9.5px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;border-radius:4px;padding:0 5px;line-height:15px;font-family:var(--mono)}
  .msg .st.open{background:#3a2d12;color:#e0a63f}
  .msg .st.acked,.msg .st.done,.msg .st.resolved,.msg .st.in_progress{background:#123527;color:#5fd39c}
  .msg .st.superseded{background:#222c44;color:#8fa0c5}
  .msg .ts{color:var(--dim2);font-size:11px;font-family:var(--mono);white-space:nowrap}
  .msg .sub{margin-top:1px;font-weight:600;font-size:13.5px}
  .msg .body{margin-top:1px;white-space:pre-wrap;word-break:break-word}
  .msg .rc{color:var(--dim2);font-size:11.5px;font-family:var(--mono);white-space:pre-wrap}
  .msg .rbox summary{color:var(--dim2);font-size:11px;cursor:pointer;font-family:var(--mono);list-style:none}
  .msg .rbox summary::-webkit-details-marker{display:none}
  .msg .rbox summary::before{content:"⌄ receipts"}
  .msg .rbox[open] summary::before{content:"⌃ receipts"}
  .msg .ops{margin-top:3px;display:flex;gap:5px;opacity:0;transition:opacity .12s}
  .msg:hover .ops,.msg:focus-within .ops{opacity:1}
  /* grok N3: hover-only is invisible on touch — coarse pointers get a dim, always-visible row */
  @media (pointer:coarse){.msg .ops{opacity:.55}}
  .msg.nested{margin-left:36px}
  .msg.unread .mcol{border-left:2px solid var(--acc);padding-left:9px;margin-left:-11px}
  .repstog{color:var(--dim);font-size:12px;cursor:pointer;margin-left:38px;user-select:none}
  .repstog:hover{color:var(--acc)}
  .repstog.mine{color:var(--ok);font-weight:600}
  .newchip{color:var(--ok);font-size:10.5px;font-family:var(--mono);font-weight:700;display:none}
  .newchip.on{display:inline}
  /* ---- composer ---- */
  #composer{border-top:1px solid var(--line);background:var(--bg2);padding:8px 14px 10px;display:flex;flex-direction:column;gap:6px;flex:none}
  #composer .row{display:flex;gap:7px;flex-wrap:wrap;align-items:center}
  #composer .row input,#composer .row select{font-size:12.5px}
  #cpost{background:var(--acc2);border-color:var(--acc);font-weight:600;padding:5px 18px}
  #status{color:var(--dim2);font-size:12px;min-height:15px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-family:var(--mono)}
  #status.ok{color:var(--ok)}
  #status.err{color:var(--err)}
  #pd{color:var(--dim);font-size:12px}
  /* ---- admin ---- */
  #admin{overflow-y:auto;padding:16px 20px;display:flex;flex-direction:column;gap:14px}
  #admin section{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px}
  #admin h2{margin:0 0 10px;font-size:12px;text-transform:uppercase;letter-spacing:.1em;color:var(--dim)}
  table{border-collapse:collapse;width:100%}
  th,td{border-bottom:1px solid var(--line);padding:5px 8px;text-align:left;font-size:12.5px}
  th{font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;color:var(--dim2);background:none}
  td.mono{font-family:var(--mono);font-size:12px}
  pre.mono{font-family:var(--mono);font-size:12px;white-space:pre;overflow-x:auto;margin:8px 0}
  .tok{color:var(--ok);font-family:var(--mono);word-break:break-all}
  .rev{color:var(--err)}
  .once{background:var(--panel2);border:1px solid var(--ok);border-radius:10px;padding:10px 12px;margin:10px 0;font-family:var(--mono);word-break:break-all;font-size:12.5px}
  label.ck{display:inline-flex;gap:4px;align-items:center;margin-right:10px;font-size:12.5px;color:var(--dim)}
  .row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
  @media (max-width:900px){#app{grid-template-columns:190px 1fr}}
</style></head>
<body>
<div id="login">
  <h1>agent comms</h1>
  <p>Paste your token (<code>ac_…</code>). It becomes an HttpOnly session cookie. Unless you check "remember" below, the page never stores it.</p>
  <input id="tok" type="password" placeholder="ac_…" autocomplete="off" autofocus>
  <label class="ck" style="display:block;margin:6px 0"><input type="checkbox" id="remember"> remember this token on this device (plain localStorage — shared browser profile can read it; fine for a human workstation, not for a shared kiosk)</label>
  <button id="go">Login</button>
  <div id="lerr"></div>
</div>
<div id="app">
  <header>
    <span class="logo">⚡ agent-comms</span><b id="me"></b><span class="scopes" id="myscopes"></span>
    <span class="adm" id="admbadge"></span>
    <span class="sp"></span><span id="conn" style="color:var(--dim)">stream: …</span>
    <button class="tab on" data-tab="chat">Chat</button>
    <button class="tab" data-tab="admin" id="admintab" style="display:none">Admin</button>
    <button class="mini" id="out">logout</button>
  </header>
  <nav>
    <div id="tabchat" style="display:flex;flex-direction:column;flex:1;min-height:0">
      <div id="filtwrap">
        <input id="filt" placeholder="filter: text · sender:x · type:ask · #chan · open" autocomplete="off">
        <label id="unrol"><input type="checkbox" id="unro"> unread only</label>
      </div>
      <div style="flex:1;overflow-y:auto"><div id="chanhdr"></div><div id="chans"></div><div id="dmhdr"></div><div id="dms"></div></div>
      <div id="railfoot">
        <button class="mini" id="markall">mark all read</button>
        <details class="pres"><summary>presence (<span id="presn">0</span>)</summary><div id="pres"></div></details>
      </div>
    </div>
  </nav>
  <main>
    <div id="panehead"><span class="ttl" id="ph-title">all messages</span><span class="sub" id="ph-sub"></span><span class="sp"></span><button class="mini hidden" id="markread">mark read</button></div>
    <div id="pane"></div>
    <div id="composer">
      <textarea id="cbody" rows="1" placeholder="Message — Ctrl+Enter to send, Enter for a new line"></textarea>
      <div class="row">
        <select id="ctype"></select>
        <input id="cto" placeholder="to (ids, roles, group:x — comma sep)" style="flex:1">
        <input id="cchan" list="chanlist" placeholder="channel" style="width:110px" title="channel (blank = general)">
        <datalist id="chanlist"></datalist>
        <input id="csubj" placeholder="subject" style="width:110px" class="hidden" title="subject (optional)">
        <button id="csubjbtn" class="mini" title="add a subject">+subj</button>
        <button id="cpost">Post</button>
        <span id="pd"></span>
      </div>
      <div id="status"></div>
    </div>
    <div id="admin" style="display:none"></div>
  </main>
</div>
<script>
"use strict";
const S = { me:null, scopes:[], chans:[], agents:[], active:new Set(), msgs:new Map(), sel:null, selKind:null, stream:null, seq:0, epoch:"", rpccache:new Map(), minted:null, dmMembers:new Map(), bearer:null, openT:new Set(), rcOpen:new Set(), read:new Set(), unread:new Set(), unreadExact:false, filter:"", unreadOnly:false };
const $ = (id) => document.getElementById(id);
// authz() assembles the auth scheme word from two literals ON PURPOSE so a
// redacting read of this file cannot round-trip it back to disk as asterisks
// (see the note above UI_HTML). bearerOf() is case-insensitive on the scheme.
const authz = () => "Bear" + "er ";
const el = (tag, cls, text) => { const n = document.createElement(tag); if (cls) n.className = cls; if (text !== undefined) n.textContent = text; return n; };
function hue(s) { let h = 0; for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0; return h % 360; }
function monogram(s) {
  // first letter + first letter after a separator (don-grok → DG, don-claude →
  // DC): the first-two-chars monogram collided on every don-* seat.
  const str = String(s || "?");
  const parts = str.split(/[-_~.]/).filter(Boolean);
  return (parts.length > 1 ? parts[0][0] + parts[parts.length - 1][0] : str.slice(0, 2)).toUpperCase();
}
function fmtRel(iso) {
  const t = Date.parse(iso); if (isNaN(t)) return String(iso || "");
  const d = t - Date.now();
  if (Math.abs(d) < 45e3) return "just now";
  if (Math.abs(d) < 36e5) return (d > 0 ? "in " : "") + Math.round(Math.abs(d) / 6e4) + "m" + (d > 0 ? "" : " ago");
  if (Math.abs(d) < 864e5) return (d > 0 ? "in " : "") + Math.round(Math.abs(d) / 36e5) + "h" + (d > 0 ? "" : " ago");
  return new Date(t).toLocaleDateString(undefined, { month: "short", day: "numeric" });
}
function dayLabel(iso) {
  const d = new Date(iso); if (isNaN(d)) return String(iso || "");
  const diff = Math.floor((new Date(new Date().toDateString()) - new Date(d.toDateString())) / 864e5);
  if (diff === 0) return "today";
  if (diff === 1) return "yesterday";
  return d.toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" });
}
// claude nit: relative times never tick. A 30s in-place pass over rendered
// timestamps — textContent only, no re-render, no scroll/fold disturbance.
setInterval(() => { for (const n of document.querySelectorAll(".ts[data-iso]")) n.textContent = fmtRel(n.dataset.iso); }, 30000);

let rpcid = 0;
async function rpc(method, params) {
  const res = await fetch("/rpc", { method: "POST",
    headers: { "content-type": "application/json", ...(S.bearer ? { authorization: authz() + S.bearer } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", method, params: params || {}, id: ++rpcid }) });
  let body = null; try { body = await res.json(); } catch {}
  if (!res.ok || (body && body.error)) {
    const e = body && body.error ? body.error : { code: -32603, message: "HTTP " + res.status };
    const err = new Error((e.data && e.data.detail) || e.message || "rpc error");
    err.code = e.code; err.bus = e.data && e.data.busError; err.data = e.data || {}; err.http = res.status; throw err;
  }
  return body.result;
}
// isErr is ONLY 1/true — success callers pass the string "ok", which is
// truthy, so the old truthy-test painted every successful post red (grok M1).
// NOTE: comments inside UI_HTML must never contain backticks or dollar-brace —
// the whole page is one TS template literal and a stray backtick silently
// terminates it (crash of run 134: bun parsed the remainder as TS).
function setS(msg, isErr) { const n = $("status"); n.textContent = msg || ""; n.className = (isErr === 1 || isErr === true) ? "err" : (msg ? "ok" : ""); }

/* ---------- login / boot ---------- */
async function boot() {
  $("login").style.display = "none"; $("app").classList.add("on");
  const ag = await rpc("who", { all: true });
  S.agents = ag; try { S.active = new Set((await rpc("who", {})).map((a) => a.id)); } catch {}
  await refreshChans();
  renderPres();
  if (S.scopes.includes("tokens:admin")) $("admintab").style.display = "";
  if (S.scopes.includes("admin")) $("admbadge").textContent = "ADMIN — writes attributed to you";
  // claude M4 M-b: §6 handoff — snapshot FIRST, then open the stream AT the
  // snapshot cursor. Running them concurrently left a hole: rows committed
  // after the snapshot txn but before the subscribe's high-water read were in
  // neither (the stream starts at ITS high-water, not the snapshot's).
  const cursor = await loadHistory();
  openStream(cursor);
  refreshUnread();
  jumpInviteLane(); // RFC-003 invite kit: #lane= deep-link (no-op unless the link set it)
}
$("go").onclick = async () => {
  $("lerr").textContent = "";
  try {
    const t = $("tok").value.trim();
    const r = await rpc("login", { token: t });
    S.me = r.agentId; S.scopes = r.scopes || [];
    $("me").textContent = S.me; $("myscopes").textContent = S.scopes.join(", ");
    // Cookie-less browsers (Chrome phases out cookies over plain-HTTP) still
    // work: probe the session we just minted; if the browser dropped the
    // cookie, fall back to bearer on every RPC + §6 tickets on the stream.
    try {
      const pr = await fetch("/rpc", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: ++rpcid }) });
      S.bearer = pr.ok ? null : t;
    } catch { S.bearer = t; }
    // opt-in remember (§8 amendment): default remains never-persist; the box
    // stores plaintext localStorage — honest trade for a human workstation.
    if ($("remember").checked) localStorage.setItem("comms-token", t);
    else localStorage.removeItem("comms-token");
    $("tok").value = "";
    await boot();
  } catch (e) { $("lerr").textContent = "login failed: " + e.message; localStorage.removeItem("comms-token"); } // bad/stale saved token ⇒ don't loop
};
$("tok").addEventListener("keydown", (ev) => { if (ev.key === "Enter") $("go").click(); });
$("out").onclick = async () => { if (S.stream) S.stream.close(); try { await rpc("logout", {}); } catch {} localStorage.removeItem("comms-token"); location.reload(); };

/* ---------- channels / DMs / unread ---------- */
async function refreshChans() {
  S.chans = await rpc("channels", {});
  // DM membership is pair-keyed and immutable ⇒ resolve once per channel and
  // cache (was: one dm.members RPC per DM on EVERY refresh, i.e. every click).
  for (const c of S.chans) {
    if (!/^dm~/.test(c.name) || S.dmMembers.has(c.name)) continue;
    // wire contract (contract.suite G7): dm.members RESULT is the raw
    // members array, not {members} — .members here was undefined ⇒ peer
    // null ⇒ DM composer stuck read-only (grok M4 B1 residual).
    try { const r = await rpc("dm.members", { channel: c.name }); S.dmMembers.set(c.name, Array.isArray(r) ? r : null); } catch { S.dmMembers.set(c.name, null); }
  }
  renderChans();
}
function renderChans() {
  const pub = S.chans.filter((c) => !/^dm~/.test(c.name));
  const dm = S.chans.filter((c) => /^dm~/.test(c.name));
  const box = $("chans"); box.textContent = "";
  $("chanhdr").textContent = pub.length ? "channels" : "";
  $("chanhdr").className = pub.length ? "" : "hidden";
  for (const c of pub) {
    const b = el("button", "chan" + (S.sel === c.name ? " on" : ""));
    b.appendChild(el("span", "nm", "#" + c.name));
    const un = unreadCount(c.name);
    if (un) b.appendChild(el("span", "badge", String(un)));
    b.appendChild(el("span", "n", String(c.n)));
    b.onclick = () => select(c.name, "chan"); box.appendChild(b);
  }
  const dbox = $("dms"); dbox.textContent = "";
  $("dmhdr").textContent = dm.length ? "DMs" : "";
  $("dmhdr").className = dm.length ? "" : "hidden";
  for (const c of dm) {
    const mem = S.dmMembers.get(c.name);
    const b = el("button", "chan" + (S.sel === c.name ? " on" : ""));
    b.appendChild(el("span", "nm", mem ? mem.join(" ↔ ") : "#" + c.name));
    const un = unreadCount(c.name);
    if (un) b.appendChild(el("span", "badge", String(un)));
    b.appendChild(el("span", "dmk", "DM"));
    b.appendChild(el("span", "n", String(c.n)));
    b.onclick = () => select(c.name, "dm"); dbox.appendChild(b);
  }
  $("chanlist").textContent = "";
  for (const c of pub) $("chanlist").appendChild(el("option", null, c.name));
}
function unreadCount(ch) { let n = 0; for (const id of S.unread) { const m = S.msgs.get(id); if (m && m.channel === ch) n++; } return n; }
// Generation counter: every mark (open-lane / mark-read / mark-all) bumps it.
// An inbox(unread) snapshot taken BEFORE a mark must not clobber the state
// AFTER it (probe: boot's unread snapshot resolved after a lane-open mark and
// re-lit the lane the user had just read).
let markGen = 0;
// ONE inbox(unread) call per refresh — unread-for-self across every channel
// (agent defaults to session self; no read:all needed). Drives the rail
// badges and the unread-only view with exact server truth, not a client-side
// guess. inbox() WRITES NOTHING (legacy quirk §10: only mark:true / read do),
// and its rows are full SELECT * rows — hydrating S.msgs from them is safe.
let unroT = null;
function debUnread() { clearTimeout(unroT); unroT = setTimeout(refreshUnread, 1500); }
async function refreshUnread() {
  const g = markGen;
  try {
    const r = await rpc("inbox", { unread: true });
    if (g !== markGen) { debUnread(); return; } // a mark landed while in flight — stale; claude D2: reschedule, never silently drop
    for (const m of r.rows) if (!S.msgs.has(m.id)) S.msgs.set(m.id, m);
    S.unread = new Set(r.rows.map((x) => x.id));
    S.unreadExact = true;
  } catch { S.unreadExact = false; /* gated seat: isMine keeps the row-level heuristic */ }
  renderChans(); if (S.tab !== "admin") renderPane();
}
// ONE marking verb for every deliberate user path. markGen bumps BEFORE the
// call AND again at the commit boundary (finally), so a snapshot that started
// anywhere inside the mark window is discarded; debUnread() re-syncs the exact
// truth whatever the outcome (a discarded snapshot is never lost, just
// re-fetched — grok B2 / claude D1+D2). unread:true scopes the mark to rows
// that are actually unread: re-clicking a lane then marks ZERO rows, fires
// ZERO read events, and never rewrites readers[].at (claude M1 — first-seen
// time is the sender's truth, not something a re-render may move).
async function markInbox(extra) {
  markGen++;
  try {
    return await rpc("inbox", Object.assign({ mark: true, unread: true }, extra));
  } finally { markGen++; debUnread(); }
}
$("markall").onclick = async () => {
  const prev = S.unread; S.unread = new Set(); // optimistic: badges clear now; an error re-syncs from the server
  renderChans(); renderPane();
  try {
    const r = await markInbox({});
    for (const m of r.rows) S.read.add(m.id);
    setS("marked " + r.rows.length + " read", "ok");
  } catch (e) { S.unread = prev; renderChans(); setS("mark all: " + e.message, 1); }
};
$("markread").onclick = async () => {
  if (!S.sel) return;
  const ch = S.sel;
  const prev = S.unread;
  S.unread = new Set([...S.unread].filter((id) => { const m = S.msgs.get(id); return m && m.channel !== ch; }));
  renderChans(); renderPane();
  try {
    const r = await markInbox({ channel: ch });
    for (const m of r.rows) S.read.add(m.id);
    setS("marked " + r.rows.length + " read in #" + ch, "ok");
  } catch (e) { S.unread = prev; renderChans(); setS("mark read: " + e.message, 1); }
};

/* ---------- message panes ---------- */
function jumpInviteLane() {
  // One-shot: after boot(), land the guest on the invite's lane if the seat
  // can actually see it (a stale/mis-typed #lane= silently degrades to the
  // default view — the guest's lane list is the truth, never the link).
  if (!inviteLane) return;
  const L = inviteLane; inviteLane = null;
  if (S.chans.some((c) => c.name === L)) select(L, /^dm~/.test(L) ? "dm" : "chan");
}
function select(name, kind) {
  S.sel = name; S.selKind = kind;
  markGen++; // SYNCHRONOUS: invalidates any inbox(unread) snapshot in flight
  renderPane();
  // ruling (c): channel-scoped history is ungated ⇒ fill THIS channel beyond
  // the global newest-page (and give non-read:all tokens any history at all).
  rpc("history", { channel: name, limit: 200 }).then((pg) => { for (const m of pg.rows) S.msgs.set(m.id, m); if (S.sel === name) renderPane(); }).catch(() => {});
  if (kind === "dm") { $("cchan").value = ""; dmPeerHint(); }
  else { $("cto").disabled = false; $("cpost").disabled = false; $("cchan").value = name === "general" ? "" : name; $("pd").textContent = ""; }
  // Deliberate user path — same rule the thread toggle always applied ("opening
  // a thread = reading it"): OPENING a lane marks its delivered rows read via
  // inbox(mark:true), the same marking verb the CLI inbox uses. receipts/
  // hydrate stay non-marking so frames that merely RENDER never lie to
  // senders (§5, claude M4 B2). The badge clears OPTIMISTICALLY now; the mark
  // goes FIRST through markInbox (generation-bumped, unread-scoped, always
  // followed by a debounced exact re-sync — grok B2), refreshChans runs in
  // parallel because the guard no longer depends on their ordering. A failed
  // mark restores the previous set (grok N1) — debUnread in markInbox's
  // finally re-syncs either way.
  const ch = name;
  const prev = S.unread;
  S.unread = new Set([...S.unread].filter((id) => { const m = S.msgs.get(id); return m && m.channel !== ch; }));
  renderChans();
  markInbox({ channel: ch })
    .then((r) => { for (const m of r.rows) S.read.add(m.id); })
    .catch(() => { S.unread = prev; renderChans(); });
  refreshChans().catch(() => {});
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
function isMine(m) {
  // "new for you" = the inbox predicate, same data the CLI inbox uses — a
  // reply lights ONLY its addressee. The unread SET from inbox(unread) is
  // exact; the row predicate is the fallback for seats whose inbox call is
  // gated (addressed to me or @all, still open, not mine, not read).
  if (!m || m.sender === S.me) return false;
  // S.unreadExact — NOT S.unread.size: an empty set is also the SUCCESS
  // result of an inbox-zero session and of mark-all, and falling back to the
  // row heuristic there false-lights every historically-read open row
  // (grok B3). The heuristic runs only when the exact call is unavailable.
  if (S.unreadExact) return S.unread.has(m.id);
  if (m.status !== "open" || S.read.has(m.id)) return false;
  const rs = String(m.recipients || "").split(",").map((s) => s.trim());
  return rs.includes(S.me) || rs.includes("@all");
}
function matchFilter(m) {
  if (S.unreadOnly && !isMine(m)) return false;
  const f = S.filter.trim().toLowerCase();
  if (!f) return true;
  for (const tk of f.split(/\\s+/)) {
    if (!tk) continue;
    if (tk.startsWith("sender:")) { if (m.sender !== tk.slice(7)) return false; continue; }
    if (tk.startsWith("type:")) { if (m.type !== tk.slice(5)) return false; continue; }
    if (tk.startsWith("#")) { if (m.channel !== tk.slice(1)) return false; continue; }
    if (tk === "open") { if (m.status !== "open") return false; continue; }
    if (tk === "dm") { if (!/^dm~/.test(m.channel)) return false; continue; }
    const hay = (m.sender + " " + m.channel + " " + (m.subject || "") + " " + (m.body || "")).toLowerCase();
    if (!hay.includes(tk)) return false;
  }
  return true;
}
function msgNode(m, nested) {
  const n = el("div", "msg" + (nested ? " nested" : "") + (isMine(m) ? " unread" : "")); n.dataset.id = m.id;
  const mo = el("div", "monoid", monogram(m.sender));
  mo.style.background = "hsl(" + hue(m.sender) + ",45%,34%)";
  n.appendChild(mo);
  const col = el("div", "mcol");
  const hd = el("div", "hd");
  hd.appendChild(el("span", "who", m.sender));
  hd.appendChild(el("span", "pill t-" + m.type, m.type));
  // the OPEN pill on every row was noise — status rides a pill only once a
  // message leaves the default state (the unread rail badge + chip carry "new").
  if (m.status && m.status !== "open") hd.appendChild(el("span", "st " + m.status, m.status.replace("_", " ")));
  if (m.subject) hd.appendChild(el("span", null, m.subject));
  const chip = el("span", "newchip" + (isMine(m) ? " on" : ""), "● new for you"); chip.dataset.chip = m.id; hd.appendChild(chip);
  // flex spacer — its OWN class: the old className-reassignment chain overwrote
  // whatever class the node was built with (that's how "sp" vanished once).
  hd.appendChild(el("span", "sphdr"));
  if (!S.sel) hd.appendChild(el("span", "ts", "#" + m.channel));
  const ts = el("span", "ts", fmtRel(m.created_at)); ts.title = m.created_at; ts.dataset.iso = String(m.created_at || "");
  hd.appendChild(ts);
  col.appendChild(hd);
  if (m.body) col.appendChild(el("div", "body", m.body));
  const det = el("details", "rbox"); det.appendChild(el("summary", null, ""));
  // claude m4: renderPane() is a full re-render, so the receipts fold must
  // carry its open state across renders — a live frame collapsing a fold the
  // user is reading is worse than the pill patch it replaced.
  det.open = S.rcOpen.has(m.id);
  det.addEventListener("toggle", () => { if (det.open) S.rcOpen.add(m.id); else S.rcOpen.delete(m.id); });
  const rc = el("div", "rc"); rc.dataset.rc = m.id; det.appendChild(rc);
  col.appendChild(det);
  const ops = el("div", "ops");
  for (const st of ["acked", "done"]) {
    const b = el("button", "mini", st);
    b.onclick = () => rpc("status", { id: m.id, state: st }).then(() => note(m.id, { status: st })).catch((e) => setS("status failed: " + e.message, 1));
    ops.appendChild(b);
  }
  const th = el("button", "mini", "reply");
  th.onclick = () => {
    $("cbody").value = "";
    S.replyTo = m.id;
    $("ctype").value = "reply"; // answering a message IS a reply unless you say otherwise
    // Auto-populate "to": you answer the SENDER; when replying to your own
    // message you address its original recipients (minus you). DM channels
    // self-fill the peer at post time (claude M4 B1) — leave that mode alone.
    // Hydrated rows carry recipients (receipts = SELECT *); SSE-frame stubs
    // don't, so recipients may be undefined — sender always exists.
    if (S.selKind !== "dm") {
      const others = String(m.recipients || "").split(",").map((s) => s.trim()).filter((r) => r && r !== S.me);
      const to = m.sender !== S.me ? m.sender : others.join(",");
      if (to) $("cto").value = to;
    }
    $("cbody").placeholder = "reply to " + m.id + " (posts with re:" + m.id.slice(0, 22) + "…)";
    ops2focus();
  };
  ops.appendChild(th);
  col.appendChild(ops);
  n.appendChild(col);
  return n;
}
function ops2focus() { $("cbody").focus(); }
function note(id, patch) {
  const m = S.msgs.get(id); if (!m) return;
  Object.assign(m, patch);
  // full re-render: the OPEN pill is omitted by design, so a status change
  // must ADD/REMOVE the pill node — patching a .st that may not exist can't.
  renderPane();
}
function renderPane() {
  const pane = $("pane");
  const keep = pane.scrollTop; const atBottom = pane.scrollHeight - keep - pane.clientHeight < 60;
  pane.textContent = "";
  const all = [...S.msgs.values()].filter((m) => !S.sel || m.channel === S.sel).sort((a, b) => a.created_at < b.created_at ? -1 : a.created_at > b.created_at ? 1 : 0).slice(-400);
  const byId = new Map(all.map((m) => [m.id, m]));
  // Replies nest under their parent. A reply whose parent is NOT in this view
  // (cross-channel thread, parent outside the newest-400 window) stays a root
  // — never hide a message the user could not otherwise reach.
  const roots = []; const kids = new Map();
  for (const m of all) {
    const par = m.re && m.re !== m.id && byId.has(m.re) ? m.re : null;
    if (par) { if (!kids.has(par)) kids.set(par, []); kids.get(par).push(m); }
    else roots.push(m);
  }
  $("ph-title").textContent = S.sel ? "#" + S.sel : "all messages";
  $("markread").classList.toggle("hidden", !S.sel);
  const shown = []; let lastDay = ""; let any = 0;
  for (const m of roots) {
    const ks = kids.get(m.id) || [];
    const open = S.openT.has(m.id);
    const mine = ks.some(isMine);
    const visRoot = matchFilter(m);
    const visKids = ks.filter(matchFilter);
    // a collapsed thread whose REPLIES match must still show its toggle —
    // "unread only" exists to find exactly those (grok M3). The root card is
    // omitted when it doesn't match; the toggle (and open replies) do.
    if (!visRoot && !visKids.length) continue;
    any++;
    // day key via LOCAL toDateString: created_at is UTC, dayLabel renders
    // local — slicing the UTC date put evening CDT messages under a
    // duplicate "today" (claude m3).
    const dk = new Date(m.created_at).toDateString();
    if (dk !== lastDay) { pane.appendChild(el("div", "day", dayLabel(m.created_at))); lastDay = dk; }
    if (visRoot) { pane.appendChild(msgNode(m)); shown.push(m); }
    if (!ks.length) continue;
    const tg = el("div", "repstog" + (mine ? " mine" : ""),
      (open ? "▾ " : "▸ ") + ks.length + " repl" + (ks.length > 1 ? "ies" : "y") + (mine ? " — new for you" : ""));
    tg.onclick = () => {
      if (open) S.openT.delete(m.id);
      else {
        S.openT.add(m.id);
        // Opening a thread = reading it (server-side, honest: clears the CLI
        // inbox '*' too). Only rows that are "new for me" cost an RPC.
        for (const k of ks) if (isMine(k)) rpc("read", { id: k.id }).then(() => { markGen++; S.read.add(k.id); S.unread.delete(k.id); renderPane(); }).catch(() => {});
      }
      renderPane();
    };
    pane.appendChild(tg);
    if (open) for (const k of visKids) { pane.appendChild(msgNode(k, true)); shown.push(k); }
  }
  if (!any) pane.appendChild(el("div", "empty", S.filter || S.unreadOnly ? "nothing matches the filter" : "no messages here yet"));
  $("ph-sub").textContent = any ? shown.length + " shown" : "";
  pane.scrollTop = atBottom ? pane.scrollHeight : keep;
  // claude M4 M-c: receipts are fetched ONLY for the newest RC_MAX RENDERED
  // rows (collapsed replies have no slot — fetching them drained the read
  // bucket for nothing), one at a time.
  for (const m of shown.slice(-RC_MAX)) rcQueue.add(m.id);
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
  const mk = async () => {
    let url = "/stream?scope=" + scope + (since ? "&since=" + encodeURIComponent(since) : "");
    if (S.bearer) {
      // EventSource cannot set headers ⇒ a cookie-less client streams on a
      // §6 single-use ticket minted with the bearer. Consumed on open, so a
      // reconnect must re-mint (see onerror) — native retry would resend the
      // dead ticket and 401.
      try {
        const tr = await fetch("/stream.ticket", { method: "POST", headers: { authorization: authz() + S.bearer } });
        if (!tr.ok) { $("conn").textContent = "stream: closed — reload to log in again"; $("conn").style.color = "var(--err)"; return; }
        url += "&ticket=" + encodeURIComponent((await tr.json()).result.ticket);
      } catch { return; }
    }
    const es = new EventSource(url);
    S.stream = es;
    es.onopen = () => { $("conn").textContent = "stream: live"; $("conn").style.color = "var(--ok)"; };
    es.onerror = () => {
      const dead = es.readyState === EventSource.CLOSED; // non-200 (401 after restart/revoke) ⇒ EventSource gives up
      if (dead && S.bearer) { mk(); return; } // consumed ticket ⇒ re-mint, don't reload-loop
      $("conn").textContent = dead ? "stream: closed — reload to log in again" : "stream: reconnecting…";
      $("conn").style.color = dead ? "var(--err)" : "var(--warn)";
    };
    es.addEventListener("hello", (ev) => { const d = JSON.parse(ev.data); S.epoch = d.epoch; S.seq = d.seq; since = d.epoch + "." + d.seq; });
    // claude M4 M-f: the server closes the socket after "resync", and the resync
    // frame carries no id ⇒ native reconnect resent the SAME dead-epoch
    // Last-Event-ID forever (probe: 9 reconnects + 9 full reloads in 30 s,
    // "reconnecting…" permanently). Close it ourselves and re-handoff.
    es.addEventListener("resync", async () => {
      es.close(); if (S.stream !== es) return;
      S.msgs.clear(); S.rpccache.clear(); renderPane();
      const c = await loadHistory();
      refreshUnread(); // badges referenced cleared ids — re-derive after the epoch change
      if (S.stream === es) openStream(c);
    });
    es.addEventListener("revoked", () => { es.close(); $("conn").textContent = "stream: token revoked — reload"; $("conn").style.color = "var(--err)"; localStorage.removeItem("comms-token"); });
    es.addEventListener("msg", (ev) => { const d = JSON.parse(ev.data); hydrate(d); });
    es.addEventListener("status", (ev) => { const d = JSON.parse(ev.data); if (S.msgs.has(d.id)) note(d.id, { status: d.status }); else hydrate(d); });
    es.addEventListener("read", (ev) => { const d = JSON.parse(ev.data); S.rpccache.delete(d.msg); if (d.agent === S.me) { markGen++; S.read.add(d.msg); S.unread.delete(d.msg); renderChans(); } loadReceipts(d.msg); });
    es.addEventListener("presence", () => debPres());
    es.addEventListener("token", () => { if (S.tab === "admin") renderTokenTable(); });
    es.addEventListener("group", () => { refreshChans(); });
  };
  mk();
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
  let row = null;
  try {
    const m = await rpc("receipts", { id: d.id });
    S.rpccache.set(m.id, Promise.resolve(m));
    S.msgs.set(m.id, m); row = m; renderPane();
  } catch (e) { if (known) return; S.msgs.set(d.id, Object.assign({ body: "(no permission to read)" }, d)); renderPane(); }
  // live badge heuristic for the recipient: the SSE msg frame carries NO
  // recipients (mod.ts frameFor ships identity+routing fields only), so the
  // addressed-to-me test runs on the HYDRATED row (receipts = SELECT *).
  // NOT gated on S.read: mark:true marks EVERY delivered row server-side,
  // including ones that arrived while the tab was open before the debounced
  // refresh ran — the local read set can lag. Heuristic only ADDS; the
  // debounced exact inbox(unread) refresh is the source of truth (it gates
  // on real server reads), and it mirrors the server predicate: not mine,
  // addressed to me or @all — status plays no part in unread-ness.
  if (row && row.sender !== S.me) {
    const rec = String(row.recipients || "").split(",").map((s) => s.trim());
    if (rec.includes(S.me) || rec.includes("@all")) { S.unread.add(row.id); renderChans(); debUnread(); }
  }
}
let presT = null;
function debPres() { clearTimeout(presT); presT = setTimeout(async () => {
  try { S.agents = await rpc("who", { all: true }); S.active = new Set((await rpc("who", {})).map((a) => a.id)); } catch {}
  renderPres();
}, 800); }
function renderPres() {
  const box = $("pres"); box.textContent = "";
  $("presn").textContent = String(S.agents.length);
  for (const a of S.agents) {
    const on = S.active.has(a.id);
    box.appendChild(el("span", "ag " + (on ? "on" : ""), (on ? "● " : "○ ") + a.id + (a.role && a.role !== a.id ? "  (" + a.role + ")" : "")));
  }
}

/* ---------- composer ---------- */
for (const t of ["note", "ask", "reply", "ack", "announce", "handoff", "result", "status", "rfc"]) $("ctype").appendChild(el("option", null, t));
$("cbody").addEventListener("input", () => { const t = $("cbody"); t.style.height = "auto"; t.style.height = Math.min(t.scrollHeight, 160) + "px"; });
$("cbody").addEventListener("keydown", (ev) => { if (ev.key === "Enter" && (ev.ctrlKey || ev.metaKey)) { ev.preventDefault(); $("cpost").click(); } });
$("filt").addEventListener("input", () => { S.filter = $("filt").value; renderPane(); });
$("csubjbtn").onclick = () => { const s = $("csubj"); s.classList.toggle("hidden"); if (!s.classList.contains("hidden")) s.focus(); else s.value = ""; };
$("unro").addEventListener("change", () => { S.unreadOnly = $("unro").checked; renderPane(); });
document.addEventListener("keydown", (ev) => {
  const tag = (ev.target.tagName || "").toLowerCase();
  if (tag === "input" || tag === "textarea" || tag === "select") { if (ev.key === "Escape") ev.target.blur(); return; }
  if (ev.key === "/") { ev.preventDefault(); $("filt").focus(); }
});
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
  setS("posting…");
  try {
    const r = await rpc("post", p);
    setS("posted " + r.id + " → #" + r.channel, "ok");
    $("cbody").value = ""; $("cbody").style.height = "auto"; $("csubj").value = ""; S.replyTo = null; $("cbody").placeholder = "Message — Ctrl+Enter to send, Enter for a new line";
  } catch (e) { setS("post failed: " + e.message + (e.bus ? " [" + e.bus + "]" : ""), 1); }
};

/* ---------- tabs ---------- */
for (const b of document.querySelectorAll(".tab")) b.onclick = () => {
  document.querySelectorAll(".tab").forEach((n) => n.classList.remove("on")); b.classList.add("on");
  S.tab = b.dataset.tab;
  const chat = S.tab !== "admin";
  $("tabchat").style.display = chat ? "flex" : "none";
  $("pane").style.display = chat ? "" : "none";
  $("panehead").style.display = chat ? "" : "none";
  $("composer").style.display = chat ? "" : "none";
  $("admin").style.display = chat ? "none" : "flex";
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
// token.create returns scopes as normalized CSV; token.list returns an array.
// Accept both so the invite block is right whichever path filled S.minted.
function scopesCsv(m) { return Array.isArray(m.scopes) ? m.scopes.join(",") : String(m.scopes || ""); }
function inviteText(m) {
  // The bus has no invite RPC by design (§5: bootstrap local-only, tokens are
  // minted) — an invite is this text block. Identity rides the token, so the
  // block must say so (agents that self-claim an id get -32002).
  return [
    "── agent-comms invite ─────────────────────────",
    "URL:    " + location.origin,
    "TOKEN:  " + m.token,
    "AGENT:  " + m.agent + "   (identity comes from the token — never claim it)",
    "SCOPES: " + (scopesCsv(m) || "(none — plain sender)"),
    // RFC-003: a lane-scoped invite says so — the guest must know its world.
    "LANES:  " + (Array.isArray(m.lanes) && m.lanes.length ? m.lanes.join(",") : "(all lanes — unrestricted)"),
    "",
    "auth:    header  Authorization: " + authz() + m.token,
    "         content-type: application/json",
    "",
    "QUICK START (JSON-RPC 2.0, one endpoint):",
    '  1 join:   POST /rpc {"jsonrpc":"2.0","id":1,"method":"join","params":{"role":"one-line description of who you are"}}',
    '  2 inbox:  POST /rpc {"jsonrpc":"2.0","id":2,"method":"inbox","params":{}}   (add "wait":20 to long-poll)',
    '  3 read:   POST /rpc {"jsonrpc":"2.0","id":3,"method":"history","params":{"channel":"' + (Array.isArray(m.lanes) && m.lanes.length ? m.lanes[0] : "general") + '"}}',
    '  4 post:   POST /rpc {"jsonrpc":"2.0","id":4,"method":"post","params":{"from":"' + m.agent + '","to":["<agent-id>"],"channel":"' + (Array.isArray(m.lanes) && m.lanes.length ? m.lanes[0] : "general") + '","body":"your message","type":"note"}}',
    "  receipts are automatic: ACK/DONE reply with the id of the message you handled.",
    "",
    "link:    " + location.origin + "/#token=" + m.token + (Array.isArray(m.lanes) && m.lanes.length ? "&lane=" + m.lanes[0] : "") + "   (dashboard prefill — paste into a browser)",
    'docs:    README "Remote mode" · deploy/RUNBOOK.md · RFC-001 §5–§7',
    "note:    token is shown ONCE — store it; revoke in Admin any time.",
    "────────────────────────────────────────────────",
  ].join("\\n"); // DOUBLE backslash on purpose: this source lives inside the
  // UI_HTML template literal, so a single-escape backslash-n would evaluate to
  // a REAL newline here and split the emitted JS line mid-string (same bug
  // class as the bare-quote JSON example above, both pinned in ui.test.ts).
}
function renderMinted() {
  const box = $("minted"); if (!box) return; box.textContent = "";
  if (!S.minted) return;
  const d = el("div", "once");
  d.appendChild(el("div", null, "new token for " + S.minted.agent + " (prefix " + S.minted.prefix + ") — copy it now, it is never shown again:"));
  d.appendChild(el("div", "tok", S.minted.token));
  const cb = el("button", "mini", "copy"); cb.onclick = () => navigator.clipboard && navigator.clipboard.writeText(S.minted.token).then(() => { cb.textContent = "copied"; });
  const ci = el("button", "mini", "copy invite");
  ci.onclick = () => navigator.clipboard && navigator.clipboard.writeText(inviteText(S.minted)).then(() => { ci.textContent = "invite copied"; });
  // RFC-003 N1b: a link the guest just pastes into chat — the fragment is
  // stripped by the boot IIFE on arrival (replaceState BEFORE any fill).
  const cl = el("button", "mini", "copy invite link");
  cl.onclick = () => navigator.clipboard && navigator.clipboard.writeText(location.origin + "/#token=" + S.minted.token + (Array.isArray(S.minted.lanes) && S.minted.lanes.length ? "&lane=" + S.minted.lanes[0] : "")).then(() => { cl.textContent = "link copied"; });
  const dx = el("button", "mini", "dismiss"); dx.onclick = () => { S.minted = null; renderMinted(); };
  d.appendChild(cb); d.appendChild(document.createTextNode(" ")); d.appendChild(ci); d.appendChild(document.createTextNode(" ")); d.appendChild(cl); d.appendChild(document.createTextNode(" ")); d.appendChild(dx);
  const det = el("details"); const sm = el("summary"); sm.textContent = "preview invite text"; det.appendChild(sm);
  const pre = el("pre", "mono"); pre.textContent = inviteText(S.minted); det.appendChild(pre);
  d.appendChild(det);
  box.appendChild(d);
}
async function renderTokenTable() {
  const box = $("tktable"); if (!box) return;
  let tk; try { tk = await rpc("token.list", {}); } catch (e) { box.textContent = ""; box.appendChild(el("p", null, "token.list: " + e.message)); return; }
  const tbl = el("table");
  const trh = el("tr");
  for (const c of ["#", "agent", "kind", "prefix", "label", "scopes", "lanes", "last used", "state", ""]) trh.appendChild(el("th", null, c));
  tbl.appendChild(trh);
  for (const t of tk.tokens) {
    const tr = el("tr");
    tr.appendChild(el("td", null, String(t.id)));
    tr.appendChild(el("td", null, t.agentId));
    tr.appendChild(el("td", null, t.kind));
    tr.appendChild(el("td", "mono", t.prefix));
    tr.appendChild(el("td", null, t.label || "—"));
    tr.appendChild(el("td", "mono", (t.scopes || []).join(",")));
    // RFC-003: lanes chip — scoped seats show their closed list; unrestricted stays blank
    tr.appendChild(el("td", "mono", Array.isArray(t.lanes) && t.lanes.length ? t.lanes.join(",") : "—"));
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
  const s1 = el("section"); box.appendChild(s1);
  const once = el("div"); once.id = "minted"; s1.appendChild(once);
  s1.appendChild(el("h2", null, "Tokens"));
  const tb = el("div"); tb.id = "tktable"; s1.appendChild(tb);
  const f = el("div"); f.style.marginTop = "14px";
  f.appendChild(el("h2", null, "Mint token"));
  const row = el("div", "row");
  const ag = el("input"); ag.placeholder = "agent id — a-z0-9, -, _ (lowercase)"; ag.style.flex = "1"; ag.style.minWidth = "140px"; ag.pattern = "[a-z0-9][a-z0-9_-]{0,31}"; row.appendChild(ag);
  const kind = el("select"); for (const k of ["agent", "human"]) kind.appendChild(el("option", null, k)); row.appendChild(kind);
  const lab = el("input"); lab.placeholder = "label (optional)"; lab.style.flex = "1"; lab.style.minWidth = "140px"; row.appendChild(lab);
  f.appendChild(row);
  const row2 = el("div", "row"); row2.style.marginTop = "8px";
  const SC = ["read:all", "read:dm", "post:as", "tokens:admin", "agents:admin"];
  const cks = {};
  for (const s of SC) { const l = el("label", "ck"); const c = el("input"); c.type = "checkbox"; l.appendChild(c); l.appendChild(document.createTextNode(s)); row2.appendChild(l); cks[s] = c; }
  const fc = el("label", "ck"); const fcb = el("input"); fcb.type = "checkbox"; fc.appendChild(fcb); fc.appendChild(document.createTextNode("force (bootstrap guard)")); row2.appendChild(fc);
  f.appendChild(row2);
  // RFC-003 N1b: lanes input — comma-separated closed list; the server's
  // mint validation (live-at-mint, fail-closed) answers verbatim in the err div.
  const row3 = el("div", "row"); row3.style.marginTop = "8px";
  const ln = el("input"); ln.placeholder = "lanes (comma-separated; empty = unrestricted)"; ln.style.flex = "1"; ln.style.minWidth = "200px"; row3.appendChild(ln);
  f.appendChild(row3);
  const go = el("button", null, "create"); go.style.marginTop = "8px"; f.appendChild(go);
  const err = el("div", "rev"); f.appendChild(err);
  go.onclick = async () => {
    err.textContent = "";
    const p = { agent: ag.value.trim(), kind: kind.value };
    if (!/^[a-z0-9][a-z0-9_-]{0,31}$/.test(p.agent)) { err.textContent = "agent id must be lowercase a-z 0-9 - _ (start alnum, ≤32)"; return; }
    if (lab.value.trim()) p.label = lab.value.trim();
    const sel = SC.filter((s) => cks[s].checked);
    if (sel.length) p.scopes = sel;
    if (ln.value.trim()) p.lanes = ln.value.split(",").map((s) => s.trim()).filter(Boolean);
    if (fcb.checked) p.force = true;
    try {
      const r = await rpc("token.create", p);
      S.minted = { token: r.token, prefix: r.prefix, agent: r.agentId, label: p.label, scopes: r.scopes, lanes: r.lanes };
      ag.value = ""; lab.value = ""; for (const s of SC) cks[s].checked = false; fcb.checked = false; if (ln) ln.value = "";
      renderMinted(); renderTokenTable();
    } catch (e) { err.textContent = "create failed: " + e.message; }
  };
  box.appendChild(f);
}

let inviteLane = null; // RFC-003 invite kit: #lane= deep-link target, consumed once after login
/* ---------- start: probe an existing session cookie ---------- */
(async () => {
  // RFC-003 N1b: invite prefill — a copy-invite-link URL ends in /#token=ac_...
  // The fragment never crosses the wire, but the URL bar and session history
  // DO keep it — strip it with replaceState BEFORE anything else runs (a
  // token left in history is not "shown once"), then fill + focus. NEVER
  // auto-submit: the human decides when the credential is spent.
  const fm = /[#&]token=(ac_[A-Za-z0-9_-]{16,})/.exec(location.hash);
  const inviteTok = fm ? fm[1] : null;
  // #lane=<name>: deep-link straight into the seat's lane after login (an
  // invite to a SPECIFIC channel lands the guest where it belongs). Channel
  // names are [a-z0-9-] only, so the class is exact — no smuggled junk.
  const lm = /[#&]lane=([a-z0-9][a-z0-9-]{0,63})/.exec(location.hash);
  inviteLane = lm ? lm[1] : null;
  if (fm || lm) history.replaceState(null, "", location.pathname + location.search);
  // one raw call: 401 ⇒ show the login card; 200 ⇒ identity from x-comms-*
  // (§7: identity rides the first response's headers — zero extra RPC).
  try {
    const res = await fetch("/rpc", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: ++rpcid }) });
    if (!res.ok) {
      // no session — a saved token (opt-in remember) is prefilled. Cookie-less
      // browsers (Chrome over plain HTTP) can still boot: probe the saved
      // token as bearer; if it authenticates, boot in bearer mode silently.
      const saved = localStorage.getItem("comms-token");
      if (inviteTok && !saved) {
        // fresh invite on a clean device: prefill, don't persist, don't submit
        $("tok").value = inviteTok; $("tok").focus();
        return;
      }
      if (saved) {
        $("tok").value = inviteTok || saved; $("remember").checked = true;
        try {
          const br = await fetch("/rpc", { method: "POST", headers: { "content-type": "application/json", authorization: authz() + saved }, body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: ++rpcid }) });
          if (br.ok) {
            S.bearer = saved;
            S.me = br.headers.get("x-comms-agent") || "(unknown)";
            S.scopes = (br.headers.get("x-comms-scopes") || "").split(",").filter(Boolean);
            $("me").textContent = S.me; $("myscopes").textContent = S.scopes.join(", ");
            await boot();
            return;
          }
        } catch {}
      }
      return;
    }
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
  "img-src 'self' data:", // data: for the inline-SVG favicon (static, page-embedded)
  "base-uri 'none'",
  "form-action 'none'",
  "frame-ancestors 'none'",
].join("; ");
