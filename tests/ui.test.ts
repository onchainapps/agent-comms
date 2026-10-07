/**
 * M4 web UI tests (RFC-001 §8): the static shell at GET /, the cookie login
 * round-trip (login → cookie RPC → logout), CSRF on cookie requests with the
 * bearer exemption, and the dashboard --url transport switch (server mode +
 * core receipts proxy). XSS hygiene is pinned statically: the shell renders
 * bus data with textContent only — no innerHTML anywhere in ui.ts.
 */
import { expect, describe, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openBus, localCtx } from "../src/bus.ts";
import { startServer } from "../src/server/mod.ts";
import { UI_HTML, UI_CSP } from "../src/server/ui.ts";

const REPO = import.meta.dir + "/..";
// HERMETIC: these suites SPAWN the CLI/server/dashboard binaries, which read
// COMMS_* from the environment (§7). A developer shell that sourced its
// agent-comms env file (COMMS_URL/COMMS_TOKEN/COMMS_HOME…) must not silently
// repoint spawned children at a live server (E1-era incident: sourced
// COMMS_URL made ui/cli-remote spawn server-mode dashboards ⇒ 2 fail).
for (const k of Object.keys(process.env)) if (k.startsWith("COMMS_")) delete process.env[k];

function tmp() { return mkdtempSync(join(tmpdir(), "comms-m4-")); }

function bootstrap(home: string) {
  const b = openBus({ home, mode: "local" });
  const t = b.tokenCreate(localCtx("bootstrap"), { agent: "root", admin: true });
  if (t.error) throw new Error(t.detail);
  const tok = t.value.token;
  b.close();
  return tok;
}

async function rpcCookie(url: string, cookie: string | null, method: string, params: unknown = {}, headers: Record<string, string> = {}) {
  const res = await fetch(`${url}/rpc`, {
    method: "POST",
    headers: { "content-type": "application/json", "origin": new URL(url).origin, ...(cookie ? { cookie } : {}), ...headers },
    body: JSON.stringify({ jsonrpc: "2.0", method, params, id: 1 }),
  });
  let body: any = null; try { body = await res.json(); } catch {}
  return { status: res.status, body, headers: res.headers };
}

describe("M4 web UI (§8)", () => {
  test("GET / serves the shell: html, no-store, DENY frame, CSP, no innerHTML on bus data", async () => {
    const home = tmp();
    const srv = startServer({ home, port: 0 });
    try {
      const res = await fetch(`${srv.url}/`);
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toContain("text/html");
      expect(res.headers.get("cache-control")).toBe("no-store");
      expect(res.headers.get("x-frame-options")).toBe("DENY");
      const html = await res.text();
      // claude M4 m-c: CSP is a HEADER, script-src is the inline script's hash
      const csp = res.headers.get("content-security-policy") ?? "";
      expect(csp).toBe(UI_CSP);
      expect(csp).toContain("default-src 'none'");
      expect(csp).toContain("frame-ancestors 'none'");
      expect(csp).toMatch(/script-src 'sha256-[A-Za-z0-9+/=]{44}'/);
      expect(/script-src[^;]*unsafe-inline/.test(csp)).toBe(false);
      const script = /<script>([\s\S]*?)<\/script>/.exec(html)![1];
      const h = new Bun.CryptoHasher("sha256").update(script).digest("base64");
      expect(csp).toContain(`'sha256-${h}'`);
      expect((html.match(/<script/g) ?? []).length).toBe(1); // one hashed script, nothing else runnable
      expect(html).toContain("agent comms");
      expect(html).toContain("/rpc");
      expect(html).toContain('"/stream?scope='); // streams from /stream (URL now built in a var for ticket fallback)
      // XSS hygiene: bus strings must never reach innerHTML in the UI module.
      expect(UI_HTML.includes("innerHTML")).toBe(false);
      // Syntax hygiene (regression: inviteText's \" emitted bare quotes into
      // the page script and killed every onclick — the whole UI went dead):
      // the rendered <script> body must parse as JS.
      const sm = UI_HTML.match(/<script>([\s\S]*)<\/script>/);
      expect(sm).not.toBeNull();
      let jsOk = true;
      try { new Function(sm![1]); } catch { jsOk = false; }
      expect(jsOk).toBe(true);
      expect(UI_HTML.includes("textContent")).toBe(true);
      // token persistence is OPT-IN only (§8 amendment): the checkbox exists,
      // the DEFAULT path never writes localStorage, and every exit path
      // (logout / revoked / failed login) clears it.
      expect(UI_HTML.includes('id="remember"')).toBe(true);
      expect(UI_HTML.includes('localStorage.setItem("comms-token"')).toBe(true);
      const setCalls = (UI_HTML.match(/localStorage\.setItem/g) ?? []).length;
      expect(setCalls).toBe(1); // exactly one write site, behind the checkbox
      expect(UI_HTML.includes('if ($("remember").checked) localStorage.setItem')).toBe(true);
      expect((UI_HTML.match(/localStorage\.removeItem\("comms-token"\)/g) ?? []).length).toBeGreaterThanOrEqual(3); // logout, revoked, failed login, unchecked-login
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("cookie round-trip: login sets HttpOnly cookie → cookie RPC works → logout kills it", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const pre = await rpcCookie(srv.url, null, "channels");
      expect(pre.status).toBe(401);
      const login = await rpcCookie(srv.url, null, "login", { token: tok });
      expect(login.status).toBe(200);
      expect(login.body.result.agentId).toBe("root");
      const sc = login.headers.get("set-cookie") ?? "";
      expect(sc).toContain("HttpOnly");
      expect(sc).toContain("SameSite=Strict");
      expect(sc).toContain("Secure");
      const cookie = sc.split(";")[0];
      const post = await rpcCookie(srv.url, cookie, "post", { from: "root", to: "someone", type: "note", body: "via cookie" });
      expect(post.status).toBe(200);
      expect(post.body.result.id).toMatch(/-root-[0-9a-f]{4}$/);
      // identity headers ride cookie responses too (banner source, §7)
      expect(post.headers.get("x-comms-agent")).toBe("root");
      const out = await rpcCookie(srv.url, cookie, "logout");
      expect(out.status).toBe(200);
      const dead = await rpcCookie(srv.url, cookie, "channels");
      expect(dead.status).toBe(401);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("CSRF on cookie requests; bearer exempt; login CSRF-gated (§8)", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const login = await rpcCookie(srv.url, null, "login", { token: tok });
      const cookie = (login.headers.get("set-cookie") ?? "").split(";")[0];
      // foreign Origin ⇒ 403
      const foreign = await rpcCookie(srv.url, cookie, "channels", {}, { origin: "https://evil.example" });
      expect(foreign.status).toBe(403);
      // form-style content type ⇒ 403 (no preflight granted)
      const formish = await fetch(`${srv.url}/rpc`, {
        method: "POST", headers: { "content-type": "text/plain;charset=UTF-8", cookie },
        body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: 1 }),
      });
      expect(formish.status).toBe(403);
      // charset param still passes (parsed media type, §8)
      const charset = await rpcCookie(srv.url, cookie, "channels", {}, { "content-type": "application/json; charset=utf-8" });
      expect(charset.status).toBe(200);
      // bearer requests are EXEMPT (no Origin at all)
      const bearer = await fetch(`${srv.url}/rpc`, {
        method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${tok}` },
        body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: 1 }),
      });
      expect(bearer.status).toBe(200);
      // login itself is CSRF-gated: form post from a foreign origin fails
      const loginCsrf = await fetch(`${srv.url}/rpc`, {
        method: "POST", headers: { "content-type": "text/plain", origin: "https://evil.example" },
        body: JSON.stringify({ jsonrpc: "2.0", method: "login", params: { token: tok }, id: 1 }),
      });
      expect(loginCsrf.status).toBe(403);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("dashboard --url transport switch: server-mode state + core receipts proxy", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    // a second agent to be the intended reader
    {
      const b = openBus({ home, mode: "local" });
      b.joinAgent(localCtx("reader1"), { agent: "reader1", role: "reader" });
      b.close();
    }
    const dash = Bun.spawn([process.execPath, join(REPO, "bin/dashboard.ts"), "--port", "0", "--url", srv.url, "--token", tok], {
      stdout: "pipe", stderr: "pipe",
    });
    try {
      const rpc = (m: string, p: any = {}) => fetch(`${srv.url}/rpc`, {
        method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${tok}` },
        body: JSON.stringify({ jsonrpc: "2.0", method: m, params: p, id: 1 }),
      }).then((r) => r.json());
      const posted: any = await rpc("post", { from: "root", to: "reader1", type: "note", body: "dash switch" });
      expect(posted.result?.id ?? posted.error).toBeTruthy();
      // read AS reader1 with its own bearer token (marks the read; a root
      // read:all peek would be non-marking per §5):
      const mint: any = await rpc("token.create", { agent: "reader1" });
      const rTok = mint.result.token;
      await fetch(`${srv.url}/rpc`, {
        method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${rTok}` },
        body: JSON.stringify({ jsonrpc: "2.0", method: "read", params: { for: "reader1", id: posted.result.id }, id: 3 }),
      });
      // dashboard URL is on stdout: "… http://localhost:PORT …"
      const dec = new TextDecoder();
      let url = "";
      const rr = dash.stdout.getReader();
      for (let i = 0; i < 50 && !url; i++) {
        const { value } = await rr.read();
        if (!value) break;
        const m = /http:\/\/localhost:(\d+)/.exec(dec.decode(value));
        if (m) url = `http://127.0.0.1:${m[1]}`;
      }
      expect(url).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
      const st = await fetch(`${url}/api/state`).then((r) => r.json());
      expect(st.serverMode).toBe(true);
      expect(st.messages.map((x: any) => x.id)).toContain(posted.result.id);
      const rc = await fetch(`${url}/api/receipts?id=${posted.result.id}`).then((r) => r.json());
      expect(rc.receipts.intended).toContain("reader1");
      expect(rc.receipts.readers.map((x: any) => x.id)).toContain("reader1");
      // direct-DB-only guard on the proxy when COMMS_HOME mode: spawn without --url
      const dash2 = Bun.spawn([process.execPath, join(REPO, "bin/dashboard.ts"), "--port", "0"], {
        stdout: "pipe", stderr: "pipe", env: { ...process.env, COMMS_HOME: home },
      });
      const dec2 = new TextDecoder();
      let url2 = "";
      const rr2 = dash2.stdout.getReader();
      for (let i = 0; i < 50 && !url2; i++) {
        const { value } = await rr2.read();
        if (!value) break;
        const m = /http:\/\/localhost:(\d+)/.exec(dec2.decode(value));
        if (m) url2 = `http://127.0.0.1:${m[1]}`;
      }
      expect(url2).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
      const st2 = await fetch(`${url2}/api/state`).then((r) => r.json());
      expect(st2.serverMode).toBeUndefined(); // direct mode: no flag
      const rc2 = await fetch(`${url2}/api/receipts?id=x`);
      expect(rc2.status).toBe(400);          // proxy refuses in direct mode
      dash2.kill();
    } finally {
      dash.kill();
      srv.stop();
      rmSync(home, { recursive: true, force: true });
    }
  });

  // ---------------- claude M4 review pins ----------------

  test("secureCookie:false (trusted-LAN plain HTTP) drops the Secure flag; default keeps it (§8)", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, secureCookie: false });
    try {
      const login = await rpcCookie(srv.url, null, "login", { token: tok });
      expect(login.status).toBe(200);
      const sc = login.headers.get("set-cookie") ?? "";
      expect(sc).toContain("HttpOnly");
      expect(sc).toContain("SameSite=Strict");
      expect(sc).not.toContain("Secure"); // browsers DROP Secure cookies over http:// — login would be dead
      const cookie = sc.split(";")[0];
      const post = await rpcCookie(srv.url, cookie, "channels");
      expect(post.status).toBe(200); // cookie still authenticates without the flag
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("claude M4 B1/B2 wire contract the shell relies on: DM post needs to=peer; receipts hydrate writes no reads row", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const b = openBus({ home, mode: "local" });
      b.joinAgent(localCtx("peer1"), { agent: "peer1", role: "peer" });
      const h = (b.tokenCreate(localCtx("bootstrap"), { agent: "human-x", kind: "human" }) as any).value.token as string;
      b.close();
      const lg = await rpcCookie(srv.url, null, "login", { token: h });
      const cookie = (lg.headers.get("set-cookie") ?? "").split(";")[0];
      // B1: the pre-fix composer body ({type, body, dm}) is usage — the core
      // requires to before the dm branch; the fixed body adds to = peer.
      const bare = await rpcCookie(srv.url, cookie, "post", { type: "note", body: "x", dm: "peer1" });
      expect(bare.body.error?.data?.busError).toBe("usage");
      const fixed = await rpcCookie(srv.url, cookie, "post", { type: "note", body: "dm from ui", dm: "peer1", to: "peer1" });
      expect(fixed.status).toBe(200);
      expect(fixed.body.result.channel).toBe("dm~human-x~peer1");
      expect(UI_HTML).toContain("p.dm = peer; p.to = peer;");
      // B2: hydrate must use receipts (non-marking), never read (marking).
      const b2 = openBus({ home, mode: "local" });
      const ask = (b2.post(localCtx("peer1"), { from: "peer1", to: "human-x", type: "ask", body: "unopened" }) as any).value.id as string;
      b2.close();
      const rc = await rpcCookie(srv.url, cookie, "receipts", { id: ask });
      expect(rc.body.result.body).toBe("unopened"); // same row the pane needs
      const b3 = openBus({ home, mode: "local" });
      const reads = (b3 as any).testDb.query("SELECT agent FROM reads WHERE msg=?").all(ask);
      b3.close();
      expect(reads).toEqual([]); // rendering is not reading
      // Rendering must never read (claude M4 B2) — but OPENING a collapsed
      // thread is a deliberate user action and MAY read. Pin the shape:
      // hydrate() uses receipts, and rpc("read") exists ONLY in the toggle.
      expect(UI_HTML).toContain('rpc("receipts", { id: d.id })');
      const readCalls = (UI_HTML.match(/rpc\("read"/g) ?? []).length;
      expect(readCalls).toBe(1);
      expect(/S\.openT\.add[\s\S]{0,400}rpc\("read"/.test(UI_HTML)).toBe(true); // and ONLY in the thread-toggle path
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("claude M4 M-a: snapshot has no backward paging — the shell does ONE capped snapshot, not a since-loop", async () => {
    const home = tmp(); const tok = bootstrap(home);
    {
      const b = openBus({ home, mode: "local" });
      b.joinAgent(localCtx("p1"), { agent: "p1", role: "p" });
      for (let i = 0; i < 12; i++) b.post(localCtx("p1"), { from: "p1", to: "root", type: "note", body: "r" + i });
      b.close();
    }
    const srv = startServer({ home, port: 0 });
    try {
      const auth = { "content-type": "application/json", authorization: `Bearer ${tok}` };
      const call = (p: any) => fetch(`${srv.url}/rpc`, { method: "POST", headers: auth, body: JSON.stringify({ jsonrpc: "2.0", method: "history", params: p, id: 1 }) }).then((r) => r.json() as any);
      const p1 = await call({ limit: 5 });
      expect(p1.result.rows.length).toBe(5);
      expect(p1.result.hasMore).toBe(true);
      const p2 = await call({ since: p1.result.cursor, limit: 5 }); // the old loop's second call
      expect(p2.result.rows.length).toBe(0);                         // ⇒ it never paged anything
      expect(UI_HTML).toContain('rpc("history", { limit: 1000 })');
      expect(UI_HTML.includes("pages > 40")).toBe(false);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("claude M4 M-e: logout closes the session's open /stream; re-login replaces; per-token cap; idle expiry", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { maxSessionsPerToken: 3, sessionIdleMs: 400, tailerMs: 50 } as any });
    try {
      const login = async (prior?: string) => {
        const r = await rpcCookie(srv.url, prior ?? null, "login", { token: tok });
        return (r.headers.get("set-cookie") ?? "").split(";")[0];
      };
      const c1 = await login();
      expect((await login(c1)) !== c1).toBe(true);
      expect(srv.sessions.size).toBe(1); // re-login from the same browser replaced c1
      for (let i = 0; i < 5; i++) await login();
      expect(srv.sessions.size).toBe(3); // cap, oldest evicted
      // stream on a fresh session, then logout ⇒ the stream ends
      const ck = await login();
      const res = await fetch(`${srv.url}/stream?scope=all`, { headers: { cookie: ck } });
      expect(res.status).toBe(200);
      const rd = res.body!.getReader();
      let ended = false;
      const pump = (async () => { try { for (;;) { const { done } = await rd.read(); if (done) { ended = true; break; } } } catch { ended = true; } })();
      await Bun.sleep(100);
      expect((await rpcCookie(srv.url, ck, "logout")).status).toBe(200);
      await Promise.race([pump, Bun.sleep(1500)]);
      expect(ended).toBe(true);
      // idle expiry
      const ck2 = await login();
      expect((await rpcCookie(srv.url, ck2, "channels")).status).toBe(200);
      await Bun.sleep(600);
      const dead = await rpcCookie(srv.url, ck2, "channels");
      expect(dead.status).toBe(401);
      expect(dead.body.error.data.detail).toBe("session expired");
      // cookie now carries Max-Age (browser drops it with the absolute expiry)
      const r = await rpcCookie(srv.url, null, "login", { token: tok });
      expect(r.headers.get("set-cookie") ?? "").toMatch(/Max-Age=\d+/);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("claude M4 B4/m-d: dashboard server mode strips DM rows unless --omniview; foreign Host refused on loopback bind", async () => {
    const home = tmp(); const tok = bootstrap(home);
    let human = "";
    {
      const b = openBus({ home, mode: "local" });
      b.joinAgent(localCtx("d1"), { agent: "d1", role: "d" });
      b.joinAgent(localCtx("d2"), { agent: "d2", role: "d" });
      human = (b.tokenCreate(localCtx("bootstrap"), { agent: "human-d", kind: "human" }) as any).value.token as string;
      const r = b.post(localCtx("d1"), { from: "d1", to: "d2", type: "note", body: "SECRET-DM", dm: "d2" });
      if (r.error) throw new Error(r.detail);
      b.post(localCtx("d1"), { from: "d1", to: "d2", type: "note", body: "public-row" });
      b.close();
    }
    const srv = startServer({ home, port: 0 });
    const spawnDash = async (extra: string[]) => {
      const p = Bun.spawn([process.execPath, join(REPO, "bin/dashboard.ts"), "--port", "0", "--url", srv.url, "--token", human, ...extra], { stdout: "pipe", stderr: "pipe" });
      const rr = p.stdout.getReader(); const dec = new TextDecoder(); let url = "";
      for (let i = 0; i < 50 && !url; i++) { const { value } = await rr.read(); if (!value) break; const m = /http:\/\/localhost:(\d+)/.exec(dec.decode(value)); if (m) url = `http://127.0.0.1:${m[1]}`; }
      return { p, url };
    };
    const a = await spawnDash([]);
    const b = await spawnDash(["--omniview"]);
    try {
      const sa = await (await fetch(`${a.url}/api/state`)).json() as any;
      expect(JSON.stringify(sa.messages)).toContain("public-row");
      expect(JSON.stringify(sa.messages).includes("SECRET-DM")).toBe(false);
      expect(sa.channels.some((c: any) => c.name.startsWith("dm~"))).toBe(false);
      const dmId = (await (await fetch(`${b.url}/api/state`)).json() as any).messages.find((m: any) => m.body === "SECRET-DM").id;
      expect(dmId).toBeTruthy(); // --omniview opts in
      expect((await fetch(`${a.url}/api/receipts?id=${dmId}`)).status).toBe(404); // DM receipts gated too
      const reb = await fetch(`${a.url}/api/state`, { headers: { host: "attacker.example" } });
      expect(reb.status).toBe(403);
    } finally { a.p.kill(); b.p.kill(); srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("claude M4 B3/M-f/B5 shell pins: minted token survives the re-render; resync closes+re-hands-off; dashboard rc null-safe", () => {
    // B3: the table re-render must not own the minted-secret node
    expect(UI_HTML).toContain("S.minted = { token: r.token");
    expect(/async function renderTokenTable\(\)[\s\S]*?\$\("tktable"\)/.test(UI_HTML)).toBe(true);
    expect(UI_HTML.includes('$("admin"); box.textContent = "";\n  const h = el("h2"')).toBe(false);
    // M-f: resync handler closes the EventSource before re-opening at the new cursor
    expect(/addEventListener\("resync", async \(\) => \{\s*es\.close\(\)/.test(UI_HTML)).toBe(true);
    // M-b: stream opens AT the snapshot cursor (§6 handoff)
    expect(UI_HTML).toContain("const cursor = await loadHistory();\n  openStream(cursor);");
  });

  test("UI/UX fold pins (grok B1-B3 / claude B1+M1+M2): bearer scheme survives redaction; marking is unread-scoped and generation-guarded at both edges", async () => {
    // B1 (both reviewers): the tool layer that reads ui.ts redacts "Bearer <tok>"
    // to asterisks in its OUTPUT; twice a redacted read was written straight back.
    // The scheme must be assembled from split literals so that hazard cannot
    // round-trip, and NO bare asterisk-triple may appear anywhere in the page.
    expect(UI_HTML).toContain('const authz = () => "Bear" + "er ";');
    expect(UI_HTML.includes("***")).toBe(false);
    // Every authorization header the page sends goes through authz() — 4 sites:
    // rpc(), the /stream.ticket mint, the remember-boot probe, the invite text.
    const authSites = (UI_HTML.match(/authorization: /g) ?? []).length + (UI_HTML.match(/Authorization: /g) ?? []).length;
    const authzUses = (UI_HTML.match(/authz\(\) \+/g) ?? []).length;
    expect(authSites).toBe(4);
    expect(authzUses).toBe(4);
    // Live cookie-less probe: the header the page ACTUALLY builds must 200.
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, secureCookie: false });
    try {
      const authz = () => "Bear" + "er "; // the page's own construction, verbatim
      const call = (method: string, params: unknown) => fetch(`${srv.url}/rpc`, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: authz() + tok },
        body: JSON.stringify({ jsonrpc: "2.0", method, params, id: 1 }),
      });
      // login reads params.token (cookie bootstrap); the bearer header is the
      // fallback for EVERY call after that — exactly the sequence the page runs.
      const lg = await fetch(`${srv.url}/rpc`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", method: "login", params: { token: tok }, id: 1 }),
      });
      expect(lg.status).toBe(200);
      const ch = await call("channels", {}); // bearer, NO cookie — the plain-HTTP Chrome path
      expect(ch.status).toBe(200);
      const tk = await fetch(`${srv.url}/stream.ticket`, { method: "POST", headers: { authorization: authz() + tok } });
      expect(tk.status).toBe(200); // the §6 mint path grok/claude found dead since 12a1a11
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }

    // M1 (claude): every deliberate mark is scoped to UNREAD rows so a re-click
    // marks zero rows, re-emits zero read events, and never rewrites readers[].at.
    expect(UI_HTML).toContain("Object.assign({ mark: true, unread: true }, extra)");
    const rawMarks = (UI_HTML.match(/mark: true/g) ?? []).length;
    expect(rawMarks).toBe(1); // ONLY inside markInbox — no site bypasses the wrapper
    // B2 (claude D1/D2): the guard bumps at BOTH edges and a discarded snapshot
    // is rescheduled, never silently dropped.
    const mi = /async function markInbox[\s\S]*?\n\}/.exec(UI_HTML)![0];
    expect(mi.includes("markGen++;")).toBe(true);
    expect(/finally\s*\{\s*markGen\+\+;\s*debUnread\(\);\s*\}/.test(mi)).toBe(true);
    expect(/if \(g !== markGen\) \{ debUnread\(\); return; \}/.test(UI_HTML)).toBe(true);
    // the thread-toggle read and the SSE self-read path bump the guard too (grok B2)
    expect(/rpc\("read", \{ id: k\.id \}\)\.then\(\(\) => \{ markGen\+\+/.test(UI_HTML)).toBe(true);
    expect(/addEventListener\("read", \(ev\) => \{[\s\S]*?if \(d\.agent === S\.me\) \{ markGen\+\+/.test(UI_HTML)).toBe(true);
    // B3: failure mode is an EXPLICIT flag, never inferred from set size
    expect(UI_HTML).toContain("if (S.unreadExact) return S.unread.has(m.id);");
    expect(UI_HTML.includes("if (S.unread.size) return")).toBe(false);
    // grok M1: success status must not paint red ("ok" is truthy)
    expect(UI_HTML).toContain("(isErr === 1 || isErr === true) ? \"err\"");
    // grok M2: post:as is impersonation, not permission — Post must not be gated on it
    expect(UI_HTML.includes('if (!S.scopes.includes("post:as")) $("cpost").disabled = true;')).toBe(false);
  });

  test("RFC-003 pin 11 (invite half): the invite text states the lane grant — a scoped guest must know its world", () => {
    // the LANES line rides in the invite block, keyed off m.lanes (array ⇒ csv,
    // null/empty ⇒ explicit unrestricted wording — never a silent omission)
    expect(UI_HTML).toContain('"LANES:  " + (Array.isArray(m.lanes) && m.lanes.length ? m.lanes.join(",") : "(all lanes — unrestricted)")');
  });
});
