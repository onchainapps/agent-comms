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
import { UI_HTML } from "../src/server/ui.ts";

const REPO = import.meta.dir + "/..";

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
      expect(html).toContain("Content-Security-Policy");
      expect(html).toContain("default-src 'self'");
      expect(html).toContain("agent comms");
      expect(html).toContain("/rpc");
      expect(html).toContain("EventSource(\"/stream");
      // XSS hygiene: bus strings must never reach innerHTML in the UI module.
      expect(UI_HTML.includes("innerHTML")).toBe(false);
      expect(UI_HTML.includes("textContent")).toBe(true);
      // token never persisted to storage (§8):
      expect(UI_HTML.includes("localStorage")).toBe(false);
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
});
