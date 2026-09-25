/**
 * M2 integration tests (RFC-001 §10-M2): the REAL server (spawned process =
 * two-process discipline) + in-process startServer for SSE/CSRF/ticket/bucket
 * probes. The contract suite already pins Bus semantics over this transport;
 * this file pins the TRANSPORT: wire errors, HTTP codes, SSE frames, CSRF,
 * tickets, rate limits, foreign-writer fan-out, rename re-keying.
 */
import { expect, describe, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openBus, localCtx } from "../src/bus.ts";
import { startServer } from "../src/server/mod.ts";
import { RpcBus } from "../src/rpc-bus.ts";

const REPO = import.meta.dir + "/..";

function tmp() { return mkdtempSync(join(tmpdir(), "comms-m2-")); }

/** bootstrap §5: local opener mints the first admin token, then hand off. */
function bootstrap(home: string) {
  const b = openBus({ home, mode: "local" });
  const t = b.tokenCreate(localCtx("bootstrap"), { agent: "root", admin: true });
  if (t.error) throw new Error(t.detail);
  const tok = t.value.token;
  b.close();
  return tok;
}

async function rpc(url: string, token: string | null, method: string, params: unknown = {}, headers: Record<string, string> = {}) {
  const res = await fetch(`${url}/rpc`, {
    method: "POST",
    headers: { "content-type": "application/json", ...(token ? { authorization: `Bearer ${token}` } : {}), ...headers },
    body: JSON.stringify({ jsonrpc: "2.0", method, params, id: 1 }),
  });
  let body: any = null;
  try { body = await res.json(); } catch { }
  return { status: res.status, body, headers: res.headers };
}

describe("M2 server transport", () => {
  test("spawned server process (two-process): full post→inbox over HTTP", async () => {
    const home = tmp();
    const rootTok = bootstrap(home);
    const proc = Bun.spawn([process.execPath, join(REPO, "bin/server.ts"), "--home", home, "--port", "0"], {
      stdout: "pipe", stderr: "pipe",
    });
    // the shell prints its URL on stderr: "agent-comms server: http://127.0.0.1:PORT"
    const errReader = proc.stderr.getReader();
    const dec = new TextDecoder();
    let url = "";
    for (let i = 0; i < 50 && !url; i++) {
      const { value } = await errReader.read();
      if (!value) break;
      const m = /http:\/\/[^\s]+/.exec(dec.decode(value));
      if (m) url = m[0];
    }
    expect(url).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
    try {
      const rpc2 = new RpcBus(url, rootTok);
      const root = rpc2.session({ token: rootTok });
      const s = await root.tokenCreate({ agent: "p2-agent" });
      expect(s.error).toBeUndefined();
      const sess = rpc2.session({ token: (s as any).value.token });
      await sess.joinAgent({ agent: "p2-agent", role: "worker" });
      const p = await root.post({ from: "root", to: "p2-agent", type: "ask", body: "cross-process" });
      expect(p.error).toBeUndefined();
      const inb = await sess.inbox({ agent: "p2-agent" });
      expect((inb as any).value.rows.map((r: any) => r.id)).toContain((p as any).value.id);
    } finally {
      proc.kill();
      rmSync(home, { recursive: true, force: true });
    }
  }, 30_000);

  test("wire errors: parse/-32700, invalid/-32600, batch rejected, unknown method/-32601", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const bad = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${tok}` }, body: "{oops" });
      expect(((await bad.json()) as any).error.code).toBe(-32700);
      expect(bad.status).toBe(400);
      const batch2 = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${tok}` }, body: JSON.stringify([{ jsonrpc: "2.0", method: "channels", id: 1 }]) });
      expect(((await batch2.json()) as any).error.code).toBe(-32600);
      const noM = await rpc(srv.url, tok, "does.not.exist");
      expect(noM.body.error.code).toBe(-32601);
      const noReq = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${tok}` }, body: JSON.stringify({ method: "channels" }) });
      expect(((await noReq.json()) as any).error.code).toBe(-32600);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("auth wire: missing bearer ⇒ 401/-32001; bad token ⇒ 401; oversized Authorization ⇒ 401 pre-HMAC", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const r0 = await rpc(srv.url, null, "channels");
      expect(r0.status).toBe(401); expect(r0.body.error.code).toBe(-32001);
      const r1 = await rpc(srv.url, "ac_definitelynotarealtoken0000", "channels");
      expect(r1.status).toBe(401); expect(r1.body.error.data.busError).toBe("unauthorized");
      const long = "ac_" + "a".repeat(200);
      const r2 = await rpc(srv.url, long, "channels");
      expect(r2.status).toBe(401);
      expect(String(r2.body.error.data?.detail ?? r2.body.error.message)).toContain("too long");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("forbidden ⇒ 403/-32002 with busError variant on the wire (RpcBus reconstructs variant)", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const s = await root.tokenCreate({ agent: "lim-agent" });
      const sess = rpc2.session({ token: (s as any).value.token });
      const h = await sess.history({ limit: 5 }); // no read:all
      expect(h.error).toBe("forbidden");
      const raw = await rpc(srv.url, (s as any).value.token, "history", { limit: 5 });
      expect(raw.status).toBe(403);
      expect(raw.body.error.code).toBe(-32002);
      expect(raw.body.error.data.busError).toBe("forbidden");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("429 rate limit + Retry-After on write bucket; body cap 413", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { writeBurst: 3, writeRefill: 0.0001 } });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const codes: (string | undefined)[] = [];
      for (let i = 0; i < 6; i++) codes.push((await root.post({ from: "root", to: "x", type: "note", body: "b" + i })).error);
      expect(codes.slice(0, 3).every((c) => c === undefined)).toBe(true);
      expect(codes[3]).toBe("rate_limited");
      const raw = await rpc(srv.url, tok, "post", { from: "root", to: "x", type: "note", body: "z" });
      expect(raw.status).toBe(429);
      expect(Number(raw.headers.get("retry-after"))).toBeGreaterThanOrEqual(1);
      expect(raw.body.error.code).toBe(-32004);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("CSRF §8: cookie request without content-type ⇒ 403; charset param passes; Origin mismatch ⇒ 403; login CSRF-guarded; bearer exempt", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      // login sets the cookie
      const li = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json; charset=utf-8" }, body: JSON.stringify({ jsonrpc: "2.0", method: "login", params: { token: tok }, id: 1 }) });
      expect(li.status).toBe(200);
      const cookie = (li.headers.get("set-cookie") ?? "").match(/comms_session=([^;]+)/)?.[1];
      expect(cookie).toBeTruthy();
      expect(li.headers.get("set-cookie")).toContain("HttpOnly");
      expect(li.headers.get("set-cookie")).toContain("SameSite=Strict");
      // cookie + correct content-type ⇒ OK
      const ok = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json", cookie: `comms_session=${cookie}` }, body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: 2 }) });
      expect(ok.status).toBe(200);
      // cookie + form content-type ⇒ 403 CSRF
      const form = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/x-www-form-urlencoded", cookie: `comms_session=${cookie}` }, body: "{}" });
      expect(form.status).toBe(403);
      expect(((await form.json()) as any).error.code).toBe(-32002);
      // cookie + Origin mismatch ⇒ 403
      const origin = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json", origin: "https://evil.example", cookie: `comms_session=${cookie}` }, body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: 3 }) });
      expect(origin.status).toBe(403);
      // login WITHOUT content-type ⇒ CSRF 403 before token check
      const badLogin = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "text/plain" }, body: "{}" });
      expect(badLogin.status).toBe(403);
      // bearer is EXEMPT from CSRF even with junk content-type
      const bearer = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "text/plain", authorization: `Bearer ${tok}` }, body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: 4 }) });
      expect(bearer.status).toBe(200);
      // logout clears
      const lo = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json", cookie: `comms_session=${cookie}` }, body: JSON.stringify({ jsonrpc: "2.0", method: "logout", params: {}, id: 5 }) });
      expect(lo.status).toBe(200);
      const after = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json", cookie: `comms_session=${cookie}` }, body: JSON.stringify({ jsonrpc: "2.0", method: "channels", params: {}, id: 6 }) });
      expect(after.status).toBe(401);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("SSE: hello frame, <epoch>.<seq> ids, scope=mine server-side filter, foreign-writer trigger fan-out, Last-Event-ID resume, resync on epoch mismatch, ≤2 streams", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40 } });
    const core = (srv.handle as any).raw;
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const s = await root.tokenCreate({ agent: "sse-a", scopes: ["read:all"] });
      const aTok = (s as any).value.token;
      await rpc2.session({ token: aTok }).joinAgent({ agent: "sse-a", role: "sse-role" });

      const ctrl = new AbortController();
      const res = await fetch(`${srv.url}/stream?scope=all`, { headers: { authorization: `Bearer ${aTok}` }, signal: ctrl.signal });
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toContain("text/event-stream");
      const reader = res.body!.getReader();
      const dec = new TextDecoder();
      const frames: string[] = [];
      const collect = async (n: number, ms = 5000) => {
        const t0 = Date.now();
        while (frames.join("").split("\n\n").filter((f) => f.includes("event:")).length < n && Date.now() - t0 < ms) {
          const { value } = await reader.read();
          if (!value) break;
          frames.push(dec.decode(value));
        }
        return frames.join("");
      };
      const hello = await collect(1);
      expect(hello).toContain("event: hello");
      const ep = core.epoch();
      expect(hello).toContain(`"epoch":"${ep}"`);

      // in-process post → frame with id=<epoch>.<seq>
      const p1 = await root.post({ from: "root", to: "sse-a", type: "note", body: "in-proc" });
      const f1 = await collect(2);
      expect(f1).toContain(`event: msg\nid: ${ep}.`);
      expect(f1).toContain((p1 as any).value.id);

      // FOREIGN writer (second connection = stale-binary/CLI scenario): the
      // DB triggers still fan out — the server saw an event it did not write.
      const foreign = openBus({ home, mode: "local" });
      const fp = foreign.post(localCtx("root"), { from: "root", to: "sse-a", type: "note", body: "foreign-writer" });
      expect(fp.error).toBeUndefined();
      const f2 = await collect(3);
      expect(f2).toContain((fp as any).value.id);
      foreign.close();

      // Last-Event-ID resume from seq 0 replays everything since
      const ctrl2 = new AbortController();
      const res2 = await fetch(`${srv.url}/stream?scope=all`, { headers: { authorization: `Bearer ${aTok}`, "last-event-id": `${ep}.0` }, signal: ctrl2.signal });
      const r2 = res2.body!.getReader();
      const replay: string[] = [];
      for (let i = 0; i < 40; i++) { const { value } = await r2.read(); if (!value) break; replay.push(dec.decode(value)); if (replay.join("").includes((fp as any).value.id)) break; }
      expect(replay.join("")).toContain((fp as any).value.id);
      ctrl2.abort();

      // epoch mismatch ⇒ resync frame, not deltas
      const ctrl3 = new AbortController();
      const res3 = await fetch(`${srv.url}/stream?scope=all`, { headers: { authorization: `Bearer ${aTok}`, "last-event-id": `${"f".repeat(16)}.7` }, signal: ctrl3.signal });
      const r3 = res3.body!.getReader();
      const { value: rv } = await r3.read();
      expect(dec.decode(rv)).toContain("event: resync");
      expect(dec.decode(rv)).toContain(`"epoch":"${ep}"`);
      expect(dec.decode(rv)).toContain('"floor"'); // m2 hygiene: floor always present
      ctrl3.abort();

      // ≤2 streams/token — FRESH token: abort listeners on the earlier
      // streams fire asynchronously, so a reused token's count could lag.
      const capTok = (await root.tokenCreate({ agent: "sse-cap" })) as any;
      const aCap = capTok.value.token;
      const c4 = new AbortController(), c5 = new AbortController(), c6 = new AbortController();
      const s4 = await fetch(`${srv.url}/stream`, { headers: { authorization: `Bearer ${aCap}` }, signal: c4.signal });
      const s5 = await fetch(`${srv.url}/stream`, { headers: { authorization: `Bearer ${aCap}` }, signal: c5.signal });
      const s6 = await fetch(`${srv.url}/stream`, { headers: { authorization: `Bearer ${aCap}` }, signal: c6.signal });
      expect(s4.status).toBe(200); expect(s5.status).toBe(200); expect(s6.status).toBe(429);
      c4.abort(); c5.abort(); c6.abort();
      ctrl.abort();
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);

  test("SSE scope=mine re-keys on rename (§5): renamed agent still gets new mail on the OLD stream", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40 } });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const s = await root.tokenCreate({ agent: "rk-a" });
      const aTok = (s as any).value.token;
      const sess = rpc2.session({ token: aTok });
      await sess.joinAgent({ agent: "rk-a", role: "rk" });
      const ctrl = new AbortController();
      const res = await fetch(`${srv.url}/stream?scope=mine`, { headers: { authorization: `Bearer ${aTok}` }, signal: ctrl.signal });
      const reader = res.body!.getReader();
      const dec = new TextDecoder();
      await reader.read(); // hello
      await root.rename({ agent: "rk-a", to: "rk-b" });
      const p = await root.post({ from: "root", to: "rk-b", type: "note", body: "after-rename" });
      const got: string[] = [];
      for (let i = 0; i < 60; i++) { const { value } = await reader.read(); if (!value) break; got.push(dec.decode(value)); if (got.join("").includes((p as any).value.id)) break; }
      expect(got.join("")).toContain((p as any).value.id); // subscription re-keyed via tokenById refresh
      ctrl.abort();
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);

  test("ticket flow (§6): POST /stream.ticket → single-use 60 s → GET /stream?ticket=", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const tr = await fetch(`${srv.url}/stream.ticket`, { method: "POST", headers: { authorization: `Bearer ${tok}` } });
      const ticket = ((await tr.json()) as any).result.ticket;
      expect(typeof ticket).toBe("string");
      const s1 = await fetch(`${srv.url}/stream?ticket=${ticket}`);
      expect(s1.status).toBe(200);
      s1.body?.cancel();
      const s2 = await fetch(`${srv.url}/stream?ticket=${ticket}`); // consumed
      expect(s2.status).toBe(401);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("idempotency crash window (§6): replay same hash ⇒ same result; different hash ⇒ conflict/-32005/409; key scoped to token", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const p1 = await root.post({ from: "root", to: "idem-x", type: "note", body: "b", idempotencyKey: "k1" });
      const p2 = await root.post({ from: "root", to: "idem-x", type: "note", body: "b", idempotencyKey: "k1" });
      expect((p1 as any).value.id).toBe((p2 as any).value.id); // replay, no second row
      const p3 = await root.post({ from: "root", to: "idem-x", type: "note", body: "CHANGED", idempotencyKey: "k1" });
      expect(p3.error).toBe("conflict");
      const raw = await rpc(srv.url, tok, "post", { from: "root", to: "idem-x", type: "note", body: "CHANGED", idempotencyKey: "k1" });
      expect(raw.status).toBe(409); expect(raw.body.error.code).toBe(-32005);
      // scoped to the authenticated token: same key on ANOTHER token is fresh
      const s = await root.tokenCreate({ agent: "idem-y" });
      const other = rpc2.session({ token: (s as any).value.token });
      const q = await other.post({ from: "idem-y", to: "idem-x", type: "note", body: "b", idempotencyKey: "k1" });
      expect(q.error).toBeUndefined();
      expect((q as any).value.id).not.toBe((p1 as any).value.id);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("traversal probes (§9): ../ in channel/type/id/re never touch the FS outside home", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const probes = [
        root.post({ from: "root", to: "x", type: "note", body: "b", channel: "../evil" }),
        root.post({ from: "root", to: "x", type: "no/te", body: "b" }),
        root.post({ from: "../up", to: "x", type: "note", body: "b" }),
        root.post({ from: "root", to: "x", type: "note", body: "b", re: "../../x" }),
        root.post({ from: "root", to: "x", type: "note", body: "b", thread: "a/../b" }),
        root.post({ from: "root", to: "x", type: "note", body: "b", as: "..%2fx" }),
        root.joinAgent({ agent: "a/b", role: "r" }),
        root.rename({ agent: "root", to: "../x" }),
      ];
      for (const p of probes) expect((await p).error).toBeTruthy();
      const { readdirSync } = await import("node:fs");
      const top = readdirSync(home);
      expect(top).not.toContain("evil");
      expect(top.every((n) => n === ".comms" || n === "messages")).toBe(true);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("history→stream handoff over HTTP (§6): snapshot cursor accepted by stream since=", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40 } });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const s = await root.tokenCreate({ agent: "ho-a", scopes: ["read:all"] });
      const aTok = (s as any).value.token;
      await rpc2.session({ token: aTok }).joinAgent({ agent: "ho-a", role: "ho" });
      for (let i = 0; i < 3; i++) await root.post({ from: "root", to: "ho-a", type: "note", body: "pre" + i });
      const snap = (await rpc2.session({ token: aTok }).history({ limit: 50 })) as any;
      expect(snap.error).toBeUndefined();
      const ctrl = new AbortController();
      const res = await fetch(`${srv.url}/stream?scope=all&since=${snap.value.cursor}`, { headers: { authorization: `Bearer ${aTok}` }, signal: ctrl.signal });
      const dec = new TextDecoder();
      const r = res.body!.getReader();
      const hello = dec.decode((await r.read()).value);
      expect(hello).toContain("event: hello"); // NOT resync — cursor valid
      const p = await root.post({ from: "root", to: "ho-a", type: "note", body: "post-handoff" });
      const got: string[] = [];
      for (let i = 0; i < 60; i++) { const { value } = await r.read(); if (!value) break; got.push(dec.decode(value)); if (got.join("").includes((p as any).value.id)) break; }
      expect(got.join("")).toContain((p as any).value.id);
      ctrl.abort();
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);

  test("waitStep over HTTP: at-least-once, no auto-advance, cursor.set commit (§6)", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const s = await root.tokenCreate({ agent: "ws-a" });
      const sess = rpc2.session({ token: (s as any).value.token });
      await sess.joinAgent({ agent: "ws-a", role: "ws" });
      const p = await root.post({ from: "root", to: "ws-a", type: "note", body: "poll" });
      const w1 = (await sess.waitStep({ consumer: "cli" })) as any;
      expect(w1.error).toBeUndefined();
      expect(w1.value.messages.map((m: any) => m.id)).toContain((p as any).value.id);
      // NOT auto-advanced: same result again
      const w2 = (await sess.waitStep({ consumer: "cli" })) as any;
      expect(w2.value.messages.map((m: any) => m.id)).toContain((p as any).value.id);
      expect((await sess.cursorSet({ consumer: "cli", cursor: w1.value.cursor })).error).toBeUndefined();
      const w3 = (await sess.waitStep({ consumer: "cli" })) as any;
      expect(w3.value.messages.length).toBe(0);
      expect(w3.value.done).toBe(false);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("rotateEpoch through raw (restore runbook): stored cursor ⇒ resync with {resync,epoch,floor} on the wire", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    const core = (srv.handle as any).raw;
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const s = await root.tokenCreate({ agent: "ro-a" });
      const sess = rpc2.session({ token: (s as any).value.token });
      await sess.joinAgent({ agent: "ro-a", role: "ro" });
      const p = await root.post({ from: "root", to: "ro-a", type: "note", body: "x" });
      const w = (await sess.waitStep({ consumer: "cli" })) as any;
      expect((await sess.cursorSet({ consumer: "cli", cursor: w.value.cursor })).error).toBeUndefined();
      void p;
      core.rotateEpoch();
      const w2 = await sess.waitStep({ consumer: "cli" });
      expect(w2.error).toBe("resync");
      expect((w2 as any).data.resync).toBe(true);
      expect((w2 as any).data.epoch).toBe(core.epoch());
      expect(typeof (w2 as any).data.floor).toBe("number"); // m2 hygiene: floor ALWAYS present
      const cg = await sess.cursorGet({ consumer: "cli" });
      expect(cg.error).toBe("resync");
      expect((cg as any).data.floor !== undefined).toBe(true);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });
});
