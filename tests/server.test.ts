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

// ---------------------------------------------------------------------------
// claude M2 review (t_ab14167c) — each test FAILS on 3289b6e.
describe("M2 review pins (claude)", () => {
  const post = (url: string, body: unknown, headers: Record<string, string>) =>
    fetch(`${url}/rpc`, { method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify(body) });
  const rq = (method: string, params: unknown = {}) => ({ jsonrpc: "2.0", method, params, id: 1 });
  async function login(url: string, token: string) {
    const li = await post(url, rq("login", { token }), {});
    return /comms_session=([^;]+)/.exec(li.headers.get("set-cookie") ?? "")![1];
  }
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  async function readFor(r: ReadableStreamDefaultReader<Uint8Array>, ms: number, until?: string) {
    const dec = new TextDecoder(); let s = ""; let done = false; const t0 = Date.now();
    while (Date.now() - t0 < ms && !(until && s.includes(until))) {
      const x = await Promise.race([r.read(), sleep(ms - (Date.now() - t0)).then(() => null)]);
      if (x === null) break; if (x.done) { done = true; break; } s += dec.decode(x.value);
    }
    return { s, done };
  }

  test("B1: cookie session re-resolves the token ROW per request — revoke kills it, rename re-keys it, retired id never posts", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const root = new RpcBus(srv.url, tok).session({ token: tok });
      const a = (await root.tokenCreate({ agent: "ck-a" })) as any;
      const sid = await login(srv.url, a.value.token);
      const ck = { cookie: `comms_session=${sid}` };
      await root.rename({ agent: "ck-a", to: "ck-b" });
      const r1 = await post(srv.url, rq("post", { to: ["root"], type: "note", body: "after rename" }), ck);
      expect(r1.status).toBe(200);
      expect(r1.headers.get("x-comms-agent")).toBe("ck-b"); // NOT the retired ck-a
      const id = ((await r1.json()) as any).result.id;
      expect(((srv.handle as any).raw.messageById(id)).sender).toBe("ck-b");
      await root.tokenRevoke({ id: a.value.id });
      const r2 = await post(srv.url, rq("channels"), ck);
      expect(r2.status).toBe(401);
      expect(srv.sessions.has(sid)).toBe(false); // session dies with the token
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("M1: per-IP 401 bucket is ENFORCED (429 + Retry-After), checked before HMAC, keyed on the socket peer not X-Forwarded-For", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const st: number[] = [];
      for (let i = 0; i < 14; i++)
        st.push((await post(srv.url, rq("channels"), { authorization: `Bearer ac_${"x".repeat(40)}${i}`, "x-forwarded-for": `10.0.0.${i}` })).status);
      expect(st.slice(0, 10).every((s) => s === 401)).toBe(true);
      expect(st.slice(10).every((s) => s === 429)).toBe(true); // XFF rotation does NOT mint fresh buckets
      const lim = await post(srv.url, rq("channels"), { authorization: `Bearer ${tok}` });
      expect(lim.status).toBe(429); // exhausted ⇒ refused BEFORE the HMAC, even for a valid token
      expect(Number(lim.headers.get("retry-after"))).toBeGreaterThanOrEqual(1);
      const lg = await post(srv.url, rq("login", { token: "ac_" + "q".repeat(40) }), {});
      expect(lg.status).toBe(429); // login shares the bucket
      const sse = await fetch(`${srv.url}/stream`, { headers: { authorization: `Bearer ac_${"z".repeat(40)}` } });
      expect(sse.status).toBe(429); // and /stream
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("M2: revoked token's open SSE stream is CLOSED (not a silent zombie); bad Last-Event-ID does not leak a stream slot", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40 } });
    try {
      const root = new RpcBus(srv.url, tok).session({ token: tok });
      const b = (await root.tokenCreate({ agent: "zz-b" })) as any;
      const res = await fetch(`${srv.url}/stream`, { headers: { authorization: `Bearer ${b.value.token}` } });
      const rd = res.body!.getReader(); await rd.read();
      await root.tokenRevoke({ id: b.value.id });
      const after = await readFor(rd, 2000);
      expect(after.done).toBe(true);
      expect(after.s).toContain("event: revoked");
      const c = (await root.tokenCreate({ agent: "zz-c" })) as any;
      const H = { authorization: `Bearer ${c.value.token}`, "last-event-id": "garbage" };
      expect((await fetch(`${srv.url}/stream`, { headers: H })).status).toBe(400);
      expect((await fetch(`${srv.url}/stream`, { headers: H })).status).toBe(400);
      expect(srv.streamCount(c.value.id)).toBe(0);
      const ok = await fetch(`${srv.url}/stream`, { headers: { authorization: `Bearer ${c.value.token}` } });
      expect(ok.status).toBe(200);
      ok.body?.cancel();
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 20_000);

  test("M3: a resuming laggard does not head-of-line block live subscribers", async () => {
    const home = tmp();
    const bb = openBus({ home, mode: "local" });
    const tok = (bb.tokenCreate(localCtx("bootstrap"), { agent: "root", admin: true }) as any).value.token as string;
    bb.testDb.exec("BEGIN");
    const ins = bb.testDb.query("INSERT INTO events(kind,msg_id,agent_id,at) VALUES('presence',NULL,'root','2026-09-25T00:00:00Z')");
    for (let i = 0; i < 6000; i++) ins.run();
    bb.testDb.exec("COMMIT");
    const ep = bb.epoch(); bb.close();
    const srv = startServer({ home, port: 0 }); // DEFAULT 250 ms tailer
    try {
      const H = { authorization: `Bearer ${tok}` };
      const live = await fetch(`${srv.url}/stream?scope=all`, { headers: H });
      const lr = live.body!.getReader(); await lr.read();
      const lag = await fetch(`${srv.url}/stream?scope=all`, { headers: { ...H, "last-event-id": `${ep}.0` } });
      const lagR = lag.body!.getReader();
      void (async () => { try { for (;;) if ((await lagR.read()).done) break; } catch { } })();
      await sleep(50);
      const t0 = Date.now();
      const id = ((await (await post(srv.url, rq("post", { to: ["x"], type: "note", body: "live" }), H)).json()) as any).result.id;
      const got = await readFor(lr, 2500, id);
      expect(got.s).toContain(id);
      expect(Date.now() - t0).toBeLessThan(1000); // was ≈2.7 s on 3289b6e (12 laggard pages × 250 ms)
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 20_000);

  test("M4: hello.seq on resume = the resume cursor (deltas apply AFTER it); same-epoch cursor above high-water ⇒ resync", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40 } });
    try {
      const root = new RpcBus(srv.url, tok).session({ token: tok });
      for (let i = 0; i < 3; i++) await root.post({ from: "root", to: "x", type: "note", body: "p" + i });
      const ep = (srv.handle as any).raw.epoch();
      const r = await fetch(`${srv.url}/stream?scope=all`, { headers: { authorization: `Bearer ${tok}`, "last-event-id": `${ep}.0` } });
      const got = await readFor(r.body!.getReader(), 600);
      // grok M4 B2: hello now carries an id line (native reconnect base).
      const hello = JSON.parse(/event: hello\n(?:id: [^\n]+\ndata: |data: )(.*)\n/.exec(got.s)![1]);
      expect(hello.seq).toBe(0);
      const seqs = [...got.s.matchAll(/event: (?!hello)[a-z]+\nid: [0-9a-f]+\.(\d+)/g)].map((m) => Number(m[1]));
      expect(seqs.length).toBeGreaterThan(0);
      expect(seqs.every((s) => s > hello.seq)).toBe(true); // was: every replayed seq <= hello.seq
      // the hello id itself = the resume cursor (quiet-bus reconnect base):
      const helloId = /event: hello\nid: ([0-9a-f]+\.\d+)\ndata:/.exec(got.s);
      expect(helloId).not.toBeNull();
      expect(helloId![1]).toBe(`${ep}.0`);
      const fut = await fetch(`${srv.url}/stream?scope=all`, { headers: { authorization: `Bearer ${tok}`, "last-event-id": `${ep}.999999` } });
      expect((await readFor(fut.body!.getReader(), 500)).s).toContain("event: resync");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("M5: chunked body with no Content-Length is capped (no unbounded pre-auth buffering)", async () => {
    const home = tmp(); bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const chunk = new Uint8Array(1024 * 1024).fill(0x20); let sent = 0;
      const body = new ReadableStream({ pull(c) { if (sent >= 4) { c.close(); return; } sent++; c.enqueue(chunk); } });
      let status = 0;
      try { status = (await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json" }, body, duplex: "half" } as any)).status; } catch { status = -1; /* reset = also capped */ }
      expect(status === 413 || status === -1).toBe(true);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("m: RpcBus.resolve returns the principal's agentId; error data deep-equals the core's; 500 never leaks exception text; wrong-typed post ⇒ usage", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const rb = new RpcBus(srv.url, tok);
      const r = (await rb.resolve({ token: tok })) as any;
      expect(r.value.agentId).toBe("root");
      const h = (await rb.session({ token: tok }).history({ since: "deadbeefdeadbeef.5" })) as any;
      const core = (srv.handle as any).raw;
      const lh = core.history({ principal: { agentId: "root", kind: "agent", scopes: ["read:all"] }, actor: "root" }, { since: "deadbeefdeadbeef.5" });
      expect(h.data).toEqual(lh.data);
      const bad = await post(srv.url, rq("post", { to: [{}], type: "note", body: { x: 1 }, tags: 5 }), { authorization: `Bearer ${tok}` });
      const bj = (await bad.json()) as any;
      expect(bad.status).toBe(400);
      expect(bj.error.data.busError).toBe("usage");
      expect(JSON.stringify(bj)).not.toContain("is not a function");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("m: non-admin scope=mine stream gets no token events for OTHER agents; ticket mint is metered; lowercase bearer scheme accepted", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40, readBurst: 5, readRefill: 0.0001 } });
    try {
      const root = new RpcBus(srv.url, tok).session({ token: tok });
      const a = (await root.tokenCreate({ agent: "pl-a" })) as any;
      const res = await fetch(`${srv.url}/stream?scope=mine`, { headers: { authorization: `Bearer ${a.value.token}` } });
      const rd = res.body!.getReader(); await rd.read();
      await root.tokenCreate({ agent: "victim-svc" });
      await root.post({ from: "root", to: "pl-a", type: "note", body: "sentinel" }); // proves the stream is live
      const seen = (await readFor(rd, 1500, "event: msg")).s;
      expect(seen).toContain("event: msg");
      expect(seen).not.toMatch(/event: token\n[^\n]*\ndata: [^\n]*victim-svc/);
      const st: number[] = [];
      for (let i = 0; i < 8; i++) st.push((await fetch(`${srv.url}/stream.ticket`, { method: "POST", headers: { authorization: `Bearer ${a.value.token}` } })).status);
      expect(st).toContain(429);
      const lc = await post(srv.url, rq("channels"), { authorization: `bearer ${tok}` });
      expect(lc.status).toBe(200);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("m: trustProxy keys the 401 bucket on the RIGHTMOST X-Forwarded-For hop (client-supplied leftmost ignored); cookie cannot mint stream tickets; pinned origin + absent Origin ⇒ CSRF 403", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, trustProxy: true, origin: "https://comms.example" });
    try {
      const st: number[] = [];
      for (let i = 0; i < 12; i++) // attacker rotates the LEFT hop; nginx appends the real peer on the right
        st.push((await post(srv.url, rq("channels"), { authorization: `Bearer ac_${"x".repeat(40)}${i}`, "x-forwarded-for": `6.6.6.${i}, 203.0.113.9` })).status);
      expect(st.slice(10)).toEqual([429, 429]);
      const other = await post(srv.url, rq("channels"), { authorization: `Bearer ${tok}`, "x-forwarded-for": "198.51.100.7" });
      expect(other.status).toBe(200); // a different real client is unaffected
      const li = await post(srv.url, rq("login", { token: tok }), { origin: "https://comms.example", "x-forwarded-for": "198.51.100.7" });
      const sid = /comms_session=([^;]+)/.exec(li.headers.get("set-cookie") ?? "")![1];
      const tk = await fetch(`${srv.url}/stream.ticket`, { method: "POST", headers: { cookie: `comms_session=${sid}`, origin: "https://evil.example", "content-type": "application/x-www-form-urlencoded" } });
      expect(tk.status).toBe(401); // tickets are bearer-only (§6: non-cookie clients) — no CSRF-unchecked cookie write
      const noOrigin = await post(srv.url, rq("channels"), { cookie: `comms_session=${sid}` });
      expect(noOrigin.status).toBe(403);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("m: read event with no message row is dropped (G2: no visibility proof ⇒ no frame)", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40 } });
    try {
      const root = new RpcBus(srv.url, tok).session({ token: tok });
      const c = (await root.tokenCreate({ agent: "rd-c" })) as any;
      const res = await fetch(`${srv.url}/stream?scope=mine`, { headers: { authorization: `Bearer ${c.value.token}` } });
      const rd = res.body!.getReader(); await rd.read();
      (srv.handle as any).raw.testDb.run("INSERT INTO events(kind,msg_id,agent_id,at) VALUES('read','no-such-msg','alice','2026-09-25T00:00:00Z')");
      await root.post({ from: "root", to: "rd-c", type: "note", body: "sentinel" });
      const seen = (await readFor(rd, 1500, "event: msg")).s;
      expect(seen).toContain("event: msg");
      expect(seen).not.toContain("no-such-msg");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("m: epoch rotated under a live stream ⇒ resync frame + close (ids carried the dead epoch)", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40 } });
    try {
      const res = await fetch(`${srv.url}/stream?scope=all`, { headers: { authorization: `Bearer ${tok}` } });
      const rd = res.body!.getReader(); await rd.read();
      (srv.handle as any).raw.rotateEpoch();
      const got = await readFor(rd, 1000);
      expect(got.s).toContain("event: resync");
      expect(got.s).toContain('"floor"');
      expect(got.done).toBe(true);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  // fold-4 (claude M1): EXECUTABLE welcome pin — every JSON sample the
  // welcome text teaches is POSTed verbatim (placeholders substituted only)
  // to a live server and must answer result-without-error; then the taught
  // lifecycle (wait → read → status acked → post re → status done) drives a
  // real ask out of unresolved. The @653d236 text FAILS this pin by design.
  test("fold-4 pin 16 (HTTP): every welcome sample executes against the live wire; taught lifecycle clears the ask", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40 } });
    const extract = (line: string): string[] => { // balanced-brace scan, verbatim substrings
      const out: string[] = [];
      for (let i = 0; i < line.length; i++) {
        if (line[i] !== "{") continue;
        let depth = 0;
        for (let j = i; j < line.length; j++) {
          if (line[j] === "{") depth++;
          else if (line[j] === "}") { depth--; if (depth === 0) { const s = line.slice(i, j + 1); try { JSON.parse(s); out.push(s); i = j; break; } catch { break; } } }
        }
      }
      return out;
    };
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const mc = await root.tokenCreate({ agent: "pin16-bot", lanes: ["general"] });
      const bTok = (mc as any).value.token;
      expect((mc as any).value.welcome).toContain("WELCOME pin16-bot");
      await rpc2.session({ token: bTok }).joinAgent({ agent: "pin16-bot", role: "w" });
      const ask = await root.post({ from: "root", to: "pin16-bot", type: "ask", subject: "do it", body: "please", channel: "general" });
      const askId = String((ask as any).value.id);
      const jw = await rpc2.session({ token: bTok }).joinAgent({ agent: "pin16-bot", role: "w" });
      const welcome = String((jw as any).value.welcome);
      const post = async (raw: string) => {
        const r = await fetch(srv.url + "/rpc", { method: "POST", headers: { "content-type": "application/json", authorization: "Bear" + "er " + bTok }, body: raw });
        return { status: r.status, body: await r.json() as any };
      };
      const sub = (s: string, cursor?: string) => s
        .split("<message id>").join(askId)
        .split("<recipient id>").join("root")
        .split("<cursor you received>").join(cursor ?? "x");
      // verbatim envelope line from the poll instruction: inbox.wait then cursor.set
      const pollLine = welcome.split("\n").find((l) => l.includes("inbox.wait"))!;
      const [waitRaw, csetRaw] = extract(pollLine);
      const w1 = await post(sub(waitRaw));
      expect([w1.status, w1.body.error]).toEqual([200, undefined]);
      const msgs = w1.body.result.messages as any[];
      expect(msgs.some((m) => m.id === askId)).toBe(true);
      const w2 = await post(sub(csetRaw, w1.body.result.cursor));
      expect([w2.status, w2.body.error]).toEqual([200, undefined]);
      for (const key of ["read:", "post:", "duty:"]) {
        const line = welcome.split("\n").find((l) => l.trimStart().startsWith(key))!;
        const [only] = extract(line);
        expect(only).toBeTruthy();
        const r = await post(sub(only));
        expect([key, r.status, r.body.error]).toEqual([key, 200, undefined]);
      }
      // taught lifecycle end-to-end: acked (duty line above executed) → done → unresolved 0
      const seat = rpc2.session({ token: bTok });
      await seat.setStatus({ agent: "pin16-bot", id: askId, state: "done" });
      const jf = await seat.joinAgent({ agent: "pin16-bot", role: "w" });
      expect((jf as any).value.unresolved).toBe(0);
      // m1: envelope-less probe now self-documents the fix in the error detail
      const bare = await post('{"method":"inbox","params":{}}');
      expect(bare.status).toBe(400);
      expect(String(bare.body.error.message)).toContain('\"jsonrpc\":\"2.0\"');
      // mandala-dev ask: presence ping refreshes presence and answers the roster
      const pr = await post('{"jsonrpc":"2.0","id":9,"method":"ping","params":{}}');
      expect([pr.status, pr.body.result.agent]).toEqual([200, "pin16-bot"]);
      expect((pr.body.result.active as any[]).some((a) => a.id === "pin16-bot")).toBe(true);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  // r3 grok M2: RFC pin 5 on the HTTP transport — lanes must reach /raw and
  // SSE principalOf; a principalOf edit that drops lanes fails HERE, not only
  // in the in-process contract suite.
  test("RFC-003 pin 5 (HTTP): scoped seat gets 404 on a foreign-lane /raw file and zero foreign SSE frames; owner still 200", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0, limits: { tailerMs: 40 } });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      expect((await root.channelCreate({ name: "pin5secret" })).error).toBeUndefined();
      const sec = await root.post({ from: "root", to: "*", type: "note", body: "secret body", channel: "pin5secret" });
      expect((sec as any).error).toBeUndefined();
      const secFile = String((sec as any).value.file).split("/").pop()!;
      const g = await root.post({ from: "root", to: "*", type: "note", body: "visible", channel: "general" });
      const gFile = String((g as any).value.file).split("/").pop()!;
      const sc = await root.tokenCreate({ agent: "pin5-guest", lanes: ["general"], scopes: ["read:all"] as any });
      const sTok = (sc as any).value.token;
      await rpc2.session({ token: sTok }).joinAgent({ agent: "pin5-guest", role: "guest" });
      // /raw: foreign-lane file is 404/-32003 (same body as a missing lane); own-lane is 200
      const foreign = await fetch(`${srv.url}/raw/messages/pin5secret/${secFile}`, { headers: { authorization: `Bearer ${sTok}` } });
      const fb: any = await foreign.json().catch(() => null);
      expect([foreign.status, fb?.error?.code].join()).toBe("404,-32003");
      const own = await fetch(`${srv.url}/raw/messages/general/${gFile}`, { headers: { authorization: `Bearer ${sTok}` } });
      expect(own.status).toBe(200);
      const owner = await fetch(`${srv.url}/raw/messages/pin5secret/${secFile}`, { headers: { authorization: `Bearer ${tok}` } });
      expect(owner.status).toBe(200);
      // SSE scope=all as the scoped seat: post AFTER opening the stream —
      // the general frame flows (id present), the secret frame never does.
      const ctrl = new AbortController();
      const res = await fetch(`${srv.url}/stream?scope=all`, { headers: { authorization: `Bearer ${sTok}` }, signal: ctrl.signal });
      expect(res.status).toBe(200);
      const reader = res.body!.getReader(); const dec = new TextDecoder();
      const hello = dec.decode((await reader.read()).value); // frames start at hello (global high-water)
      expect(hello).toContain("event: hello");
      const sGen = await root.post({ from: "root", to: "*", type: "note", body: "flows", channel: "general" });
      const sSec = await root.post({ from: "root", to: "*", type: "note", body: "stays", channel: "pin5secret" });
      const frames: string[] = [];
      const t0 = Date.now();
      while (Date.now() - t0 < 5000) {
        const { value, done } = await reader.read();
        if (done) break;
        frames.push(dec.decode(value));
        if (frames.join("").includes((sGen as any).value.id)) break;
      }
      ctrl.abort();
      const joined = frames.join("");
      expect(joined).toContain((sGen as any).value.id); // own lane flows
      expect(joined).not.toContain((sSec as any).value.id); // foreign lane never flows
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  // ---------- §7 GET /raw/messages/<channel>/<file> ----------
  test("§7 /raw: mirror bytes served to addressee; 401 without cred; traversal + regex rejected pre-join", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const p = await root.post({ from: "root", to: "peeker", type: "note", body: "raw bytes here" });
      expect(p.error).toBeUndefined();
      const file = String((p as any).value.file).split("/").pop()!;
      const hit = await fetch(`${srv.url}/raw/messages/general/${file}`, { headers: { authorization: `Bearer ${tok}` } });
      expect(hit.status).toBe(200);
      expect(await hit.text()).toContain("raw bytes here");
      const noauth = await fetch(`${srv.url}/raw/messages/general/${file}`);
      expect(noauth.status).toBe(401);
      // regex gates run BEFORE any join:
      for (const evil of ["../etc/passwd", "msg-x.txt", "msg%2Fx.md", "MSG-x.md"]) {
        const r = await fetch(`${srv.url}/raw/messages/general/${evil}`, { headers: { authorization: `Bearer ${tok}` } });
        const rb: any = await r.json().catch(() => null);
        expect([r.status, rb?.error?.code].join()).toBe("404,-32003");
      }
      const badChan = await fetch(`${srv.url}/raw/messages/..%2Fx/${file}`, { headers: { authorization: `Bearer ${tok}` } });
      expect(badChan.status).toBe(404);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("§7 /raw DM: participant 200; non-party 404 (invisible==missing); read:dm holder 200", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    try {
      const rpc2 = new RpcBus(srv.url, tok);
      const root = rpc2.session({ token: tok });
      const t1 = await root.tokenCreate({ agent: "r1" });
      const t2 = await root.tokenCreate({ agent: "r2" });
      const tD = await root.tokenCreate({ agent: "rdm", scopes: ["read:dm"] });
      const s1 = rpc2.session({ token: (t1 as any).value.token });
      const s2 = rpc2.session({ token: (t2 as any).value.token });
      await s1.joinAgent({ agent: "r1", role: "r1" });
      await s2.joinAgent({ agent: "r2", role: "r2" });
      const p = await s1.post({ from: "r1", to: "r2", type: "note", body: "dm secret", dm: "r2" });
      expect(p.error).toBeUndefined();
      const chan = (p as any).value.channel, file = String((p as any).value.file).split("/").pop()!;
      expect(chan).toMatch(/^dm~/);
      const part = await fetch(`${srv.url}/raw/messages/${chan}/${file}`, { headers: { authorization: `Bearer ${(t2 as any).value.token}` } });
      expect(part.status).toBe(200);
      expect(await part.text()).toContain("dm secret");
      const tOut = await root.tokenCreate({ agent: "rout" });
      const out = await fetch(`${srv.url}/raw/messages/${chan}/${file}`, { headers: { authorization: `Bearer ${(tOut as any).value.token}` } });
      expect(out.status).toBe(404); // plain agent, non-member, no read:dm ⇒ invisible == missing
      const peek = await fetch(`${srv.url}/raw/messages/${chan}/${file}`, { headers: { authorization: `Bearer ${(tD as any).value.token}` } });
      expect(peek.status).toBe(200);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });
});
