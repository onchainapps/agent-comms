/**
 * M5 MCP adapter tests (§10-M5): spawn bin/mcp.ts against a live server and
 * drive the stdio protocol end-to-end — initialize/tools/list, join/post/
 * inbox/read over MCP only, the wait→cursor.set at-least-once pair, typed
 * error mapping to isError, and protocol hygiene (batches, unknown method,
 * schema validation). "A Hermes profile joins/posts/awaits over MCP only."
 */
import { expect, describe, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openBus, localCtx } from "../src/bus.ts";
import { startServer } from "../src/server/mod.ts";

const REPO = import.meta.dir + "/..";

function tmp() { return mkdtempSync(join(tmpdir(), "comms-m5-")); }

function bootstrap(home: string) {
  const b = openBus({ home, mode: "local" });
  const t = b.tokenCreate(localCtx("bootstrap"), { agent: "root", admin: true });
  if (t.error) throw new Error(t.detail);
  const tok = t.value.token;
  b.close();
  return tok;
}

/** a line-delimited MCP client over a spawned bin/mcp.ts. Responses are
 *  matched BY ID (concurrent calls finish out of order); id-less / unmatched
 *  messages go to the FIFO used by raw(). */
function mcpClient(url: string, token: string, extraEnv: Record<string, string> = {}, args: string[] = []) {
  // claude E1 MINOR-3: scrub ambient COMMS_* like golden/cli-remote do — a
  // sourced COMMS_FINGERPRINT would otherwise leak into join identity.
  const env: Record<string, string | undefined> = { ...process.env };
  for (const k of Object.keys(env)) if (k.startsWith("COMMS_")) delete env[k];
  const proc = Bun.spawn([process.execPath, join(REPO, "bin/mcp.ts"), ...args], {
    stdin: "pipe", stdout: "pipe", stderr: "pipe",
    env: { ...env, COMMS_URL: url, COMMS_TOKEN: token, ...extraEnv } as Record<string, string>,
  });
  let buf = "";
  const queue: ((m: any) => void)[] = [];
  const byId = new Map<number, (m: any) => void>();
  const seen: any[] = [];
  (async () => {
    const dec = new TextDecoder();
    const rr = proc.stdout.getReader();
    for (;;) {
      const { value, done } = await rr.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let nl: number;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1);
        if (!line) continue;
        let m: any; try { m = JSON.parse(line); } catch { continue; }
        seen.push(m);
        const w = byId.get(m.id);
        if (w) { byId.delete(m.id); w(m); continue; }
        const q = queue.shift();
        if (q) q(m);
      }
    }
  })();
  let id = 0;
  function send(method: string, params?: object, notify = false): { id: number; done: Promise<any> } {
    const rid = ++id;
    const line = JSON.stringify({ jsonrpc: "2.0", method, ...(params ? { params } : {}), ...(notify ? {} : { id: rid }) });
    const done = notify ? Promise.resolve(undefined) : new Promise<any>((res) => byId.set(rid, res));
    proc.stdin.write(line + "\n");
    return { id: rid, done };
  }
  const rpc = (method: string, params?: object, notify = false): Promise<any> => send(method, params, notify).done;
  return {
    rpc,
    send,
    proc,
    seen,
    /** send one raw line, resolve with the next unmatched message that arrives. */
    raw(line: string): Promise<any> {
      let settled = false;
      const pend = new Promise<any>((res) => queue.push((m) => { settled = true; res(m); }));
      proc.stdin.write(line + "\n");
      return Promise.race([
        pend,
        new Promise<any>((r) => setTimeout(() => { if (!settled) queue.shift(); r({ __timeout: true }); }, 2000)),
      ]);
    },
    async init(protocolVersion = "2024-11-05") {
      const r = await rpc("initialize", { protocolVersion, capabilities: {}, clientInfo: { name: "t", version: "0" } });
      await rpc("notifications/initialized", {}, true);
      return r;
    },
    async callTool(name: string, args: object = {}) {
      const r = await rpc("tools/call", { name, arguments: args });
      return { text: r.result?.content?.[0]?.text ?? "", isError: !!r.result?.isError, err: r.error };
    },
    kill() { proc.kill(); },
  };
}

describe("M5 MCP adapter (§10-M5)", () => {
  test("full lifecycle over MCP stdio only: init → tools → join → post → inbox → read → wait → cursor", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    const c = mcpClient(srv.url, tok);
    try {
      const init = await c.rpc("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "test", version: "0" } });
      expect(init.result.serverInfo.name).toBe("agent-comms");
      expect(init.result.capabilities.tools).toBeTruthy();
      await c.rpc("notifications/initialized", {}, true);
      const tl = await c.rpc("tools/list");
      const names = tl.result.tools.map((t: any) => t.name);
      for (const n of ["comms_join", "comms_post", "comms_inbox", "comms_read", "comms_wait", "comms_cursor_set", "comms_history", "comms_receipts", "comms_dm_members", "comms_group_join"])
        expect(names).toContain(n);
      // grok M5 #2: parity with the server's dispatch set (§10-M5 "same RPC
      // methods") — every remotely-callable method except the web-cookie/SSE
      // ones (login/logout/stream.ticket) must have a tool.
      for (const n of ["comms_rename", "comms_token_create", "comms_token_list", "comms_token_revoke", "comms_group_create", "comms_group_delete", "comms_group_leave", "comms_cursor_get", "comms_who", "comms_channels", "comms_thread", "comms_status", "comms_history", "comms_channel_create", "comms_channel_delete"])
        expect(names).toContain(n);
      for (const t of tl.result.tools) expect(typeof t.description).toBe("string");

      // join (identity from the token row = root)
      const join = await c.callTool("comms_join", { role: "mcp-tester", caps: "mcp" });
      expect(join.isError).toBe(false);
      expect(JSON.parse(join.text).agent.id).toBe("root");

      // post to a second agent
      const b = openBus({ home, mode: "local" });
      b.joinAgent(localCtx("peer1"), { agent: "peer1", role: "peer1" });
      const pt = b.tokenCreate(localCtx("root"), { agent: "peer1" });
      const peerTok = (pt as any).value.token;
      b.close();
      const posted = await c.callTool("comms_post", { to: "peer1", type: "ask", body: "hello over mcp", subject: "mcp hello" });
      expect(posted.isError).toBe(false);
      const mid = JSON.parse(posted.text).id;
      expect(mid).toMatch(/-root-[0-9a-f]{4}$/);

      // peer client: inbox → read (marks) → receipts (no mark)
      const c2 = mcpClient(srv.url, peerTok);
      try {
        await c2.rpc("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "p", version: "0" } });
        const inb = await c2.callTool("comms_inbox", { open: true });
        expect(JSON.parse(inb.text).rows.map((r: any) => r.id)).toContain(mid);
        const rd = await c2.callTool("comms_read", { id: mid });
        expect(JSON.parse(rd.text).body).toBe("hello over mcp");
        const rc = await c2.callTool("comms_receipts", { id: mid });
        expect(JSON.parse(rc.text).receipts.readers.map((x: any) => x.id)).toContain("peer1");
        const st = await c2.callTool("comms_status", { id: mid, state: "acked" });
        expect(st.isError).toBe(false);

        // wait → messages → cursor.set commit (at-least-once pair)
        const posted2 = await c.callTool("comms_post", { to: "peer1", type: "note", body: "second" });
        const mid2 = JSON.parse(posted2.text).id;
        const w = await c2.callTool("comms_wait", { timeout: 5 });
        expect(w.isError).toBe(false);
        const wv = JSON.parse(w.text);
        expect(wv.messages.map((m: any) => m.id)).toContain(mid2);
        expect(typeof wv.cursor).toBe("string");
        const cs = await c2.callTool("comms_cursor_set", { cursor: wv.cursor });
        expect(cs.isError).toBe(false);
        // second wait after commit: no redelivery of mid2 within a short poll
        const w2 = await c2.callTool("comms_wait", { timeout: 2 });
        const w2v = w2.isError ? { messages: [] } : JSON.parse(w2.text);
        expect((w2v.messages ?? []).map((m: any) => m.id)).not.toContain(mid2);

        // grok M5 #1: the wait must actually HOLD. Start a 5 s wait, post
        // ~900 ms AFTER the call began, assert the same call returns it and
        // elapsed ≥ the post delay (pre-fix this returned in ~1 ms empty).
        const t0 = Date.now();
        const waitP = c2.callTool("comms_wait", { timeout: 5 });
        await Bun.sleep(900);
        const posted3 = await c.callTool("comms_post", { to: "peer1", type: "note", body: "during-wait" });
        const mid3 = JSON.parse(posted3.text).id;
        const w3 = await waitP;
        const el3 = Date.now() - t0;
        expect(w3.isError).toBe(false);
        expect(JSON.parse(w3.text).messages.map((m: any) => m.id)).toContain(mid3);
        expect(el3).toBeGreaterThanOrEqual(900);
        const cs3 = await c2.callTool("comms_cursor_set", { cursor: JSON.parse(w3.text).cursor });
        expect(cs3.isError).toBe(false);

        // grok M5 #1: an empty wait runs the CLOCK, not a ~0 ms scan.
        const t1 = Date.now();
        const w4 = await c2.callTool("comms_wait", { timeout: 2 });
        const el4 = Date.now() - t1;
        const w4v = w4.isError ? { messages: [] } : JSON.parse(w4.text);
        expect((w4v.messages ?? []).length).toBe(0);
        expect(el4).toBeGreaterThanOrEqual(1800);
      } finally { c2.kill(); }
      // typed error → isError content, variant named
      const nf = await c.callTool("comms_read", { id: "20200101T000000-zzz-0000" });
      expect(nf.isError).toBe(true);
      expect(nf.text).toContain("error(not_found)");
      // schema validation at the edge (adapter): missing required + enum +
      // pattern + unknown prop ⇒ isError `usage` content the MODEL can see
      // and self-correct from; unknown TOOL stays a protocol error.
      for (const args of [{ to: "x" }, { to: "x", type: "nope", body: "b" }, { to: "x", type: "note", body: "b", bogus: 1 }, { to: "x", type: "note", body: "b", toString: "p" }]) {
        const bad = await c.callTool("comms_post", args);
        expect(bad.err).toBeUndefined();
        expect(bad.isError).toBe(true);
        expect(bad.text).toStartWith("error(usage): ");
      }
      const bad3 = await c.callTool("comms_cursor_set", { cursor: "not-a-cursor" });
      expect(bad3.isError).toBe(true);
      const bad5 = await c.callTool("comms_history", { channel: "general", limit: 1.5 }); // was SQLite "datatype mismatch" ⇒ internal
      expect(bad5.text).toStartWith("error(usage): ");
      const badTool = await c.callTool("no_such_tool", {});
      expect(badTool.err?.code).toBe(-32602);
      const protoTool = await c.callTool("toString", {});
      expect(protoTool.err?.code).toBe(-32602);
    } finally {
      c.kill(); srv.stop(); rmSync(home, { recursive: true, force: true });
    }
  }, 30_000);

  test("protocol hygiene: batches rejected, unknown method -32601, notifications silent, DM + group tools", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    const c = mcpClient(srv.url, tok);
    try {
      await c.rpc("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "t", version: "0" } });
      // batches ⇒ -32600 (same ruling as the HTTP wire, §6)
      const bm = await c.raw(JSON.stringify([{ jsonrpc: "2.0", method: "ping", id: 1 }]));
      expect(bm.error?.code).toBe(-32600);
      // malformed line ⇒ -32700
      const pm = await c.raw("{not json");
      expect(pm.error?.code).toBe(-32700);
      // unknown method
      const um = await c.rpc("bogus/method");
      expect(um.error.code).toBe(-32601);
      // unknown notification: NO response (a response would desync ids) —
      // prove liveness instead: ping right after must answer.
      await c.rpc("some/notification", {}, true);
      const ping = await c.rpc("ping");
      expect(ping.result).toEqual({});
      // DM tool surface
      const b = openBus({ home, mode: "local" });
      b.joinAgent(localCtx("dm-a"), { agent: "dm-a", role: "dm-a" }); b.close();
      const ta: any = await c.callTool("comms_post", { to: "dm-a", type: "note", body: "dm via mcp", dm: "dm-a" });
      expect(ta.isError).toBe(false);
      const chan = JSON.parse(ta.text).channel;
      expect(chan).toMatch(/^dm~/);
      const mem = await c.callTool("comms_dm_members", { channel: chan });
      expect(JSON.parse(mem.text).sort()).toEqual(["dm-a", "root"].sort());
      // group tools
      const gj = await c.callTool("comms_group_join", { name: "mcp-grp" });
      expect(gj.isError).toBe(false);
      const gl = await c.callTool("comms_group_list");
      expect(JSON.parse(gl.text).groups.map((g: any) => g.name)).toContain("mcp-grp");
      const gs = await c.callTool("comms_group_show", { name: "mcp-grp" });
      expect(JSON.parse(gs.text).members).toContain("root");
    } finally {
      c.kill(); srv.stop(); rmSync(home, { recursive: true, force: true });
    }
  }, 30_000);
});

/** claude M5 review pins — each fails on 8ebabde. */
describe("M5 review fold (claude)", () => {
  const FAST = { COMMS_MCP_POLL_MS: "100" };
  function seed(home: string) {
    const b = openBus({ home, mode: "local" });
    b.joinAgent(localCtx("ag1"), { agent: "ag1", role: "worker" });
    const ag1 = (b.tokenCreate(localCtx("root"), { agent: "ag1" }) as any).value.token as string;
    b.joinAgent(localCtx("noise"), { agent: "noise", role: "noise" });
    b.close();
    return ag1;
  }
  const postLocal = (home: string, from: string, to: string, body: string, n = 1) => {
    const b = openBus({ home, mode: "local" });
    for (let i = 0; i < n; i++) b.post(localCtx(from), { from, to, type: "note", body } as any);
    b.close();
  };

  test("B1 comms_wait actually blocks (server inbox.wait is one non-blocking step) and wakes on arrival", async () => {
    const home = tmp(); bootstrap(home); const ag1 = seed(home);
    const srv = startServer({ home, port: 0 });
    const c = mcpClient(srv.url, ag1, FAST);
    try {
      await c.init();
      // nothing pending: must hold for ~timeout, not return in 7 ms
      let t0 = Date.now();
      const idle = await c.callTool("comms_wait", { timeout: 1 });
      expect(Date.now() - t0).toBeGreaterThanOrEqual(900);
      expect(JSON.parse(idle.text).messages).toEqual([]);
      // arrival mid-wait wakes it well before the deadline
      t0 = Date.now();
      setTimeout(() => postLocal(home, "noise", "ag1", "wake"), 600);
      const w = await c.callTool("comms_wait", { timeout: 20 });
      const el = Date.now() - t0;
      expect(el).toBeGreaterThanOrEqual(500);
      expect(el).toBeLessThan(5000);
      expect(JSON.parse(w.text).messages.map((m: any) => m.body)).toEqual(["wake"]);
    } finally { c.kill(); srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);

  test("B1 a message behind >500 irrelevant events is delivered by ONE comms_wait (drain, no false-empty)", async () => {
    const home = tmp(); bootstrap(home); const ag1 = seed(home);
    postLocal(home, "noise", "root", "n", 600); // > one 500-event scan page
    postLocal(home, "root", "ag1", "for-ag1");
    const srv = startServer({ home, port: 0 });
    const c = mcpClient(srv.url, ag1, FAST);
    try {
      await c.init();
      const w = await c.callTool("comms_wait", { timeout: 5 });
      expect(JSON.parse(w.text).messages.map((m: any) => m.body)).toEqual(["for-ag1"]);
    } finally { c.kill(); srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);

  test("B2 non-request JSON lines (null / 3 / {}) are -32600, never a crash", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    const c = mcpClient(srv.url, tok);
    try {
      await c.init();
      for (const line of ["null", "3", "{}", '"x"', '{"jsonrpc":"2.0","id":{"o":1},"method":"ping"}']) {
        const r = await c.raw(line);
        expect(r.error?.code).toBe(-32600);
      }
      expect((await c.rpc("ping")).result).toEqual({});
      expect(c.proc.exitCode).toBeNull();
    } finally { c.kill(); srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);

  test("B3 post survives an ambiguous transport timeout exactly once (auto idempotency key + replay-safe retry)", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    let delayed = false;
    // proxy: the FIRST post commits upstream but its response is held past
    // the adapter's per-RPC timeout ⇒ RpcBus sees `unavailable/timeout`.
    const proxy = Bun.serve({
      port: 0, hostname: "127.0.0.1",
      async fetch(req) {
        const body = await req.text();
        const up = await fetch(`${srv.url}/rpc`, { method: "POST", headers: { "content-type": "application/json", authorization: req.headers.get("authorization") ?? "" }, body });
        const txt = await up.text();
        if (!delayed && body.includes('"method":"post"')) { delayed = true; await Bun.sleep(900); }
        return new Response(txt, { status: up.status, headers: { "content-type": "application/json", "x-comms-agent": up.headers.get("x-comms-agent") ?? "" } });
      },
    });
    const c = mcpClient(`http://127.0.0.1:${proxy.port}`, tok, {}, ["--timeout-ms", "300"]);
    try {
      await c.init();
      const r = await c.callTool("comms_post", { to: "root", type: "note", body: "idem-probe", channel: "idem" });
      expect(r.isError).toBe(false);
      const h = await c.callTool("comms_history", { channel: "idem" });
      expect(JSON.parse(h.text).rows.filter((x: any) => x.body === "idem-probe").length).toBe(1);
      expect(JSON.parse(h.text).rows[0].id).toBe(JSON.parse(r.text).id);
    } finally { c.kill(); proxy.stop(true); srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);

  test("M1 send-then-close: a large response to a slow pipe reader is delivered whole (no process.exit truncation)", async () => {
    const home = tmp(); bootstrap(home); const ag1 = seed(home);
    const b = openBus({ home, mode: "local" });
    for (let i = 0; i < 300; i++) b.post(localCtx("root"), { from: "root", to: "ag1", type: "note", body: "x".repeat(900) } as any);
    b.close();
    const srv = startServer({ home, port: 0 });
    const env: Record<string, string | undefined> = { ...process.env };
    for (const k of Object.keys(env)) if (k.startsWith("COMMS_")) delete env[k];
    const proc = Bun.spawn([process.execPath, join(REPO, "bin/mcp.ts")], { stdin: "pipe", stdout: "pipe", stderr: "pipe", env: { ...env, COMMS_URL: srv.url, COMMS_TOKEN: ag1 } as Record<string, string> });
    try {
      proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "p", version: "0" } } }) + "\n");
      proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/call", params: { name: "comms_inbox", arguments: { limit: 500 } } }) + "\n");
      proc.stdin.end();
      await Bun.sleep(1500); // reader stalls: the child's stdout backs up past the 64 KB pipe buffer
      const out = await new Response(proc.stdout).text();
      const lines = out.trim().split("\n").map((l) => JSON.parse(l));
      const inbox = JSON.parse(lines.find((m) => m.id === 2).result.content[0].text);
      expect(inbox.rows.length).toBe(300);
      expect(await proc.exited).toBe(0);
    } finally { proc.kill(); srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);

  test("M2 inbox is bounded by default (newest 50 + total/truncated); mark:true is never truncated", async () => {
    const home = tmp(); bootstrap(home); const ag1 = seed(home);
    postLocal(home, "root", "ag1", "row", 80);
    const srv = startServer({ home, port: 0 });
    const c = mcpClient(srv.url, ag1);
    try {
      await c.init();
      const v = JSON.parse((await c.callTool("comms_inbox", {})).text);
      expect(v.rows.length).toBe(50);
      expect(v.total).toBe(80);
      expect(v.truncated).toBe(30);
      const m = JSON.parse((await c.callTool("comms_inbox", { mark: true, limit: 5 })).text);
      expect(m.rows.length).toBe(80);
      expect(m.truncated).toBe(0);
    } finally { c.kill(); srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);

  test("M3 lifecycle: version negotiation, pre-initialize gate, cancellation, EOF aborts a pending wait", async () => {
    const home = tmp(); const tok = bootstrap(home);
    const srv = startServer({ home, port: 0 });
    const c = mcpClient(srv.url, tok, FAST);
    try {
      expect((await c.rpc("ping")).result).toEqual({}); // ping is allowed pre-init
      expect((await c.rpc("tools/list")).error?.code).toBe(-32002);
      expect((await c.init("2025-06-18")).result.protocolVersion).toBe("2025-06-18");
      // cancellation: the cancelled wait never gets a response, and the loop stops
      const w = c.send("tools/call", { name: "comms_wait", arguments: { timeout: 20 } });
      await Bun.sleep(300);
      await c.rpc("notifications/cancelled", { requestId: w.id, reason: "test" }, true);
      expect((await c.rpc("ping")).result).toEqual({});
      await Bun.sleep(400);
      expect(c.seen.some((m) => m.id === w.id)).toBe(false);
      // EOF with a wait in flight: it returns promptly and the process exits 0
      const w2 = c.send("tools/call", { name: "comms_wait", arguments: { timeout: 20 } });
      await Bun.sleep(300);
      const t0 = Date.now();
      c.proc.stdin.end();
      const r2 = await w2.done;
      expect(r2.result.isError).toBeUndefined();
      expect(await c.proc.exited).toBe(0);
      expect(Date.now() - t0).toBeLessThan(3000);
    } finally { c.kill(); srv.stop(); rmSync(home, { recursive: true, force: true }); }
    const c2 = mcpClient(srv.url, tok);
    try { expect((await c2.init("1999-01-01")).result.protocolVersion).toBe("2024-11-05"); } finally { c2.kill(); }
  }, 30_000);

  test("M4 stale durable cursor after epoch rotation ⇒ comms_wait performs the §6 recovery commit (agent tokens cannot re-baseline via unfiltered history)", async () => {
    const home = tmp(); bootstrap(home); const ag1 = seed(home);
    let srv = startServer({ home, port: 0 });
    let c = mcpClient(srv.url, ag1, FAST);
    try {
      await c.init();
      postLocal(home, "root", "ag1", "before");
      const w = JSON.parse((await c.callTool("comms_wait", { timeout: 2 })).text);
      expect((await c.callTool("comms_cursor_set", { cursor: w.cursor })).isError).toBe(false);
      c.kill(); srv.stop();
      const b = openBus({ home, mode: "local" }); b.rotateEpoch(); b.close();
      srv = startServer({ home, port: 0 });
      c = mcpClient(srv.url, ag1, FAST);
      await c.init();
      expect((await c.callTool("comms_cursor_get", {})).text).toStartWith("error(resync)");
      const r = JSON.parse((await c.callTool("comms_wait", { timeout: 1 })).text);
      expect(r.resynced).toBe(true);
      expect((await c.callTool("comms_cursor_get", {})).isError).toBe(false);
      postLocal(home, "root", "ag1", "after");
      const w2 = JSON.parse((await c.callTool("comms_wait", { timeout: 5 })).text);
      // at-least-once: the recovery commit is <epoch>.<floor>, so RETAINED
      // pre-rotation events may be redelivered — never lost.
      expect(w2.messages.map((m: any) => m.body)).toContain("after");
    } finally { c.kill(); srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 30_000);
});
