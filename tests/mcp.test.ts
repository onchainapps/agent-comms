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

/** a line-delimited MCP client over a spawned bin/mcp.ts */
function mcpClient(url: string, token: string) {
  const proc = Bun.spawn([process.execPath, join(REPO, "bin/mcp.ts")], {
    stdin: "pipe", stdout: "pipe", stderr: "pipe",
    env: { ...process.env, COMMS_URL: url, COMMS_TOKEN: token },
  });
  let buf = "";
  const queue: ((m: any) => void)[] = [];
  const msgs: any[] = []; void msgs;
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
        const w = queue.shift();
        if (w) w(m); else msgs.push(m);
      }
    }
  })();
  let id = 0;
  async function rpc(method: string, params?: object, notify = false): Promise<any> {
    const rid = ++id;
    const line = JSON.stringify({ jsonrpc: "2.0", method, ...(params ? { params } : {}), ...(notify ? {} : { id: rid }) });
    if (notify) { proc.stdin.write(line + "\n"); return undefined; } // NO resolver — a notification never gets a response, and a queued resolver would steal the NEXT message
    const pend = new Promise<any>((res) => queue.push(res));
    proc.stdin.write(line + "\n");
    return pend;
  }
  return {
    rpc,
    proc,
    /** send one raw line, resolve with the next message that arrives. */
    raw(line: string): Promise<any> {
      let settled = false;
      const pend = new Promise<any>((res) => queue.push((m) => { settled = true; res(m); }));
      proc.stdin.write(line + "\n");
      return Promise.race([
        pend,
        new Promise<any>((r) => setTimeout(() => { if (!settled) queue.shift(); r({ __timeout: true }); }, 2000)),
      ]);
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
      for (const n of ["comms_rename", "comms_token_create", "comms_token_list", "comms_token_revoke", "comms_group_create", "comms_group_delete", "comms_group_leave", "comms_cursor_get", "comms_who", "comms_channels", "comms_thread", "comms_status", "comms_history"])
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
      // schema validation server-side (adapter): missing required + enum + pattern
      const bad1 = await c.callTool("comms_post", { to: "x" });
      expect(bad1.err?.code).toBe(-32602);
      const bad2 = await c.callTool("comms_post", { to: "x", type: "nope", body: "b" });
      expect(bad2.err?.code).toBe(-32602);
      const bad3 = await c.callTool("comms_cursor_set", { cursor: "not-a-cursor" });
      expect(bad3.err?.code).toBe(-32602);
      const bad4 = await c.callTool("comms_post", { to: "x", type: "note", body: "b", bogus: 1 });
      expect(bad4.err?.code).toBe(-32602);
      const badTool = await c.callTool("no_such_tool", {});
      expect(badTool.err?.code).toBe(-32602);
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
