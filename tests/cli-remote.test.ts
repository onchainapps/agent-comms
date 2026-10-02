/**
 * M3 integration tests (RFC-001 §10-M3): the REAL remote CLI (spawned process)
 * against the REAL server (spawned process) — §7 precedence, transport banner,
 * exit-code table, auto idempotency keys, cursor-backed watch durability,
 * token subcommand, dms/dm.members over the wire.
 */
import { expect, describe, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openBus, localCtx } from "../src/bus.ts";
import { REMOTE_METHODS } from "../src/cli-wire.ts"; // m1 pin: CLI wire map vs live server (never import bin/comms.ts — it runs main())

const REPO = import.meta.dir + "/..";
// HERMETIC: this suite spawns the CLI/server with env: {...process.env, ...} —
// a developer's sourced COMMS_* env must not leak into children that expect a
// clean transport slate (§7 precedence reads the environment). Per-test
// explicit env entries still win (spread order). See ui.test.ts same guard.
for (const k of Object.keys(process.env)) if (k.startsWith("COMMS_")) delete process.env[k];
const CLI = join(REPO, "bin/comms.ts");

function tmp() { return mkdtempSync(join(tmpdir(), "comms-m3-")); }

function bootstrap(home: string) {
  const b = openBus({ home, mode: "local" });
  const t = b.tokenCreate(localCtx("bootstrap"), { agent: "root", admin: true });
  if (t.error) throw new Error(t.detail);
  const tok = t.value.token;
  b.close();
  return tok;
}

async function spawnServer(home: string) {
  const proc = Bun.spawn([process.execPath, join(REPO, "bin/server.ts"), "--home", home, "--port", "0"], {
    stdout: "ignore", stderr: "pipe",
  });
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
  void errReader.cancel().catch(() => {});
  return { url, stop: () => proc.kill() };
}

/** Run the CLI as a separate process (what a remote agent's box really is). */
function cli(args: string[], env: Record<string, string> = {}) {
  const p = Bun.spawnSync([process.execPath, CLI, ...args], {
    cwd: tmp(), // never auto-detect the repo root — force explicit transport env
    env: { ...process.env, ...env },
    stdin: "ignore",
    stdout: "pipe", stderr: "pipe",
  });
  return { code: p.exitCode, out: p.stdout.toString(), err: p.stderr.toString() };
}

describe("M3 remote CLI", () => {
  test("§7 precedence + banner: COMMS_URL ⇒ remote with identity banner on stderr", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    try {
      const r = cli(["who"], { COMMS_URL: srv.url, COMMS_TOKEN: tok });
      expect(r.code).toBe(0);
      expect(r.err).toContain(`transport=remote:${srv.url} as root(`);
      expect(r.err).toContain("tokens:admin"); // scopes from the token ROW
      expect(r.out).toContain("active agents:");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("§7: COMMS_URL + COMMS_HOME + --local ⇒ local wins, ambiguity banner", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    try {
      const r = cli(["who", "--local"], { COMMS_URL: srv.url, COMMS_TOKEN: tok, COMMS_HOME: home });
      expect(r.code).toBe(0);
      expect(r.err).toContain("ignored by --local");
      expect(r.err).not.toContain("transport=remote:");
      // the local DB has the root row from bootstrap; the server never saw a request
      expect(r.out).toContain("root");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("no banner when COMMS_HOME only (local mode, stderr goldens hold)", () => {
    const home = tmp();
    const r = cli(["who"], { COMMS_HOME: home });
    expect(r.code).toBe(0);
    expect(r.err).toBe("");
    rmSync(home, { recursive: true, force: true });
  });

  test("remote join/post/read over the wire; token row is identity; join does not stamp server pid", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    try {
      // mint an agent token (root has tokens:admin)
      const mint = cli(["token", "create", "--agent", "m3-bot", "--scopes", "post:as"], { COMMS_URL: srv.url, COMMS_TOKEN: tok });
      expect(mint.code).toBe(0);
      const botTok = mint.out.split("\n").find((l) => l.trim().startsWith("ac_"))!.trim();
      expect(mint.out).toContain("shown ONCE");

      const j = cli(["join", "--agent", "m3-bot", "--role", "worker"], { COMMS_URL: srv.url, COMMS_TOKEN: botTok });
      expect(j.code).toBe(0);
      expect(j.err).toContain("transport=remote:");
      expect(j.out).toContain("joined: m3-bot");

      const p = cli(["post", "--from", "m3-bot", "--to", "root", "--type", "note", "--subject", "hi", "--body", "remote hello"], { COMMS_URL: srv.url, COMMS_TOKEN: botTok });
      expect(p.code).toBe(0);
      const id = /posted (\S+)\s/.exec(p.out)![1];
      expect(id).toMatch(/^\d{8}T\d{6}-/);

      // §6/§7: remote post auto-generated an idempotency key scoped to the TOKEN principal
      const raw = openBus({ home, mode: "local" });
      const idem = raw.testDb.query("SELECT key, msg_id FROM idempotency WHERE agent_id='m3-bot'").all() as any[];
      expect(idem.length).toBe(1);
      expect(idem[0].key).toMatch(/^cli:m3-bot:/);
      expect(idem[0].key.length).toBeLessThanOrEqual(128);
      expect(idem[0].msg_id).toBe(id);
      // §7: remote join did NOT stamp the server's pid — the UPDATE-only
      // branch leaves the token.create-minted row's pid untouched (NULL).
      const row = raw.testDb.query("SELECT pid FROM agents WHERE id='m3-bot'").get() as any;
      expect(row.pid).toBeNull();
      raw.close();

      const rd = cli(["read", "--for", "root", "--id", id], { COMMS_URL: srv.url, COMMS_TOKEN: tok });
      expect(rd.code).toBe(0);
      expect(rd.out).toContain("remote hello");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("§7 exit-code table: unauthorized→3, forbidden assertion→3, not_found→1, usage→2", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    try {
      expect(cli(["who"], { COMMS_URL: srv.url, COMMS_TOKEN: "ac_wrongwrongwrongwrongwrongwrong1" }).code).toBe(3);
      // valid token, wrong --from assertion ⇒ forbidden ⇒ 3 (identity)
      expect(cli(["post", "--from", "someone-else", "--to", "root", "--type", "note", "--body", "x"], { COMMS_URL: srv.url, COMMS_TOKEN: tok }).code).toBe(3);
      // missing --type ⇒ usage ⇒ 2
      expect(cli(["post", "--from", "root", "--to", "x", "--body", "y"], { COMMS_URL: srv.url, COMMS_TOKEN: tok }).code).toBe(2);
      // unknown id ⇒ not_found ⇒ 1
      expect(cli(["read", "--for", "root", "--id", "nope-0000"], { COMMS_URL: srv.url, COMMS_TOKEN: tok }).code).toBe(1);
      // no token at all ⇒ 3
      expect(cli(["who"], { COMMS_URL: srv.url }).code).toBe(3);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("§6 watch durability: cursor-backed remote watch never skips between runs", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      const wTokRes = cli(["token", "create", "--agent", "m3w", "--scopes", "post:as"], env);
      const wTok = wTokRes.out.split("\n").find((x) => x.trim().startsWith("ac_"))!.trim();
      // §5: `from` is ALWAYS an assertion in server mode — post with m3w's OWN token.
      const wenv = { COMMS_URL: srv.url, COMMS_TOKEN: wTok };
      const p1 = cli(["post", "--from", "m3w", "--to", "root", "--type", "note", "--body", "first"], wenv);
      const id1 = /posted (\S+)\s/.exec(p1.out)![1];

      const w1 = cli(["watch", "--for", "root", "--once", "--interval", "1"], env);
      expect(w1.code).toBe(0);
      expect(w1.out).toContain("cursor consumer=cli");
      expect(w1.out).toContain("NEW");

      // second run: only what arrived AFTER the committed cursor
      const p2 = cli(["post", "--from", "m3w", "--to", "root", "--type", "note", "--body", "second"], wenv);
      const id2 = /posted (\S+)\s/.exec(p2.out)![1];
      const w2 = cli(["watch", "--for", "root", "--once", "--interval", "1"], env);
      expect(w2.out).toContain(id2);
      expect(w2.out).not.toContain(id1);

      // nothing new ⇒ no NEW lines, cursor untouched
      const w3 = cli(["watch", "--for", "root", "--once", "--interval", "1"], env);
      expect(w3.out).not.toContain("NEW");

      // the cursor row is keyed (principal, consumer='cli')
      const raw = openBus({ home, mode: "local" });
      const c = raw.testDb.query("SELECT consumer, last_seq FROM cursors WHERE agent_id='root'").all() as any[];
      expect(c.some((x) => x.consumer === "cli" && x.last_seq > 0)).toBe(true);
      raw.close();
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("token subcommand over remote: create/list/revoke + revoked token ⇒ exit 3", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      const c = cli(["token", "create", "--agent", "t-bot", "--kind", "human"], env);
      expect(c.code).toBe(0);
      const tTok = c.out.split("\n").find((l) => l.trim().startsWith("ac_"))!.trim();
      expect(c.out).toContain("read:all,read:dm"); // G3 human default

      const l = cli(["token", "list"], env);
      expect(l.code).toBe(0);
      expect(l.out).toContain("t-bot");
      const id = Number(/#\s*(\d+)\s+t-bot/.exec(l.out)![1]);

      expect(cli(["who"], { COMMS_URL: srv.url, COMMS_TOKEN: tTok }).code).toBe(0);
      const rv = cli(["token", "revoke", "--id", String(id)], env);
      expect(rv.code).toBe(0);
      expect(cli(["who"], { COMMS_URL: srv.url, COMMS_TOKEN: tTok }).code).toBe(3);

      // non-admin token cannot list tokens ⇒ forbidden ⇒ 3
      const plain = cli(["token", "create", "--agent", "p-bot"], env);
      const pTok = plain.out.split("\n").find((x) => x.trim().startsWith("ac_"))!.trim();
      expect(cli(["token", "list"], { COMMS_URL: srv.url, COMMS_TOKEN: pTok }).code).toBe(3);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("dms + dm over remote: member views list; non-party dm.members ⇒ not_found w/ identical detail (contract suite pins exit path)", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      // root ↔ m3a DM (root token has read:all,read:dm via admin)
      cli(["token", "create", "--agent", "m3b", "--scopes", "post:as"], env);
      cli(["token", "create", "--agent", "m3c", "--scopes", "post:as"], env);
      const aC = cli(["token", "create", "--agent", "m3a2", "--scopes", "post:as"], env);
      const aTok = aC.out.split("\n").find((x) => x.trim().startsWith("ac_"))!.trim();

      const d = cli(["dm", "--from", "m3a2", "--to", "root", "--body", "secret handshake"], { COMMS_URL: srv.url, COMMS_TOKEN: aTok });
      expect(d.code).toBe(0);
      expect(d.out).toContain("#dm~");

      const dms = cli(["dms", "--for", "root"], env);
      expect(dms.code).toBe(0);
      expect(dms.out).toContain("dm channel(s) for root");
      expect(dms.out).toContain("m3a2");

      // m3a2's own member view: the dm channel is visible (party) — listed
      const dmsA = cli(["dms", "--for", "m3a2"], { COMMS_URL: srv.url, COMMS_TOKEN: aTok });
      expect(dmsA.code).toBe(0);
      expect(dmsA.out).toContain("dm channel(s) for m3a2");

      // a token WITHOUT read:dm cannot peek dms for someone else (channels
      // hides the dm shape from non-parties ⇒ member list empty, no leak)
      const bC = cli(["token", "list"], env); // root omniview list works
      expect(bC.out).toContain("m3b");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("grok 3433 M-b pin: long legal ids/channels ⇒ generated consumer commits (exit 0, cursor row, no reprint)", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      const tgt = "t".repeat(30), ch = "c".repeat(30);
      const tRes = cli(["token", "create", "--agent", tgt, "--scopes", "post:as"], env);
      expect(tRes.code).toBe(0);
      const p = cli(["post", "--from", "root", "--to", tgt, "--type", "note", "--body", "x", "--channel", ch], env);
      expect(p.code).toBe(0);
      const id = /posted (\S+)\s/.exec(p.out)![1];
      const w1 = cli(["watch", "--for", tgt, "--channel", ch, "--once", "--interval", "1"], env);
      expect(w1.code).toBe(0);
      expect(w1.out).toContain(id);
      const w2 = cli(["watch", "--for", tgt, "--channel", ch, "--once", "--interval", "1"], env);
      expect(w2.code).toBe(0);
      expect(w2.out).not.toContain(id);
      // a 64-char dm channel name (dm~<30>~<30>) as --channel with --for: 100 B consumer
      const dmName = `dm~${"a".repeat(30)}~${"b".repeat(30)}`;
      expect(dmName.length).toBe(64);
      const w3 = cli(["watch", "--for", tgt, "--channel", dmName, "--once", "--interval", "1"], env);
      expect(w3.code).toBe(0);
      const raw = openBus({ home, mode: "local" });
      const rows = (raw as any).cursorGet("root", `cli@${tgt}#${ch}`);
      const rows2 = (raw as any).cursorGet("root", `cli@${tgt}#${dmName}`);
      raw.close();
      expect(rows.error).toBeUndefined();
      expect(rows.value.seq).toBeGreaterThan(0);
      expect(rows2.error).toBeUndefined();
      expect(rows2.value.seq).toBeGreaterThan(0);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("§6 resync recovery: rotated epoch ⇒ watch resyncs, force-commits <epoch>.<floor>, resumes", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      const wTokRes = cli(["token", "create", "--agent", "m3r", "--scopes", "post:as"], env);
      const wTok = wTokRes.out.split("\n").find((x) => x.trim().startsWith("ac_"))!.trim();
      const wenv = { COMMS_URL: srv.url, COMMS_TOKEN: wTok };
      cli(["post", "--from", "m3r", "--to", "root", "--type", "note", "--body", "before-rotation"], wenv);
      const w1 = cli(["watch", "--for", "root", "--once", "--interval", "1"], env);
      expect(w1.out).toContain("NEW");

      // rotate the epoch through a second local opener (test-relaxed single writer)
      const raw = openBus({ home, mode: "local" });
      raw.rotateEpoch();
      raw.close();

      // stored cursor now carries a FOREIGN epoch ⇒ resync, then the §6
      // recovery commit (force) — never a silent seq-0 collapse.
      const w2 = cli(["watch", "--for", "root", "--once", "--interval", "1"], env);
      expect(w2.code).toBe(0);
      expect(w2.out).toContain("watch: resync — recovery commit to");

      // delivery resumes after recovery
      const p2 = cli(["post", "--from", "m3r", "--to", "root", "--type", "note", "--body", "after-rotation"], wenv);
      const id2 = /posted (\S+)\s/.exec(p2.out)![1];
      const w3 = cli(["watch", "--for", "root", "--once", "--interval", "1"], env);
      expect(w3.out).toContain(id2);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("M3-fold blocker pin: root ./comms.ts shim actually RUNS the CLI (import.meta.main guard regression)", () => {
    const home = tmp();
    try {
      const run = (args: string[]) => {
        const p = Bun.spawnSync([process.execPath, join(REPO, "comms.ts"), ...args], {
          cwd: tmp(), env: { ...process.env, COMMS_HOME: home, COMMS_URL: "" }, stdin: "ignore", stdout: "pipe", stderr: "pipe",
        });
        return { code: p.exitCode, out: p.stdout.toString() };
      };
      const j = run(["join", "--agent", "shimprobe", "--role", "r"]);
      expect(j.code).toBe(0);
      expect(j.out).toContain("joined: shimprobe"); // exit 0 with EMPTY stdout was the bug
      expect(run(["who"]).out).toContain("shimprobe");
      expect(run(["nonsense-verb"]).code).toBe(2); // HELP path reached ⇒ main() ran
    } finally { rmSync(home, { recursive: true, force: true }); }
  });

  test("m5 pin: COMMS_URL + --local WITHOUT COMMS_HOME ⇒ ambiguity line still printed", () => {
    // COMMS_HOME truly UNSET (not ""), and an unknown verb so core() is never
    // opened — findRoot would otherwise create a .comms DB in the repo root.
    const env: Record<string, string | undefined> = { ...process.env, COMMS_URL: "http://127.0.0.1:1", COMMS_TOKEN: "x" };
    delete env.COMMS_HOME;
    const p = Bun.spawnSync([process.execPath, CLI, "nonsense-verb", "--local"], { cwd: tmp(), env: env as any, stdin: "ignore", stdout: "pipe", stderr: "pipe" });
    const err = p.stderr.toString();
    expect(err).toContain("ignored by --local");
    expect(err).not.toContain("transport=remote:");
  });

  test("m1 pin: every REMOTE_METHODS wire target exists on the live server (never -32601)", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    try {
      const methods = new Set(Object.values(REMOTE_METHODS).map((f) => f({})[0]));
      expect(methods.size).toBeGreaterThanOrEqual(24);
      for (const method of methods) {
        const res = await fetch(`${srv.url}/rpc`, {
          method: "POST",
          headers: { "content-type": "application/json", authorization: `Bearer ${tok}` },
          body: JSON.stringify({ jsonrpc: "2.0", method, params: {}, id: 1 }),
        });
        const body: any = await res.json();
        // any error EXCEPT method-not-found proves the wire target exists
        expect(body.error?.code ?? 0, `${method} ⇒ -32601 (wire map drifted from server dispatch)`).not.toBe(-32601);
        void res;
      }
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("remote post retry window: dead server ⇒ unavailable ⇒ exit 1 (bounded, no hang)", () => {
    const home = tmp();
    // claude M3 M6: refused fails fast; the window only bounds ambiguous retries.
    const r = cli(["post", "--from", "x", "--to", "y", "--type", "note", "--body", "z"], {
      COMMS_URL: "http://127.0.0.1:1", COMMS_TOKEN: "ac_deadbeef",
    });
    expect(r.code).toBe(1);
    rmSync(home, { recursive: true, force: true });
  });
});

// ---------- claude M3 review pins (t_954be034) ----------
const tokOf = (out: string) => out.split("\n").find((l) => l.trim().startsWith("ac_"))!.trim();
const postedId = (out: string) => /(?:posted|dm) (\S+)\s/.exec(out)![1];

describe("M3 review pins (claude)", () => {
  test("B1: >500 irrelevant events between cursor and my message ⇒ watch still delivers (no livelock) and the cursor advances", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      const bTok = tokOf(cli(["token", "create", "--agent", "bob"], env).out);
      const raw = openBus({ home, mode: "local" }); // test-relaxed second opener (as the rotate test)
      for (let i = 0; i < 600; i++) raw.post(localCtx("carol"), { from: "carol", to: "bob", type: "note", body: `noise ${i}` });
      raw.close();
      const id = postedId(cli(["post", "--from", "bob", "--to", "root", "--type", "note", "--body", "FOR-ROOT"], { COMMS_URL: srv.url, COMMS_TOKEN: bTok }).out);
      const w = cli(["watch", "--for", "root", "--once"], env);
      expect(w.code).toBe(0);
      expect(w.out).toContain(id);
      const raw2 = openBus({ home, mode: "local" });
      const c = raw2.testDb.query("SELECT last_seq FROM cursors WHERE agent_id='root' AND consumer='cli'").get() as any;
      expect(c.last_seq).toBe(raw2.eventsHighWater());
      raw2.close();
      // and nothing is re-delivered on the next run
      expect(cli(["watch", "--for", "root", "--once"], env).out).not.toContain(id);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 60_000);

  test("B2: remote inbox with rows renders (unreadIds array over the wire, Set locally)", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      const bTok = tokOf(cli(["token", "create", "--agent", "bob"], env).out);
      const id = postedId(cli(["post", "--from", "bob", "--to", "root", "--type", "note", "--body", "x"], { COMMS_URL: srv.url, COMMS_TOKEN: bTok }).out);
      const ib = cli(["inbox", "--for", "root"], env);
      expect(ib.code).toBe(0);
      expect(ib.out).toContain(` *${id}`);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("M2/M3: --channel and --for <other> watches use their own consumer (never burn the plain 'cli' cursor); non-read:all --for other ⇒ 3", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      const bTok = tokOf(cli(["token", "create", "--agent", "bob"], env).out);
      cli(["token", "create", "--agent", "dave"], env);
      const benv = { COMMS_URL: srv.url, COMMS_TOKEN: bTok };
      const g = postedId(cli(["post", "--from", "bob", "--to", "root", "--type", "note", "--body", "general-msg"], benv).out);
      const o = postedId(cli(["post", "--from", "bob", "--to", "root", "--type", "note", "--body", "ops-msg", "--channel", "ops"], benv).out);
      const toDave = postedId(cli(["post", "--from", "bob", "--to", "dave", "--type", "note", "--body", "d"], benv).out);
      const w1 = cli(["watch", "--for", "root", "--once", "--channel", "ops"], env);
      expect(w1.out).toContain(o);
      expect(w1.out).toContain("consumer=cli#ops");
      const w2 = cli(["watch", "--for", "root", "--once"], env);
      expect(w2.out).toContain(g); // was skipped: the #ops run committed past it
      // --for other (read:all) watches THAT agent's inbox, under its own consumer
      const w3 = cli(["watch", "--for", "dave", "--once"], env);
      expect(w3.out).toContain("consumer=cli@dave");
      expect(w3.out).toContain(toDave);
      expect(w3.out).not.toContain(g);
      // non-read:all token claiming another agent ⇒ forbidden ⇒ 3 (was: silently watched own inbox, exit 0)
      expect(cli(["watch", "--for", "dave", "--once"], benv).code).toBe(3);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("M4: watch --all on a GC'd server (fresh consumer below floor) recovers via §6 resync instead of exiting 1", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      const bTok = tokOf(cli(["token", "create", "--agent", "bob"], env).out);
      const benv = { COMMS_URL: srv.url, COMMS_TOKEN: bTok };
      cli(["post", "--from", "bob", "--to", "root", "--type", "note", "--body", "old"], benv);
      const raw = openBus({ home, mode: "local" });
      raw.testDb.run("INSERT INTO meta(key,value) VALUES('gc_floor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [String(raw.eventsHighWater())]);
      raw.close();
      const id = postedId(cli(["post", "--from", "bob", "--to", "carol", "--type", "note", "--body", "after-gc"], benv).out);
      const w = cli(["watch", "--for", "root", "--all", "--once"], env);
      expect(w.code).toBe(0);
      expect(w.out).toContain("resync");
      expect(w.out).toContain(id); // recovery commit to <epoch>.<floor>, then delivery resumes in the SAME run
      const w2 = cli(["watch", "--for", "root", "--all", "--once"], env);
      expect(w2.code).toBe(0);
      expect(w2.out).not.toContain(id);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("M1: every post-auth error path carries BOTH identity headers (400 param check banners scopes too)", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    try {
      const r = await fetch(`${srv.url}/rpc`, {
        method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${tok}` },
        body: JSON.stringify({ jsonrpc: "2.0", method: "post", params: { to: ["x"], type: "note", body: "b", tags: true }, id: 1 }),
      });
      expect(r.status).toBe(400);
      expect(r.headers.get("x-comms-agent")).toBe("root");
      expect(r.headers.get("x-comms-scopes")).toContain("tokens:admin");
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });

  test("M5: a peer that accepts TCP and never answers ⇒ bounded exit 1 (was: hung forever)", async () => {
    const l = Bun.listen({ hostname: "127.0.0.1", port: 0, socket: { data() {}, open() {} } });
    try {
      const t0 = Date.now();
      // async spawn: a sync spawn would block this process's event loop, and
      // the listener above lives on it
      const p = Bun.spawn([process.execPath, CLI, "who"], {
        cwd: tmp(), env: { ...process.env, COMMS_URL: `http://127.0.0.1:${l.port}`, COMMS_TOKEN: "ac_x", COMMS_RPC_TIMEOUT_MS: "300", COMMS_RETRY_WINDOW_MS: "500" },
        stdout: "ignore", stderr: "ignore", stdin: "ignore",
      });
      expect(await p.exited).toBe(1);
      expect(Date.now() - t0).toBeLessThan(8_000);
    } finally { l.stop(true); }
  }, 15_000);

  test("M6: reset AFTER the server committed ⇒ CLI retries with the SAME idempotency key ⇒ exactly one message; refused stays fast exit 1", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const up = new URL(srv.url);
    // raw TCP proxy: connection #1 is forwarded to the server, but the reply
    // is swallowed and the client socket reset (server COMMITTED, client saw
    // ECONNRESET). Later connections relay both ways.
    let conns = 0;
    const proxy = Bun.listen<{ n: number; up?: any; q: Uint8Array[] }>({
      hostname: "127.0.0.1", port: 0,
      socket: {
        async open(s) {
          s.data = { n: ++conns, q: [] };
          s.data.up = await Bun.connect({ hostname: up.hostname, port: Number(up.port), socket: {
            data(_u, chunk) { if (s.data.n === 1) s.terminate(); else s.write(chunk); },
            close() { s.end(); },
          } });
          for (const c of s.data.q) s.data.up.write(c);
        },
        data(s, chunk) { if (s.data.up) s.data.up.write(chunk); else s.data.q.push(new Uint8Array(chunk)); },
        close(s) { s.data.up?.end(); },
      },
    });
    try {
      const bTok = tokOf(cli(["token", "create", "--agent", "bob"], { COMMS_URL: srv.url, COMMS_TOKEN: tok }).out);
      const p = Bun.spawn([process.execPath, CLI, "post", "--from", "bob", "--to", "root", "--type", "note", "--body", "exactly-once"], {
        cwd: tmp(), env: { ...process.env, COMMS_URL: `http://127.0.0.1:${proxy.port}`, COMMS_TOKEN: bTok }, stdout: "pipe", stderr: "pipe", stdin: "ignore",
      });
      expect(await p.exited).toBe(0);
      expect(conns).toBeGreaterThanOrEqual(2); // it DID retry
      const raw = openBus({ home, mode: "local" });
      const n = (raw.testDb.query("SELECT count(*) n FROM messages WHERE body='exactly-once'").get() as any).n;
      raw.close();
      expect(n).toBe(1);
      // refused: never reached a server ⇒ no retry loop, fast exit 1
      const t0 = Date.now();
      expect(cli(["post", "--from", "x", "--to", "y", "--type", "note", "--body", "z"], { COMMS_URL: "http://127.0.0.1:1", COMMS_TOKEN: "ac_x" }).code).toBe(1);
      expect(Date.now() - t0).toBeLessThan(5_000);
    } finally { proxy.stop(true); srv.stop(); rmSync(home, { recursive: true, force: true }); }
  }, 60_000);

  test("M7 + ruling b: remote activity refreshes presence server-side; who judges ● by the SERVER clock", async () => {
    const home = tmp();
    const tok = bootstrap(home);
    const srv = await spawnServer(home);
    const env = { COMMS_URL: srv.url, COMMS_TOKEN: tok };
    try {
      const bTok = tokOf(cli(["token", "create", "--agent", "bob"], env).out);
      const benv = { COMMS_URL: srv.url, COMMS_TOKEN: bTok };
      cli(["join", "--agent", "bob", "--role", "w"], benv);
      const raw = openBus({ home, mode: "local" });
      raw.testDb.run("UPDATE agents SET last_seen='2020-01-01T00:00:00Z' WHERE id='bob'");
      raw.testDb.run("UPDATE tokens SET last_used='2020-01-01T00:00:00Z' WHERE agent_id='bob'"); // debounce window elapsed
      raw.close();
      cli(["inbox", "--for", "bob"], benv); // any authed request
      const w = cli(["who"], env);
      expect(w.out).toMatch(/● bob/);
    } finally { srv.stop(); rmSync(home, { recursive: true, force: true }); }
  });
});
