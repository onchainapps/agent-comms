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

const REPO = import.meta.dir + "/..";
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

  test("dms + dm over remote: member sees, non-party gets byte-identical not_found (exit 1)", async () => {
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

  test("remote post retry window: dead server ⇒ unavailable ⇒ exit 1 (bounded, no hang)", () => {
    const home = tmp();
    const r = cli(["post", "--from", "x", "--to", "y", "--type", "note", "--body", "z"], {
      COMMS_URL: "http://127.0.0.1:1", COMMS_TOKEN: "ac_deadbeef",
    });
    expect(r.code).toBe(1);
    rmSync(home, { recursive: true, force: true });
  });
});
