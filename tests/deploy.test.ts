/**
 * M6 deploy pins (claude M6 review): restore.sh is an EPOCH EVENT with
 * invariants I1..I4, backup.sh names only verified artifacts. Spawns the REAL
 * scripts. Skips when the host lacks sqlite3/flock (they are deploy prereqs,
 * not test deps).
 */
import { expect, describe, test } from "bun:test";
import { mkdtempSync, rmSync, readdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { openBus, localCtx } from "../src/bus.ts";

const REPO = import.meta.dir + "/..";
const have = (b: string) => Bun.spawnSync(["sh", "-c", `command -v ${b}`]).exitCode === 0;
const HAS = have("sqlite3") && have("flock") && have("fuser");
const BUN_DIR = dirname(process.execPath);
const PATH = `${BUN_DIR}:/usr/bin:/bin`;

function sh(script: string, args: string[], env: Record<string, string> = {}) {
  const p = Bun.spawnSync(["bash", join(REPO, "deploy", script), ...args], {
    env: { PATH, ...env }, stdout: "pipe", stderr: "pipe",
  });
  return { code: p.exitCode, out: p.stdout.toString(), err: p.stderr.toString() };
}
function seed(home: string) {
  const b = openBus({ home, mode: "local" });
  const c = localCtx("root");
  const admin = (b.tokenCreate(localCtx("bootstrap"), { agent: "root", admin: true }) as any).value!.token;
  const bot = (b.tokenCreate(c, { agent: "bot" }) as any).value!;
  for (let i = 0; i < 3; i++) b.post(c, { from: "root", to: "root", type: "note", body: "pre-" + i });
  const [ep, seq] = (b.history(c, {}) as any).value!.cursor.split(".");
  expect(b.cursorSet("root", "cli", ep, Number(seq)).error).toBeUndefined();
  b.close();
  return { admin, bot };
}
const epochOf = (home: string) => { const b = openBus({ home, mode: "local" }); const e = b.epoch(); b.close(); return e; };

describe.skipIf(!HAS)("M6 deploy: backup/restore invariants", () => {
  test("I2 happy path: epoch rotated, gc_floor 0, stored cursor ⇒ resync, single clean file", () => {
    const d = mkdtempSync(join(tmpdir(), "m6-")), home = join(d, "h");
    try {
      seed(home);
      const e0 = epochOf(home);
      expect(sh("backup.sh", [home, join(d, "bk")]).code).toBe(0);
      const art = readdirSync(join(d, "bk"));
      expect(art.every((f) => /^comms-\d{8}T\d{6}Z\.db$/.test(f))).toBe(true); // no .part left
      const r = sh("restore.sh", [join(d, "bk", art[0]), home]);
      expect(r.code).toBe(0);
      const b = openBus({ home, mode: "local" });
      expect(b.epoch()).not.toBe(e0);
      expect(b.gcFloor()).toBe(0);
      const g = b.cursorGet("root", "cli");
      expect(g.error).toBe("resync");
      b.close();
      expect(readdirSync(home).some((f) => f.startsWith(".restore-stage"))).toBe(false);
    } finally { rmSync(d, { recursive: true, force: true }); }
  });

  test("I1: refuses while the §9 latch is held (live server) — image untouched", async () => {
    const d = mkdtempSync(join(tmpdir(), "m6-")), home = join(d, "h");
    try {
      seed(home);
      sh("backup.sh", [home, join(d, "bk")]);
      const e0 = epochOf(home);
      const holder = Bun.spawn(["flock", "-n", join(home, ".server.lock"), "sleep", "5"]);
      await Bun.sleep(200);
      const r = sh("restore.sh", [join(d, "bk", readdirSync(join(d, "bk"))[0]), home]);
      holder.kill();
      expect(r.code).toBe(1);
      expect(r.err).toContain("single writer");
      expect(epochOf(home)).toBe(e0);
    } finally { rmSync(d, { recursive: true, force: true }); }
  });

  test("I2: missing bun fails BEFORE touching the live image (no restored-image-with-old-epoch state)", () => {
    const d = mkdtempSync(join(tmpdir(), "m6-")), home = join(d, "h");
    try {
      seed(home);
      sh("backup.sh", [home, join(d, "bk")]);
      const b = openBus({ home, mode: "local" });
      b.post(localCtx("root"), { from: "root", to: "root", type: "note", body: "post-backup" });
      b.close();
      const e0 = epochOf(home);
      const p = Bun.spawnSync(["bash", join(REPO, "deploy/restore.sh"), join(d, "bk", readdirSync(join(d, "bk"))[0]), home], {
        env: { PATH: "/usr/bin:/bin", BUN: "/nonexistent/bun" }, stdout: "pipe", stderr: "pipe",
      });
      expect(p.exitCode).toBe(1);
      const after = openBus({ home, mode: "local" });
      expect(after.epoch()).toBe(e0);
      expect((after.history(localCtx("root"), {}) as any).value!.rows.some((r: any) => r.body === "post-backup")).toBe(true);
      after.close();
    } finally { rmSync(d, { recursive: true, force: true }); }
  });

  test("I3 + I4: safety copy keeps committed WAL rows; post-backup revocation is carried forward", () => {
    const d = mkdtempSync(join(tmpdir(), "m6-")), home = join(d, "h");
    try {
      const { bot } = seed(home);
      sh("backup.sh", [home, join(d, "bk")]);
      // revoke the (leaked) bot token and leave committed rows in -wal (crash: no close/checkpoint)
      const k = Bun.spawnSync([process.execPath, "-e", `
        const {openBus,localCtx}=await import(${JSON.stringify(join(REPO, "src/bus.ts"))});
        const b=openBus({home:${JSON.stringify(home)},mode:"local"});
        b.tokenRevoke(localCtx("root"),{id:${bot.id}});
        for(let i=0;i<4;i++)b.post(localCtx("root"),{from:"root",to:"root",type:"note",body:"wal-"+i});
        process.kill(process.pid,"SIGKILL");`]);
      expect(k.signalCode).toBe("SIGKILL");
      expect(existsSync(join(home, ".comms/comms.db-wal"))).toBe(true);
      const r = sh("restore.sh", [join(d, "bk", readdirSync(join(d, "bk"))[0]), home]);
      expect(r.code).toBe(0);
      expect(r.out).toContain("revocations carried forward: 1");
      const b = openBus({ home, mode: "local" });
      expect(b.tokenVerify(bot.token).error).toBe("unauthorized");
      b.close();
      const pre = readdirSync(join(home, ".comms")).find((f) => f.includes("prerestore"))!;
      const q = Bun.spawnSync(["sqlite3", join(home, ".comms", pre), "select count(*) from messages where body like 'wal-%'"]);
      expect(q.stdout.toString().trim()).toBe("4");
    } finally { rmSync(d, { recursive: true, force: true }); }
  });
});
