/**
 * Golden harness (RFC-001 §10-M1, finding 4 rebuild):
 *  - runs under `bun test` (golden.test.ts imports this)
 *  - FROZEN seams via COMMS_TEST_SEAMS (clock/pid; rng stays REAL across CLI
 *    processes to avoid seeded id collisions) → ids/timestamps are still
 *    volatile because the de4ed3b BASELINE runs on wall-clock time, so masks
 *    (<ID>/<TS>/seen/last/pid/tmp + sorted ●/○ runs) remain necessary — the
 *    seams make the NEW impl deterministic; parity is against the old one.
 *  - mirror snapshots keyed SORTED (readdir order is filesystem-hash order —
 *    the old harness was flaky because of this, not behavior)
 *  - runs the script TWICE: on an empty home AND on a LEGACY fixture DB
 *    (de4ed3b schema, pre-trigger rows) to exercise the migration path
 *  - --update captures the baseline (use it ONLY against a known-good impl)
 */
import { mkdtempSync, rmSync, readdirSync, readFileSync, existsSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { Database } from "bun:sqlite";

const UPDATE = process.argv.includes("--update");
// Baseline capture: --cli <path> points at a DIFFERENT implementation's CLI
// (e.g. `git show de4ed3b:bin/comms.ts > /tmp/old-cli.ts`) so the golden is
// captured from the known-good implementation without stashing the tree.
const cliIdx = process.argv.indexOf("--cli");
const CLI = cliIdx >= 0 ? process.argv[cliIdx + 1] : join(import.meta.dir, "..", "bin", "comms.ts");
const SEAMS = JSON.stringify({ at: "2026-01-02T03:04:05.000Z", pid: 4242 }); // rng stays real; suffix masked

// Deterministic scripted session. With frozen seams, ids/timestamps repeat.
// Covers: happy path, thread/reply, filters, receipts, errors (usage→2,
// not_found→1, identity→3), rename, watch --once, and BOTH rejected-post
// variants that must still auto-register (legacy touch-before-validate).
const SCRIPT: Array<{ args: string[] }> = [
  { args: ["join", "--agent", "golden-1", "--role", "lab", "--caps", "gpu,kernels"] },
  { args: ["join", "--agent", "golden-2", "--role", "research"] },
  { args: ["post", "--from", "golden-1", "--to", "golden-2,research", "--type", "ask", "--subject", "Need GPU numbers", "--body", "Benchmark the kernel at 4k tiles.", "--tags", "perf,gpu"] },
  { args: ["post", "--from", "golden-2", "--to", "golden-1", "--type", "reply", "--re", "@LAST", "--subject", "re: Need GPU numbers", "--body", "Got numbers: 3.2 TFLOPs. See attached."] },
  { args: ["inbox", "--for", "golden-1", "--open"] },
  { args: ["read", "--for", "golden-1", "--id", "@FIRST"] },
  { args: ["receipts", "--id", "@FIRST"] },
  { args: ["channels"] },
  { args: ["who"] },
  { args: ["ack", "--from", "golden-1", "--id", "@FIRST"] },
  { args: ["ack", "--for", "golden-2", "--id", "@FIRST"] },                                    // exercises the real --for→agent path (finding 15)
  { args: ["status", "--from", "golden-1", "--id", "@FIRST", "--state", "done"] },
  { args: ["post", "--from", "golden-1", "--to", "@all", "--type", "announce", "--channel", "golden-chan", "--subject", "Hello", "--body", "cross-channel announce"] },
  { args: ["post", "--from", "golden-1", "--to", "ghost", "--type", "note", "--re", "@FIRST", "--subject", "thread inherit", "--body", "should inherit general"] },
  { args: ["thread", "--id", "@FIRST"] },
  { args: ["inbox", "--for", "golden-2", "--unread"] },
  { args: ["status", "--from", "golden-1", "--id", "@FIRST", "--state", "bogus"] },      // exit 2
  { args: ["read", "--for", "golden-1", "--id", "nope-0000"] },                          // exit 1
  { args: ["join", "--agent", "golden-1", "--role", "lab", "--fingerprint", "fp-golden"] },
  { args: ["join", "--agent", "impostor", "--role", "x", "--fingerprint", "fp-golden"] }, // exit 3
  { args: ["rename", "--agent", "golden-2", "--to", "golden-2b", "--fingerprint", "fp-none-ok"] },
  { args: ["post", "--from", "late-agent", "--to", "x", "--type", "bogus-type", "--body", "x"] }, // exit 2 BUT registers late-agent (quirk)
  { args: ["who", "--all"] },                                                            // proves late-agent got registered
  { args: ["watch", "--for", "golden-1", "--once"] },
];

function mask(s: string, home: string): string {
  // The BASELINE is captured from the pre-refactor CLI, which has no seam hook
  // and runs on the wall clock — so ids/timestamps must be masked for
  // cross-implementation comparison. The frozen seams still remove tie-order
  // instability (who/watch ordering) and pid noise on the new side.
  const masked = s
    .replace(/\d{8}T\d{6}-[a-z0-9-]+-[0-9a-f]{4}/g, "<ID>")
    .replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z/g, "<TS>")
    .replace(/seen=\S+/g, "seen=<TS>")
    .replace(/last=\S+/g, "last=<TS>")
    .replace(/baseline=\d+ msgs/g, "baseline=<N> msgs")
    .split(home).join("<TMP>");
  // presence lines: legacy ORDER BY last_seen DESC has no tiebreak, so tie
  // order differs run-to-run on the old side → sort contiguous ●/○ runs.
  const lines = masked.split("\n");
  const out: string[] = [];
  let run: string[] = [];
  const flush = () => { if (run.length) { out.push(...run.sort()); run = []; } };
  for (const l of lines) {
    if (/^\s+[●○] /.test(l)) run.push(l);
    else { flush(); out.push(l); }
  }
  flush();
  return out.join("\n");
}

function snapshotFiles(home: string): Record<string, string> {
  const out: Record<string, string> = {};
  const maskKey = (k: string) => k.replace(/msg-\d{8}T\d{6}-/, "msg-<TS>-").replace(/-([0-9a-f]{4})\.md$/, "-<SUF>.md");
  const walk = (dir: string) => {
    for (const e of readdirSync(dir, { withFileTypes: true })) {
      const p = join(dir, e.name);
      if (e.isDirectory()) walk(p);
      else if (e.name.endsWith(".md")) out[maskKey(relative(home, p))] = maskFile(readFileSync(p, "utf8"));
    }
  };
  if (existsSync(join(home, "messages"))) walk(join(home, "messages"));
  // SORTED keys — object key order must not depend on readdir order (finding 4)
  return Object.fromEntries(Object.entries(out).sort((a, b) => a[0].localeCompare(b[0])));
}
function maskFile(s: string): string {
  return s.replace(/\d{8}T\d{6}-[a-z0-9-]+-[0-9a-f]{4}/g, "<ID>").replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z/g, "<TS>");
}

function buildLegacyFixture(home: string) {
  // de4ed3b-era DB: old columns, rows that predate triggers + recipient index.
  mkdirSync(join(home, ".comms"), { recursive: true });
  const d0 = new Database(join(home, ".comms", "comms.db"), { create: true });
  d0.exec(`
    CREATE TABLE agents(id TEXT PRIMARY KEY NOT NULL, role TEXT, caps TEXT, pid INTEGER, joined_at TEXT, last_seen TEXT, meta TEXT);
    CREATE TABLE messages(id TEXT PRIMARY KEY, thread TEXT, re TEXT, sender TEXT, recipients TEXT, type TEXT,
      status TEXT, tags TEXT, subject TEXT, body TEXT, file TEXT, created_at TEXT, updated_at TEXT, channel TEXT NOT NULL DEFAULT 'general');
    CREATE TABLE reads(agent TEXT, msg TEXT, read_at TEXT, PRIMARY KEY(agent, msg));
    CREATE TABLE channels(name TEXT PRIMARY KEY NOT NULL, purpose TEXT, created_at TEXT, created_by TEXT);
    INSERT INTO agents VALUES('legacy-1','lab','',1,'2025-01-01T00:00:00Z','2025-01-01T00:00:00Z','{}');
    INSERT INTO messages VALUES('legacy-1','legacy-1',NULL,'legacy-1','golden-1','note','open','','old','pre-trigger body','','2025-01-01T00:00:00Z','2025-01-01T00:00:00Z','general');
  `);
  d0.close();
}

function runScript(home: string): { transcript: any[]; files: Record<string, string> } {
  let lastId = "", firstId = "";
  const transcript: any[] = [];
  for (const step of SCRIPT) {
    const args = step.args.map((a) => (a === "@LAST" ? lastId : a === "@FIRST" ? firstId : a));
    const proc = Bun.spawnSync(["bun", CLI, ...args], {
      cwd: home,
      env: { ...process.env, COMMS_HOME: home, COMMS_TEST_SEAMS: SEAMS },
      stdin: "ignore",
    });
    const stdout = proc.stdout.toString();
    const stderr = proc.stderr.toString();
    const idMatch = stdout.match(/posted (\S+)\s/);
    if (idMatch) {
      lastId = idMatch[1];
      if (!firstId) firstId = idMatch[1];
    }
    transcript.push({ cmd: step.args, code: proc.exitCode, stdout: mask(stdout, home), stderr: mask(stderr, home) });
  }
  return { transcript, files: snapshotFiles(home) };
}

export function captureGolden(): unknown {
  const home = mkdtempSync(join(tmpdir(), "comms-golden-"));
  try {
    const fresh = runScript(home);
    const home2 = mkdtempSync(join(tmpdir(), "comms-golden-"));
    try {
      buildLegacyFixture(home2);
      const migrated = runScript(home2);
      return { seams: SEAMS, fresh, migrated };
    } finally { rmSync(home2, { recursive: true, force: true }); }
  } finally { rmSync(home, { recursive: true, force: true }); }
}

export async function checkGolden() {
  const goldenPath = join(import.meta.dir, "golden.json");
  const got = JSON.stringify(captureGolden());
  if (UPDATE) {
    await Bun.write(goldenPath, got + "\n");
    console.log("golden updated");
    return;
  }
  const want = (await Bun.file(goldenPath).text()).trim();
  if (want === got) return; // byte-identical
  // first divergence for a readable failure
  let i = 0; while (i < want.length && want[i] === got[i]) i++;
  throw new Error(`GOLDEN MISMATCH at char ${i}:\n  want: …${want.slice(Math.max(0, i - 60), i + 60)}\n  got:  …${got.slice(Math.max(0, i - 60), i + 60)}`);
}

if (UPDATE) { await checkGolden(); }
