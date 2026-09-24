/**
 * Golden harness (RFC-001 §10-M1): runs the CLI against a throwaway COMMS_HOME
 * with a scripted sequence, captures stdout+stderr+exit+mirror-file bytes, and
 * writes/compares a golden JSON. Run with --update to (re)capture the baseline
 * from the CURRENT implementation; CI compares.
 *
 * Usage:
 *   bun tests/golden.ts --update          # capture baseline (pre-refactor)
 *   bun tests/golden.ts                   # compare (post-refactor)
 */
import { mkdtempSync, rmSync, readdirSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";

const UPDATE = process.argv.includes("--update");
const CLI = join(import.meta.dir, "..", "bin", "comms.ts");

// Deterministic scripted session. Timestamps/ids vary run-to-run, so we capture
// structure + bodies + mirror bytes, with volatile fields masked by regex.
const SCRIPT: Array<{ args: string[]; stdin?: string }> = [
  { args: ["join", "--agent", "golden-1", "--role", "lab", "--caps", "gpu,kernels"] },
  { args: ["join", "--agent", "golden-2", "--role", "research"] },
  { args: ["post", "--from", "golden-1", "--to", "golden-2,research", "--type", "ask", "--subject", "Need GPU numbers", "--body", "Benchmark the kernel at 4k tiles.", "--tags", "perf,gpu"] },
  { args: ["post", "--from", "golden-2", "--to", "golden-1", "--type", "reply", "--re", "@LAST", "--subject", "re: Need GPU numbers", "--body", "@BODY1"] },
  { args: ["inbox", "--for", "golden-1", "--open"] },
  { args: ["read", "--for", "golden-1", "--id", "@FIRST"] },
  { args: ["receipts", "--id", "@FIRST"] },
  { args: ["channels"] },
  { args: ["who"] },
  { args: ["ack", "--from", "golden-1", "--id", "@FIRST"] },
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
  { args: ["watch", "--for", "golden-1", "--once"] },
];

function mask(s: string): string {
  const masked = s
    .replace(/\d{8}T\d{6}-[a-z0-9-]+-[0-9a-f]{4}/g, "<ID>")
    .replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z/g, "<TS>")
    .replace(/seen=\S+/g, "seen=<TS>")
    .replace(/last=\S+/g, "last=<TS>")
    .replace(/baseline=\d+ msgs/g, "baseline=<N> msgs")
    .replace(/pid=\d+/g, "pid=<PID>")
    .replace(/\/tmp\/comms-golden-[A-Za-z0-9]+/g, "<TMP>");
  // presence lines: timestamps are second-resolution and tie-order is
  // wall-clock dependent → sort contiguous ●/○ runs for run stability
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
  const walk = (dir: string) => {
    for (const e of readdirSync(dir, { withFileTypes: true })) {
      const p = join(dir, e.name);
      if (e.isDirectory()) walk(p);
      else if (e.name.endsWith(".md")) out[relative(home, p)] = readFileSync(p, "utf8");
    }
  };
  if (existsSync(join(home, "messages"))) walk(join(home, "messages"));
  return out;
}

const home = mkdtempSync(join(tmpdir(), "comms-golden-"));
let lastId = "";
let firstId = "";
const transcript: any[] = [];

for (const step of SCRIPT) {
  const args = step.args.map((a) =>
    a === "@LAST" ? lastId : a === "@FIRST" ? firstId : a === "@BODY1" ? "Got numbers: 3.2 TFLOPs. See attached." : a,
  );
  const proc = Bun.spawnSync(["bun", CLI, ...args], {
    cwd: home,
    env: { ...process.env, COMMS_HOME: home },
    stdin: "ignore",
  });
  const stdout = proc.stdout.toString();
  const stderr = proc.stderr.toString();
  const idMatch = stdout.match(/posted (\S+)\s/) ?? stdout.match(/([0-9]{8}T[0-9]{6}-[a-z]+-[0-9a-f]{4})/);
  if (idMatch) {
    lastId = idMatch[1];
    if (!firstId) firstId = idMatch[1];
  }
  transcript.push({ cmd: step.args, code: proc.exitCode, stdout: mask(stdout), stderr: mask(stderr) });
}

function maskKey(k: string): string {
  // mask volatile parts of mirror filenames but KEEP sender/type (so distinct
  // messages don't collapse onto one golden key)
  return k.replace(/msg-\d{8}T\d{6}-/, "msg-<TS>-").replace(/-([0-9a-f]{4})\.md$/, "-<SUF>.md");
}
const files = Object.fromEntries(
  Object.entries(snapshotFiles(home)).map(([k, v]) => [maskKey(k), mask(v)]),
);
const golden = { home: "<TMP>", transcript, files };
const goldenPath = join(import.meta.dir, "golden.json");

if (UPDATE) {
  Bun.write(goldenPath, JSON.stringify(golden, null, 2) + "\n");
  console.log(`golden updated: ${golden.transcript.length} steps, ${Object.keys(files).length} mirror files`);
} else {
  const want = JSON.parse(await Bun.file(goldenPath).text());
  const got = JSON.stringify(golden);
  if (JSON.stringify(want) === got) {
    console.log("GOLDEN OK — behavior byte-identical (modulo masked ids/timestamps)");
  } else {
    console.log("GOLDEN MISMATCH");
    const w = JSON.stringify(want, null, 2).split("\n");
    const g = JSON.stringify(golden, null, 2).split("\n");
    for (let i = 0; i < Math.max(w.length, g.length); i++)
      if (w[i] !== g[i]) { console.log(`line ${i}:\n  want: ${w[i]}\n  got:  ${g[i]}`); break; }
    process.exit(1);
  }
}
rmSync(home, { recursive: true, force: true });
