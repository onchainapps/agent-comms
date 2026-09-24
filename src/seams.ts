/**
 * Injectable seams (RFC-001 §10-M1, grok finding G6).
 *
 * The core is deterministic ONLY through these seams: clock, rng, pid, mirror
 * sink. Goldens and contract tests freeze them; production uses the defaults.
 * Without this, "byte-identical refactor" is unfalsifiable.
 */

export interface Seams {
  /** Wall clock. Core calls this for every timestamp (never Date.now directly). */
  now(): Date;
  /** n cryptographically-random bytes. Core calls this for ids and tokens. */
  rng(n: number): Uint8Array;
  /** Process identity for local-mode agent rows (server mode must not stamp it). */
  pid(): number;
  /** Human/git mirror sink. Writes content under dir, returns the display path. */
  mirror(dir: string, fname: string, content: string): string;
}

import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export const defaultSeams: Seams = {
  now: () => new Date(),
  rng: (n) => crypto.getRandomValues(new Uint8Array(n)),
  pid: () => process.pid,
  mirror: (dir, fname, content) => {
    mkdirSync(dir, { recursive: true });
    const path = join(dir, fname);
    writeFileSync(path, content);
    return path;
  },
};

/** Test seam: frozen instant + fixed pid (+ optional seeded rng for IN-PROCESS
 *  tests only — a seeded LCG resets per process, so two CLI invocations would
 *  mint identical ids and collide; the golden harness therefore keeps rng real
 *  and masks the suffix instead). */
export function testSeams(opts: { at?: string; seed?: number; pid?: number; files?: Map<string, string> }): Seams {
  const t = Date.parse(opts.at ?? "2026-01-02T03:04:05.000Z");
  let s = opts.seed ?? 1;
  const files = opts.files ?? new Map<string, string>();
  return {
    now: () => new Date(t),
    rng: opts.seed === undefined
      ? (n) => crypto.getRandomValues(new Uint8Array(n))
      : (n) => {
          const a = new Uint8Array(n);
          for (let i = 0; i < n; i++) s = (s * 1103515245 + 12345) & 0x7fffffff, a[i] = (s >>> 16) & 0xff;
          return a;
        },
    pid: () => opts.pid ?? 4242,
    mirror: (dir, fname, content) => {
      // write to disk (the CLI golden harness snapshots real files) AND record
      // in the map when one is supplied (in-process assertions)
      mkdirSync(dir, { recursive: true });
      const path = join(dir, fname);
      writeFileSync(path, content);
      files.set(path, content);
      return path;
    },
  };
}
