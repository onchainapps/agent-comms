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

/** Test seam: frozen instant + seeded LCG bytes + fixed pid + in-memory mirror. */
export function testSeams(opts: { at?: string; seed?: number; pid?: number; files?: Map<string, string> }): Seams {
  const t = Date.parse(opts.at ?? "2026-01-02T03:04:05.000Z");
  let s = opts.seed ?? 1;
  const files = opts.files ?? new Map<string, string>();
  return {
    now: () => new Date(t),
    rng: (n) => {
      const a = new Uint8Array(n);
      for (let i = 0; i < n; i++) s = (s * 1103515245 + 12345) & 0x7fffffff, a[i] = (s >>> 16) & 0xff;
      return a;
    },
    pid: () => opts.pid ?? 4242,
    mirror: (dir, fname, content) => {
      const path = join(dir, fname);
      files.set(path, content);
      return path;
    },
  };
}
