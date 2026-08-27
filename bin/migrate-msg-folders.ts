// One-shot migration: move flat messages/*.md into messages/<channel>/ and
// normalize the display-only `file` column to messages/<channel>/<basename>.
// Idempotent: safe to re-run. The bus DB is the source of truth; this only
// relocates the human/git mirror.
import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, renameSync } from "node:fs";
import { join, basename } from "node:path";

const ROOT = join(import.meta.dir, "..");
const MSG = join(ROOT, "messages");
const db = new Database(join(ROOT, ".comms", "comms.db"));

const rows = db.query("SELECT id, channel, file FROM messages").all() as any[];
let moved = 0, fixed = 0, missing = 0;
for (const r of rows) {
  const ch = r.channel || "general";
  const base = basename(String(r.file || `${r.id}.md`));
  const rel = join("messages", ch, base);
  const dest = join(MSG, ch, base);
  const flat = join(MSG, base);
  mkdirSync(join(MSG, ch), { recursive: true });
  if (!existsSync(dest)) {
    if (existsSync(flat)) { renameSync(flat, dest); moved++; }
    else missing++;
  }
  if (r.file !== rel) { db.run("UPDATE messages SET file=? WHERE id=?", [rel, r.id]); fixed++; }
}
console.log(`moved=${moved} file-col-normalized=${fixed} missing-on-disk=${missing} total=${rows.length}`);
