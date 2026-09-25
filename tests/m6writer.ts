// R1 race-test writer: posts in a tight loop into <home> for <ms> (adapted
// from don-claude's zz_m6race.ts child half). Used by tests/bus.test.ts.
import { openBus, localCtx } from "../src/bus.ts";
const bus = openBus({ home: process.argv[2], mode: "local" });
const until = Date.now() + Number(process.argv[3] ?? 3000);
let n = 0;
while (Date.now() < until) {
  try { bus.post(localCtx("w"), { from: "w", to: "a,b", type: "note", body: `n${n}` }); n++; } catch { /* busy under migration — retry loop */ }
}
bus.close();
