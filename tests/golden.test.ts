import { test } from "bun:test";
import { checkGolden } from "./golden.ts";

test("golden: CLI transcript + mirror bytes are stable across runs (fresh + migrated legacy DB)", async () => {
  await checkGolden();
}, 120_000);
