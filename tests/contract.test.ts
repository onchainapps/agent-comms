import "./contract.suite.ts"; // CoreAsServer registration lives there (M1)
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openBus, localCtx } from "../src/bus.ts";
import { contractSuite } from "./contract.suite.ts";
import { startServer } from "../src/server/mod.ts";
import { RpcBus } from "../src/rpc-bus.ts";

// M2 impl: RpcBus over real HTTP against the real server (§3 acceptance —
// ONE suite, divergence between transports = CI failure by construction).
// raw = a SECOND local-mode connection to the same DB: integration probes for
// foreign-writer fan-out + row-level assertions (the hosted single-writer rule
// is relaxed ONLY in tests, same as the contract factory above uses a second
// opener for bootstrap).
contractSuite("RpcBusHttp", async () => {
  const home = mkdtempSync(join(tmpdir(), "comms-rpc-"));
  const bootBus = openBus({ home, mode: "local" });
  const boot = bootBus.tokenCreate(localCtx("bootstrap"), { agent: "root", admin: true });
  if (boot.error) throw new Error(boot.detail);
  const rootToken = boot.value.token;
  const srv = startServer({ home, port: 0, limits: { writeBurst: 1e9, writeRefill: 1e9, readBurst: 1e9, readRefill: 1e9 } });
  // semantic contract suite: buckets effectively off (limits get their OWN
  // dedicated integration tests below — buckets are transport behavior, not
  // Bus behavior).
  const rpc = new RpcBus(srv.url, rootToken);
  // raw = the SERVER core behind the HTTP front (same object the mw uses):
  // foreign-writer probes (testDb writes fire triggers), rotateEpoch, gc.
  (rpc as any).raw = (srv.handle as any).raw;
  (rpc as any).srv = srv;
  return {
    handle: rpc as any,
    root: rpc.session({ token: rootToken }),
    cleanup: () => { srv.stop(); bootBus.close(); rmSync(home, { recursive: true, force: true }); },
  };
});
