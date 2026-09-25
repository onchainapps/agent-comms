/**
 * Type-level probes (finding B1): these MUST fail tsc --strict, and tsc -p
 * enforces this file in CI via @ts-expect-error. If someone weakens the mode
 * typing back to an optional localRoot field on a shared Principal, the
 * expect-error becomes "unused" and the build breaks. Runtime backstops for
 * the erased case are contract-tested (contract.suite.ts).
 */
import { openBus, localCtx, type Ctx } from "../src/bus.ts";

const server = openBus({ home: "/nonexistent-probe", mode: "server" });

// local-root ctx is unnameable on a server-mode bus:
const rootCtx: Ctx<"local"> = localCtx("mallory");
// @ts-expect-error localRoot ctx must not be accepted by Bus<"server">
server.setStatus(rootCtx, { agent: "mallory", id: "x", state: "done" });
// @ts-expect-error hand-built localRoot principal must not be accepted
server.post({ principal: { agentId: "m", kind: "agent", scopes: [], localRoot: true }, actor: "m" }, { from: "m", to: "x", type: "note", body: "b" });

// a plain server ctx IS accepted (positive control — must NOT error):
server.setStatus({ principal: { agentId: "m", kind: "agent", scopes: [] }, actor: "m" }, { agent: "m", id: "x", state: "done" });

// H1..H3 (round-2 M1): the handle seam must be mode-typed too — a local core
// is NOT a server core, and a local ctx is NOT wrappable over a server bus.
import { openBus as ob2, localCtx as lc2 } from "../src/bus.ts";
import { wrapSession, serverHandle } from "../src/bus-iface.ts";
const srv2 = ob2({ home: "/nonexistent-probe2", mode: "server" });
const loc2 = ob2({ home: "/nonexistent-probe3", mode: "local" });
// @ts-expect-error H1: local ctx must not wrap over a server bus
wrapSession(srv2, lc2("mallory"));
// @ts-expect-error H2: a local core is not assignable to a server handle
serverHandle(loc2);
// @ts-expect-error H3: local core must not satisfy Bus<"server">
const _asServer: typeof srv2 = loc2;
void _asServer;
