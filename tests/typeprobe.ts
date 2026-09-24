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
