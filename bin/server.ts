#!/usr/bin/env bun
/**
 * bin/server.ts — the hosted server shell (RFC-001 §3.4). Args/env only;
 * all behavior lives in src/server/mod.ts. §9: run as the service user that
 * owns .comms (0700) and comms.db (0600); exactly ONE server per DB file.
 *
 * usage: bun bin/server.ts [--home <dir>] [--port <p>] [--host <ip>] [--origin <https://…>]
 *   COMMS_HOME env (default ~/.comms-home) selects the bus dir.
 */
import { homedir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { startServer } from "../src/server/mod.ts";

const REPO = join(fileURLToPath(import.meta.url), "../..");

function arg(flag: string): string | undefined {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

const home = arg("--home") ?? process.env.COMMS_HOME ?? join(homedir(), ".comms-home");
const port = Number(arg("--port") ?? process.env.COMMS_PORT ?? 8700);
const host = arg("--host") ?? process.env.COMMS_HOST ?? "127.0.0.1";
const origin = arg("--origin") ?? process.env.COMMS_ORIGIN;

if (!Number.isInteger(port) || port < 0 || port > 65535) {
  console.error("error: --port must be 0-65535");
  process.exit(2);
}

const srv = startServer({ home, port, hostname: host, origin });
console.error(`agent-comms server: ${srv.url}  home=${home}`);
console.error(`bootstrap (local, one-time): bun -e 'const {openBus,localCtx}=await import("${join(REPO, "src/bus.ts")}");const b=openBus({home:${JSON.stringify(home)},mode:"local"});console.log(b.tokenCreate(localCtx("bootstrap"),{agent:"root",admin:true}).value?.token??"(admin exists)");b.close()'`);
console.error(`(the comms token CLI verb ships with the remote CLI, M3)`);

process.on("SIGTERM", () => { srv.stop(); process.exit(0); });
process.on("SIGINT", () => { srv.stop(); process.exit(0); });
