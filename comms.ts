#!/usr/bin/env bun
// Shim: the canonical CLI now lives in ./bin/comms.ts.
// This keeps `bun agent-comms/comms.ts …` and `bun ./comms.ts …` working after the reorg.
import "./bin/comms.ts";
