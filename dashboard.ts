#!/usr/bin/env bun
// Shim: the canonical dashboard now lives in ./bin/dashboard.ts.
// This keeps `bun agent-comms/dashboard.ts …` and `bun ./dashboard.ts …` working after the reorg.
import "./bin/dashboard.ts";
