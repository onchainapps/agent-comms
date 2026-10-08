/**
 * CLI remote wire map (RFC-001 §7). Lives in its own side-effect-free module so
 * tests can import it without executing bin/comms.ts — an import.meta.main
 * guard in the CLI would silently no-op every command run through the root
 * ./comms.ts shim (`import "./bin/comms.ts"` ⇒ main is false in the import).
 * Mirrors src/rpc-bus.ts makeSession (same server dispatch). Pinned against a
 * live server by tests/cli-remote.test.ts (no target may be -32601). Stopgap
 * until makeSession grows an onMeta passthrough and this map is deleted.
 */
export const csvSplit = (to: string) => (to ? String(to).split(",").map((s) => s.trim()).filter(Boolean) : []);

export const REMOTE_METHODS: Record<string, (p: any) => [string, Record<string, unknown>]> = {
  joinAgent: (p) => ["join", p],
  listAgents: (p) => ["who", { all: !p.activeOnly }],
  pingAgent: () => ["ping", {}],
  post: (p) => ["post", { ...p, to: csvSplit(p.to) }],
  inbox: (p) => ["inbox", p],
  read: (p) => ["read", p],
  threadOf: (p) => ["thread", { id: p.id }],
  receipts: (p) => ["receipts", { id: p.id }],
  setStatus: (p) => ["status", p],
  channels: () => ["channels", {}],
  rename: (p) => ["rename", p],
  history: (p) => ["history", p],
  waitStep: (p) => ["inbox.wait", p],
  cursorGet: (p) => ["cursor.get", p],
  cursorSet: (p) => ["cursor.set", p],
  tokenCreate: (p) => ["token.create", p],
  tokenList: () => ["token.list", {}],
  tokenRevoke: (p) => ["token.revoke", p],
  groupCreate: (p) => ["group.create", p],
  channelCreate: (p) => ["channel.create", p],
  channelDelete: (p) => ["channel.delete", p],
  groupJoin: (p) => ["group.join", p],
  groupLeave: (p) => ["group.leave", p],
  groupDelete: (p) => ["group.delete", p],
  groupList: () => ["group.list", {}],
  groupShow: (p) => ["group.show", p],
  dmMembers: (p) => ["dm.members", { channel: p.channel }],
};
