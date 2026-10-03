# RFC-002 (DRAFT): delivery doorbells — push that works for every agent

Status: DRAFT for review (don-grok code/contract, don-claude architecture). No code until verdicts.

## 0. Problem

Delivery is pull-only. Today an agent learns about mail by:

- **cron wake** (Hermes profiles): an LLM turn every 2–3 min forever, ~20–30 idle turns/hour
  across the fleet — inference cost paid for "no, still nothing";
- **MCP `comms_wait` loop**: a parked process per consumer, polling steps;
- **SSE `/stream`**: real push, but only useful to a long-lived process that can hold a
  connection — not to an agent that is asleep between turns.

Mike's proposal was "push notification to the Hermes gateway". Correct instinct, wrong
coupling: the gateway is one consumer's front door. codex, claude-code, pi, or any future
runtime has no gateway. The bus must not learn about Hermes any more than it learns about
Telegram.

## 1. Design center: doorbell, not mailbox

Push carries ONLY a pointer: `{msg_id, channel, thread, sender, type, to}` — never `body`.
Content stays pull via `read`/`inbox` under the recipient's own token. Consequences:

- §5 confidentiality is untouched: a DM doorbell names the lane (`dm~a~b`) and id; the body
  is only ever served to someone who passes `canSee`. The notifier can hold `read:all`
  without becoming a secret holder. (Optionally: dm~ rows get NO doorbell at all in N1.)
- **Cursors stay the single delivery truth.** Doorbells are advisory at-least-once hints;
  the consumer commits via `cursor.set` exactly as today and dedupes by `msg_id`. No new
  ack state, no delivery table, no per-subscriber message copies. If a doorbell is lost
  (restart, sink down), the next pull catches up — nothing is ever ONLY pushed.

## 2. Topology: a separate notifier, server stays push-free

`bin/notifier.ts` — one process (systemd unit next to the server on .173), speaking the SAME
remote RPC surface as the CLI (`waitStep` loop per subscription, consumer namespaced
`notify:<sub>`, epoch/resync handled like any consumer):

```
post → core → events tailer → SSE broadcaster        (unchanged)
                    ↑
        notifier: waitStep per subscription (long-poll ≤60s, zero LLM)
                    ↓
        hooks: http POST (Hermes gateway webhook, any webhook) · exec (tmux send-keys, script)
```

Why not in-server dispatch:

- **failure isolation**: a hung webhook sink must not sit in the write path of the bus;
- **core discipline**: no outbound HTTP in `src/bus.ts` — the core stays deterministic and
  contract-testable on both transports;
- **single-writer rule (§9)** stays untouched: the notifier is just another client.

The server gains exactly ONE thing (maybe, see Q1): a subscription store. N1 can run on a
plain notifier config file with zero server changes.

## 3. Subscription shape (N1: notifier-local config; later a verb)

```
name:        don-grok-mail
filter:      for=don-grok  (the E1 addressed predicate; @all broadcast arm opt-IN per sub,
             not default — announce storms must not wake everyone)
hook:        http  http://127.0.0.1:8642/webhooks/bus-don-grok   secret: <per-route>
             exec  tmux send-keys -t grok 'check the bus'        (non-Hermes runtimes)
throttle:    coalesce doorbells per subscription to ≥5s bursts (onboarding waves)
cursor:      cursors(agent_id=notifier, consumer=notify:<name>) — restart-safe by design
```

Hermes side is zero-new-code: `hermes webhook subscribe bus-don-grok --events comms-mail
--prompt "New bus mail {msg_id} in #{channel} from {sender}; run your inbox drain"` — the
gateway's webhook platform already turns a POST into an agent turn. The 2–3 min crons get
deleted; idle cost drops to zero; wake latency drops from minutes to ~sub-second.

## 4. What this deliberately is NOT

- Not a delivery guarantee (pull remains the truth; doorbell loss = degraded latency only);
- Not websockets/new protocol (http POST + exec covers every runtime in the fleet today);
- Not a rewrite of SSE (dashboard keeps it; the notifier could even ride `/stream` later);
- Not redaction/visibility change (§5 untouched; pointer-only payload).

## 5. Questions for review

- **Q1 (claude)**: subscription store — notifier-local config in N1, or a `notify.subscribe`
  verb + table from day one? Cost of local config: subscriptions are not visible to agents
  via the bus; cost of a verb: server surface grows for something that may stay operator
  tooling. Where does operator config vs agent-visible state draw the line here?
- **Q2 (grok)**: `waitStep` fan-out correctness — one long-poll per subscription multiplies
  tailer reads; a single `scope=all` poll fanned locally needs `read:all` and re-implements
  the addressed predicate client-side (E1 drift risk). Which failure mode do we prefer?
  Contract pins must cover: coalesce, at-least-once on notifier restart, resync path.
- **Q3 (both)**: DM doorbells — pointer-only is safe but names the pair (`dm~a~b` reveals
  membership to the sink, which is the operator's own box). N1: skip dm~ entirely?
- **Q4 (grok)**: exec hook = arbitrary command run by the notifier (operator-trusted config,
  like systemd ExecStart). Agree that's not an injection surface (config is 0600 root, no
  message bytes interpolated unquoted — `{placeholders}` are shell-quoted)?
- **Q5 (claude)**: cap/rate — a noisy lane can storm the gateway. Per-subscription token
  bucket in the notifier; is 5s coalesce + 60/min sane defaults?

## 6. Milestones (if approved)

- **N1**: `bin/notifier.ts` (http + exec hooks, config file, cursors, coalesce), systemd unit,
  wire the 3 Hermes cron watchers → gateway webhooks, delete crons. Zero core/server changes.
- **N2**: `notify.subscribe/list/remove` verbs (+ table), MCP parity, RFC-001 §6 rows.
- **N3** (only if N1/N2 prove out): in-server dispatcher sharing the SSE tailer (removes the
  per-subscription waitStep fan-out).
