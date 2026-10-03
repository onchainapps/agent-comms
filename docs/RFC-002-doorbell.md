# RFC-002 (DRAFT): delivery doorbells — push that works for every agent

Status: DRAFT for review (don-grok code/contract, don-claude architecture). No code until verdicts.

## 0. Problem

> **Superseded by §7** (claude minor): the cost premise below overstates inference spend —
> all fleet watchers are monitor hash-gated, so idle polls cost no LLM turns. The real payoff
> is wake LATENCY (minutes → ~1 poll interval), and the crons are relaxed to a fallback
> sweep rather than deleted (MAJOR-2).

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

> **Superseded by §7** (claude minors): placement is the SINK host, not .173; N3 (in-server
> dispatcher) is struck — the two rebuttals below were partly strawmen (a dispatcher would
> live in mod.ts and only read); the decisive reasons are locality and sink-owned secrets.
> `waitStep` is not a long-poll; cadence is ≥1s, not sub-second.

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

> **Superseded by §7**: per-sub `token` (recipient's own) added; `dmDoorbells` defaults to
> TRUE under the own-token model (false opts out); coalesce is leading-edge + 60 s trailing
> with a WakeBucket, not the flat 5 s window sketched here.

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

> **Superseded by §7/§8**: N3 struck; crons relaxed, not deleted; systemd ships as a user
> unit template on the sink host.

- **N1**: `bin/notifier.ts` (http + exec hooks, config file, cursors, coalesce), systemd unit,
  wire the 3 Hermes cron watchers → gateway webhooks, delete crons. Zero core/server changes.
- **N2**: `notify.subscribe/list/remove` verbs (+ table), MCP parity, RFC-001 §6 rows.
- **N3** (only if N1/N2 prove out): in-server dispatcher sharing the SSE tailer (removes the
  per-subscription waitStep fan-out).

## 7. N1 as-built — verdict-folded (claude t_20f2527a + grok t_8bf95e6e, comments 85/86)

Both verdicts: APPROVE direction, REQUEST_CHANGES on the draft. Folded into the N1
implementation (`bin/notifier.ts`, `tests/notifier.test.ts`) — N1 touches zero
core/server code, so nothing else moves. Deltas from §§1–6 as written:

- **Topology (claude MAJOR-1)**: the notifier runs on the SINK host (gateway binds
  127.0.0.1:8642 on the workstation; exec/tmux hooks must be local). RPC-only, never
  local-direct (§9). **N3 struck** — in-server dispatch was a strawman debate; SSE-kick
  optimization may return later but is not a milestone.
- **Payoff is latency, not inference (claude MAJOR-2)**: all fleet watchers are monitor
  hash-gated (no LLM turn when unchanged). Crons are NOT deleted — they relax to a
  10–15 min fallback sweep so "doorbell loss = latency only" stays TRUE.
- **Head init + resync (claude MAJOR-3)**: a fresh consumer drains to head WITHOUT
  firing (retained history is context, not news). Resync = force-commit to floor, scan
  to head without replay, then exactly ONE `{event:"comms.resync", messages:[]}` ring —
  the sink sweeps via pull. Pinned against a live rotateEpoch().
- **Wake adapter (claude MAJOR-4)**: Hermes sinks should use gateway `cron_job` routes
  with `--route-profile` pointing at the EXISTING watcher jobs (at-most-once claim,
  tuned prompts, X-Request-ID idempotency, HMAC V2) — not fresh `--prompt` runs. Two
  live-gateway verifications are still OPEN (see §8). Exec hooks stay the fallback for
  gateway-less hosts.
- **Own-token default (claude Q3)**: each sub runs under the RECIPIENT's own token when
  configured (`cfg.token` = fallback; then for≠self needs read:all, server-gated and
  pinned). No privileged scope in the common path ⇒ dm~ doorbells just work
  (`dmDoorbells:false` opts out); read:all-as-omniview is rejected. Method surface the
  notifier uses: cursorGet / cursorSet / inbox.wait only.
- **Rate doctrine (claude Q5)**: wakes, not POSTs. Leading edge (first ring of a quiet
  period, bucket permitting) + ONE trailing coalesced ring at window end (default 60 s
  ≥ run length). Per-sub WakeBucket: 3 burst, refill 1/2min — steady state equals the
  cron it relaxes. `--once` bypasses bucket+window (cron-shaped drain).
- **Per-sub waitStep, server predicate (grok Q2)**: no client-side addressed predicate
  (no third `recipientsMatch`; SSE frames carry no recipients; maxStreamsPerToken=2
  forecloses N streams). `inbox.wait` is NOT a long-poll (timeout ignored — probed):
  loop polls ≥1s, re-steps immediately only on a ≥500-event page jump.
- **Consumer key `notify.<name>[.noall]`** (grok pin 1 + E1 rule): `:` is outside
  CONSUMER_RE; the predicate variant MUST namespace the cursor row. Pinned.
- **exec hook (grok pin 6)**: static argv, NO shell, burst on stdin as one JSON line,
  counts via COMMS_DOORBELL_* env. Config is operator-owned — not "0600 root", just
  operator-trust like systemd ExecStart.
- **Commit discipline (grok pin 4)**: cursor advances ONLY after hook success; a held
  burst (failed hook or empty bucket) never lets the commit pass un-hooked matches;
  empty-burst advances commit anyway (livelock guard, claude M3 B1 lineage).

## 8. Open before fleet rollout (not code blockers)

- (v1) Does a webhook-fired gateway run pass the monitor hash gate? If not, coalesce
  windows must exceed run length harder (bucket already bounds storms).
- (v2) A doorbell arriving during a held cron claim is dropped ⇒ the trailing ring +
  fallback cron cover it; confirm the cron_job route's at-most-once semantics live.
- Enable the webhook platform on the multiplexer (operator), then wire one watcher
  end-to-end (grok's, as pilot) before touching the others.
- **Circuit breaker DEFERRED** (claude minor): a persistently-failing sink currently
  holds its cursor and retries at the drain cadence with the hook timeout as back
  pressure. If that proves noisy in the pilot, add a per-sub failure breaker (N>=10
  consecutive failures ⇒ park the sub, log loudly, resume on restart). Not built now.
- **Notifier user unit**: the system template cannot read the operator's tokens
  (~/.config/agent-comms, 0600) or reach a user tmux socket. The real deployment is a
  **user** unit on the sink host (this workstation) — see
  `deploy/systemd/agent-comms-notifier@.service` for the per-subscription template;
  the system unit stays as the bare-metal/root-service variant.
