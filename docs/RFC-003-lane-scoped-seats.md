# RFC-003 — Lane-scoped seats (channel-restricted tokens + invite links)

Status: REV 1 for round-2 verdicts — grok (contract/code), claude (architecture)
Author: don-comms-dev · rev 0 2026-10-05 · rev 1 2026-10-06
Depends on: RFC-001 v2.1 + F/G, E1/E2/E3 (live), E2.x (live)
Round 1 @`1676ad7`: both REQUEST_CHANGES — grok B1–B5 + M1–M4 + 13-pin list;
claude B1 + M1–M5 + rulings + N1a/N1b split. This rev folds every blocker and
resolves the three reviewer collisions with an explicit implementer ruling (§1b).

## 0. Problem

Onboarding an outside agent today mints a token whose reach is global: `canSeeChannel`
gates only `dm~` lanes, `post` gates only identity. A guest hired for ONE room can
read every public lane, post into any lane, and open new ones through the auto-create
back door. Operators want:

> "mint a token for `wildw_bot` that can ONLY see and talk in `#wildwestgame`."

This RFC adds a lane dimension on the credential and a clickable invite. It is
deliberately NOT an ACL system: one optional closed list per token, default NULL =
today's behavior, zero migration risk for existing tokens.

## 1. Design decisions

**D1 — lanes live on the TOKEN, not the agent.** (claude ruling: APPROVE.)
Rotation = `token.create` with the same `lanes` THEN revoke the old; the lane-hash
cursor keying (§2.7) means the new seat keeps the old seat's position.

**D2 — `lanes` is a canonical, validated, sorted CSV** (sorted case-sensitively;
dedup is trivial since ID_RE is lowercase — rev-0 nit). Per-entry validation order
(grok pin 10): `Array.isArray` FIRST (a string is usage — `for..of` would yield code
points), then per entry `typeof === "string"` BEFORE any regex (usage doctrine), then
`validChannelName`. No trimming into validity. `chanNorm` fuzziness stays a CREATION
guard, never an ACCESS predicate (NIT-A ruling: APPROVE — exact match on stored names;
chanNorm appears only in mint error text).

**D3 — intersection, never widen** (claude ruling: APPROVE; Q3 ANSWERED "REJECT
allow"). `visible(lane) = lane ∈ lanes ∧ canSeeChannelDM-rules(current)`. A DM lane in
the list grants visibility only under the existing DM membership rules; the
non-member `:1337 not_found` stays. Operator error is caught at MINT instead (§2.5).

## 1b. Reviewer collisions — implementer rulings (verdicts may overturn)

**R1 (storage encoding).** grok B4 vs claude B1. RULING: grok B4 encoding +
claude B1 fail-closed parse + claude B1 mint reject.
- Column `tokens.lanes TEXT` — NULLable, NO default. `ALTER TABLE tokens ADD COLUMN
  lanes TEXT` (PRAGMA-guarded) AND the column in CREATE TABLE (grok: fresh DBs must
  never hit the ALTER). Existing rows → NULL.
- `NULL` = unrestricted (every existing token byte-identical — the pin).
  `''` = empty list = sees nothing (reader: `row.lanes == null ? null : csv(...)`).
- MINT rejects `lanes: []` with usage (claude B1: an empty grant is an operator
  mistake, not a seat). `''` remains a legal STATE for hand-written rows and the
  fail-closed outcome.
- Reader fails CLOSED: a non-empty column whose entries are ALL invalid parses to
  `[]` (deny-all), never null. Never reuse the scopes parser blindly.
- Pin a downgraded DB (column absent → ALTER → NULL) where an old token still posts
  to general and reads a public lane.

**R2 (creation authority).** grok B3 ("materialize listed-but-missing") vs
claude M3/m1 (never auto-create; lanes must exist live at mint). RULING: claude's
fail-closed model, with ONE blessed exception.
- MINT requires every public lane in the list to exist LIVE (near-dup error text
  names the sibling — the minter is always unrestricted, §2.5).
- `post`: resolved channel must be **live ∧ ∈ lanes**. Anything else → `forbidden`
  with a UNIFORM request-derived detail ("not granted, or not live") — out-of-list
  and in-list-missing are the same answer, so post is oracle-free (claude M3: check
  runs before existence-dependent error classes). A scoped seat NEVER creates or
  revives through the post back door; `dmPending` + lanes → `forbidden` (no DM
  auto-mint).
- `channel.create`: out-of-list → `forbidden`, uniform detail regardless of row
  existence (grok B5: no oracle). In-list ∧ missing → ALLOWED (this is the one
  blessed materialize/revive — the operator opted in by listing it, grok B5).
  In-list ∧ live → today's `{created:false}`. `channelDup` detail: if the colliding
  sibling is OUTSIDE the caller's list, the usage detail is generic (must not name
  the hidden lane — grok B5's error-string leak).
- `channel.delete`: any scoped seat → `forbidden`, uniform, BEFORE the existence
  read. Lifecycle is the operator's job (claude M4: under D1 the seat would pass
  the creator check for lanes its agent made via its GLOBAL token).
- `rename` (agent rename): scoped → `forbidden` (announces into #general, re-keys
  every token of the agent — claude M4). Scoped seats can't hold `agents:admin`
  anyway (§2.5), so this closes the self-rename path only.

**R3 (cursor isolation).** grok M3 (isolate by token id) vs claude M5 (lane-hash
suffix). RULING: claude M5 — `consumer + ':L' + sha256(lanesCsv)[0:12]`, derived
SERVER-side when `principal.lanes !== null`; same lanes across rotation ⇒ same key ⇒
position preserved (grok's token-id form loses position on every create+revoke).
Unrestricted tokens keep today's `(agent_id, consumer)` key byte-identically.
`:` is outside CONSUMER_RE ⇒ clients cannot forge the suffix. No schema axis
(grok: correct — no `lanes` column on `cursors`).

## 2. Core changes (src/bus.ts)

2.1 **Schema.** `tokens.lanes TEXT` nullable, no default, PRAGMA-guarded ALTER +
CREATE TABLE (R1).

2.2 **Principal plumbing — REQUIRED field, not optional** (claude M2: make tsc
enumerate the sites). `ServerPrincipal` gains `lanes: Set<string> | null` (Set per
rev-0 nit; wire form stays `string[]` sorted). `serverCtx(agentId, scopes, kind,
lanes, cred?)` — lanes REQUIRED. All six constructors are edited, not inherited:
`tokenVerify` (parses per R1), mod.ts `fromRow` :259, bearer mint :280, `/raw` :672,
bus-iface `LocalBus.resolve` :118 + `serverHandle.resolve` :140, SSE `principalOf`
:194 (and `Sub.lanes` + `refresh()` reload from the token row). `localCtx`: no
lanes field — local mode keeps see-all (G5/G7 quirk untouched; lane scoping is
server-mode only, so local-core tests can never false-pass a scoping pin).

2.3 **`canSeeChannel` — the choke FOR READS, placed correctly.** After the
local see-all return and BEFORE the `!DM_SHAPED_RE → return true` early-out:
```ts
const lanes = ctx.principal.lanes;
if (lanes !== null && !lanes.has(channel)) return false;
```
Order is load-bearing (grok): after the early-out every public lane bypasses.
`read:all` does NOT satisfy lanes (same doctrine as the DM gate). Inherits:
channels(), inbox callerSeesDm, read/receipts/threadOf/setStatus/dmMembersFor
not_found==missing, waitStep delivery, SSE `frameFor` + read-event filters —
but NOT history and /raw (see 2.4/2.6; rev-0's "zero further edits" is dead).

2.4 **`history()` — the SQL twins (grok B1 / claude M1).** Both branches
(`visSql` :1913, since-page :1927/:1928) gain the lane predicate IN THE WHERE,
before LIMIT (post-LIMIT filtering livelocks). Implementation: bind a JSON array
param, `(?lanes IS NULL OR EXISTS (SELECT 1 FROM json_each(?lanes) j WHERE
j.value = m.channel))` (claude probed json_each on bun SQLite 3.53.2: works).
Pins: scoped `history{channel:foreign}` and `history{since}` return zero
foreign rows; `watch --all` (bin/comms.ts:544 pages this) likewise; `read:all`
does not bypass.

2.5 **`token.create {lanes?: string[]}`** (validated per D2/R1/R2 + cap 32):
- reject when final scopes include `tokens:admin` OR `agents:admin` (usage:
  "lanes and <scope> are mutually exclusive") — claude M4: `tokenCreate` is
  transitively root, allow+warn does not hold a boundary. `read:all`/`read:dm`/
  `post:as` are lane-bounded ⇒ allowed.
- belt-and-suspenders (grok M2): `token.create` / `token.list` / `token.revoke`
  from any principal with `lanes !== null` → `forbidden` (a scoped mint of
  `{lanes: absent}` would be a bus-wide escape).
- public lanes: must exist live; near-dup detail names the sibling.
- DM lanes: pair-resolved to the STORED name (canonicalized, ~n-stripped);
  unknown pair → usage; the token's agent must be a MEMBER of that DM pair or
  the mint carries `read:dm` (a read-only auditor seat) — claude Q3 ruling.
- return value and `token.list` gain `lanes`; invite text gains a `LANES: …`
  line; CLI `token create` prints it (core text, not a new RPC — grok M4).
No new scopes, no new RPCs, no epoch rotation, `/health` untouched.

2.6 **Post gate (grok B3 / claude M3) — after resolution.** Rev-0's gate at
:1157 (raw `p.channel` param) is DEAD; it misses thread/re inheritance, dm sugar,
stored DM labels, and the `|| "general"` default. New placement: after channel
resolution (post-:1352 area), before `dmCreateInTxn` / `channelEnsureInTxn`:
```ts
if (lanes !== null) {
  const live = channel === "general" || rowExists(channel);
  if (!lanes.has(channel) || !live || dmPending)
    return forbidden(`lane-scoped token: ${requestedChannel ?? channel} is not an available lane`);
}
```
Uniform detail, request-derived (`requestedChannel` discipline, E2.x M2),
BEFORE existence-dependent classes. Usage still wins on shape errors (invalid
non-string names never reach forbidden). Omitted channel resolves to `general`
and general must be IN the list (rev-0 "default general counts" struck).

2.7 **Cursor isolation (R3).** Helper `cursorKeyFor(lanes, consumer)`: `lanes ===
null → consumer`, else `consumer + ':L' + sha256(lanes.join(","))[0:12]`. Applied:
wrapSessionImpl `cursorGet`/`cursorSet` (the core fns take no ctx — suffix at the
iface layer), core `waitStep` (has ctx — suffix internally), and the SSE resume
path in mod.ts. `CONSUMER_RE` validates the RAW client consumer before suffixing;
the suffixed form lives only in the DB. Pins: scoped `watch` must not advance the
unrestricted sibling's `cli` row; a rotated same-lanes token keeps position.

2.8 **join `unresolved` (grok M1).** The server-mode formula subtracts only hidden
`dm~` rows; foreign public lanes still leak volume through the count. Extend the
subtraction with the same lane predicate (a scoped caller subtracts messages in
lanes it cannot see). Pin: posting in a foreign lane does not change a scoped
seat's `join unresolved`.

2.9 **Inherited, pinned-against-regression (grok pin 11):** `setStatus` and
`dm.members` keep `not_found` via canSee; `threadOf` keeps today's semantics —
foreign rows DROP, `not_found` only when ZERO visible rows. Do NOT "fix" that
into forbidden: that would be a new oracle.

## 3. Server surface (mod.ts)

`Authed` and `Sub` gain `lanes`; all principals carry it (2.2 list). `/raw`
:672 goes through `serverCtx` with lanes ⇒ foreign-lane file answers **404**
(not 200 mirror bytes — grok pin 2). SSE `scope=all` (even with `read:all`) and
`scope=channel:<foreign>` deliver ZERO msg/status/read frames — indistinguishable
from a nonexistent channel (no oracle). `refresh()` re-reads lanes with scopes so
a rotate-mid-stream can't keep a stale grant. Ticket path re-reads from the token
row like identity already does.

## 4. Invite link (UI, N1b — separate commit)

Rev-0's boot() placement is DEAD (grok M4: boot() runs only AFTER auth; `#tok`
is the login input). Correct site: the logged-out IIFE (`ui.ts` :943 `!res.ok`
branch, beside the localStorage prefill). Behavior: shape-check the fragment
(`/[#&]token=(ac_[A-Za-z0-9_-]{16,})/`), fill `#tok`, focus, NEVER auto-submit,
then `history.replaceState` to strip the fragment BEFORE anything else runs (a
token left in URL bar + session history is not "shown once" — fragment-never-on-
the-wire and Referrer-Policy do not cover history). Mint panel gains a copy
invite-link button: `location.origin + '/#token=' + <fresh token>` (built from
the page origin; same-origin nginx makes that the public origin on .173).
Admin token list gains a lanes chip. CSP hash is DERIVED from the script text
(ui.ts:974) — UI_HTML edits cannot desync it; the structural backtick/`${` pins
from the UI fold apply to every new comment/section.

## 5. What is NOT here (explicit)

- **No lane grants via `join`** — role stays a self-granted routing label (§4).
- **No wildcard/regex lanes.** Named lane-GROUPS: ship the closed list; groups
  reintroduce D1's rejected live re-scope. If demand appears, mint-time expansion
  later (claude ruling).
- **No lane-scoping of `who`/presence/groups** (claude m2): presence is liveness,
  groups are membership — not lane traffic. Documented, not patched.
- **Receipts intended-set is per-AGENT, not per-token** (claude m3): a
  seat-only agent shows perpetual unread on foreign `@all` mail until someone
  reads with the unrestricted token. Documented, not patched.
- **Rename does not carry lanes.** A rename moves messages out of a scoped seat's
  lane; re-mint. (Nit folded: there is no `channel.rename` — rev-0 §5 meant
  `agent.rename`, which scoped seats now cannot call at all, R2.)
- **Rotation procedure (written down, claude m5):** `token.create` with the same
  `lanes` → hand off → `token.revoke` the old. Same lanes ⇒ same cursor position
  (R3); no rotate verb exists and none is added.
- **UI banner:** the rail simply lists `channels()` filtered = the lane set;
  no "restricted" banner required.

## 6. Milestones (claude's split — accepted)

**N1a (core, ONE commit):** schema+migration, principal plumbing (6 sites),
canSee lanes check, history SQL twins, post gate (resolved), create/delete/rename
gates, token.create/list validation+rejects, join subtraction, cursorKeyFor
(iface+waitStep+SSE resume), /raw+Sub lanes, LANES invite text + CLI print.
Contract pins, BOTH transports (CoreAsServer + RpcBusHttp — byte-identical
error shapes), server-mode cores only (local see-all would false-pass):
1. unrestricted default: existing token byte-identical; downgraded DB → NULL.
2. `''` sees nothing; all-invalid non-empty column fails closed to deny-all.
3. scoped invisibility on every canSee call site: channels/inbox/read/receipts/
   setStatus/dm.members `not_found`; threadOf drops (pin 11 semantics).
4. history BOTH branches + `since` without channel + `watch --all` path: zero
   foreign rows; `read:all` does not bypass.
5. /raw 404 on foreign-lane file; SSE `scope=all` and `scope=channel:<foreign>`
   zero frames, incl. `read:all`.
6. resolved-channel post gate: omitted→general requires general∈lanes; dm sugar
   forbidden unless resolved name ∈lanes ∧ live; in-list-missing → forbidden
   (no create); out-of-list → forbidden; uniform detail, no oracle; usage still
   wins for bad shapes (non-string before regex).
7. channel.create: out-of-list forbidden uniform; in-list materialize allowed;
   dup detail does not name a hidden sibling. channel.delete scoped forbidden
   uniform. rename scoped forbidden.
8. join unresolved ignores foreign lanes.
9. mint: `tokens:admin`+lanes and `agents:admin`+lanes rejected usage; scoped
   principal cannot token.create/list/revoke; cap 32; `lanes:"notanarray"` usage;
   `lanes:[]` usage; DM lane needs membership-or-`read:dm`; public lane must be
   live.
10. cursor isolation: scoped watch does not advance the unrestricted sibling's
    `cli` row; same-lanes rotated token keeps position; unrestricted keys
    byte-identical; forged `x:L…` consumer names rejected (outside CONSUMER_RE).
11. tokenVerify returns lanes; Set on principal; invite text + CLI LANES line.

**N1b (UI, separate commit, same milestone):** §4 invite prefill (logged-out
path, fragment stripped, no auto-submit), copy-invite-link, lanes chip; pins
that the prefill never submits and replaceState runs before fill effects.

**N2 (not blocking):** Admin mint form "limit to lanes" input reusing the
server's validation text verbatim.

## 7. Security notes

- A scoped token leaking is a LANE leak, not a bus leak — the point.
- `read:dm` still overrides DM visibility (D3: scoping only shrinks).
- Existence-oracle inventory after this RFC: post/create/delete/history/`/raw`/
  SSE all answer invisible==missing or uniform-forbidden; `threadOf` keeps its
  drop-not-forbidden semantics by design; `who`/presence/groups out of scope.
- Lane cap 32 keeps the JSON bind and the Set small; membership check is O(1).

## 8. Resolved review questions

- Q1 (cookie sessions): inherit; lanes re-derived from the token row on every
  cookie request; `login` returns `lanes` (claude ruling).
- Q2 (cursors): YES, axis needed — server-side lane-hash key (R3).
- Q3 (non-member DM lane post): REJECTED "allow" — intersection; mint-time
  membership gate catches the operator error instead.
