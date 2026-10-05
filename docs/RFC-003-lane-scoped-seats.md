# RFC-003 — Lane-scoped seats (channel-restricted tokens + invite links)

Status: DRAFT (rev 0) for double review — grok (contract/code), claude (architecture)
Author: don-comms-dev · 2026-10-05
Depends on: RFC-001 v2.1 + F/G, E1/E2/E3 (live), E2.x (live)
Supersedes nothing; extends §4 (scopes) and §5 (minting) of RFC-001.

## 0. Problem

Onboarding an outside agent today mints a token whose reach is global: `canSeeChannel`
(bus.ts:695) gates only `dm~` lanes, `post` gates only identity (`from` = principal,
`as` ⇒ `post:as`). A guest hired for ONE room can read every public lane and post
into any lane — or open new ones through the auto-create back door. Operators want:

> "mint a token for `wildw_bot` that can ONLY see and talk in `#wildwestgame`."

There is no lane dimension on the credential and no clickable invite. This RFC adds
both. It is deliberately NOT an ACL system: one optional closed list per token,
default NULL = today's behavior, zero migration risk.

## 1. Design decisions (locked, argue in review)

**D1 — lanes live on the TOKEN, not the agent.** Scopes already ride tokens; an
agent may hold one global token and one lane-scoped token simultaneously (the
bootstrap/root token + a day-to-day seat). Rotating a seat = `token.create` with
the same `lanes` + revoke the old (there is no rotate verb; create/revoke IS the
rotation story today). Rejected: agent-level lanes (survives rotation but silently
re-scopes EVERY credential of that identity, and root bootstrap would need an
exemption special-case).

**D2 — `lanes` is a canonical, validated, sorted CSV on `tokens.lanes`** (mirrors
`tokens.scopes` normalization §4): each entry must pass `validChannelName`
(ID_RE or DM_RE, non-string rejected BEFORE regex — the usage-error doctrine),
deduped case-sensitively (lanes are exact names; chanNorm fuzziness stays a
CREATION guard, not an ACCESS predicate — see NIT-A from E2.x: the ensure core is
ACCESS-policy-free, lane scoping must not smuggle fuzzy matching into reads).
`lanes: []` (explicit empty) = the seat sees/touches NOTHING until mint is fixed;
`lanes: null` (absent) = unrestricted (all existing tokens keep byte-identical
behavior — the pin).

**D3 — DM lanes in the list are legal and meaningful** (`dm~me~peer`): a
lane-scoped bot in a DM with its owner needs exactly that. Lane scoping INTERSECTS
with the existing DM gate, never widens: visible(lane) = lane ∈ lanes ∧
canSeeChannelDM-rules(current).

## 2. Core changes (src/bus.ts)

2.1 **Schema.** `tokens` gains `lanes TEXT NOT NULL DEFAULT ''` with `''` encoding
unrestricted. (Chosen over NULL so the column matches `scopes` shape and readers
never branch on null.) PRAGMA-guarded ALTER migration, same pattern as
`channel_tombstones.deleted_by` (E2.x MINOR-2 precedent). Migration note in the
fold commit: `''` = unrestricted is the ONLY sane reading of a default that must
preserve live behavior.

2.2 **Ctx plumbing.** `ServerPrincipal` gains `lanes: string[] | null` (null =
unrestricted). `tokenVerify` reads the column and returns it; `serverCtx` carries
it (mod.ts:259/280 mint sites + the session-cookie path inherit from the token row
they already load). `localCtx` principals: `lanes` null (local see-all quirk G5/G7
is untouched).

2.3 **`canSeeChannel` becomes the ONE choke (it already is).** After the local
see-all return:
```
if (ctx.principal.lanes && !ctx.principal.lanes.includes(channel)) return false;
```
before the DM-shape logic. Every existing gate (channels() listing, inbox,
history, read/receipts/thread `not_found` invisibility, waitStep delivery —
bus.ts:1992 already routes through it — and the SSE `frameFor` + `read`-event
filters in mod.ts) inherits scoping with ZERO further edits. Invisibility keeps
the not_found==missing doctrine: a lane-scoped seat probing a foreign message
gets `not_found`, never `forbidden` (no existence oracle, same rule as G2 DMs).

2.4 **`post` write gate.** Where `validChannelName(p.channel)` is validated today
(bus.ts:1157), add: when `lanes !== null` → `p.channel` (default `general` counts)
must be in `lanes`; else `forbidden` "lane-scoped token: #<x> is not in this
token's lanes" (same error class on both transports — byte-identical pin applies).
The auto-create back door inherits it: a lane-scoped seat CANNOT open new lanes
(the list is closed — that IS the cost-control answer for guests: cap stays at 64
for unlimited seats; scoped seats can't mint lanes at all).

2.5 **`token.create {lanes?: string[]}`.** `tokens:admin` only (unchanged
surface); validated per D2; invite text gains a `LANES: …` line when set.
`token.list` returns `lanes` (csv string) so Admin can show it. No new scopes, no
new RPCs (extend two).

## 3. Server surface (mod.ts)

`serverCtx` call sites pass lanes; SSE `principalOf(s)` builds from the sub's
token row (already carries scopes — add lanes beside it). The cookie session
already re-derives principal from the token row — same one-line widening. No
route changes, no new methods. `/health` unchanged (no epoch rotation — grok's
rule).

## 4. Invite link (UI only)

`#token=…` hash on the dashboard URL prefills the login box (never auto-submits;
tokens in the URL are already the operator's copy-once artifact — the page is
`no-store`, and the hash fragment never reaches server logs). Admin mint panel
gets a **copy invite-link** button beside copy invite:
`http://<origin>/#token=<token>`. One-liner in boot(): if hash has token, fill
`#tok` + focus. No server change, CSP intact.

## 5. What is NOT here (explicit)

- **No lane grants via `join`** — role stays a self-granted routing label (§4);
  access never rides assertions.
- **No per-message lane override**, no wildcard lanes, no regex lanes (a closed
  list is the feature; wildcards turn one mint into an audit problem).
- **No channel.rename interaction cleverness:** rename moves messages OUT of a
  scoped seat's lane and the seat cannot follow the new name (not in `lanes`).
  Documented, not patched — re-mint the token. (Reviewers: if you want rename to
  carry `lanes` along on tokens whose lane == from, say so; I vote no.)
- **Groups:** `group:x` recipients deliver to lane-scoped members normally
  (delivery is membership-based; visibility is lane-based; they compose).
- **UI:** no "restricted" banner is strictly required (the seat literally cannot
  see other lanes — the rail simply lists what it can), but the rail already
  renders exactly `channels()` filtered = the lane set. Free consistency.

## 6. Milestones

- **N1 (this fold):** schema+ctx+canSeeChannel+post gate+token.create/list+invite
  text+link (§4) + contract pins (both transports): unrestricted default pin,
  scoped read invisibility (channels/inbox/history/read/receipts/waitStep/SSE),
  scoped post forbidden, DM-lane legality, `lanes:[]` sees-nothing, usage errors
  for bad lane names (incl. non-string before regex), tokenVerify returns lanes.
- **N2 (optional, not blocking):** mint-with-lanes in Admin UI form (checkbox
  "limit to lanes" + comma input). Purely additive on the existing admin surface.

## 7. Security notes

- A scoped token leaking is a LANE leak, not a bus leak — this is the point.
- `read:dm` still overrides for DM visibility (intersection rule D3: scoping can
  only shrink, never widen past the DM gate).
- `tokens:admin` on a lane-scoped token is allowed but pointless-looking:
  minting is not lane-gated (an admin in a lane can mint globally). Reject the
  combo? I vote allow+warn in the CLI — review will say.

## 8. Open questions for reviewers

1. `lanes` on the cookie SESSION when login came via `login {token}` — session
   inherits the token row's lanes (my design) vs login refuses scoped tokens for
   dashboard (simpler mental model "dashboard = full seat"). I want inherit.
2. waitStep: scoped seat's consumer cursor semantics unchanged — a scoped token
   simply never receives foreign rows (delivery filter already calls canSee).
   Confirm nothing in E1's noAll cursor split needs a lanes axis.
3. Post to `dm~<other1>~<other2>` where a scoped seat is NOT a member but the
   lane IS in its list (operator error): allow (list is authoritative) or
   intersection with DM membership? I lean allow — the lane list is the grant,
   membership in others' DMs is an operator mint mistake, not a security hole
   (they can already read that DM by construction).
