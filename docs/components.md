# Agent Mesh — Components & Features

What each working part is and the features it must support. Tiers: **[MVP]** = needed for the thesis to work at all, **[v1+]** = makes it good, **[later]** = scale/hardening. Audit lessons (from `audit-multi-agent-pm.md`) are baked in.

See `concepts.md` for what each term means and `mockups/topology.html` for the picture.

---

## 0. Envelope module (shared library, not a process)

*Everything imports this. Build it first — it is the contract.*

- **[MVP]** One **versioned envelope**: `{ schemaVersion, kind, id, from, to?, subject, ts, ...payload }`. `kind` ∈ status/announce/claim/ask/answer/note/heartbeat/…
- **[MVP]** **encode + decode co-located** in one file — so no CLI/sidecar can invent its own format (audit: the markdown-regex contract trap).
- **[MVP]** **Typed result enums** for every operation: `claimed|lost|error`, `answered|pending|timed_out|expired|no_such_ticket`. Never fake-success.
- **[MVP]** **ULID/UUIDv7 ids** (time-ordered, collision-safe) — not `Date.now()+random`.
- **[MVP]** Strict validation on decode; malformed → typed `unparseable`, never a crash.
- **[v1+]** Schema-version negotiation (forward-compat as kinds evolve).

## 1. NATS + JetStream (infra — we run/embed, don't build)

- **[MVP]** JetStream **on**. **Streams**: `notes`, `ticket-events`, `audit`. **KV buckets**: `registry`, `claims`, `tickets`, `inbox`.
- **[MVP]** **Per-key TTL** on `claims` + `registry` (lease expiry).
- **[MVP]** **CAS** on KV (single-winner claims).
- **[MVP]** Loopback / unix-socket bind — no public port.
- **[MVP]** Retention + compaction config on every stream from day one (audit: bound it or it grows forever).
- **[v1+]** Per-sidecar **credentials** + **subject ACLs** (an agent may publish `answer.*` only for its own tickets).
- **[v1+]** Embedded inside the coordinator binary (drop the separate process).

## 2. Sidecar — `meshd --sidecar` (1 per agent)

*The persistent memory + connection the CLI lacks.*

- **[MVP]** **Unix-socket server** — accept `mesh` CLI requests, file-permissioned.
- **[MVP]** **NATS client** — one long-lived connection; publish/subscribe on this agent's behalf.
- **[MVP]** **Heartbeat loop** — renew the presence lease every N s.
- **[MVP]** **Hold this agent's state** — open tickets, file-claims (authority is JetStream KV; sidecar caches + reacts).
- **[MVP]** **Answer caching** — hold an arrived answer for the next `poll`.
- **[MVP]** **Envelope encode/validate** at the publish edge (single chokepoint — clamp/shape/validate before the wire).
- **[v1+]** **Inbox subscription + bounded depth** + backpressure (not unbounded RAM buffering — audit).
- **[v1+]** **Hook endpoints** — fast local answers for pre-edit (claim check) + between-turn (inbox drain).
- **[v1+]** Reconnect handling — tear down + re-subscribe cleanly on a NATS blip (audit: leaked listeners).
- **[MVP]** Clean teardown on `leave` — release claims, deregister.

## 3. Coordinator — `meshd --coordinator` (1 total)

*Pure control plane. Decides who, never carries what.*

- **[MVP]** **Registry** — consume register/leave, maintain the `registry` KV (one authority for "who exists").
- **[MVP]** **Presence / two-tier lease eviction** — `away` (degraded, still listed) vs `evict`; registration grace period.
- **[MVP]** **Reclaim-on-death** — on evict: re-open that agent's accepted-but-unanswered tickets + release its claims.
- **[MVP]** **Role → subject routing** — compute the target subject for `ask --role`; publish there; sidecars self-claim via CAS (pull-routing). Coordinator never assigns.
- **[v1+]** **Policy** — per-agent rate-limit, **dedup** identical in-flight asks.
- **[v1+]** **Audit log** — append every ask/answer/note/claim to the `audit` stream.
- **[v1+]** Routing intelligence — rules first, small classifier later.
- **[later]** Consensus tie-break; expert-pool spawn trigger.
- **[MVP]** **Pure reducer** over bus events — no payload handling, no data-path role.

## 4. mesh CLI (short-lived client)

*The entire agent-facing API.*

- **[MVP]** Verbs: `join/leave`, `who`, `status`, `announce`, `claim/release`, `ask`, `poll`, `inbox`, `answer`, `note`, `context`.
- **[MVP]** **Thin client** — open socket → 1 request → print → exit. Zero state.
- **[MVP]** `--json` on every verb; **meaningful exit codes** (`0/3/4/5`).
- **[MVP]** **Async contract** — `ask` prints a ticket id and returns *now*; never awaits a peer (audit: their `request()` awaiting inline = the anti-pattern).
- **[v1+]** `ask --wait` (blocking convenience for scripts only).
- **[v1+]** `inbox --watch` (long-poll stream).
- **[v1+]** Autostart sidecar if not running (so `mesh join` "just works").

## 5. Dashboard — `meshd --dashboard` (1 total)

*Read-only observer.*

- **[MVP]** Subscribe `mesh.>`; **WebSocket bridge** → browser; serve static UI.
- **[MVP]** Render: roster + presence, status, announces/claims, ticket lifecycle, notes feed.
- **[MVP]** **Read-only** — never publishes to the mesh.
- **[v1+]** **Authenticated subscriber + subject-filter scoping** (audit: no global unauth firehose).
- **[v1+]** Metrics by **counting the registry at read time**, not hand-rolled counters (audit: dead/miswired metrics).
- **[v1+]** Replace `mockups/dashboard-bus.html` scripted feed with the live tap (this is P4).

## 5b. Runtime observer — `mesh ops` + `meshd --observe` (1 total)

*Ops-plane inspector, separate from the product dashboard.*

- **[MVP]** `mesh ops [--json]` — one-shot runtime snapshot: coordinator PID/bus socket, sidecar sockets, registry drift, log paths. No join required.
- **[MVP]** `meshd --mode observe` — read-only HTTP on `:8739` (`GET /api/snapshot`, minimal auto-refresh UI). Never publishes to the mesh.
- **[MVP]** Sidecar `runtime` verb — sidecar-reported child CLI PIDs (`TrackChild`/`MarkChildExited` hooks for P2/P3 spawns).
- **[v1+]** Prometheus/OpenTelemetry export from the same `internal/observe` collector.

## 6. Hooks (config/glue per agent CLI)

- **[MVP]** **Claude Code first** — `PreToolUse`-on-Edit (claim check before edit) + `Stop` hook (drain inbox between turns).
- **[v1+]** **Codex CLI adapter** — `SessionStart` join/announce, `PreToolUse` on `apply_patch` targets, `Stop` inbox drain; prompt leave-on-exit via `codex-mesh.sh` launcher because Codex does not currently document `SessionEnd`.
- **[v1+]** Remaining per-CLI adapters: Cursor, Aider — **each its own integration**, parity not assumed (the #1 risk).
- **[v1+]** Struggle-detection nudge (test-fail loop / repeated edits → "ask the mesh?").
- **[later]** Mid-turn answer injection (varies by CLI).

## 7. Expert pool *(later)*

- **[later]** Spawn a warm Agent-SDK responder when no live agent owns a routed topic.
- **[later]** Session reuse (skip cold context reload); semantic answer cache.

---

## The shared spine

Three things every component touches and must agree on:

1. **The envelope** (§0) — the wire format.
2. **The subject taxonomy** — `mesh.<verb>.<scope>` (e.g. `mesh.status.<id>`, `mesh.ask.role.<role>`, `mesh.ticket.<id>.events`).
3. **The KV authority** — one record per fact, in JetStream.

Get those three right and the components compose. Get them wrong and you rebuild the audit's two-sources-of-truth mess.

## Smallest first slice (P0)

Envelope (§0) + JetStream setup (§1) + a stub sidecar (§2) + `mesh status` / `mesh who` (§4): one agent posts a status, it travels the bus, `mesh who` and the dashboard see it. Proves the spine end-to-end across a real process boundary.
