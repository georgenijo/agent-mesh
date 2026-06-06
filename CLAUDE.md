# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state

**P0 + P1 implemented.** Go module `github.com/georgenijo/agent-mesh`, zero external dependencies, stdlib only. The spine is real end-to-end across separate processes — CLI → sidecar unix socket → coordinator-embedded bus → registry/claims KV — with heartbeat leases and two-tier eviction (live → away → evict).

- **P0 (presence):** `mesh join/leave/who/status`, autostart, read-only SSE dashboard.
- **P1 (conflict avoidance + blackboard, #12–#16):** `mesh claim/release` (CAS file-claims; typed `claimed|lost|error`; exit 6 on lost; TTL leases with reclaim-on-death and re-establishment across a coordinator restart; paths canonicalized repo-relative so a hook's absolute path and a manual relative path collide on one key), `mesh announce` (advisory pub/sub), `mesh note/context` (durable per-repo blackboard streams persisted to `$MESH_DIR/streams/<name>.jsonl`, replayed across restarts). Claude Code `PreToolUse` claim-guard hook in `hooks/claude-code/`. Dashboard shows live claims + notes.
- **Ops plane:** `mesh ops` + `meshd --mode observe`.
- **P2 (async ask/answer, #17–#22):** `mesh ask/poll/inbox/answer` across real processes — role-routed asks CAS-accept exactly one responder; the asker never blocks (ticket returned immediately, answer collected via `poll`). Tickets KV is the one authority.
- **Expert responder loop (first non-manual slice of #27):** `mesh expert serve --name N --role R` (a foreground `meshd --mode expert`) joins as a role-owning expert and auto-answers its accepted asks through a resident `internal/runtime` stream-json child (`MESH_EXPERT_CLI`, default `claude`) — no human runs `inbox`/`answer`. Same single answer path; no fake-success.

GitHub: `georgenijo/agent-mesh`.

### Build / test commands

```sh
make build       # bin/meshd + bin/mesh
make test        # all unit tests + cross-process e2e (test/e2e, ~4s)
make test-race   # unit tests with the race detector
make e2e         # just the cross-process e2e suite, verbose
make vet         # go vet ./...
make fmt         # gofmt -l -w .
make ci          # exactly what CI runs: fmt-check + build + vet + test
```

Try it: `make build && PATH="$PWD/bin:$PATH" MESH_DIR=/tmp/mymesh mesh join --name me --role builder` — `mesh join` autostarts the sidecar and coordinator; `meshd --mode dashboard` serves the live observer.

### Where things live

- `cmd/{meshd,mesh}` — thin entrypoints (daemon modes: sidecar | coordinator | dashboard).
- `internal/envelope` — the wire contract: versioned envelope, kinds, typed result enums, UUIDv7 ids, subject taxonomy. **Every message goes through this package.**
- `internal/bus` — the P0 transport: coordinator-embedded star bus over a unix socket (pub/sub + KV CAS/TTL + bounded streams). The `bus.Client` API is the seam if NATS ever replaces it (see DECISIONS.md).
- `internal/socket` — one-shot CLI↔sidecar request/reply.
- `internal/sidecar`, `internal/coordinator`, `internal/dashboard`, `internal/cli`, `internal/agentcard`, `internal/meshapi`, `internal/autostart`, `internal/config`.
- `test/e2e` — cross-process acceptance tests (real binaries, real sockets — the "done" gate).
- `ARCHITECTURE.md` — the full system design. ⚠️ §1/§11/§12 partly predate the autonomous-pivot and the P0 star-bus decision; `docs/decisions/DECISIONS.md` (newest-first) wins on conflict.
- `docs/mockups/dashboard-bus.html` — design prototype for the eventual P4 production dashboard (the P0 observer is `internal/dashboard`).

## What Agent Mesh is

A local-first coordination fabric so multiple heterogeneous coding agents (Claude Code, Codex CLI, Cursor CLI, Aider, …) can discover each other, share status, ask each other questions, and avoid editing the same files — all through one `mesh` CLI. Collaboration is **opt-in per message**, not forced per turn: agents work solo by default and touch the mesh only when work overlaps or they're stuck.

## Architecture: the big picture

Four planes, joined by a NATS bus. Understanding the **control-plane / data-plane split** is the key to the whole design:

- **Sidecar daemon** — one per agent. Owns the unix socket the `mesh` CLI talks to, registers the agent, emits heartbeats (every 5s), bridges to NATS. The `mesh` CLI itself is a *thin* client: open socket → one request → print reply → exit. All state lives in the sidecar + NATS, never in the CLI.
- **NATS + JetStream** — the bus. Agents address **subjects** (`mesh.>`), never each other by hostname. Pub/sub + request/reply (ephemeral) + durable streams (the blackboard).
- **Coordinator** — **control plane only**. Maintains the registry, routes role-addressed asks, enforces policy (rate limits, dedup), breaks consensus ties. It is deliberately **not in the data path** — question/answer payloads travel *directly* between agents' sidecars, so the hub is never a throughput bottleneck.
- **Dashboard** — pure observer; subscribes `mesh.>` and renders. The Expert pool is on-demand responder agents the coordinator spawns when no live agent owns a topic.

### Two principles that constrain almost every decision

1. **Async by default — never block a turn waiting.** An `ask` returns a ticket *immediately*; the asker keeps working and collects the answer later via `poll` or a hook. The only real cost in the system is the responder's LLM turn (5–60s); NATS transport is sub-millisecond and effectively free. Keep that LLM cost off the asker's critical path.
2. **CLI at the edge, not MCP.** Agents interact through the `mesh` binary because it's context-cheap (no schemas injected every turn), universal (every agent can run bash), and composable. Preserve this — don't reach for an MCP tool surface.

### The `mesh` CLI is the entire agent-facing API

`join`/`leave`, `who`/`status`/`announce`, `ask`/`poll`/`inbox`/`answer`, `note`/`context`. Every command takes `--json`. Exit codes carry meaning: `0` ok, `3` no-answer-yet, `4` no-such-ticket, `5` not-joined, `6` claim-lost, `7` dirty (ops doctor/down). See `ARCHITECTURE.md` §4 for the full surface and §5 for the subject → payload → listener table.

### Key message flows (ARCHITECTURE.md §7)

- **status / announce** — fire-and-forget pub/sub, no LLM turn. Conflict avoidance (`announce "editing X" --repo R`) is the highest-value, cheapest primitive.
- **ask / answer** — async ticket lifecycle; the responder's Stop-hook runs `mesh inbox` between turns, answers, and the asker's sidecar caches the reply for the next `poll`.
- **note / context** — `mesh.note.<repo>` is a **JetStream durable stream** (the blackboard). It is the one durable subject; everything else is ephemeral. Late-joining agents replay the full decision history with `mesh context`.

## Build phases (roadmap)

Build in this order (ARCHITECTURE.md §12, revised cheap-core-first): **P0** walking skeleton (presence: `join/who/status` + heartbeat lease + dashboard tail) → **P1** conflict avoidance + blackboard (`announce` + CAS file-claims, `note/context`) → **P2** async ask/answer (ticket FSM + role-routing, Claude Code hook) → **P3** experts, caching, rate limits, audit log, multi-CLI hooks → **P4** live dashboard (promote `dashboard-bus.html` to a real `mesh.>` tap).

## Tech choices (when implementing)

Sidecar + CLI in **Go** with an **embedded NATS server** (one static binary, `meshd`/`mesh` by mode). Transport **NATS + JetStream** (also the durable store — no separate DB). Local IPC over a **unix socket** (permissioned, no open TCP port). Isolation via **Docker / cgroups** (later). Warm responders via **Claude Agent SDK** sessions. Agent identity via **A2A-style agent cards**. Rationale in ARCHITECTURE.md §11.

## Working notes

- **`docs/decisions/DECISIONS.md`** — running log of locked architectural decisions (language, phase order, CAS locks, TTL leases, envelope/authority invariants). Read it before changing direction.
- **Log major decision forks.** When a meaningful fork is resolved in conversation — architectural choice, scope cut/deferral, phase ordering, tradeoff resolution, convention adoption, or superseding a prior call — proactively invoke the **`/decisions`** skill to append it to the log (don't wait to be asked). Skip trivia, bug fixes, and anything the code or git history already captures. When superseding, flip the old entry's status rather than rewriting it.
- **`docs/concepts.md`** — glossary of the building blocks (daemon, NATS/JetStream, KV bucket, sidecar, coordinator, meshd, hooks). Start here if a term is unclear.
- **`docs/components.md`** — per-component feature breakdown, tiered MVP/v1+/later.
- **`docs/repo-layout.md`** — target Go repo structure (`cmd/`+`internal/`); create `internal/` dirs as each phase needs them.
- **`docs/audit-multi-agent-pm.md`** — patterns mined from a sibling project (`steal`/`avoid`); the source of several locked decisions.
- **`docs/mockups/`** — HTML prototypes: `dashboard-bus.html` (bus visualizer), `dashboard-full.html` (full dashboard concept), `topology.html` (runtime topology diagram). No build step — `open` in a browser.
- Build order is **cheap-core-first** (ARCHITECTURE.md §12, revised): presence → announce+blackboard → ask/answer. Start homogeneous (Claude Code only).
