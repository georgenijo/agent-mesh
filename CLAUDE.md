# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Current state

**Pre-implementation.** The repo holds a design spec and a UI prototype — no source code, build system, tests, or package manifest exist yet, and it is not a git repo.

- `ARCHITECTURE.md` — the full system design (the source of truth; read it before building anything).
- `mockup.html` — a self-contained, dependency-free dashboard prototype. Open it directly in a browser (`open mockup.html`); it runs a scripted, animated NATS-bus visualization with no server or build step. Per the spec (Phase P4) this is intended to become the production dashboard, with the scripted feed replaced by a live WebSocket tap on `mesh.>`.

When implementation starts, add the real build/lint/test commands to this file.

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

`join`/`leave`, `who`/`status`/`announce`, `ask`/`poll`/`inbox`/`answer`, `note`/`context`. Every command takes `--json`. Exit codes carry meaning: `0` ok, `3` no-answer-yet, `4` no-such-ticket, `5` not-joined. See `ARCHITECTURE.md` §4 for the full surface and §5 for the subject → payload → listener table.

### Key message flows (ARCHITECTURE.md §7)

- **status / announce** — fire-and-forget pub/sub, no LLM turn. Conflict avoidance (`announce "editing X" --repo R`) is the highest-value, cheapest primitive.
- **ask / answer** — async ticket lifecycle; the responder's Stop-hook runs `mesh inbox` between turns, answers, and the asker's sidecar caches the reply for the next `poll`.
- **note / context** — `mesh.note.<repo>` is a **JetStream durable stream** (the blackboard). It is the one durable subject; everything else is ephemeral. Late-joining agents replay the full decision history with `mesh context`.

## Build phases (roadmap)

Build in this order (ARCHITECTURE.md §12): **P0** walking skeleton (NATS + one sidecar + `join/who/status` + dashboard tail) → **P1** async ask/answer + role-routing → **P2** `announce` + JetStream blackboard → **P3** warm experts, semantic cache, rate limits, audit log → **P4** live dashboard (promote `mockup.html` to a real `mesh.>` tap).

## Tech choices (when implementing)

Sidecar + CLI in **Go** with an **embedded NATS server** (one static binary, `meshd`/`mesh` by mode). Transport **NATS + JetStream** (also the durable store — no separate DB). Local IPC over a **unix socket** (permissioned, no open TCP port). Isolation via **Docker / cgroups** (later). Warm responders via **Claude Agent SDK** sessions. Agent identity via **A2A-style agent cards**. Rationale in ARCHITECTURE.md §11.

## Working notes

- **`docs/decisions/DECISIONS.md`** — running log of locked architectural decisions (language, phase order, CAS locks, TTL leases, envelope/authority invariants). Read it before changing direction.
- **Log major decision forks.** When a meaningful fork is resolved in conversation — architectural choice, scope cut/deferral, phase ordering, tradeoff resolution, convention adoption, or superseding a prior call — proactively invoke the **`/decisions`** skill to append it to the log (don't wait to be asked). Skip trivia, bug fixes, and anything the code or git history already captures. When superseding, flip the old entry's status rather than rewriting it.
- **`docs/concepts.md`** — glossary of the building blocks (daemon, NATS/JetStream, KV bucket, sidecar, coordinator, meshd, hooks). Start here if a term is unclear.
- **`docs/components.md`** — per-component feature breakdown, tiered MVP/v1+/later.
- **`docs/audit-multi-agent-pm.md`** — patterns mined from a sibling project (`steal`/`avoid`); the source of several locked decisions.
- **`topology.html`** — runtime topology diagram (`open topology.html`).
- Build order is **cheap-core-first** (ARCHITECTURE.md §12, revised): presence → announce+blackboard → ask/answer. Start homogeneous (Claude Code only).
