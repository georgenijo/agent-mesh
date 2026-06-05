# Agent Mesh

A local-first coordination fabric that lets heterogeneous coding agents
(Claude Code, Codex CLI, Cursor CLI, Aider, …) discover each other, share
status, ask each other questions, and avoid stepping on each other's work —
through a single `mesh` command-line tool.

You run several coding agents at once. Today they're blind to each other: two
edit the same file, a third re-derives a fact a fourth already figured out, and
you babysit all of them. Agent Mesh gives them a shared nervous system so they
can **announce** what they're working on, **ask** a question and get an answer
from whichever agent (or human) knows, read a shared **blackboard** of
decisions, and be **observed** from one live dashboard — all opt-in per message,
not forced per turn.

## Status

Pre-implementation. This repo currently holds the design and a UI prototype:

- **[ARCHITECTURE.md](ARCHITECTURE.md)** — the full system design (source of truth).
- **[docs/](docs/)** — concepts glossary, per-component features, the decisions log, and the multi-agent-pm audit.
- **[docs/mockups/](docs/mockups/)** — HTML prototypes (no build step, open in a browser):
  - `dashboard-bus.html` — the NATS-bus visualizer; slated to become the production dashboard.
  - `dashboard-full.html` — a fuller dashboard concept (tickets, file claims, streams, notes, policy, cache, experts, audit).
  - `topology.html` — runtime topology diagram.

## Build phases

**P0** walking skeleton (NATS + one sidecar + `join`/`who`/`status` + dashboard tail) →
**P1** async ask/answer + role-routing →
**P2** `announce` + JetStream blackboard →
**P3** warm experts, semantic cache, rate limits, audit log →
**P4** live dashboard.

See [ARCHITECTURE.md](ARCHITECTURE.md) §12 for detail.
