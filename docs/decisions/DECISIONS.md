# Decisions Log

Running log of architectural, scope, and process decisions for this project. Newest entries at the top. Each entry is short — for deep rationale on a single locked decision, write an ADR alongside in `docs/decisions/YYYY-MM-DD-*.md` and reference it here.

Maintained via the `/decisions` skill. See `~/.claude/skills/decisions/SKILL.md` for the entry format and invocation rules.

---

## 2026-06-05: Adopt standard Go cmd/+internal repo layout; mockups under docs/

**Decision:** Use the standard Go layout — `cmd/{meshd,mesh}` entrypoints over private `internal/` packages (one per component plus a `envelope`/`bus`/`socket` shared spine), `web/` for the embedded production dashboard UI, `hooks/` for per-CLI glue, `test/e2e/` for cross-process tests, `deploy/` for later packaging. No speculative `pkg/`. HTML prototypes moved to `docs/mockups/` (`dashboard-bus.html`, `dashboard-full.html`, `topology.html`). `internal/` dirs are created as each phase needs them, not scaffolded empty up front. Full tree in `docs/repo-layout.md`.

**Rationale:** `cmd`+`internal` is the idiomatic Go convention — `internal/` prevents accidental public API leakage, thin `cmd/` keeps logic testable, and the spine split (`envelope`/`bus`/`socket` with no upward imports) prevents cycles. Mockups belong with design docs, not at repo root, now that real code dirs are coming. First-class `test/e2e/` answers the audit lesson that unit tests over mocks hid a non-running system.

**Status:** active

**References:** docs/repo-layout.md, docs/components.md

---

## 2026-06-05: Build meshd/mesh in Go with an embedded NATS server

**Decision:** Implement the single `meshd`/`mesh` binary in Go. Embed the NATS server + JetStream as a Go library inside the coordinator mode rather than running NATS as a separate installed process for the local-first MVP.

**Rationale:** Go gives a single static binary, trivial daemons/goroutines, and — uniquely — NATS server + JetStream are Go libraries that embed cleanly, collapsing the infra to one shipped artifact. Rust was considered (stronger types/perf) but async daemons are heavier and NATS embeds less cleanly, slowing the path to a walking skeleton. Speed-to-first-running-slice won.

**Status:** active

**References:** ARCHITECTURE.md §11, docs/concepts.md, docs/audit-multi-agent-pm.md

---

## 2026-06-05: Reorder build phases — cheap coordination core before async ask/answer

**Decision:** Build the no-LLM-in-the-loop primitives first — presence (join/who/status/heartbeat), `announce`+claims (conflict avoidance), and `note`/`context` (blackboard) — and only then build the async ask/answer ticket machinery. Supersedes the original ARCHITECTURE.md §12 ordering (P1 ask/answer before P2 collaboration primitives). Also: start homogeneous (Claude Code only) to solve hook parity once before generalizing to Codex/Cursor/Aider.

**Rationale:** announce + blackboard are pure pub/sub — guaranteed value, zero LLM-turn cost, low risk. ask/answer is the expensive, hallucination-compounding, hook-dependent part. The original plan front-loaded the risky LLM-coordination before the cheap guaranteed wins. Prove the thesis with the cheap core; defer the research bet.

**Status:** active

**References:** ARCHITECTURE.md §12 (revised), §9

---

## 2026-06-05: JetStream KV revision-CAS is the single claim/lock primitive

**Decision:** Use one mechanism — a revision-guarded compare-and-set `put` on a JetStream KV key — for both `announce` pre-edit file locks and ask-ticket claims. Every claim returns a typed result `claimed | lost | error`. `announce` stays as advisory pub/sub, but real file edits additionally take a CAS lock (advisory-only is insufficient).

**Rationale:** Direct lesson from the multi-agent-pm audit: their atomic guarded-claim (`updateMany where claimedBy:null`, count==0 == lost) was the one correctly-designed concurrency primitive, while their in-memory "simulate atomic operation" was a race safe only on a single event loop. Our agents are separate processes, so the claim commit must be a single atomic op. Resolves the earlier open question "advisory announce vs real lock."

**Status:** active

**References:** docs/audit-multi-agent-pm.md (Steal #1, Avoid #2), ARCHITECTURE.md §7c

---

## 2026-06-05: Every claim and presence record is a TTL lease with reclaim-on-death

**Decision:** Claims and presence are TTL-leased KV keys renewed by sidecar heartbeat, with two-tier eviction (`away` = degraded but still listed, vs `evict` = gone) and a registration grace period. On eviction the coordinator re-opens that agent's accepted-but-unanswered tickets and releases its file claims.

**Rationale:** The audit's worst failure mode: release happened only on a cooperative `agent:offline` event, so a hard-crashed/OOM-killed agent held its claim forever and stranded work. A TTL lease makes a crash self-healing. Two-tier separates "temporarily unreachable" from "gone."

**Status:** active

**References:** docs/audit-multi-agent-pm.md (Steal #5, Avoid #3), ARCHITECTURE.md §6

---

## 2026-06-05: One versioned envelope, one authority per fact, open-data roles/caps

**Decision:** Adopt three wire/state invariants from day one: (1) a single versioned envelope module with co-located encode+decode (`schemaVersion`+`kind`, JSON/CBOR, ULID/UUIDv7 ids, typed result enums, never fake-success); (2) exactly one source of truth per fact — the JetStream record — that every consumer subscribes to and never re-derives; (3) roles/caps are open data registered at join, exact-token matched, treated as claims to verify — not a closed compile-time enum, never substring-matched.

**Rationale:** Each maps to a pervasive audit failure: contract-decoded-from-markdown regexes (the cross-CLI parity trap), two-unreconciled-sources-of-truth (DB vs in-memory FSM with different vocab), and substring capability matching (`'go'` matched `'mongo'`) plus closed enums a third-party CLI can't extend. Locking these as invariants prevents rebuilding the same mess.

**Status:** active

**References:** docs/audit-multi-agent-pm.md (Avoid #1/#6/#7, Steal #3/#7/#8), docs/components.md §0

---
