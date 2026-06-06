# Decisions Log

Running log of architectural, scope, and process decisions for this project. Newest entries at the top. Each entry is short — for deep rationale on a single locked decision, write an ADR alongside in `docs/decisions/YYYY-MM-DD-*.md` and reference it here.

Maintained via the `/decisions` skill. See `~/.claude/skills/decisions/SKILL.md` for the entry format and invocation rules.

---

## 2026-06-06: `mesh up` = idempotent infra bring-up in autostart; ops scope unchanged

**Decision:** One command — `mesh up [--dashboard-addr A] [--observe-addr A]` — idempotently brings up coordinator + dashboard + observe and prints their URLs. The spawn logic lives in `internal/autostart` (which already starts coordinators and sidecars); `internal/ops` stays inspect + teardown + janitor and never spawns, preserving the 2026-06-05 actuator-verbs scope. Supporting protocol: dashboard/observe write run files under MESH_DIR (`<name>.pid` first, then `<name>.addr` atomically with the REAL bound address — the addr file is both the readiness gate and the one authority for "where is the UI"), spawn carries the `--mesh-dir` argv ownership marker so `ops down/doctor/clean` cover the services, and a foreign holder on the configured port triggers an EADDRINUSE-only fallback to `127.0.0.1:0` (other listen errors stay fatal). "Already running" = pidfile alive AND addr dialable; a live-but-not-serving pid is a typed error, never a respawn.

**Rationale:** Three manual commands to get UI + monitoring was the real "local is annoying" pain (ports never were — sockets are MESH_DIR-namespaced; the two loopback TCP ports are the one global resource, hence the fallback). All machinery existed (EnsureCoordinator flock pattern, daemon modes, ops verbs); this is glue plus a run-file readiness protocol, not architecture. Scope stops at infrastructure: agents join themselves, worker spawning stays with the coordinator (P3).

**Status:** active

**References:** internal/autostart/services.go, internal/observe/runfiles.go, internal/cli/up.go, test/e2e/up_test.go; extends 2026-06-05 "Ops plane gains scoped actuator verbs"

---

## 2026-06-05: P4 dashboard tap ships as SSE on the existing /events contract, present-day events only

**Decision:** The #31 production dashboard (`web/`, served read-only at `/ui/` by the dashboard server) consumes the existing SSE `/events` contract — data-only frames discriminated by the JSON `type` field (`event` | `roster` | `claims`) — not the WebSocket transport the issue text named. Scope is what the mesh emits today: presence roster, status, heartbeats, announce, claims (rebuilt wholesale from the authoritative claims-KV snapshot frame, never derived from claim/leave envelopes), and blackboard notes. P2 tickets and P3 experts/workers get honest placeholder panels that populate only from real envelopes — nothing invented.

**Rationale:** Go's stdlib has no WebSocket and the zero-dependency constraint is locked; the P0 observer's SSE contract is already proven and now contract-tested (`TestSSEPresenceLifecycleContract`). Rendering ticket/expert lifecycles before P2/P3 emit traffic would mean fake data. #31's WebSocket + ticket-lifecycle acceptance items are deferred to when P2/P3 land, not dropped.

**Status:** active

**References:** web/, internal/dashboard/webui.go, #31, #40

---

## 2026-06-05: #27 persistent experts land as a prep slice — internal/runtime proxy only, wiring waits for P2

**Decision:** The resident-expert runtime ships as the self-contained `internal/runtime` package: a Proxy supervising one long-lived `claude -p --input-format stream-json --output-format stream-json --verbose` child (held-open stdin, typed events tolerant of unknown shapes, one in-flight ask, typed child-death errors, `--resume` as recovery-only, bounded close). No sidecar wiring, no role routing — those wait for P2's #19/#20. One adopted hardening beyond the locked resident-process decision: a result whose success discriminators (`subtype`/`is_error`/`api_error_status`) are type-degraded is never a success, and a cancelled ask's late result is dropped (stream-json results carry no correlation id) so it cannot misroute the next ask.

**Rationale:** #27's full acceptance depends on role routing and inbox caching that don't exist yet; building the supervision boundary now, against the spike-verified contract and a fake re-exec child, de-risks the hard part (process lifecycle under failure) without coupling to unbuilt P2 machinery. Package name shadows Go's `runtime` — importers alias; noted in the package doc.

**Status:** active

**References:** internal/runtime, docs/spikes/M0-feasibility.md, #27, #19, #20; extends 2026-06-05 "Persistent experts = a resident stream-json claude process"

---

## 2026-06-05: Claim paths are canonicalized repo-relative at the sidecar

**Decision:** A claim path is folded to its repo-relative form before it becomes a claim key: the sidecar relativizes an absolute in-tree path against the agent's `card.CWD`, then `claim.NormalizePath` cleans it (slash form, reject `..` escapes), and `Key(repo, path)` joins the parts with a NUL byte. `NormalizePath` itself does not relativize (it has no repo root); that one job lives at the sidecar, which does. Absolute paths outside the repo root are kept absolute (no common base to fold against).

**Rationale:** The Claude Code edit hook hands the tool an absolute `file_path` while a human running `mesh claim src/foo.go` passes a repo-relative one. Keyed verbatim, the two spellings of one file produced different keys and *both* won the create-only CAS — a lock two spellings slipped past (confirmed by the P1 adversarial review, F1/F2). Folding to one canonical key closes the bypass. NUL-joining keys prevents `(a, b/c)` aliasing `(a/b, c)` under any printable delimiter a path may contain.

**Status:** active

**References:** internal/claim/claim.go (NormalizePath, Key), internal/sidecar/verbs_p1.go (repoRelative), #12, docs/reports/2026-06-05-p1-build-report.md

---

## 2026-06-05: Blackboard stream persistence = append-only JSONL per stream

**Decision:** Durable bus streams persist as append-only JSONL at `$MESH_DIR/streams/<name>.jsonl`, one `json.Marshal(StreamEntry)` (seq/ts/data) per line. Loaded on bus-server `Start`; in-memory keeps the last `MaxStreamLen` entries but seq numbering continues from the full on-disk history. Disk is bounded by compaction — rewrite the retained window to a same-dir temp file and commit with an atomic `os.Rename` once a file exceeds `2x MaxStreamLen` lines. Load is corruption-tolerant: a torn final line is truncated, a corrupt mid-file line skipped (degrade-don't-throw); only an unreadable dir/file fails `Start`. No per-append `fsync` (process-crash-safe via the page cache; OS-crash tail loss is documented out of scope for a local-first bus). Gated by `bus.Options.StreamDir` — empty means pure in-memory (P0 behavior unchanged); the coordinator enables it with `cfg.StreamsDir()`.

**Rationale:** Resolves the one genuinely new design fork in P1 (#15): how `mesh note`/`context` survive a coordinator restart. JSONL is the leanest durable shape that keeps the stdlib-only constraint, replays trivially, and bounds disk without a compaction daemon. KV (the claims/registry authority) deliberately stays in-memory — only the blackboard is durable.

**Status:** active

**References:** internal/bus/persist.go, internal/coordinator/coordinator.go, #15, docs/reports/2026-06-05-p1-build-report.md

---

## 2026-06-05: Ops plane gains scoped actuator verbs (doctor / down / clean)

**Decision:** Extend `mesh ops` beyond the read-only snapshot with three actuator verbs: **`mesh ops doctor`** (classify runtime state — healthy / orphan / stale-socket / dead-pidfile; nonzero exit when dirty), **`mesh ops down [--mesh DIR | --all]`** (graceful fleet teardown: SIGTERM the pidfile∪registry union, SIGKILL escalation after a timeout), **`mesh ops clean`** (remove stale sockets/pidfiles under MESH_DIR — mesh-owned paths only, never roams $TMPDIR). Kills are scoped by **ownership** (pidfiles + argv-matched MESH_DIR), never by process-name match. The e2e harness dogfoods `ops down` + `ops --json` for its zero-leak assertion, guarded by one raw-`ps` ground-truth test. Scope stops at inspect + teardown + janitor — no watch mode, no restart/respawn; anything that *starts* processes belongs to the coordinator (P3 scheduler).

**Rationale:** Dogfood proof from a live session: an agent diagnosing leaked e2e sidecars (#33, #34) had to fall back to raw `ps`/`pkill`/`rm` — the exact unscoped, untyped surface the product thesis ("CLI at the edge is the agent API") exists to replace. Ownership-scoped kills are required because multiple meshes coexist per machine (every e2e run is one); a name-based `pkill` nukes them all. A nonzero `doctor` exit extends the existing exit-code taxonomy so agents and CI gate cheaply.

**Status:** active

**References:** internal/observe, #33, #34; extends 2026-06-05 "Runtime observability = separate ops plane"

---

## 2026-06-05: Runtime observability = separate ops plane (`mesh ops` + `meshd --mode observe`)

**Decision:** Add a dedicated runtime observability layer in `internal/observe`, separate from the product dashboard. Primary surface is **`mesh ops [--json]`** (no join required); secondary surface is **`meshd --mode observe`** on `127.0.0.1:8739` (`GET /api/snapshot` + minimal HTML). The collector compares filesystem facts (coordinator.pid, bus.sock, agent sockets, logs) against the registry KV and sidecar `runtime` IPC. Child agent CLI PIDs are reported by the sidecar (`TrackChild`/`MarkChildExited`), not OS process-tree scraping.

**Rationale:** The dashboard answers "what is happening on the mesh?" (bus events + roster). Ops needs "what processes are actually running and do they match registry state?" — a different concern, different port, different consumer. CLI-first keeps scripts cheap; HTTP reuses the same snapshot for polling. Sidecar-owned child tracking preserves stdlib-only, cross-platform observe code and wires cleanly into future runtime-proxy spawns.

**Status:** active

**References:** internal/observe, internal/meshapi (VerbRuntime), cmd/meshd (--mode observe), internal/cli (ops)

---

## 2026-06-05: P0 transport = Go + coordinator-embedded star bus over a unix socket

**Decision:** Resolve the reopened transport/language fork for P0: the language stays **Go**; the transport is a **coordinator-embedded local bus/store (star topology)** over a permissioned unix socket, implemented in `internal/bus` — pub/sub with NATS-style subject matching (`*`, `>`), KV buckets with revision-CAS and per-key TTL leases, and bounded in-memory streams. No embedded NATS server and no external broker for P0. The JetStream-contingent specifics of earlier active decisions (CAS as the single claim primitive, TTL leases with reclaim-on-death, "the durable record" as the one authority) map onto bus KV/stream equivalents unchanged.

**Rationale:** Issue #5 required settling the fork before coding the spine, and the P0 build directive specified Go plus a "local bus/store spine sufficient for P0". A star bus over one unix socket avoids the distributed tax (broker process, credentials, ops) while preserving the transport-independent invariants — one versioned envelope, one authority per fact, typed result enums, async-never-block — so a later swap to NATS/JetStream (if lateral peer comms or multi-host arrive) is mechanical: the `bus.Client` API is the seam.

**Status:** active

**References:** internal/bus, docs/repo-layout.md, #5, #8; supersedes 2026-06-05 "Transport reopened (bus vs star) and language under review"

---

## 2026-06-05: Persistent experts = a resident stream-json claude process; --resume is recovery-only

**Decision:** A warm/persistent expert is a long-lived `claude -p --input-format stream-json --output-format stream-json --verbose` process owned by the expert's sidecar, which holds the child's stdin pipe open, writes one user-message per routed ask, and reads typed JSON from stdout. "Warm" = conversation context held in the running process's RAM. `--resume <session-id>` (reload the session jsonl from `~/.claude/projects/`) is the crash-recovery / cold-start path only — not steady state; a respawned expert rehydrates via `--resume` + `mesh context`, with the blackboard as the durable backstop. Ephemeral workers stay one-shot `claude -p`. Prefer structured stream-json over PTY-driven interactive sessions; PTY/tmux is a fallback only for CLIs lacking a structured streaming mode. Resolves fork A (warm-expert mechanism: resident stream-json vs Agent SDK vs resume).

**Rationale:** `claude -p` is one-shot and `--resume` reloads from disk and pays a prompt-cache miss after the ~5min TTL, so resume is not true warmth. A resident streaming process keeps context in RAM for lowest per-ask latency and real continuity — proven in the M0 spike: one resident process answered a second-turn recall correctly (`num_turns=2`, single `session_id`). Cost is supervising a child (restart, memory, compaction), acceptable because experts amortize across many tickets. Structured stdout preserves the never-scrape-prose / one-versioned-envelope invariant.

**Status:** active

**References:** docs/spikes/M0-feasibility.md, docs/mockups/agent-startup.html, ARCHITECTURE.md §11, #27

---

## 2026-06-05: Pivot to an autonomous, hands-off product (Mode B)

**Decision:** Agent Mesh is a service + UI where the user drops a ticket/issue and the system **autonomously** triages it, spawns a team of agents, executes, and reports — the user watches and drives nothing. This **supersedes** the original "opt-in per message, agents work solo 95%" framing ("Mode A": a human driving long-lived agents that the mesh merely assists). `ARCHITECTURE.md` §1 vision/principles and the build-phase plan are to be re-derived for the autonomous model.

**Rationale:** The user's goal is hands-off automation from a ticket, not assistive coordination of human-driven sessions. The two imply different topologies, agent lifecycles, and infra; conflating them muddied the design.

**Status:** active

**References:** prompts/PROMPT_CHAT.md (Current frontier), docs/mockups/topology-hybrid.html, docs/mockups/flow.html

---

## 2026-06-05: Hybrid agent model — persistent experts + ephemeral workers; blackboard = expert memory

**Decision:** Two agent lifetimes. **Persistent experts** (per domain/codebase) stay warm — they hold the codebase map + decisions, answer questions, and **review** worker output, living across many tickets. **Ephemeral workers** are pipeline-spawned per subtask and exit when done. The **blackboard** (durable store) is the experts' long-term memory, so an expert can restart from it without losing knowledge (with compaction + re-sync on file changes). Workers ask experts **by role** through a router.

**Rationale:** A warm expert pays the expensive codebase-context load once, making every additional question/review cheap and high-value (it catches cross-cutting issues isolated workers can't see). Combines Mode B's repeatable worker pipeline with Mode A's long-lived advisor. Dynamic worker churn + ask-by-role is exactly the loosely-coupled shape a message/router layer is for.

**Status:** active

**References:** docs/mockups/topology-hybrid.html

---

## 2026-06-05: All cognition is a CLI invocation on the user's subscription; experts are headless CLI sessions

**Decision:** The mesh **never calls an LLM API**. Every "brain" — workers, experts, and the triage/planner — is an agent-CLI invocation (`claude -p`, `codex exec`, …) that reuses the user's existing on-disk **subscription** login. No API key in the core (an API key is an off-machine fallback only). An expert and a worker differ only by **model + prompt + injected context**, not by auth. Adapters drive each CLI in **headless + structured-output** mode (e.g. `claude -p --output-format json`) so answers are captured as typed results, never scraped from prose.

**Rationale:** Keeps the product subscription-native with zero key management, and makes heterogeneous CLI↔CLI message-passing tractable (the envelope stays structured; free text is opaque `content`). **Open risk:** rate-limits / ToS for a spawned headless fleet on a consumer subscription — to be verified before relying on it.

**Status:** active

**References:** prompts/PROMPT_CHAT.md, docs/mockups/flow.html

---

## 2026-06-05: Transport reopened (bus vs star) and language under review *(resolved)*

**Decision:** The autonomous model is a coordinator-spawned **tree**, not a flat peer mesh, so a full NATS broker is no longer assumed. Choose between (a) a lighter **coordinator + role-router + durable store** (star) and (b) a **NATS/JetStream bus** — deferring the bus until lateral peer comms or multi-host genuinely require it. This **supersedes** "Build meshd/mesh in Go with an embedded NATS server" (the embedded-NATS commitment is dropped pending this choice), and **reopens the language** (Go vs TS — a star + store + websocket is equally comfortable in TS). JetStream-dependent specifics in other still-active entries (CAS-lock, TTL-lease, "the JetStream record" as authority, the Go repo layout) are **contingent** on this choice.

**Rationale:** "Don't pay the distributed tax until distributed." A coordinator that spawns and knows every agent already has discovery/presence; the bus's headline strengths (flat discovery, dynamic peer membership) mostly don't apply until workers/experts talk laterally at scale. Keep the transport-independent invariants (one versioned envelope, one authority per fact, async ask→ticket, durable blackboard) regardless.

**Status:** superseded by 2026-06-05 (P0 transport: Go + coordinator-embedded star bus)

**References:** docs/mockups/topology-hybrid.html, docs/components.md

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

**Status:** superseded by 2026-06-05 (Transport reopened + language under review)

**References:** ARCHITECTURE.md §11, docs/concepts.md, docs/audit-multi-agent-pm.md

---

## 2026-06-05: Reorder build phases — cheap coordination core before async ask/answer

**Decision:** Build the no-LLM-in-the-loop primitives first — presence (join/who/status/heartbeat), `announce`+claims (conflict avoidance), and `note`/`context` (blackboard) — and only then build the async ask/answer ticket machinery. Supersedes the original ARCHITECTURE.md §12 ordering (P1 ask/answer before P2 collaboration primitives). Also: start homogeneous (Claude Code only) to solve hook parity once before generalizing to Codex/Cursor/Aider.

**Rationale:** announce + blackboard are pure pub/sub — guaranteed value, zero LLM-turn cost, low risk. ask/answer is the expensive, hallucination-compounding, hook-dependent part. The original plan front-loaded the risky LLM-coordination before the cheap guaranteed wins. Prove the thesis with the cheap core; defer the research bet.

**Status:** superseded by 2026-06-05 (Pivot to an autonomous, hands-off product)

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
