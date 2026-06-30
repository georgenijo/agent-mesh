# Audit brief: should agent-mesh integrate with / harvest from / ignore hangar?

**Prepared:** 2026-06-05, for an independent auditing agent (second pair of eyes).
**Ask:** read this brief, inspect both repos, and recommend an integration strategy
(§6) with rationale. Challenge our assumptions; we want disconfirmation, not
ratification.

- Repo A (this repo): `agent-mesh` — `/Users/george-mac-mini/Documents/code/agent-mesh` (GitHub `georgenijo/agent-mesh`)
- Repo B: `hangar` — `/Users/george-mac-mini/Documents/code/hangar` (GitHub `georgenijo/hangar`)

Both repos are owned by the same person (George). There is no licensing or
permission issue — the question is purely engineering strategy.

---

## 1. The problem we were solving when we found hangar

**agent-mesh** is a pre-implementation project: an autonomous coordination
fabric where the user drops a ticket, a **coordinator** triages it into a task
DAG (via a headless `claude -p` planner call), spawns a team — **ephemeral
workers** (one subtask, then exit, each in an isolated git worktree) and
**persistent warm experts** (long-lived, per-domain; answer workers' questions
by role and review their output) — and the user just watches. Key invariants:
all cognition is a CLI invocation on the user's existing subscription (no API
key in the core); one versioned envelope; typed results, never scraped prose;
async ask→ticket; a durable blackboard as shared memory.

State of agent-mesh today:
- Design docs + 31 GitHub issues across 6 milestones (M0 feasibility spike →
  P0 presence skeleton → P1 conflict avoidance/blackboard → P2 async ask/answer
  → P3 autonomous coordinator+experts+workers → P4 live dashboard).
- **No production code yet** (Go module + entrypoint stubs only).
- Locked decisions (see `docs/decisions/DECISIONS.md`, all 2026-06-05): **Go**
  with an **embedded NATS/JetStream** server (single binary); JetStream KV
  revision-CAS as the single claim/lock primitive; TTL leases +
  reclaim-on-death; one versioned envelope; cheap-core-first phase order.
- M0 spike **completed** (see `docs/spikes/M0-feasibility.md`): a subscription
  headless fleet is viable — 30 concurrent `claude -p` calls, zero
  rate-limiting, real parallelism; binding constraint is local CPU, not the
  API. Claude adapter contract verified (`-p --output-format json` envelope;
  `--bare` is forbidden because it disables subscription/OAuth auth).

The immediate design question that led us to hangar: **how does a "persistent
warm expert" actually stay alive?** `claude -p` is one-shot (proc exits after
the result). We empirically proved a resident alternative: one `claude -p
--input-format stream-json --output-format stream-json --verbose` process
stays open on a held stdin pipe, accepts multiple user messages over time, and
retains in-RAM context across turns (verified: second-turn recall succeeded,
`num_turns=2`, one `session_id`). We logged this as a decision ("Persistent
experts = a resident stream-json claude process; `--resume` is recovery-only",
DECISIONS.md top entry). Then George remembered he had already solved
"keep agent sessions open and write into them" in a prior project: hangar.

## 2. What hangar is

A **self-hosted control plane for AI coding agents** (Claude Code, Codex,
shell) — "tmux + kubectl + OBS for someone who lives inside multiple Claude
sessions." v0.2.0; Phases 1–6 shipped; currently migrating from an Optiplex
host to a containerized deploy on the Mac mini (ADR-0017).

- Stack: **Rust** (axum, tokio, sqlx/SQLite, portable-pty, nix), SvelteKit +
  xterm.js dashboard, Caddy, ntfy push, systemd / Docker.
- Scale: backend ≈ **9.4k LoC Rust** (46 files), frontend ≈ **7.1k LoC**.
- Built **by a multi-agent pipeline dogfooding itself** (`.pipeline/`:
  context-gatherer → architect → senior-reviewer → builder → tester → fixer,
  each agent in its own git worktree + tmux session, filing issues and
  shipping PRs).
- Health caveats: vault project note is ~7 weeks stale vs the repo (repo has
  May 13 work the note lacks). Known live bugs at last vault-out: #57 SQLite
  DB malformed on the box (all DELETE → 500), #58 FTS query 500 on
  hyphen/colon, #59 parallel-DELETE race, plus untriaged: spawn `cwd` ignored
  (falls back to backend cwd), label filter returns all, broadcast types into
  shell sessions unconditionally.

## 3. What we found in hangar, and exactly where

### 3a. The session-keeping mechanism (the thing we went looking for)

| capability | where | notes |
|---|---|---|
| Own-the-PTY session spawn | `backend/src/pty.rs` (portable-pty `openpty`, `CommandBuilder`; child reaping strategy documented at top of file) | ADR-0003 chose Rust+portable-pty over Go+creack/pty, Node+node-pty, Python |
| **Sessions survive backend restart** | `backend/src/bin/hangar-supervisor.rs`, `backend/src/supervisor_client.rs`, `backend/src/supervisor_protocol.rs`, `backend/src/raw_fd_master.rs`; spike at `backend/src/bin/fd_passing_spike.rs` | ADR-0010: a tiny always-on supervisor holds PTY master fds + child pids; backend reconnects over `~/.local/state/hangar/supervisor.sock` and receives fds back via **`SCM_RIGHTS`**; children re-parented (subreaper/double-fork) so they survive supervisor restarts too. **Battle-tested 2026-04-18**: hangard restarted under two live agent sessions, both reattached (`backend/tests/supervisor_restart.rs`) |
| Drive a live session programmatically | `backend/src/api/prompt.rs` (`POST /api/v1/sessions/:id/prompt`), `backend/src/api/key.rs` (`/key`), `backend/src/api/broadcast.rs` | This is "write into a pseudo-spawned terminal session" as a REST endpoint |
| Session model / lifecycle | `backend/src/session.rs`, `backend/src/api/sessions.rs` | create/list/get/delete; ULID session ids |
| Scrollback persistence | `backend/src/ringbuf.rs` + SQLite (`backend/src/db.rs`) | ADR-0004/0011/0012: ring file + events, 100MB history cap |

### 3b. Typed agent events (how they avoid raw byte soup)

| capability | where | notes |
|---|---|---|
| Claude Code hooks → typed events over a socket | `backend/src/cc_hook_socket.rs`; env injection in `backend/src/drivers/claude_code.rs` (`HANGAR_SESSION_ID`, `HANGAR_HMAC_KEY`) | CC hooks emit `AgentEvent`s to a local socket, HMAC-authenticated |
| TUI stream parsing (fallback/augment) | `backend/src/drivers/claude_code.rs` — `strip_ansi` + a `ParserState` machine emitting `ModelChanged`, `ContextWindowSizeChanged`, `AwaitingPermission`, `ThinkingBlock`, `ToolCallStarted`, … | **Important nuance: the driver DOES scrape ANSI** for TUI state (model, context %, permission prompts), with hooks as the typed channel (`hooks_active` flag). Hung-session detection forces Idle (line ~532) |
| Per-CLI driver abstraction | `backend/src/drivers/mod.rs` (`AgentDriver`, `SpawnCfg`, `StateCtx`), `drivers/claude_code.rs`, `drivers/codex.rs`, `drivers/shell.rs`, `drivers/raw_bytes.rs` | The "adapter per CLI" pattern agent-mesh planned for P3 (#30), already shipped, with driver tests (`backend/tests/claude_code_driver_test.rs`, `codex_driver_test.rs`) |
| Event model | `backend/src/events.rs` (`AgentEvent`, `Event`, `EventBus`) | hangar's equivalent of agent-mesh's envelope (process-local, not a wire format) |

### 3c. Orchestration / pipeline (agent-mesh's P3 territory)

| capability | where | notes |
|---|---|---|
| Multi-agent role pipeline | `.pipeline/pipeline.sh`, `parallel.sh`, `wave.sh`, `batch.sh`; roles in `.pipeline/agents/{context-gatherer,architect,senior-reviewer,builder,tester,fixer}.md`; `lib/{logging,recovery}.sh`; isolation test `.pipeline/tests/test-pipeline-isolation.sh` | Shell-based; each agent gets its own **git worktree + tmux session** (see `docs/RUNBOOK.md` ~line 68); built hangar itself |
| Worktree support in the product | `backend/src/api/worktree.rs` (`/worktree/tree`, `/worktree/file`, `/worktree/diff`), `.worktrees/` dir | Worktree-per-agent is productized, not just pipeline glue |
| Pipeline API | `backend/src/api/pipeline.rs` | REST surface over pipeline runs |
| Sandbox | `backend/src/sandbox/{manager,types}.rs` | podman + nftables (per vault note); ADR-0006 deferred sandbox for MVP then added |
| Observability | `backend/src/api/{events,output,search,costs,metrics,host_metrics,logs}.rs`, `ws/` (WS bridges), frontend xterm.js dashboard | Live terminal view, cost/context scraper, FTS search over history |
| Push escalation | `backend/src/push.rs`, ntfy (ADR-0016) | "Claude is waiting for a yes/no" notifications |

### 3d. What hangar does NOT have (agent-mesh's net-new)

Filed as future issues in hangar but **never built**: #44 "conductor + push
escalation (Phase 9 inter-agent)" and #52 "cluster-mode / remote-node
dispatch". Concretely missing vs agent-mesh's design:
- No coordinator/triage (ticket → task DAG → dependency-gated scheduling).
- No inter-agent communication at all (no ask-by-role, no router, no tickets).
- No shared durable blackboard / decision memory across agents.
- No claim/lock primitive (CAS file claims) — collision avoidance is "one
  worktree per agent" only.
- No presence/registry semantics beyond the session list; no roles/caps model.
- Single-node by design (cluster-mode is an unstarted issue).

## 4. Overlap matrix (agent-mesh plan ↔ hangar reality)

| agent-mesh planned (issue) | hangar shipped equivalent | overlap |
|---|---|---|
| #26 worker runtime: `claude -p` in worktree, structured result | drivers + pty.rs + worktree API + pipeline roles | **high** (different invocation style: hangar = interactive-TUI-in-PTY; mesh = headless `-p`) |
| #27 persistent warm expert (resident stream-json) | supervisor-held PTY sessions + `--resume`-able CC sessions + prompt API | **high** — and hangar's restart-survival (fd-passing) is *stronger* than our respawn+`--resume` plan |
| #30 per-CLI adapters | `drivers/` (claude_code, codex, shell, raw_bytes) + tests | **high** |
| #25 scheduler / lifecycle | `.pipeline/*.sh` (waves, parallel, recovery) | **medium** — shell, no DAG, no dependency gating |
| #10/#31 dashboard | SvelteKit + xterm.js + WS, costs, search | **high** (different center: terminal mirror vs bus visualizer) |
| #5 bus (embedded NATS/JetStream) | none — EventBus is in-process; SQLite for durability | **none** |
| #12/#13 CAS claims + TTL leases | none | **none** |
| #15 blackboard note/context | none (SQLite session history ≠ shared decision memory) | **none** |
| #17–22 ask/answer tickets + role routing | none (hangar #44 unbuilt) | **none** |
| #24 triage → DAG | none | **none** |

Summary: **hangar owns the agent-session substrate; agent-mesh's distinctive
value is the coordination layer hangar never built.** The overlap is almost
exactly hangar's strengths = agent-mesh's P3 worker/expert mechanics, and
hangar's gaps = agent-mesh's P0–P2 (envelope/bus/claims/blackboard/tickets) +
P3 coordinator brain (triage/DAG/scheduling).

## 5. Tensions the auditor must weigh

1. **Language/runtime collision.** agent-mesh locked **Go + embedded NATS**
   (NATS server embeds only in Go). Hangar is **Rust**. Options conflict:
   harvesting hangar's code means Rust (no embedded NATS → separate
   nats-server process, or drop NATS); layering keeps both alive in two
   languages; rebuilding in Go duplicates ~thousands of LoC of proven PTY/
   supervisor/driver work (and ADR-0003 explicitly rejected Go's pty story).
2. **Interaction model mismatch.** Hangar drives the **interactive TUI in a
   PTY** and scrapes/parses it (plus hooks) — built to *mirror terminals to
   humans*. agent-mesh experts/workers are **headless** (`-p` / stream-json,
   typed JSON out, nothing to render). agent-mesh has a hard invariant:
   *typed results, never scraped prose*; hangar's ANSI `ParserState` is
   version-coupled to Claude Code's TUI and is the kind of fragility
   agent-mesh's audit doc (`docs/audit-multi-agent-pm.md`) warns against.
   Question: does the substrate's PTY orientation even fit headless agents,
   or would we use hangar only as supervisor/lifecycle and run headless
   children under it?
3. **Restart-survival vs simplicity.** Hangar's supervisor + `SCM_RIGHTS`
   fd-passing is proven and strictly stronger than agent-mesh's planned
   "respawn + `--resume` + blackboard rehydrate." But it's also the most
   intricate piece (fd passing, subreapers). Is restart-survival of *experts*
   worth that machinery when the blackboard already makes restarts cheap?
4. **Substrate health & coupling.** Hangar has live data-layer bugs (#57
   malformed SQLite on the box, #58, #59), an unfinished container migration
   (ADR-0017), a stale ops story (backend not systemd-managed), and bugs
   directly relevant to programmatic use (spawn `cwd` ignored!). Building on
   it inherits that surface. It is also deeply single-user/single-node shaped.
5. **Two personal projects, one person.** Maintaining both = split attention
   (this already happened once: hangar's own competitor eval vs agent-deck
   concluded "personal-tool motivation is the real reason to keep hangar").
   A Layer strategy makes agent-mesh hostage to hangar's bugs; a Rebuild
   strategy abandons working code; Harvest forks it.

## 6. The options to audit

- **A. Layer:** agent-mesh = coordination layer (coordinator, triage/DAG,
  blackboard, claims, ask-by-role, envelope) talking to hangar's REST/WS API
  to spawn/drive/observe sessions. Hangar = session substrate. Mesh stays Go
  or becomes anything; hangar stays Rust.
- **B. Harvest:** port/embed hangar's proven pieces (supervisor pattern,
  driver abstraction, possibly pty handling) into agent-mesh's codebase;
  hangar continues separately as the human dashboard.
- **C. Rebuild:** keep agent-mesh fully independent per its current plan
  (headless `claude -p` / stream-json children under Go sidecars; no PTY at
  all). Treat hangar as prior art + a pattern mine (like
  `docs/audit-multi-agent-pm.md` did for another sibling project).
- **D. Merge:** agent-mesh becomes hangar Phase 9 (#44/#52) — build the
  coordinator/blackboard *inside* hangar (Rust), retire the standalone
  agent-mesh repo.

Our current lean (to be challenged): **C for the core with B-style borrowing
of the supervisor *pattern* later**, because (i) headless agents don't need a
PTY or TUI parsing, (ii) the Go+embedded-NATS single-binary decision is load-
bearing for the local-first story, (iii) hangar's value to agent-mesh is
mostly *already-validated patterns* (worktree-per-agent, driver-per-CLI,
supervisor restart-survival, hook-socket events) rather than directly linkable
code. But we are genuinely unsure about D — building inside hangar avoids two
half-products.

## 7. Questions for the auditor

1. Given §3d/§4, is the session substrate (hangar's strength) actually the
   hard part of agent-mesh, or is the coordination layer? Where does the
   engineering risk concentrate?
2. Does a PTY-based substrate make sense for **headless** workers/experts, or
   is PTY only justified when a human watches the terminal? Specifically:
   should agent-mesh experts run as resident stream-json processes (our
   logged fork-A decision) or as supervisor-held PTY sessions driven via
   prompt-injection (hangar style)?
3. Is supervisor + `SCM_RIGHTS` fd-passing worth importing for expert
   restart-survival, vs respawn + `--resume` + blackboard rehydrate? Under
   what failure modes does the cheaper plan actually lose work?
4. Evaluate option D seriously: what would "agent-mesh as hangar Phase 9"
   cost vs the standalone plan, given hangar is Rust, single-node, SQLite,
   and has the §5.4 bug surface? Would the Go+embedded-NATS decision survive?
5. If C (rebuild): rank the hangar patterns worth stealing on day one vs
   later (driver trait shape, hook-socket + HMAC, hung-session detection,
   reap-on-delete, ring-buffer history, worktree API design).
6. If A (layer): audit hangar's REST API fitness as a programmatic substrate
   (spawn-cwd bug, auth model = tailnet-only ADR-0013, event granularity,
   backpressure) and the operational coupling risk.
7. Anything we're not asking that we should be?

## 8. Suggested reading order for the auditor

1. This brief.
2. agent-mesh: `CLAUDE.md` → `docs/decisions/DECISIONS.md` (newest-first) →
   `ARCHITECTURE.md` → `docs/spikes/M0-feasibility.md` → issues #23–#31
   (`gh issue list --repo georgenijo/agent-mesh --milestone "P3 — Autonomous coordinator + experts"`)
   → `docs/audit-multi-agent-pm.md` (how we audited the *previous* sibling
   project; same steal/avoid framing wanted here).
3. hangar: `README.md` → `docs/ARCHITECTURE.md` → ADRs `0003`, `0010`, `0017`
   → `backend/src/pty.rs` → `backend/src/bin/hangar-supervisor.rs` +
   `supervisor_protocol.rs` → `backend/src/drivers/{mod,claude_code,codex}.rs`
   → `backend/src/cc_hook_socket.rs` → `backend/src/api/mod.rs` (route map) →
   `.pipeline/README.md` + `pipeline.sh` → `docs/RUNBOOK.md`.
4. hangar's open issues #44, #52, #57–#59 for the unbuilt-conductor framing
   and current bug surface.

## 9. Appendix: hard data from the M0 spike (agent-mesh)

- Fleet: 10/10, 20/20, 30/30 parallel `claude -p --model haiku` succeeded;
  0 rate-limited; walls 6.0s / 10.0s / 12.3s (single ≈ 2.4s). Subscription
  (OAuth) auth, no API key. Binding constraint: local process spawn, not API.
- Resident-session proof: one `claude -p --input-format stream-json
  --output-format stream-json --verbose` process; two user messages over one
  held stdin; second turn recalled state planted in the first; one
  `session_id`, `num_turns: 2`.
- Adapter contract + `--bare` auth trap: `docs/spikes/M0-feasibility.md`.
