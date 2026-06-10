# Decisions Log

Running log of architectural, scope, and process decisions for this project. Newest entries at the top. Each entry is short — for deep rationale on a single locked decision, write an ADR alongside in `docs/decisions/YYYY-MM-DD-*.md` and reference it here.

Maintained via the `/decisions` skill. See `~/.claude/skills/decisions/SKILL.md` for the entry format and invocation rules.

---

## 2026-06-10: Worker dependency inheritance — a dependent task's worktree merges its done deps' output branches, so a DAG edge carries committed code, not just order

**Decision:** The #26 worker driver now makes a `dependsOn` edge carry **code**, not only execution order. On success a worker records its output branch on the task (`task.Record.Branch`, set by the scheduler via the new `task.Store.SetBranch`, threaded through `scheduler.Result.Branch`); when a later task is spawned, `worker.Driver.mergeDeps` merges each done dependency's recorded branch into the fresh worktree **before** the worker starts — and captures the post-merge SHA as `baseSHA`, so the task's own reported diff still excludes the inherited changes. Independent (no-dep) tasks still branch off base, preserving parallel isolation; the per-task branch is still the work product and is still never deleted. A merge conflict between sibling deps (both edited the same lines) is a **typed spawn failure** with an aborted merge — honest, never a silent half-merged tree — and the CAS file-claim remains the advisory signal meant to keep siblings off the same files in the first place. `task.Record.Branch` is an additive `omitempty` field (golden-pinned wire contracts and the frozen five-state `TaskState` are untouched).

**Rationale:** Found by **dogfooding the autonomous mesh on its own backlog**: with every worktree branched off base `main`, a builder and a tester decomposed from one ticket produced contradictory implementations because neither could see the other's diff, and the reviewer task got a worktree with neither the fix nor the test in it — nothing to review — so it spun and hit the worker timeout, failing the whole job under fail-fast. Branch-stacking via direct deps gives the full transitive closure (each dep branch already contains its own ancestors) while keeping the scheduler the sole post-creation writer of task records (no new authority, no CAS race in practice). Proven at three levels — unit (real git worktrees: a dependent inherits its dep's commit; conflicting siblings fail typed), cross-process e2e (the default `impl→review` chain's dependent worktree holds **both** markers, `[1 2]` not `[1 1]`), and full `make ci` green.

**Status:** active

**References:** #26, #25, PR #86; internal/worker/worker.go (mergeDeps), internal/task/task.go (Record.Branch, SetBranch), internal/scheduler/driver.go (Result.Branch), internal/scheduler/scheduler.go, internal/worker/worker_test.go, test/e2e/worker_test.go. Secondary findings deferred to follow-ups: a review-task worker timing out discards otherwise-good work, and fail-fast fails a job whose actual work (fix+test) committed cleanly — both argue for review-aware timeouts and not discarding committed task branches on a downstream failure.

---

## 2026-06-10: #64 triage retry/backoff — transient codes retry with durable exponential backoff under an attempt cap; permanent codes fail fast; supersedes one-attempt-per-lifetime

**Decision:** The #24 triage failure policy moves from "one attempt per coordinator lifetime, any typed failure → open→failed" to a retry/backoff policy that splits typed failures into **TRANSIENT** (may not recur — retry) and **PERMANENT** (a retry of the same prompt reproduces it — fail fast). Classification: `planner_unavailable` (CLI couldn't run: timeout/crash/missing binary) = transient; `planner_failed` is **split on api_error_status** — WITH a non-null `api_error_status` (rate-limit 429 / overload 529 / 5xx, the issue's "possibly planner_failed with api_error_status") it is transient, WITHOUT one (prose/non-result stdout, plain error subtype, is_error) it is deterministic → permanent; `internal` (store/bus/CAS hiccup) = transient-with-cap (documented call); `bad_plan` and `invalid_dag` (planner ran, produced garbage) = permanent. A PERMANENT failure transitions the job open→failed on the first attempt. A TRANSIENT failure keeps the job **open** (no new JobState — the frozen enum is untouched) and either schedules a backed-off retry or, once the attempt cap is reached, fails it open→failed with the last typed code. Backoff is bounded exponential: `MESH_TRIAGE_BACKOFF` base (default 30s) × 2^(attempts-1), capped at 30m; `MESH_TRIAGE_MAX_ATTEMPTS` (default 4) bounds total planner turns. Attempt state (count, last code, next-retry deadline) lives in a **new durable KV bucket `triage-attempts`** keyed by job id, persisted alongside jobs/tasks (#65) so a job mid-backoff resumes its schedule across a coordinator restart instead of restarting from attempt 0 — and a job in backoff is skipped each sweep until its deadline, so a down planner is never hammered every tick. The record is deleted once the job leaves open. This **supersedes the "one attempt per coordinator lifetime / retry-backoff is deferred policy" sub-decision** of the 2026-06-09 triage entry; the rest of that entry (opt-in via MESH_PLANNER_CLI, KV sweep, tasks-first commit, never-fake-success parsing, typed degrade-don't-crash) stays in force.

**Rationale:** The old policy permanently failed a job on a transient blip (planner binary briefly missing, a 429 under load) that a human then had to resubmit. Retrying only TRANSIENT failures recovers from those for free; failing PERMANENT ones fast honors the locked hard-cap billing posture — each attempt is one planner LLM turn = money, so retrying deterministic garbage (bad_plan/invalid_dag, or malformed stdout) burns credit for nothing, and the attempt cap stops a genuinely-down planner from being retried infinitely. Splitting `planner_failed` on `api_error_status` (a discriminator internal/runtime already exposes via `HasAPIError()`) is exactly the precision the issue asked for and keeps the existing garbage-output e2e failing fast. A **separate durable bucket** (not new fields on `job.Record`, which is golden-pinned) keeps the frozen contract intact while making backoff survive restart — mirroring #65's persisted-bucket precedent rather than the scheduler's in-memory `retryAt` (which resets on restart). Keeping the job in `open` with attempt metadata avoids reshaping the frozen JobState enum (no "retrying" state).

**Status:** active

**References:** #64, #24, #65; internal/triage/attempts.go (classification + durable attempt store + backoff), internal/triage/triage.go (onFailure/failTerminal/sweepOnce), internal/config (MESH_TRIAGE_MAX_ATTEMPTS / MESH_TRIAGE_BACKOFF), internal/envelope/subjects.go (BucketTriageAttempts), internal/coordinator/coordinator.go (PersistBuckets), test/e2e/fakeplanner (transient-then-ok mode), test/e2e/triage_test.go; supersedes the one-attempt sub-decision of 2026-06-09 "Triage = coordinator-embedded sweep loop"

---

## 2026-06-10: #29 policy — first slice is the unified audit log (coordinator fans mesh.> lifecycle events into one stream); the rest deferred via issue comments

**Decision:** Of the #29 policy bundle (rate-limit, dedup, audit log, semantic cache, durable budget metering), the first shipped slice is the **unified audit log**, because the issue names it "the substrate the others log into" and it lands entirely inside the one lane this round owns (internal/coordinator). The coordinator already subscribed to `mesh.>` and already wrote a presence/claim-only audit stream (`envelope.StreamAudit`); #29 promotes it: a new `auditObserved` fans the major lifecycle event of every domain — claim attempt, ask opened, ticket FSM transition, answer, job/task/triage/worker/fleet — into the same stream, so one ordered `StreamRead(StreamAudit)` reconstructs how any ticket/job/task reached its state (the issue's only mechanically-checkable acceptance). The audit record stays the coordinator's `AuditEntry` (its sole writer = one authority per fact), extended **additively**: the pre-#29 presence/claim fields are byte-identical (golden-pinned), the new correlation fields (Ticket/Job/Task/Role/By/State/Result/Detail) are all omitempty. The typed `AuditCategory` vocabulary lives in `internal/envelope` beside the other enums (golden-pinned in `TestContractStrings`). Knob `MESH_AUDIT_FANOUT` (on by default; off keeps the always-on presence/claim audits but suppresses the bus fan-out — for test determinism). Two dedup subtleties handled: the routed inbox copy of a role-ask is skipped (audited once on its origin subject), and presence/claim stay audited at their mutation site (the reduced outcome), not re-observed off the wire. Notes are deliberately NOT audited — they are stream-only, never published, so the coordinator cannot observe them; documented as a known gap.

**Rationale:** Dedup of duplicate job submissions (the explicitly-#29-deferred item) and an exact-match answer cache both require touching `job.Store.Create` and its call sites in internal/sidecar + internal/dashboard, which are outside this round's lane (dashboard is explicitly off-limits; one-authority says dedup belongs in the Store, not bolted on the coordinator). Rate-limiting asks/spawns spans the sidecar ask path and the scheduler core logic, also off-limits. The semantic cache needs embeddings (no external deps / no API key in core), so it is out regardless. The audit log is the only slice that is simultaneously highest-value (the named substrate), in-lane, additive, and contract-safe. Building it first means the deferred features have a typed stream to log into instead of inventing one each.

**Status:** active

**References:** #29, internal/coordinator/audit.go (auditObserved/auditEntryFor), internal/coordinator/coordinator.go (AuditEntry), internal/envelope/results.go (AuditCategory), internal/config (MESH_AUDIT_FANOUT/AuditFanout); deferred items (dedup, exact-match cache, rate-limit, durable budget metering) filed as #29 issue comments; extends 2026-06-08 "work-hierarchy: dedup is #29 policy" and the 2026-06-09 fleet-billing durable-metering deferral

---

## 2026-06-10: #30 per-CLI adapter abstraction = internal/cliexec.Adapter; ClaudeAdapter verified; Codex/Cursor/Aider stubbed with ErrNotImplemented

**Decision:** The one-shot headless exec contract (`<cli> -p --output-format json [--model M] <prompt>`) is abstracted behind `internal/cliexec.Adapter` (interface: `Invoke(ctx, prompt, InvokeOptions) ([]byte, error)` + `Capabilities() Capabilities`). `ClaudeAdapter` is the sole verified implementation (M0 spike, docs/spikes/M0-feasibility.md, 2026-06-05): it wraps the exact flags the spike confirmed and never scrapes prose. `CodexAdapter`, `CursorAdapter`, and `AiderAdapter` are typed stubs that return `ErrNotImplemented` with explicit verification notes — AiderAdapter is structurally blocked (its `--json` output is streaming status lines, not a single result envelope; a shim is required). The worker.Driver gains an `Adapter` field (nil = default ClaudeAdapter); `NewDriverWithAdapter` injects a custom adapter for tests or future non-Claude CLIs. Hook parity: Claude Code has full hooks (hooks/claude-code/); all other CLIs have stub READMEs in hooks/{codex,cursor,aider}/ documenting the gaps (no hook system → pre/post-exec wrappers are the only option). The `Capabilities` struct surfaces what each adapter supports so callers can degrade honestly rather than silently skipping features.

**Rationale:** Never-scrape-prose and never-fake-success are hard requirements (runbook); any adapter that can't satisfy the single-result-envelope contract must return ErrNotImplemented rather than silently producing unparseable output. Stubbing the unverified CLIs at the type level (rather than omitting them) makes the gap explicit and gives a future implementer a clear interface to fill in. Wiring the seam into worker.Driver (not triage or CLIDriver) is the narrowest additive change that addresses the issue's "worker adapter boundary" requirement without touching frozen scheduler/coordinator contracts.

**Status:** active

**References:** #30, internal/cliexec (adapter.go, adapter_test.go), internal/worker (NewDriverWithAdapter, effectiveAdapter), hooks/codex/README.md, hooks/cursor/README.md, hooks/aider/README.md, docs/spikes/M0-feasibility.md; Aider structural block noted in AiderAdapter doc comment

---

## 2026-06-10: #26 worker runtime = worktree-per-task driver with an embedded per-worker sidecar; repo names map via MESH_REPOS_DIR; branch is the work product; blocked-on-ask = `mesh ask --wait`

**Decision:** The production scheduler.Driver is `internal/worker.Driver`. **Spawn** resolves the job's repo NAME to a checkout at `MESH_REPOS_DIR/<name>` (required — a coordinator with `MESH_WORKER_CLI` set refuses to start without the mapping; a worker must never guess which tree it may rewrite), creates one fresh git worktree per dispatch under `$MESH_DIR/workers/<task-id>` on branch `mesh/worker/<task-id>` (numbered suffixes keep preserved earlier attempts intact), and joins an **embedded in-process sidecar per worker** (name `w-<id12>`, the task's role, CWD = the worktree). **Run** drives the one-shot `<MESH_WORKER_CLI> -p --output-format json --model <M>` child in the worktree with `MESH_DIR`/`MESH_SOCKET` pointing at that sidecar — so `mesh claim/context/note/ask` work from inside the run, claims canonicalize against the worktree CWD, and task-local blocking is the EXISTING `mesh ask --wait` (no new enum, no scheduler change; a wait failure is a typed error result). Results parse with internal/runtime's never-fake-success discriminators; on success the diff is auto-committed onto the task branch and base/head SHAs + changed files travel in Result.Summary — a success whose diff cannot be committed/described is a typed worker_failed, never a metadata-less ok. **Teardown** leaves the mesh then applies `MESH_KEEP_WORKTREES`: `on-failure` (default) removes the worktree only after a typed success — **the branch is the work product and is never deleted** — and preserves it otherwise for inspection; `always`/`never` override. The sidecar dependency is a `worker.Session`/`JoinFunc` seam wired in cmd/meshd (and injected as `Coordinator.WorkerJoin`): the worker and coordinator packages cannot import internal/sidecar because sidecar's own tests import the coordinator — the same composition-site pattern as the expert loop's ExpertFunc.

**Rationale:** Worktree-per-worker was locked 2026-06-08; the open questions were repo resolution, mesh access, and retention. An embedded sidecar reuses every proven mechanism for free (heartbeat leases, claim canonicalization, ask routing, blackboard primer via BuildMemoryPrimer) instead of inventing a worker-special path, and `ask --wait` already existed, keeping the frozen Driver/Result/WorkerErrorCode contracts untouched. Committing-on-success makes worktree removal lossless (the ref preserves the diff in the shared repo), which is what makes a remove-by-default policy safe; preserving failures gives the operator the crime scene. A name→path mapping under one operator-set root keeps repo identifiers as bus-safe tokens (subjects/claim keys) while never letting job payloads address arbitrary filesystem paths. CI keeps costing $0: e2e points `MESH_WORKER_CLI` at test/e2e/fakeworker, which exercises the REAL worktree driver and real `mesh` child calls without an LLM.

**Status:** active

**References:** #26, internal/worker, internal/coordinator (WorkerJoin), cmd/meshd (workerJoin/workerSession), internal/config (MESH_REPOS_DIR / MESH_KEEP_WORKTREES / WorkersDir), test/e2e/worker_test.go, test/e2e/fakeworker; extends 2026-06-08 "P3 execution plan — worker isolation = worktree-per-worker" and 2026-06-09 "#25 scheduler Driver seam"

---

## 2026-06-09: #25 scheduler = coordinator-embedded DAG sweep; Driver seam with exactly-once teardown; computed gating states; budget pause = queued-never-failed

**Decision:** The #25 scheduler mirrors the triage loop: a coordinator-embedded sweep (cadence HeartbeatInterval/2), **opt-in via `MESH_WORKER_CLI`** (unset = no scheduling — a bare `mesh join` coordinator never spawns workers), stateless over the durable jobs/tasks buckets so a restart resumes mid-job and an orphaned `running` task is simply re-dispatched. The #25↔#26 seam is `scheduler.Driver` — `Spawn(ctx, task) → Worker{Run(ctx) (Result, error); Teardown() error}` — with **Teardown guaranteed exactly once per spawned worker** (one deferred call in the worker goroutine, panic-safe). #25 ships a fake driver for tests plus the provisional one-shot `CLIDriver` (the same M0 exec contract as the triage planner, driven by `test/e2e/fakeworker` in CI), which #26 replaces with worktree-isolated workers behind the same interface. The issue's richer node states are **computed gating** (`scheduler.Gate`: queued/runnable/running/done/failed/blocked/skipped) derived from a node's deps plus the frozen five-state `TaskState` — `cancelled` is the typed terminal state for skipped dependents; nothing new is persisted; fail-fast cancels every not-yet-running sibling of a failed task. Budget enforcement implements the locked fleet posture: per-result `total_cost_usd` accumulates in-memory per coordinator lifetime against `MESH_BUDGET_USD` (0 = unlimited); hitting the cap or any `billing_error` **pauses the fleet** (`KindFleet` event; jobs/tasks stay queued/pending, never failed; restart after credit refresh is the reset); `rate_limited` backs off and retries; every other code fails the task. Additive golden-pinned wire contract: `KindWorker` (`mesh.worker.<task>`, typed `WorkerResult`/`WorkerErrorCode` + costUSD) and `KindFleet` (`mesh.fleet`, `FleetState`/`FleetPauseCode` + spent/budget).

**Rationale:** The triage-loop shape (opt-in env, KV sweep, typed degrade-don't-crash failures) is proven and keeps LLM-process spawning behind an explicit operator decision. A Driver interface keeps #26's hard part (isolation, diffs) out of the scheduling logic and satisfies the acceptance "runs against a fake worker driver before #26"; structural single-site teardown is cheaper and stronger than reference counting. Computing the rich states instead of persisting them preserves the frozen TaskState contract (runbook: never reshape a contract in a feature branch) while still giving the dashboard/audit stream the full picture. In-memory budget metering avoids inventing a new persisted authority for spend before the real cost shape lands with #26; deferred there too is detecting `billing_error` in the real CLI's output (the enum and pause path are tested via the fake driver). Fail-fast on a doomed job conserves the budget — the locked scarce resource.

**Status:** active

**References:** #25, #26, internal/scheduler (Driver, Gate, CLIDriver), internal/task (Transition), internal/coordinator, internal/config (MESH_WORKER_CLI / MESH_WORKER_MODEL / MESH_WORKER_TIMEOUT / MESH_BUDGET_USD / MESH_MAX_WORKERS), internal/envelope/scheduler.go, test/e2e/fakeworker; extends 2026-06-09 "Fleet billing posture = hard cap, no overflow" and 2026-06-09 "Triage = coordinator-embedded sweep loop"

---

## 2026-06-09: Fleet billing posture = hard cap, no overflow; budget is the scheduler's first-class scarce resource

**Decision:** Resolving the fleet-feasibility spike gate (#68, verdict FEASIBLE-WITH-LIMITS): keep subscription **usage credits DISABLED** (no pay-as-you-go overflow) and treat the monthly Agent-SDK credit as the first-class scarce scheduling resource. Per Anthropic Help Center article 15036540 (verified 2026-06-09), from **2026-06-15** `claude -p` / Agent SDK usage moves off the subscription's usage limits onto a separate monthly credit (Pro $20 / Max 5x $100 / Max 20x $200), metered at standard API rates, that **hard-stops when exhausted** unless usage credits are enabled. So the #25 scheduler must: accumulate per-task `total_cost_usd` (present in every `--output-format json` envelope), enforce a configurable budget cap (e.g. `MESH_BUDGET_USD`), and on hitting the cap or a `billing_error` **pause the fleet** (jobs stay queued, never failed) until credit refresh; back off only on `rate_limit`/`overloaded`; always pin `--model`; never pass `--bare` (breaks subscription OAuth and is slated to become the `-p` default). Safe parallelism is 4–8 workers (host-bound, not API-bound).

**Rationale:** Empirical concurrency was clean (16/16 at up to 8-way today; M0 saw 30-way), so concurrency is not the constraint — monthly dollars are. A hard cap with queue-don't-fail semantics is the cheapest, surprise-bill-proof posture and keeps the "no API key in core" lock intact economically (revisiting that lock was raised by the spike but deferred — post-June-15 a key buys little but reshapes the architecture). Building/testing #25/#26 costs $0 regardless: CI drives the fake planner/worker binaries (`fakeplanner`/`fakeclaude`), so real spend begins only when the operator points the runtime at the real `claude`.

**Status:** active

**References:** #68, #25, #26, docs/spikes/fleet-feasibility.md; extends 2026-06-05 "All cognition is a CLI invocation on the user's subscription" and 2026-06-08 "P3 execution plan — cheap-end-first, fleet-gated"

---

## 2026-06-09: Jobs/tasks KV durability = a persisted-bucket mode in the bus (op-log JSONL), not event-stream replay

**Decision:** Resolve #65 (jobs/tasks must survive a coordinator restart) with a **persisted-bucket mode in the bus** gated like `Options.StreamDir`: a new `Options.PersistDir` + `Options.PersistBuckets []string` mirror each named bucket to an append-only op-log `<PersistDir>/bucket-<name>.jsonl` (one `{op,key,value,rev,expiresAt}` line per put/delete), replayed on `Server.Start` *before the socket binds*. The in-memory bucket stays the one authority; the op log is its durable mirror. The coordinator enables it for exactly `BucketJobs` + `BucketTasks` (`cfg.BucketsDir()` = `$MESH_DIR/buckets/`). The bucket revision counter resumes past the highest replayed rev (no post-restart CAS collision); a lease that expired while down is dropped on load (no-op for the untimed job/task records, honest for any future leased persisted bucket); corruption degrades exactly like streams (torn-tail truncate, mid-file skip, atomic-rename compaction bounded at `max(2×liveKeys, 64)` lines). Registry and claims are deliberately NOT persisted — explicit non-goal. The triage one-attempt set resets on restart, so a still-open durable job is re-swept on the next lifetime: the self-healing sweep working as intended (tested).

**Rationale:** Of the two options the issue named, **event-stream replay (a) violates one-authority-per-fact**: the `job.Event`/`task.Event` streams are derived observability that carry only a subset of the record (no `Repo`/`Title`/`Body`/`Role`/`DependsOn`/`Files`), so rebuilding the authoritative record from them would require fattening golden-pinned event shapes and reconstructing fields the events never stored. The persisted-bucket mode (b) reuses the proven `persist.go` blackboard precedent verbatim, keeps the KV record the sole authority, and touches no wire/golden contract. The Start-ordering guarantee falls out for free: the bus loads buckets before `listenUnix`, and the triage loop / #25 scheduler only start after `c.srv.Start()` returns and the client dials in — buckets are whole before any sweep reads them.

**Status:** active

**References:** #65, #23, #24, #25; internal/bus/persistkv.go, internal/bus/server.go (Options.PersistDir/PersistBuckets), internal/config (BucketsDir), internal/coordinator/coordinator.go (Start), internal/bus/persistkv_test.go, internal/coordinator/durability_test.go; extends 2026-06-05 "Blackboard stream persistence = append-only JSONL per stream"

---

## 2026-06-09: Triage = coordinator-embedded sweep loop, opt-in via MESH_PLANNER_CLI; one attempt per job; tasks-first commit

**Decision:** #24 triage runs inside the coordinator as a sweep loop (cadence HeartbeatInterval/2, same as the presence janitor), gated on `MESH_PLANNER_CLI` being set — **no default planner binary**: unset means triage is off. The planner is one one-shot `<cli> -p --output-format json` child per job (model pinned via `MESH_PLANNER_MODEL`, default `sonnet`; wall-clock bound `MESH_TRIAGE_TIMEOUT`, default 2m, with a WaitDelay so a grandchild holding the stdout pipe cannot wedge the loop); stdout is parsed with internal/runtime's never-fake-success result discriminators. Plan-document shape + DAG validation (cycles via Kahn, duplicate/missing node ids, unknown deps, role exact-token match) live in `internal/task` beside the golden-pinned Task record (`BucketTasks`, deps stored as resolved task ids); prompt/model/invocation/orchestration live in `internal/triage`. Each job is attempted **once per coordinator lifetime**; any typed failure (`planner_unavailable | planner_failed | bad_plan | invalid_dag | internal`) transitions the job open→failed — retry/backoff is deferred policy. Commit order is tasks-first, then CAS job open→triaged, so a job that never reaches triaged can never expose a partial DAG to the #25 scheduler (which only reads tasks of triaged jobs).

**Rationale:** Every `mesh join` autostarts a coordinator, so a loop that spawns LLM processes must be explicitly enabled (CI/e2e point it at a fake binary; production opts in with `claude`) — defaulting it on would make `mesh submit` silently burn subscription turns on any dev machine. A KV sweep self-heals missed events and keeps the planner's seconds-to-minutes turn off the reducer's single delivery goroutine. One-attempt-per-lifetime keeps a failing planner from being hammered every tick while keeping the board truthful (failed, with a typed code, not silently stuck open).

**Status:** active — EXCEPT the "one attempt per coordinator lifetime / retry-backoff is deferred policy" sub-decision, **superseded by 2026-06-10 "#64 triage retry/backoff"** (transient codes now retry with durable exponential backoff under an attempt cap; permanent codes fail fast). The rest of this entry (opt-in gating, KV sweep, tasks-first commit, never-fake-success parsing) remains in force.

**References:** #24, internal/triage, internal/task, internal/job (Transition), internal/coordinator, internal/config (MESH_PLANNER_CLI/MESH_PLANNER_MODEL/MESH_TRIAGE_TIMEOUT), test/e2e/fakeplanner; extends 2026-06-08 "Autonomous work hierarchy = Job → Task → (ask)Ticket"

---

## 2026-06-09: Expert memory = a compacted blackboard primer injected into the warm child; no separate checkpoint store

**Decision:** An expert's long-term memory is the durable per-repo blackboard, not a new artifact (#28). On (re)start the responder loop builds a **memory primer** — a byte-bounded, compacted projection of `mesh.note.<repo>` (decisions/summaries kept ahead of context/other when the budget bites, elision disclosed honestly) — and injects it into the warm runtime child as one context-setting turn before answering. It re-primes on two signals: a new note landing on the blackboard (high-water seq advance — the in-mesh "worker recorded a decision after landing a diff" signal) and after a `--resume` restart (whose on-disk session may be cold/stale vs the durable record), the latter via a concurrency-safe `ResyncSignal` the runtime closure raises. The blackboard is never mutated by priming. Authority split is now documented (ARCHITECTURE.md §7e): the note stream is authoritative for durable facts, the child's RAM is a volatile non-authoritative cache. `internal/sidecar/memory.go` + `ServeExpertWithMemory`; the pre-#28 `ServeExpert` stays as the no-memory loop.

**Rationale:** The hybrid-model decision already named the blackboard as expert memory, and notes already persist decisions, so a dedicated "checkpoint store" (repo map / session-id artifact) would be a second source of truth for facts the blackboard already owns — a one-authority violation. Compaction at the *injection* layer (bounded primer) complements the existing JSONL storage compaction: the durable record keeps everything, the finite runtime context gets a bounded, value-ranked slice. Notes are stream-only (never published), so re-sync is a high-water poll on the loop's existing tick, not a new pub/sub path. Deferred (filed as #28 follow-up comments): a literal session-id/repo-map checkpoint artifact, and a filesystem-watch re-sync trigger (the in-mesh note signal is the documented file-change signal for now).

**Status:** active

**References:** internal/sidecar/memory.go, internal/sidecar/expert.go (ServeExpertWithMemory, ResyncSignal), cmd/meshd/main.go (runExpert prime/restart), test/e2e/expert_memory_test.go, ARCHITECTURE.md §7e, #28; extends 2026-06-05 "Hybrid agent model — blackboard = expert memory" and 2026-06-06 "Expert responder loop"

---

## 2026-06-09: P4 dashboard write path — POST /api/jobs on the dashboard server, protected by a local bearer token

**Decision:** The job-submit form (issue #47) is backed by a `POST /api/jobs` endpoint on the dashboard's own HTTP server, delegating to `job.Store.Create` (the one authority, same as `mesh submit`). A 32-byte random hex bearer token is generated on `Start`, written to `MESH_DIR/dashboard.token` (owner-only), and removed on `Stop`. The UI fetches it from `GET /api/write-token` (same loopback origin). Observer endpoints (`/`, `/events`, `/api/roster`, `/api/claims`, `/api/notes`, `GET /api/jobs`) remain unauthenticated.

**Rationale:** The issue spec says the write path lives on the coordinator's control plane, but the coordinator has no HTTP server and adding one solely for this endpoint is heavier than mounting two routes on the existing dashboard server. The dashboard already holds a bus client used for reads; `job.Store` needs only that client, so no new connection is required. One authority per fact is preserved because the endpoint delegates entirely to `job.Store`, not a parallel path. The token keeps localhost-only semantics: it does not harden against a local attacker who can read files, but it prevents an accidental cross-origin fetch from a different tab from creating jobs.

**Status:** active

**References:** #47; internal/dashboard/dashboard.go, web/jobform.js, internal/config/config.go (DashboardTokenFile)

---

## 2026-06-08: Autonomous work hierarchy = Job → Task → (ask)Ticket; new domain packages, ask-ticket stays frozen

**Decision:** The autonomous product's work unit is a **Job** (top-level intake created by `mesh submit`, `internal/job`, `BucketJobs`, lifecycle `open→triaged→scheduled→running→done|failed|cancelled`, `KindJob`/`SubjectJob`/`StreamJobs`). Triage (#24) decomposes a Job into **Tasks** (DAG nodes, `internal/task`). The existing `internal/ticket` package stays the **P2 agent-to-agent async ask** — unchanged and frozen. Duplicate submissions are allowed, each creating a new job id (dedup is #29 policy).

**Rationale:** `ticket.Record` models a *question* (Q/Asker/Answer; open→routed→accepted→answered→closed). A Job is a *unit of work* that owns a DAG and has a different terminal lifecycle. Overloading `ticket` would reshape a frozen, golden-pinned contract (the runbook forbids it) and force a job through a Q&A FSM that does not fit. One authority per fact → a new domain package, mirroring the proven `ticket` shape.

**Status:** active

**References:** #23, #24; internal/job, internal/task, internal/ticket

---

## 2026-06-08: P3 execution plan — cheap-end-first, fleet-gated; worker isolation = worktree-per-worker

**Decision:** P3 build order: #23 intake → #24 triage (+`internal/task` DAG) → **[fleet-feasibility spike gate]** → #25 scheduler → #26 worker. Ungated work (#27 expert review, #28 expert memory, test debt #38/#39/#40) interleaves. Worker isolation resolves to **worktree-per-worker** (physical isolation); P1 CAS file-claims remain the cross-worker advisory conflict signal — both, not either.

**Rationale:** #23/#24 carry no fleet risk (#24 is a single planner invocation, not a fan-out). The one hard external risk — subscription rate-limits/ToS for N concurrent `claude -p` — is isolated behind a spike that gates only #25/#26, instead of blocking the whole build. Worktree gives clean physical isolation; claims give the mesh its own conflict signal.

**Status:** active

**References:** #23–#26, #27, #28, #38–#40; frontier open-questions Q1 (fleet feasibility) / Q4 (worker isolation)

---

## 2026-06-06: Expert responder loop = role-owning sidecar + inbox-draining loop over the runtime proxy (first non-manual P2 slice of #27)

**Decision:** An *expert* is an ordinary role-owning sidecar plus a responder loop, not a new agent type. `Sidecar.ServeExpert` (`internal/sidecar/expert.go`) polls the agent's own already-accepted inbox — the existing role-ask subscription auto-accepts role-routed tickets via `handleIncomingAsk`, so the loop only *drains* what is accepted — and answers each ticket through `internal/runtime.Proxy`, the resident stream-json child. The child binary is swappable via `MESH_EXPERT_CLI` (default `claude`), so CI fakes the LLM (`test/e2e/fakeclaude`) while production drives real `claude`. Answers commit through the one existing path (`recordAndPublishAnswer`, factored out of `handleAnswer`): **tickets KV stays the sole authority, no coordinator answer-payload path, no fake-success** — only a `runtime.TurnAnswered` writes an answer; lost/error turns are skipped and poison tickets are tracked in-memory and left to TTL expiry (no new FSM state). Surface: `meshd --mode expert` (the daemon, autostarts the coordinator like `--mode sidecar`) plus a thin foreground `mesh expert serve` that execs it with the `--mesh-dir` ownership marker so `mesh ops down` tears it (and its tracked runtime child) down. Best-effort crash recovery: on `ErrProcessExited` the loop's runtime fn tries `proxy.Restart` (`--resume`) once.

**Rationale:** This is the smallest honest dogfood after P2 — the manual `mesh inbox`/`mesh answer` step was the only thing between a routed ask and an automatic answer, and the auto-accept + single answer path already existed, so the slice is a loop plus a swappable runtime binary, not new machinery. Keeping the loop in `internal/sidecar` (where `ticketStore`/`publishTicket`/`TrackChild` live) behind an `ExpertFunc` seam keeps `internal/runtime` out of the sidecar package (no import cycle) and the runtime wiring in `cmd/meshd`. Faking only the LLM binary — not the proxy — exercises the real stream-json process boundary end-to-end in CI without an API key, honoring never-scrape-prose / one-authority / async-never-block.

**Status:** active

**References:** internal/sidecar/expert.go, internal/sidecar/verbs_p2.go (recordAndPublishAnswer), cmd/meshd/main.go (runExpert), internal/cli/expert.go, internal/config (MESH_EXPERT_CLI), internal/runtime, test/e2e/expert_test.go, test/e2e/fakeclaude; #27, #19, #20; extends 2026-06-05 "#27 persistent experts land as a prep slice" and "Persistent experts = a resident stream-json claude process"

---

## 2026-06-06: Claim loss is surfaced to the holder, never silent; no restart grace window

**Decision:** Resolve #43: keep one-CAS-winner, lost-means-lost semantics — a rival that claims in the coordinator-restart gap legitimately wins, and there is no restart grace window. What changes: a holder whose claim is lost on **any** path (re-establishment loses after a coordinator bounce, eviction reclaim, any future race) must be **notified** — the sidecar emits a claim-lost event surfaced to the agent (hook-consumable and visible via its status surface), instead of silently dropping the claim from the held set. `TestClaimsReestablishedAfterCoordinatorRestart` is realigned to the documented semantics: it asserts the holder *observes the loss*, not that it always wins re-establishment.

**Rationale:** The actual hazard in #43 is two agents editing one file while the original holder doesn't know it lost its lock — the silent forget, not the loss itself. A restart grace window would add special restart state for a rare event and close only one loss path; notification covers all of them and keeps CAS semantics honest (the claim was decided; the bug was not telling the loser). Test-encoding a guarantee the product doesn't make was the flake's root cause.

**Status:** active

**References:** #43, internal/sidecar/verbs_p1.go (reestablishClaims), internal/sidecar/verbs_p1_test.go; extends 2026-06-05 "Every claim and presence record is a TTL lease with reclaim-on-death"

---

## 2026-06-06: `mesh up` = idempotent infra bring-up in autostart; ops scope unchanged

**Decision:** One command — `mesh up [--dashboard-addr A] [--observe-addr A]` — idempotently brings up coordinator + dashboard + observe and prints their URLs. The spawn logic lives in `internal/autostart` (which already starts coordinators and sidecars); `internal/ops` stays inspect + teardown + janitor and never spawns, preserving the 2026-06-05 actuator-verbs scope. Supporting protocol: dashboard/observe write run files under MESH_DIR (`<name>.pid` first, then `<name>.addr` atomically with the REAL bound address — the addr file is both the readiness gate and the one authority for "where is the UI"), spawn carries the `--mesh-dir` argv ownership marker so `ops down/doctor/clean` cover the services, and a foreign holder on the configured port triggers an EADDRINUSE-only fallback to `127.0.0.1:0` (other listen errors stay fatal). "Already running" = pidfile alive AND addr dialable; a live-but-not-serving pid is a typed error, never a respawn.

**Rationale:** Three manual commands to get UI + monitoring was the real "local is annoying" pain (ports never were — sockets are MESH_DIR-namespaced; the two loopback TCP ports are the one global resource, hence the fallback). All machinery existed (EnsureCoordinator flock pattern, daemon modes, ops verbs); this is glue plus a run-file readiness protocol, not architecture. Scope stops at infrastructure: agents join themselves, worker spawning stays with the coordinator (P3).

**Status:** active

**References:** internal/autostart/services.go, internal/observe/runfiles.go, internal/cli/up.go, test/e2e/up_test.go; extends 2026-06-05 "Ops plane gains scoped actuator verbs"

---

## 2026-06-06: KV record shapes live in their domain package, not envelope; result enums live in envelope

**Decision:** Authoritative KV record shapes live in the domain package that owns the fact — `agentcard.RegistryRecord` (presence), `claim.Record` (claims), and the new `ticket.Record` (tickets, `internal/ticket`, record-only until #17 builds the FSM there) — each frozen by a golden in that package's `testdata/`. `internal/envelope` owns the rest of the wire contract: kinds, subjects, payloads, and *all* result enums — `ReleaseResult` moved from `internal/claim` to `envelope/results.go` beside `ClaimResult`, with a type alias + const re-exports left in `claim` so call sites are unchanged.

**Rationale:** Issue #37 spec'd a `envelope/records.go` before P1 landed; #12 had meanwhile placed `claim.Record`/`Key` in `internal/claim`, matching the `RegistryRecord` precedent. Duplicating the shape into envelope would create two unreconciled sources of truth — goldens pinning one copy while runtime uses the other. Wire pinning needs a golden somewhere, not a type move. P1 splitting `ReleaseResult` away from its sibling `ClaimResult` was the actual inconsistency, so that enum moved.

**Status:** active

**References:** internal/envelope, internal/claim/claim.go, internal/ticket, #37, #12, #17; partially supersedes the records.go layout in #37's text

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
