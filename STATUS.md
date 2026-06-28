# Agent Mesh — Overnight Autonomous Dogfooding Run

**Run window:** 2026-06-26, ~02:58 → 05:48 EDT (≈3h) · **Director:** Claude (autonomous, unattended)
**Mandate:** run the mesh against its own GitHub backlog, audit every result, leave a clean merged `main`.

---

## TL;DR for leadership

The fleet did **not** autonomously complete or merge any backlog issue. **0 of 30 jobs reached `done`; 0 PRs merged.** That is the honest headline.

The run's value is not merged code — it is a **high-yield dogfooding bug report**. Running the mesh hard against real work surfaced **six distinct, evidence-backed defects**, three of which I fixed by hand during the night (PR-ready on a branch). It also proved that two of the four planes (**presence** and **ask/answer**) work well under load, while the **worker → review → merge pipeline does not yet survive real concurrency.** The single biggest finding: **the architecture has a one-expert throughput ceiling and a worker-process leak that, together, make multi-job runs collapse.** Both are fixable.

Cost of the run: **$0.00** — see "Cost" below (it's a real answer, not a missing number).

---

## What we shipped (director-authored, PR-ready)

The fleet couldn't build, so I hand-fixed the bugs that were blocking it. Branch **`issue/85-review-redispatch`** (pushed to `georgenijo/agent-mesh`):

| Commit | Fix | Status |
|--------|-----|--------|
| `a10e83f` | **#85** bounded review re-dispatch — `request_changes` re-dispatches with feedback (≤ `MESH_REVIEW_RETRIES`) instead of cascade-failing the job | tested, pushed |
| (in branch) | **`MESH_EXPERT_MODEL` honored** — reviewers/experts were silently running the account-default model, not opus | tested, pushed |
| `e5ea53a` | **Cost logging** — `spent` is now logged per accrual so it survives an unlimited budget | tested, pushed |

These are real, `gofmt`/`vet`/`build`/test-green, and ready to open as a PR. **0 fleet-authored commits merged.**

---

## The numbers (full-night telemetry)

| Metric | Value |
|--------|-------|
| Jobs submitted | **30** (across 3 runs + re-tunes) |
| Jobs `done` | **0** — all 30 `failed` |
| Tasks total | **271** → 9 `done` (3.3%), 49 `failed`, 213 `cancelled` |
| **Yield (merges / jobs)** | **0%** |
| Review verdicts | approve **15**, request_changes **13**, error **18** |
| Worker errors | repo-slug **11**, timeout **10**, other (SIGTERM/exit143) **6** |
| Ask/answer round-trips | **8 asks, 6 fully closed (75%)**, 2 orphaned by restarts |
| Cost | **$0.0000** (19 accruals logged; all $0 — see below) |
| Coordinator crashes | **0** (the control plane was stable throughout) |
| Director interventions | 3 config re-tunes, 1 surgical process cleanup, 3 hand-fixes |

**213 cancelled tasks** is the story in one number: nearly every task was cancelled as collateral when a *sibling* task in its DAG failed (cascade-cancel). The fleet rarely failed on bad code — it failed on infrastructure.

---

## Root-cause bug list (the real deliverable)

Ordered by severity. Each is reproduced and evidenced in tonight's logs.

### 1. Worker teardown leaks the `claude` child process  ⟶ **critical**
On timeout/cancel, `worker.Teardown` does not kill the runtime child. **29 hung `claude` sessions** accumulated across the night, exhausting the account's concurrent-session capacity, so *new* workers' `claude` invocations hung and hit the 10-min timeout — spawning yet more zombies. A self-reinforcing collapse that masquerades as "worker timeouts."
**Fix:** kill the runtime process tree in Teardown. **Until fixed, every long run will strangle itself.**

### 2. One expert is a throughput ceiling for the whole fleet  ⟶ **critical (architectural)**
A single resident `expert-reviewer-1` serializes **every** review *and* **every** worker ask as 45–110s opus turns. With 8 workers, reviews queued past the 5-min gate → **18 review `error`s = "no review verdict within timeout"** → cascade. Lowering concurrency and widening the gate only delays it.
**Fix:** an **expert/reviewer pool** (N responders per role), or bound worker concurrency to review throughput. This is the single highest-leverage change.

### 3. Issue-sourced jobs store the wrong repo  ⟶ **high**
`mesh submit --issue owner/repo#N` sets `job.repo = "owner/repo"` (full slug); the worker resolves `MESH_REPOS_DIR/<repo>` and rejects slashes (path-traversal guard) → **11 instant spawn failures.** Every issue-submitted job failed this way until I added `--repo agent-mesh` as a workaround.
**Fix:** normalize `job.repo` to the last path segment during issue ingestion.

### 4. Review gate + cascade-cancel is brittle for multi-task jobs  ⟶ **high**
`#85` (shipped tonight) softens this — `request_changes` now retries with feedback — but `reject`/`error` still fail-fast, and one failed task cancels its whole DAG. With over-decomposed plans (below), a single strict verdict sinks a 12-task job.
**Fix:** consider per-task isolation or job-level partial success.

### 5. Planner over-decomposes  ⟶ **medium**
Opus split modest features into **10–13 tasks** (e.g. a "self-terminate when idle" feature → 9 tasks). Every extra task multiplies load on the single expert and widens the cascade surface.
**Fix:** decomposition budget / "keep it to N tasks unless truly warranted."

### 6. Cost is unobservable under an unlimited budget  ⟶ **low (fixed tonight)**
`spent` was in-memory and surfaced only on a budget-pause, which can't fire at `MESH_BUDGET_USD=0`. Fixed (`e5ea53a`).

---

## Cost: the real answer

**Cost tracking works** — 19 accruals were captured (`total_cost_usd` → `Result.CostUSD` → `spent`). Every value is **$0.0000** because this is a **Max/subscription account**: the `claude` CLI is not billed per-token, so the figure is genuinely zero, not missing. On an API-key account the same plumbing would report real dollars. (Before tonight's `e5ea53a` fix it was *also* unobservable; now it's logged either way.)

---

## What worked

- **Presence/registry plane** — heartbeats, join/leave, two-tier eviction: flawless. **Coordinator never crashed** across 5+ bounces.
- **Ask/answer plane** — **6 of 8 asks completed the full `open→routed→accepted→answered→closed` round-trip** (the other 2 were orphaned by my restarts, not failures). CAS-accept routed each ask to exactly one responder, no double-answers, no drops. One opus answer even queried the live audit stream to detect a sibling worker editing the same file — real cross-agent conflict-avoidance.
- **`#85`, expert-model, and cost fixes** — authored, tested, pushed.
- **Triage** — the planner reliably produced valid DAGs (over-decomposed, but structurally valid).

## What didn't

- **Worker → review → merge pipeline** — never completed a single job end-to-end under real load. Bottlenecks #1 and #2 above.
- **Autonomy** — the run required heavy director intervention (re-tunes, a process cleanup, hand-fixes). It was not "set and forget."

---

## Recommended next steps (priority order)

1. **Fix the worker child-process leak (#1).** Nothing else matters until long runs stop strangling themselves.
2. **Add a reviewer/expert pool (#2).** The one-expert ceiling is the architectural blocker to any real throughput.
3. **Normalize `job.repo` on issue ingestion (#3)** — one-line fix, unblocks the whole `--issue` path.
4. **Open the PR** for `issue/85-review-redispatch` (#85 + expert-model + cost logging).
5. Then re-run this exact dogfooding exercise — it's a high-quality stress test and should be kept.

---

## Housekeeping / state at handoff

- Mesh is **left running** (coordinator pid 904023, dashboard on :8800) so you can inspect the dashboard; tear down with `mesh ops down` (env: `source /tmp/mesh-overnight-env.sh`).
- All telemetry preserved under `/tmp/mesh/{logs,streams,buckets}`; running ledger at `/tmp/mesh-merge-ledger.md`.
- 29 stale fleet-zombie `claude` processes were cleaned up; **your own `claude` sessions were not touched.**
- The animated replay of the night is `docs/overnight-run.html`.

*Bottom line: no merges, but a genuinely valuable night — six real bugs found by doing, three fixed, the two healthy planes confirmed, and a clear, prioritized path to making the fleet actually ship.*
