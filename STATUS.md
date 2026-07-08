# Agent Mesh — Overnight Autonomous Run (2026-06-28)

**An autonomous director ran the mesh against its own GitHub backlog and merged 9 issues, unattended, with `main` green throughout.** Drop a ticket → a Sonnet worker builds it → an Opus expert reviews it → it merges.

See `docs/reports/2026-06-28-overnight-run.html` for the animated story.

## Result

| Metric | Value |
|--------|-------|
| Issues built, reviewed & merged | **9** (#96 #97 #98 #99 #105 #110 #111 #119 + the infra fixes) |
| PRs merged to `main` | **10** (#124 #126–#136) |
| `main` broken at any point | **never** |
| Worker spend | ≈ **$22.82** across 11 worker runs |
| Models | planner **Opus** · workers **Sonnet** · reviewer **Opus** · review gating **ON** |

**Before / after:** the prior run (2026-06-26) merged **0 of 30** jobs — it could coordinate agents but collapsed before finishing work. This run closed the loop.

## What unlocked it — three root fixes

1. **Worker child-process leak (#122, fixed in #124).** `claude` workers spawned a subprocess tree; on timeout only the direct child was killed, so grandchildren orphaned and exhausted account session capacity until new workers hung too. Fix: own process group + group-kill on cancel (regression-tested).
2. **Planner over-decomposition (#124).** Opus split modest tickets into 10–13 tasks. Fix: default to **one task**, split only for genuinely independent work.
3. **Worker "Definition of Done" hardening (#130) — the key fix.** Workers did correct work but the Opus reviewer kept issuing `request_changes` for fixable nits (an unaligned `gofmt` line, a feature left backend-only), and `main` has no auto-retry (see #85), so good work hard-failed. Fix: the worker now runs `gofmt`/`go build`/`go test` and re-checks every acceptance criterion before finishing. Jobs flipped from nit-failing to **first-try approvals** — four clean passes in a row right after it landed.

## The review is substantive (real reviewer quotes)

- **Approve (#98):** "Per-agent cost/model keying correctly matches the worker's registered card name; lock discipline and SSE/REST/contract conventions are sound."
- **Request changes (#99):** "The backend is solid and well-tested… However, no frontend change renders the cost window, so acceptance criterion #1 is unmet."
- **Request changes (#105):** "The implementation is correct and well-tested… However, the added `EnvExpertIdleTTL` const line is not gofmt-aligned."

## Merged this run

| Issue | Shipped | PR(s) |
|-------|---------|-------|
| #96 | Group dashboard tasks by job + readable labels | #127 |
| #97 | Show GitHub issue link on issue-sourced jobs | #126 |
| #98 | Per-agent model + cumulative cost in dashboard | #136 |
| #99 | Persistent cost window — ledger + API + UI | #128, #131 |
| #105 | Idle agent reaper (self-terminate on TTL) | #129 |
| #110 | Submit a job from a GitHub issue link | #134 |
| #111 | Natural-language job control (issue N / range / all) | #133 |
| #119 | HTTP `POST /jobs` dispatch ingress | #135 |
| infra | worker-leak fix + planner curb; worker Definition-of-Done | #124, #130 |

## Bugs surfaced / filed

- **#122** — worker child-process leak. **Fixed (#124).**
- **#123** — single-reviewer throughput ceiling. **Shipped** — `MESH_REVIEW_POOL_SIZE` maintains N resident reviewers for `MESH_REVIEW_ROLE` (restart-class setting).
- **#125** — cold-start review re-deliver race. **Shipped** — readiness KV (`BucketExpertReady`) plus spaced re-deliver retries on ready-timeout so the first cold review lands.
- **#85** — `request_changes` hard-fails on `main` (no auto re-dispatch with feedback). **Shipped** — bounded re-dispatch with `MESH_REVIEW_RETRIES` / ReviewFeedback.

## Honest notes

- **Cold-start** used to cost reviews (#97 / #111) to #125; readiness signalling + timeout re-deliver retries close that gap for auto-spawned reviewers.
- **Dashboard jobs collide on `web/app.js`** — parallel UI jobs conflict at merge time (claims are advisory within a run, not across git branches). Resolved by sequencing UI work and keep-both rebases.
- **Reviewer pool (#123)** lifts the single-Opus serial ceiling; set `MESH_REVIEW_POOL_SIZE` > 1 when running more than ~2 concurrent workers.
