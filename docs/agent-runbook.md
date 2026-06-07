# Agent Runbook

Rules for any agent (or human) doing implementation work on this repo. These exist because
multiple agents work the board in parallel; every rule below was earned by a real incident
or near-miss. If a rule and your judgment conflict, comment on the issue — don't improvise.

## The board is the contract

- **Work issue-driven only.** Pick an open issue; its spec is the scope. No issue, no work.
- **Scope discovered mid-work gets its own issue**, filed in the same PR that needs it —
  never silently absorbed. (`mesh up` shipped issueless once; it worked out, the habit doesn't.)
- **One issue per PR** where possible. The PR body says `Closes #N` so the board stays truthful.

## Worktrees and branches

- **Never work in the main checkout.** Always a worktree:
  `git worktree add .claude/worktrees/<slug> -b issue/<n>-<slug> origin/main`
- **Branch from fresh `origin/main`** — fetch first. Building on a stale base against this
  board's merge rate guarantees painful rebases (a dashboard agent once built SSE tests
  against pre-claims markup).
- **Rebase on `origin/main` before opening the PR**, and again before merge if main moved.
- **After your PR merges:** remove the worktree, delete the local and remote branch.

## Contracts are frozen by other people's work

- `internal/envelope` owns kinds, subjects, payloads, and result enums. KV record shapes
  live in their domain package (`internal/claim`, `internal/ticket`, `internal/agentcard`) —
  one authority per fact.
- **Never rename or reshape a contract identifier inside a feature branch.** If the contract
  doesn't fit, comment on the issue and wait for the call. This loop works: the exit-code
  6/7 collision between the P1 and ops branches was caught and fixed by one issue comment.
- Golden tests (`testdata/*.json`) pin the wire format. A golden diff in your PR is a
  deliberate contract change and must be called out in the PR body, or it's a bug.
- Current exit-code taxonomy: `0` ok · `2` usage · `3` no-answer-yet · `4` no-such-ticket ·
  `5` not-joined · `6` claim lost · `7` ops dirty. Claiming a new code = envelope-level
  decision, not a local choice.

## Merge order

- **PRs touching `test/e2e/` serialize** — coordinate via issue comments before starting.
  `harness_test.go` is shared infrastructure: extend it only when your issue explicitly
  says so, never refactor it opportunistically.
- **Contract PRs land before the implementation PRs that consume them.**
- Second PR to touch a shared file rebases; first to green merges.

## Done means verified

- `make ci` green locally before pushing (fmt-check + build + vet + test, including the
  cross-process e2e suite — the real done-gate).
- `make test-race` green for anything touching concurrency (bus, sidecar, coordinator, claims).
- Tests assert **typed results and `--json` fields, never prose** — log lines and human
  text are not contracts.
- Leave no processes behind: `mesh ops doctor` exits clean after your e2e runs
  (`mesh ops down` is the teardown; raw `pkill` kills other meshes on the machine).

## Where truth lives

1. `docs/decisions/DECISIONS.md` — locked decisions, newest first. Read before deviating.
2. The issue you're working — including its comments; re-scopes land there.
3. `CLAUDE.md` — repo map and current state.
4. This file — process. When any of these conflict, newest decision wins; say so on the issue.
