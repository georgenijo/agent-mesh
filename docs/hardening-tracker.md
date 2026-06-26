# Agent Mesh — Hardening Tracker

Running list of things to fix/improve. Newest insight wins. Status: ⬜ todo · 🔄 in progress · ✅ done

## Top priority (blocks trusting the fleet)
- ✅ **Workers can't edit files** — FIXED: added `--dangerously-skip-permissions` to the worker's `claude` invocation (`internal/cliexec/adapter.go`). Was the root cause of the empty pilot run; confirmed fixed — a worker wrote a correct real fix to `config.go`. On `session/hardening`.
- ⬜ **Objective "done" gate** — a task can't be `done` unless it actually changed code AND `go build` + `go test` pass. (Audit Fork E; confirmed live: ticket #1 marked "done" with zero changes.)
- ⬜ **Capture worker output** — when a worker does nothing/fails, we can't see why. Save the agent's output so failures are diagnosable. (Audit #93)
- ⬜ **Tester step doesn't really test** — the "run go test/build" task fake-passed on code with no fix. The verify step must actually run the commands and fail on real failures. (Live, ticket #1)
- ✅ **Resident expert never starts (GitHub #101)** — FIXED: startup no longer blocks on the session id (which claude only emits after the first message); it's captured lazily on first Ask. Verified live: worker→architect ask accepted, answered in ~36s, worker recorded guidance + built. The full long-running-agent loop now works. On `session/hardening`.
- ⬜ **Presence ≠ readiness** — the expert showed "live" while its brain was dead (the #101 deadlock). Asks should fail fast / not route to a non-ready expert. (Still open.)
- ⬜ **Durable budget meter** — spend resets on restart and ignores planner + expert turns, so the cap isn't real. (Audit Fork A)
- ⬜ **Review gate destroys good work** — a slow/missing reviewer fails work that committed cleanly. (Audit Fork B)
- ⬜ **Crash recovery double-runs work** — restarts re-plan and re-run tasks; orphaned workers keep going. (Audit Fork C)

## UX / ops
- ⬜ **Arm-once then drop-and-run** — single command to bring up + arm the fleet (coordinator triage/scheduler + dashboard), keeping the explicit safety gate. Today it's multiple manual steps. (GitHub #95)

## Efficiency / cost
- ⬜ **Planner over-decomposes** — split a 5-line fix into 4 tasks = 4 LLM turns. Trivial tickets should stay 1 task. (Live, ticket #1)
- ⬜ **Model routing** — cheap model for cheap tasks, escalate when needed. Doesn't exist yet. (Audit, model-routing design)
- ⬜ **Pin the expert model** — expert currently runs unpinned = most expensive tier. Add `MESH_EXPERT_MODEL`. (Audit, quick win)

## Done
- ✅ **Dashboard remote viewing** — added `MESH_DASHBOARD_ALLOWED_HOSTS` so the dashboard works over tailnet (was loopback-only). On branch `session/hardening`, not yet merged.

## Reference
- Full audit (22 verified bugs + 5 architectural locks + model-routing design): `/tmp/.../tasks/wie3am57v.output`
- The 22 individual bugs are the detailed backlog feeding the items above.
