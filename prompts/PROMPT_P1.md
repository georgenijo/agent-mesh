# Agent Mesh — P1 Build Brief (conflict avoidance + blackboard)

You are picking up Agent Mesh after P0 shipped (presence spine, green CI, main branch). Your job is **P1 only**: CAS file-claims, announce, and the durable blackboard — plus one hook pulled forward so dogfooding can start the day you finish.

## Read first, in this order

1. `AGENTS.md` — current state + build/test commands (`make ci` mirrors CI).
2. `docs/decisions/DECISIONS.md` — newest-first; the 2026-06-05 "P0 transport = Go + coordinator-embedded star bus" entry governs everything you touch.
3. `docs/reports/2026-06-05-p0-build-report.md` — what exists, how it's tested, what was hardened and why.
4. GitHub issues `#12 #13 #14 #15 #16` (`gh issue view N`) — full scope + acceptance per issue.
5. `internal/bus` — the primitives you'll build on (KV revision-CAS + TTL is already race-tested; streams exist but are in-memory).
6. `internal/meshapi` + `internal/sidecar` + `internal/cli` — the verb pattern to copy for new verbs.
7. `test/e2e/p0_test.go` — the e2e harness pattern (#16 extends this).

## Build (in this order)

1. **#12 claim/release verbs** — `mesh claim <path> [--repo R]` / `mesh release <path>`: `KVPut` with `CreateOnly()` on a `claims` bucket, value `{agent, path, repo, ts}`, TTL lease. Typed result `claimed|lost|error` surfaces in the CLI (exit 0 on claimed; nonzero distinct code on lost — document it). Release is delete-if-owner.
2. **#13 TTL + reclaim-on-death** — claims carry TTL renewed by the sidecar heartbeat path; coordinator sweep releases an evicted agent's claims (extend the existing sweep; audit each release).
3. **#14 announce** — `mesh announce "<intent>" [--paths a,b] [--repo R]`: pure pub/sub on `mesh.announce.<repo>`, `AnnouncePayload` already exists. Advisory only; real edits take a claim.
4. **#15 note/context blackboard** — `mesh note "<decision>" [--repo R]` appends to a durable stream; `mesh context [--repo R]` replays it. **This needs the one real new piece: disk persistence for bus streams.** Lean: append-only JSONL per stream under `$MESH_DIR/streams/<name>.jsonl`, loaded on server start, bounded retention preserved. Late joiner must replay notes written before it existed — including across a coordinator restart.
5. **#16 e2e** — cross-process: two CLI agents race a claim (exactly one wins); evicted agent's claim is reclaimed; note written → coordinator restarted → late-joining agent replays it via `mesh context`.
6. **Pulled forward from #21 (partial):** the **PreToolUse claim-check hook** for Claude Code only — a small script in `hooks/claude-code/` that, before an Edit/Write tool call, runs `mesh claim` on the target path and blocks/warns on `lost`. Do NOT build the Stop/inbox hook (that's P2 — ask/answer doesn't exist).
7. **Minimal UI wiring only:** add a claims panel (reads the real `claims` KV via a small `/api/claims` endpoint or KV list pushed over the existing SSE roster tick) and make the notes panel read the real stream. No redesign — P4 does polish; this is just making dogfooding visible.

## Invariants you must not break

- One versioned envelope; all wire shapes live in `internal/envelope`. New payload fields go there with validators + round-trip tests.
- One authority per fact: the claims KV record is the lock; announce is advisory; never two sources of truth.
- Typed results everywhere; never fake-success; `lost` ≠ `error`.
- Sender-bound mutations: a claim/release/note carries `from` == acting agent id; the coordinator/bus side must not honor mismatches (same rule P0 enforces for presence).
- Async-never-block; CLI stays a thin one-request client; no state in the CLI.
- Zero new external dependencies unless truly forced (P0 is stdlib-only — keep it).

## Constraints

- Do NOT build ask/answer, tickets, role routing, workers, experts, scheduler. No LLM calls anywhere.
- Keep `docs/spikes/` untouched (P3 evidence).
- gofmt + `go vet` + `make ci` green; race detector on `internal/...`; e2e must pass as separate processes.
- Run an adversarial review pass over your diff before finishing (concurrency, protocol, spec-compliance vs issue acceptance, robustness); fix confirmed findings with regression tests.
- Log any resolved design fork via the `/decisions` skill (e.g. the stream-persistence format).
- Post short completion notes on #12–#16; close only if acceptance is fully met. No AI attribution anywhere.

## Acceptance (must work literally)

```sh
make ci
# terminal A                                # terminal B
mesh join --name a --role builder           mesh join --name b --role builder
mesh claim src/foo.go --repo demo           mesh claim src/foo.go --repo demo   # exactly one wins, typed
mesh note "events store UTC" --repo demo
# kill -9 agent a's sidecar → a's claims released after evict (visible in who/claims)
mesh context --repo demo                    # b replays the note
# restart coordinator → mesh context still replays the note (disk persistence)
```

## Done = dogfood-ready

Finish = the mesh protects parallel coding sessions on a real repo: claims block file collisions (hook-enforced for Claude Code), notes persist decisions across sessions and restarts. Write a short build report in `docs/reports/`, update AGENTS.md/CLAUDE.md current-state, commit and push (small logical commits, no attribution), confirm CI green.
