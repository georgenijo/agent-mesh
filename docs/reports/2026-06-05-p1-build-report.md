# P1 build report — conflict avoidance + blackboard (2026-06-05)

Issues #12–#16. Built on the P0 spine; zero external dependencies (stdlib
only). `make ci` green; cross-process e2e green under `-race -count=2`.

## What shipped

| Verb | Behavior |
| --- | --- |
| `mesh claim <path> [--repo R]` | CAS file-claim. Create-only `KVPut` on the `claims` bucket, TTL-leased. Typed result `claimed` (exit 0) \| `lost` (exit 6, names the owner + since) \| `error`. |
| `mesh release <path> [--repo R]` | Release-if-owner via revision-guarded delete. `released` \| `not_owner` \| `error`. |
| `mesh announce "<intent>" [--paths a,b] [--repo R]` | Advisory pub/sub on `mesh.announce.<repo>`. Fire-and-forget; never a lock. |
| `mesh note "<text>" [--repo R] [--kind K] [--ticket T]` | Append a full validated envelope to the durable per-repo blackboard stream. |
| `mesh context [--repo R]` | Replay the blackboard, sender-bound, oldest first. |

Plus: the Claude Code `PreToolUse` claim-guard hook (`hooks/claude-code/`),
and dashboard wiring for a live claims panel + real note tail.

## Design decisions (logged in DECISIONS.md)

- **Stream disk persistence** — durable streams are append-only JSONL under
  `$MESH_DIR/streams/<name>.jsonl`, one `StreamEntry` per line, loaded on
  bus-server start, bounded by atomic-rename compaction at 2x `MaxStreamLen`,
  torn-tail tolerant, no per-append fsync (process-crash-safe via page cache;
  OS-crash tail loss out of scope). Gated by `bus.Options.StreamDir`; empty =
  pure in-memory (P0 behavior unchanged). The coordinator turns it on with
  `cfg.StreamsDir()`.
- **Claim keys are repo-relative** — the sidecar folds an absolute in-tree
  path to repo-relative (against the agent's `card.CWD`) before normalizing,
  so the edit hook's absolute `file_path` and a manual `mesh claim src/foo.go`
  collide on one key. `Key(repo, path)` is NUL-joined so `(a, b/c)` and
  `(a/b, c)` can't alias.

## Invariants held

One versioned envelope (notes are stored as full envelopes; new payload
fields validated at the publish edge). One authority per fact — the claims KV
record is the lock; announce is advisory; the notes stream is the blackboard
authority; the dashboard is a pure observer. Typed results, `lost` != `error`,
never fake-success. TTL leases with reclaim-on-death (coordinator sweep +
graceful leave release a dead agent's claims, audited) and re-establishment
on reconnect. Sender-bound mutations (`from` == acting id) on claims, notes,
and note replay. CLI stays a thin one-request client.

## Build method

Base contract written inline, then three package-disjoint builder lanes ran
in parallel (bus stream persistence; claim engine + coordinator reclaim;
Claude Code hook), integrated and wired into sidecar/CLI/dashboard, then a
42-agent adversarial review (4 dimensions x refutation verifiers) over the
diff.

## Adversarial review outcome

Five **major** findings confirmed after refutation (14 others refuted,
including one initially-rated "critical" that verifiers downgraded), all
fixed with regression tests:

1. **Path-aliasing lock bypass (F1/F2)** — hook absolutized paths while
   manual CLI claims stayed relative -> two keys for one file, both won.
   Fixed by repo-relative canonicalization at the sidecar. Regression:
   `TestClaimAbsAndRelCollide`.
2. **Hook "not joined" contract false (F3)** — the single-socket fallback let
   a never-joined session claim as another agent. Hook now requires
   `MESH_SOCKET` and no-ops without it. README contract + hook test updated.
3. **Stream-slot exhaustion (F4)** — `StreamRead` lazily allocated a slot, so
   `mesh context` / dashboard tailing of never-written repos leaked the
   64-slot budget. A read of a nonexistent stream now allocates nothing.
   Regression: `TestStreamReadDoesNotAllocateSlot`.
4. **Claims lost on coordinator restart (F5)** — claims KV is in-memory. The
   sidecar now tracks held claims and re-takes them on reconnect (as presence
   re-registers). Regressions: sidecar unit + e2e.

## Acceptance (verified literally)

    make ci                                     # green
    mesh claim src/foo.go --repo demo           # two agents race -> one claimed, one exit 6
    mesh note "events store UTC" --repo demo
    kill -9 <holder sidecar>                    # claim reclaimed after evict
    mesh context --repo demo                    # late joiner replays the note
    # coordinator restart -> mesh context still replays (disk persistence)
    #                     -> held claim re-established (peer still loses)

## Known limitations / deferred

- The claim-guard hook acquires but never auto-releases (release via
  `mesh release`, `mesh leave`, or eviction reclaim) — auto-release is post-P1.
- No per-claim path-length bound at the socket edge (minor, refuted as
  non-blocking; abnormal-input only).
- P2 (ask/answer tickets, role routing) not started.
