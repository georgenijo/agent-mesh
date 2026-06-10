# Claude Code mesh hooks

Four hooks that close the coordination loop for Claude Code — a session
joins the mesh, claims files it edits, answers asks between turns, and
leaves (freeing its claims) when it ends, **with zero manual mesh commands**:

| Hook | Event | Does |
| ---- | ----- | ---- |
| `mesh-session-start.sh` | `SessionStart` | joins as a session-scoped agent (`cc-<sid8>`); announces startup; injects mesh identity + roster into the model's context |
| `mesh-claim-guard.sh` | `PreToolUse` (Edit/Write/MultiEdit/NotebookEdit) | takes a CAS claim on the target file; blocks the edit (exit 2) naming the owner when another agent holds it |
| `mesh-inbox-drain.sh` | `Stop` | when accepted asks are pending, feeds them to the model (`decision:block`) so it answers before finishing the turn |
| `mesh-session-end.sh` | `SessionEnd` | `mesh leave` — the coordinator promptly releases every claim the session still holds |

## Install

1. Put `mesh` on `PATH` (e.g. `make build` then add `bin/` to PATH).
2. Merge `settings-snippet.json` into your project's `.claude/settings.json`
   (or `~/.claude/settings.json`), replacing each `command` with the absolute
   path to the hook script in your clone.
3. Launch Claude Code. That's it — the session joins on startup and leaves on
   exit. Verify locally with `./test-claim-guard.sh` and
   `./test-session-lifecycle.sh` (both stub `mesh`; need only bash + python3).

Optional environment knobs (set before launching the session):

- `MESH_ROLE` — role joined under (default `builder`).
- `MESH_REPO` — pins the repo id for the join and every claim.
- `MESH_SOCKET` — explicit sidecar socket; overrides session-derived
  identity in every hook (the pre-P3 manual flow keeps working).

## Identity: session-scoped, derived per hook

Claude Code runs each hook as a separate process, so an environment
variable exported by one hook can never reach another. The one fact every
hook shares is the `session_id` in its stdin JSON. So:

- `SessionStart` joins as **`cc-` + the first 8 alphanumerics of the
  session id**; the sidecar socket lands at the default path
  `$MESH_DIR/agents/cc-<sid8>.sock` (default `MESH_DIR` is `~/.mesh`).
- Every other hook re-derives that same socket path from its own stdin.
  If the socket does not exist, this session never joined → the hook is a
  perfect no-op.

The "never guess" identity rule is preserved: the derived name is unique
to the session, and no hook ever falls back to "whatever single socket is
under `$MESH_DIR/agents`" — that could act as the wrong agent. An explicit
`$MESH_SOCKET` always wins, so deliberately-named agents (`mesh join
--name me …` + export) still work exactly as before.

## Claim-guard exit-code contract

The guard runs `mesh claim "<path>" --json` and maps its exit code
(`internal/cli/cli.go`):

| `mesh claim` exit | meaning                                  | hook exit | effect in Claude Code |
| ----------------- | ---------------------------------------- | --------- | --------------------- |
| 0                 | claimed — this agent holds the path      | 0         | edit proceeds |
| 6                 | lost — another agent holds the path      | 2         | tool call blocked; stderr (`claimed by <owner> since <ts>`) is fed back to the model |
| 5                 | not joined — no sidecar for this session | 0         | silent no-op |
| anything else     | error / usage / bus down                 | 0         | fail-open: the guard is advisory and must never brick editing |

The hook also exits 0 without calling `mesh` for: no resolvable identity
(no `MESH_SOCKET` and no live session socket), tools that don't mutate
files, unparseable hook JSON, and machines missing `python3` or `mesh`.

## Stop hook: how pending asks reach the model

A Stop hook's plain stdout goes to the transcript, **not** to the model.
To make the model actually answer pending asks, `mesh-inbox-drain.sh`
emits the Stop-hook decision JSON:

```json
{"decision": "block", "reason": "mesh inbox: pending asks…\n- <ticket> from <agent>: <question>\nAnswer each with: mesh answer <ticket> \"<answer>\""}
```

Claude Code feeds the `reason` to the model, which answers each ticket
with `mesh answer` and then stops normally. Two guards prevent loops and
noise: when `stop_hook_active` is true the hook only prints to the
transcript (never blocks twice in a row), and an empty inbox produces no
output at all. The hook never fabricates answers and never marks a ticket
handled.

## Lifecycle of a claim taken by the guard

Claims are TTL leases renewed by the sidecar heartbeat, so a session holds
the files it edited until it ends. They are freed by the first of:

- `mesh-session-end.sh` → graceful `mesh leave` → coordinator releases all
  of the agent's claims (the common case, prompt);
- coordinator reclaim when the presence lease expires (crash / kill -9);
- the claims-bucket TTL backstop (coordinator itself down);
- an explicit `mesh release <path>` mid-session.

## Fail-open, everywhere

Every hook exits 0 — never blocking, never erroring — for: missing
`python3` or `mesh`, unparseable stdin, a session that never joined, a
down bus, or any mesh error. The mesh is a coordination aid; its absence
must never break editing or session start/end.
