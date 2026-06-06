# Claude Code claim-guard hook

A `PreToolUse` hook that takes a mesh CAS claim on the target file before
every mutating tool call (`Edit`, `Write`, `MultiEdit`, `NotebookEdit`).
If another agent already holds the path, the tool call is blocked (hook
exit 2) and the model is told who owns it and since when — so agents
coordinate instead of colliding on the same file.

## Install

1. Put `mesh` on `PATH` and join the mesh in the session
   (`mesh join --name me --role builder`). Not joined → the hook is a
   silent no-op.
2. Merge `settings-snippet.json` into your project's `.claude/settings.json`
   (or `~/.claude/settings.json`), replacing `command` with the absolute
   path to `mesh-claim-guard.sh` in your clone.
3. Verify locally with `./test-claim-guard.sh` (stubs `mesh`; needs only
   bash + python3).

## Exit-code contract

The hook runs `mesh claim "<path>" --json` and maps its exit code
(`internal/cli/cli.go`):

| `mesh claim` exit | meaning                                | hook exit | effect in Claude Code |
| ----------------- | -------------------------------------- | --------- | --------------------- |
| 0                 | claimed — this agent holds the path    | 0         | edit proceeds |
| 6                 | lost — another agent holds the path    | 2         | tool call blocked; stderr (`claimed by <owner> since <ts>`) is fed back to the model |
| 5                 | not joined — no sidecar for this session | 0       | silent no-op |
| anything else     | error / usage / bus down               | 0         | fail-open: the guard is advisory and must never brick editing |

The hook also exits 0 without calling `mesh` for: tools that don't mutate
files, unparseable hook JSON, and machines missing `python3` or `mesh`.

## Repo override

By default the sidecar derives the claim's repo from the agent card. Export
`MESH_REPO=<repo-id>` in the session environment to pin it explicitly
(forwarded as `mesh claim --repo "$MESH_REPO"`). With several agents on one
machine, also export `MESH_SOCKET` so the claim is taken as the right agent
— otherwise socket resolution is ambiguous and the hook fails open.

## P1 limitation: claims are taken, never auto-released by the hook

The hook only acquires. Claims are freed by `mesh release <path>`,
`mesh leave`, or coordinator reclaim when the holder's presence lease
expires. Auto-release (e.g. on Stop) is deferred past P1.
