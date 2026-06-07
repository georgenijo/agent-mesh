# Claude Code mesh hooks

Two hooks for the first homogeneous Claude Code integration:

- `mesh-claim-guard.sh`: a `PreToolUse` hook that takes a mesh CAS claim on
  the target file before mutating tools.
- `mesh-inbox-drain.sh`: a `Stop` hook that runs `mesh inbox --json` between
  turns and prints pending accepted asks with the exact `mesh answer` command.

The claim guard takes a mesh CAS claim on the target file before
every mutating tool call (`Edit`, `Write`, `MultiEdit`, `NotebookEdit`).
If another agent already holds the path, the tool call is blocked (hook
exit 2) and the model is told who owns it and since when — so agents
coordinate instead of colliding on the same file.

## Install

1. Put `mesh` on `PATH` and join the mesh in the session
   (`mesh join --name me --role builder`).
2. **Export `MESH_SOCKET` to this session's sidecar socket** in the same
   shell, before launching the agent — e.g.
   `export MESH_SOCKET="$MESH_DIR/agents/me.sock"` (default `MESH_DIR` is
   `~/.mesh`). This is how the hook knows *which* agent it is. Without it the
   hook is a silent no-op (see "Identity" below) — it will not guess.
3. Merge `settings-snippet.json` into your project's `.claude/settings.json`
   (or `~/.claude/settings.json`), replacing each `command` with the absolute
   path to the hook script in your clone.
4. Verify locally with `./test-claim-guard.sh` (stubs `mesh`; needs only
   bash + python3).

## Identity: `MESH_SOCKET` is required

The hook claims as the agent behind `$MESH_SOCKET`. It is mandatory, not a
convenience. If it were unset, `mesh` would fall back to "the one socket
under `$MESH_DIR/agents`" and a Claude Code session that never joined would
silently take claims under whatever agent happens to be the only one on the
machine — a lock recorded for the wrong identity. So the hook no-ops when
`MESH_SOCKET` is unset rather than guess. Each session that should
participate exports its own socket.

## Exit-code contract

The hook runs `mesh claim "<path>" --json` and maps its exit code
(`internal/cli/cli.go`):

| `mesh claim` exit | meaning                                | hook exit | effect in Claude Code |
| ----------------- | -------------------------------------- | --------- | --------------------- |
| 0                 | claimed — this agent holds the path    | 0         | edit proceeds |
| 6                 | lost — another agent holds the path    | 2         | tool call blocked; stderr (`claimed by <owner> since <ts>`) is fed back to the model |
| 5                 | not joined — no sidecar for this session | 0       | silent no-op |
| anything else     | error / usage / bus down               | 0         | fail-open: the guard is advisory and must never brick editing |

The hook also exits 0 without calling `mesh` for: `MESH_SOCKET` unset, tools
that don't mutate files, unparseable hook JSON, and machines missing
`python3` or `mesh`.

## Repo override

By default the sidecar derives the claim's repo from the agent card. Export
`MESH_REPO=<repo-id>` in the session environment to pin it explicitly
(forwarded as `mesh claim --repo "$MESH_REPO"`). `MESH_SOCKET` is required
regardless (see "Identity" above).

## P1 limitation: claims are taken, never auto-released by the hook

The hook only acquires. Claims are freed by `mesh release <path>`,
`mesh leave`, or coordinator reclaim when the holder's presence lease
expires. Auto-release (e.g. on Stop) is deferred past P1.

## Stop hook inbox drain

`mesh-inbox-drain.sh` is read-only. It requires the same `MESH_SOCKET`
identity guard as the claim hook, then runs `mesh inbox --json`. If pending
asks exist, it prints a stable block like:

```text
mesh inbox: pending asks
- <ticket> from <agent>: <question>
  context: <context>
Reply with: mesh answer <ticket> "<answer>"
```

It exits 0 for not joined, empty inbox, missing `mesh`/`python3`, malformed
JSON, or mesh transport errors. The hook never fabricates answers and never
marks a ticket handled; the responder must explicitly run `mesh answer`.
