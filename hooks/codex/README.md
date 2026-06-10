# Codex mesh hooks

Codex CLI supports lifecycle hooks through `hooks.json` or `.codex/config.toml`.
These hooks make Codex participate in Agent Mesh with minimal friction:

| Hook | Event | Does |
| ---- | ----- | ---- |
| `mesh-session-start.sh` | `SessionStart` | joins as `cx-<sid8>`, announces startup, and injects roster/context |
| `mesh-claim-guard.sh` | `PreToolUse` for `apply_patch` | claims every file touched by the patch and blocks if another agent owns one |
| `mesh-inbox-drain.sh` | `Stop` | emits Codex `decision:block` JSON when accepted asks are waiting |

## Install

1. Put `mesh` on `PATH` (`make build`, then add `bin/` to PATH).
2. Copy `hooks.json` to your repo's `.codex/hooks.json`, replacing each
   `/path/to/agent-mesh/...` command with an absolute path to this clone.
3. Start Codex and run `/hooks` to review and trust the project hooks.

Optional environment knobs:

- `MESH_ROLE` - role joined under (default `builder`).
- `MESH_REPO` - pins the repo id for join, announce, and claims.
- `MESH_SOCKET` - explicit sidecar socket; overrides session-derived identity
  in claim/inbox hooks.

## Release on exit

Codex documents `SessionStart` and `Stop`, but not a `SessionEnd` event. That
means project hooks can join, claim, and drain inbox automatically, while prompt
release on process exit needs either:

- `hooks/codex/codex-mesh.sh` as the launcher, which joins before `codex` and
  runs `mesh leave` when the Codex process exits. It exports `MESH_SOCKET`, so
  the `SessionStart` hook reuses that identity instead of joining a second one;
  or
- the normal Agent Mesh TTL/reclaim backstops, plus manual `mesh leave` when
  you want immediate cleanup.

## Codex-specific behavior

- File edits arrive as `tool_name: "apply_patch"` with the patch in
  `tool_input.command`.
- `PreToolUse` can block by exiting `2` with a stderr reason.
- `Stop` expects JSON stdout; empty stdout is the no-op path.
- Project-local hooks load only when the project `.codex/` layer is trusted.

## Verify

Run:

```sh
./test-codex-hooks.sh
```

The test stubs `mesh`, feeds sample Codex hook JSON into each script, and
checks join/announce, patch claims, multi-file release-on-loss, inbox drain,
and fail-open behavior.
