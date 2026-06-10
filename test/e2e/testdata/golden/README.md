# CLI Contract Goldens

These JSON files pin the `--json` output shape of every `mesh` verb. They are
the de-facto v1 CLI contract (issue #39). The `mesh` CLI's `--json` output is
the agent-facing API; a golden diff in a PR is a deliberate contract change and
must be called out in the PR body.

## What each file gates

| File | Verb | Condition |
|------|------|-----------|
| `join_fresh.json` | `mesh join` | First join (sidecar autostarts) |
| `join_rejoined.json` | `mesh join` | Second join to same running sidecar |
| `status.json` | `mesh status` | Happy path, status text posted |
| `who.json` | `mesh who` | Happy path, one agent in registry |
| `leave.json` | `mesh leave` | Graceful leave |
| `ops.json` | `mesh ops` | Happy path, one sidecar running |
| `error_ask_no_role.json` | `mesh ask` | Error path: typed `{"ok":false,...}` object |

## Volatile field placeholders

All volatile values are replaced before comparison so goldens contain no
host-specific data:

- `<id>` — UUIDv7 agent/verb IDs
- `<pid>` — process IDs
- `<ts>` — timestamps (registeredAt, lastSeen, collectedAt, etc.)
- `<meshDir>` — MESH_DIR temp path
- `<socket>` — unix socket path
- `<cwd>` — working directory
- `<logPath>` — log file path
- `<pidFile>` — PID file path
- `<message>` — free-text error messages
- `<uptime>` — uptime strings

## How to regenerate

```
go test ./test/e2e -run TestCLIContract -update
```

Running `-update` twice on an unchanged tree leaves `git status` clean.

## Known asymmetries (frozen as-is)

1. **Pre-socket failures bypass `emit()`** — when `resolveSocket` returns
   not-joined (no socket file exists at all), the CLI prints to stderr even
   with `--json`. Only errors that reach `doVerb` and return a typed socket
   response go through `emit()` and produce a JSON object on stdout. This
   asymmetry is frozen as-is in the current CLI design.

2. **`runOps` does not use `emit()`** — it marshals `observe.Snapshot` directly
   and never produces the `{"ok":false,...}` error object; errors from `ops`
   go to stderr in both human and `--json` mode.

3. **The socket frame's `"v":1` is stripped** — `emit()` prints raw `resp.Data`
   (the verb payload), not the full socket response frame, so CLI JSON is
   currently unversioned. These goldens are the de-facto v1 contract.
