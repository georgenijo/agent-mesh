# Agent Mesh

A local-first coordination fabric that lets heterogeneous coding agents
(Claude Code, Codex CLI, Cursor CLI, Aider, …) discover each other, share
status, ask each other questions, and avoid stepping on each other's work —
through a single `mesh` command-line tool.

You run several coding agents at once. Today they're blind to each other: two
edit the same file, a third re-derives a fact a fourth already figured out, and
you babysit all of them. Agent Mesh gives them a shared nervous system so they
can **announce** what they're working on, **ask** a question and get an answer
from whichever agent (or human) knows, read a shared **blackboard** of
decisions, and be **observed** from one live dashboard — all opt-in per message,
not forced per turn.

## Status

**Fully implemented (P0–P2 + fleet automation).** Go module
`github.com/georgenijo/agent-mesh`, zero external dependencies, stdlib only.

- **P0 (presence):** `mesh join/leave/who/status`, autostart, live SSE dashboard.
- **P1 (conflict avoidance + blackboard):** `mesh claim/release/announce/note/context`.
- **P2 (async ask/answer):** `mesh ask/poll/inbox/answer` across real processes.
- **Fleet automation:** triage planner, worker scheduler, resident experts, review
  gating, per-agent model pinning, budget cap, audit log.
- **`mesh up`:** one command to bring up the full fleet and arm it (see below).

## Quick start

```sh
make build
PATH="$PWD/bin:$PATH"

# Bring up coordinator + dashboard + observe (idempotent):
mesh up

# Or arm the full fleet in one shot:
mesh up --planner-cli claude --worker-cli claude --repos-dir /path/to/repos

# Dashboard is at the URL printed on startup (default http://127.0.0.1:8737).
```

## `mesh up` — one-command fleet bring-up

`mesh up` starts the coordinator, dashboard, and observe server idempotently,
then prints the dashboard URL and fleet arm status. It is the canonical way to
start a new mesh session or verify an existing one is healthy.

```
mesh up [--config FILE]
        [--dashboard-addr HOST:PORT] [--observe-addr HOST:PORT]
        [--planner-cli CMD] [--planner-model MODEL]
        [--worker-cli CMD]  [--worker-model MODEL]
        [--repos-dir DIR]
        [--review-role ROLE]
        [--budget USD]
        [--auto-experts on|off]
        [--jobs-addr HOST:PORT]
        [--github-repo owner/repo]
        [--json]
```

### Config file

Fleet settings can be written to a JSON file instead of exporting `MESH_*`
env vars by hand. The default path is `$MESH_DIR/config.json`
(`~/.mesh/config.json` if `MESH_DIR` is unset). Use `--config <path>` for a
custom location.

**Precedence (lowest → highest):** config file → environment variables → CLI flags.

The spawned coordinator inherits the current process's environment, so settings
applied at `mesh up` time take effect in the coordinator on first bring-up.

Example `~/.mesh/config.json`:

```json
{
  "plannerCLI":   "claude",
  "plannerModel": "sonnet",
  "workerCLI":    "claude",
  "workerModel":  "sonnet",
  "reposDir":     "/home/alice/projects",
  "reviewRole":   "reviewer",
  "budgetUSD":    50.0,
  "autoExperts":  false,
  "githubRepo":   "alice/my-project"
}
```

All fields are optional. Unset fields leave the corresponding `MESH_*` env var
untouched, so you can mix config file and env vars freely.

| JSON field      | Env var                  | Description                                           |
|-----------------|--------------------------|-------------------------------------------------------|
| `plannerCLI`    | `MESH_PLANNER_CLI`       | CLI the triage planner drives; empty = triage off     |
| `plannerModel`  | `MESH_PLANNER_MODEL`     | `--model` for planner (default `sonnet`)              |
| `workerCLI`     | `MESH_WORKER_CLI`        | CLI the worker scheduler drives; empty = scheduler off|
| `workerModel`   | `MESH_WORKER_MODEL`      | `--model` for workers (default `sonnet`)              |
| `reposDir`      | `MESH_REPOS_DIR`         | Dir mapping repo names → git checkouts (workers)      |
| `reviewRole`    | `MESH_REVIEW_ROLE`       | Role that reviews worker diffs; empty = gating off    |
| `budgetUSD`     | `MESH_BUDGET_USD`        | Fleet budget cap in USD; 0/omit = unlimited           |
| `autoExperts`   | `MESH_AUTO_EXPERTS`      | Auto-spawn experts on demand (`on`/`off`)             |
| `jobsAddr`      | `MESH_JOBS_ADDR`         | HTTP listen address for `POST /jobs`; empty = off     |
| `githubRepo`    | `MESH_GITHUB_REPO`       | `owner/repo` for `mesh work` NL control               |
| `dashboardAddr` | `MESH_DASHBOARD_ADDR`    | Dashboard listen address (default `127.0.0.1:8737`)   |
| `observeAddr`   | `MESH_OBSERVE_ADDR`      | Observe server listen address (default `127.0.0.1:8739`) |

### Example output

```
mesh up (/home/alice/.mesh)
coordinator: started (bus /home/alice/.mesh/bus.sock)
dashboard:   http://127.0.0.1:8737  (started, pid 12345)
observe:     http://127.0.0.1:8739  (started, pid 12346)
fleet:
  planner:   claude (model: sonnet)
  worker:    claude (model: sonnet)
  reviewer:  role "reviewer"
  experts:   manual (--auto-experts on to enable)
  budget:    $50.00
```

## Build

```sh
make build       # bin/meshd + bin/mesh
make test        # unit + cross-process e2e tests (~4s)
make ci          # fmt-check + build + vet + test (what CI runs)
```

## CLI reference

```
mesh up        bring up coordinator + dashboard; arm fleet; print dashboard URL
mesh join      register this agent (autostarts sidecar + coordinator)
mesh leave     deregister this agent
mesh who       show the roster
mesh status    post what this agent is doing
mesh claim     take a CAS lock on a file path (exit 6 if lost)
mesh release   release a claim
mesh announce  broadcast advisory intent
mesh note      append to the repo blackboard
mesh context   replay the repo's blackboard history
mesh ask       create an async ask ticket
mesh poll      collect an answer (exit 3 until ready)
mesh inbox     list questions accepted by this agent
mesh answer    answer an accepted ticket
mesh submit    record a top-level job
mesh work      natural-language job control (requires MESH_GITHUB_REPO)
mesh expert    run a resident expert that auto-answers role-routed asks
mesh ops       runtime health snapshot; ops doctor/down/clean
```

Exit codes: `0` ok · `1` error · `2` usage · `3` no-answer-yet · `4` no-such-ticket ·
`5` not-joined · `6` claim-lost · `7` dirty

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full system design and
[docs/decisions/DECISIONS.md](docs/decisions/DECISIONS.md) for the running
decisions log (newest-first; wins on conflicts with ARCHITECTURE.md).

Key concepts:
- **Sidecar** — one per agent; owns the unix socket the `mesh` CLI talks to.
- **Coordinator** — control plane only; registry, role-routing, policy. Never in the data path.
- **Dashboard** — pure observer; live SSE view of the mesh at `http://127.0.0.1:8737`.
- **Blackboard** — durable per-repo decision stream (`mesh note`/`mesh context`).
- **Experts** — resident agents that auto-answer role-routed asks.
- **Workers** — ephemeral agents the coordinator spawns per task in isolated git worktrees.
