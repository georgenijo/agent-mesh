# Agent Mesh — Repository Layout

Target structure for the Go implementation, designed for longevity and maintainability. Standard Go conventions (`cmd/` + `internal/`), one module, one shipped binary with modes. Directories under `internal/` are created **as each phase needs them** — this is the destination, not a day-one scaffold of empty folders.

See `components.md` for what each package implements and `concepts.md` for the vocabulary.

---

## Top-level tree

```
agent-mesh/
├── go.mod                      # module: github.com/georgenijo/agent-mesh
├── go.sum
├── Makefile                    # build / test / lint / run targets
├── README.md
├── CLAUDE.md
├── LICENSE
├── .gitignore
│
├── cmd/                        # entrypoints — thin mains, no logic
│   ├── meshd/                  #   the daemon: --sidecar | --coordinator | --dashboard
│   │   └── main.go
│   └── mesh/                   #   the CLI: join/who/status/ask/...
│       └── main.go
│
├── internal/                   # all private packages (not importable outside the module)
│   ├── envelope/               # §0 wire format: kinds, encode/decode, ids, result enums
│   ├── bus/                    # §1 NATS/JetStream wrapper: embedded server, streams, KV, CAS, TTL
│   ├── socket/                 # unix-socket server + client (shared by sidecar and CLI)
│   ├── sidecar/                # §2 per-agent daemon: NATS conn, heartbeat, ticket/claim cache
│   ├── coordinator/            # §3 control plane: registry, presence/lease, routing, policy, audit
│   ├── dashboard/              # §5 read-only observer: mesh.> tap + WS bridge + static serving
│   ├── observe/                # §5b ops plane: runtime snapshot collector + HTTP observe server
│   ├── cli/                    # §4 verb implementations (the body behind cmd/mesh)
│   ├── agentcard/              # agent card + registry types (role, caps)
│   ├── ticket/                 # ask-ticket FSM (states + legal transitions as data)
│   ├── claim/                  # file-claim + lease CAS logic
│   ├── config/                 # config loading + defaults
│   └── meshlog/                # structured logging helper (avoids clashing with stdlib log)
│
├── web/                        # dashboard frontend (production UI; embedded via go:embed)
│   ├── index.html              #   evolves from docs/mockups/dashboard-*.html
│   ├── app.js
│   └── style.css
│
├── hooks/                      # §6 agent-CLI integration glue (shipped, not compiled in)
│   ├── claude-code/            #   Claude Code lifecycle hooks
│   ├── codex/                  #   Codex CLI lifecycle hooks + optional launcher
│   └── README.md               #   per-CLI install instructions; parity caveats
│
├── deploy/                     # packaging (the multi-host / isolation path — later)
│   ├── docker-compose.yml
│   └── Dockerfile
│
├── scripts/                    # dev helpers (mesh up/down wrappers, seed, smoke)
│
├── test/                       # cross-process end-to-end tests (real boundaries, not mocks)
│   └── e2e/
│
├── prompts/                    # advisor-mode prompt(s)
│   └── PROMPT_CHAT.md
│
└── docs/                       # design + reference (source-controlled knowledge)
    ├── ARCHITECTURE.md         # (currently at repo root; may move here later)
    ├── concepts.md
    ├── components.md
    ├── repo-layout.md          # this file
    ├── audit-multi-agent-pm.md
    ├── decisions/
    │   ├── DECISIONS.md        # running log
    │   └── YYYY-MM-DD-*.md     # ADRs for deep one-off rationale (as needed)
    ├── reports/                # dated build + autonomous-run reports
    │   └── YYYY-MM-DD-*.{md,mdx,html}
    ├── spikes/                 # feasibility evidence (answered; kept as record)
    ├── mockups/                # HTML prototypes (no build step)
    │   ├── dashboard-bus.html
    │   ├── dashboard-full.html
    │   └── topology.html
    └── archive/                # completed one-off artifacts (not load-bearing)
```

---

## Why this shape

- **`cmd/` holds only entrypoints.** Each `main.go` parses flags and calls into `internal/`. No business logic in `cmd/` — keeps the binaries swappable and the logic testable. `meshd` and `mesh` are separate entrypoints over the same `internal/` packages (could later collapse to one binary dispatched by argv0).
- **`internal/` enforces privacy.** Go refuses imports of `internal/...` from outside the module, so nothing here leaks into a public API by accident. Everything is `internal/` until we *deliberately* want a reusable public package (then it graduates to `pkg/`). We have no `pkg/` yet — don't create one speculatively.
- **One package per component + the shared spine split out.** `envelope`, and the `bus` (subject taxonomy + KV authority) are the three things every other package depends on (`components.md` "shared spine"), so they're their own packages with no upward imports. `socket` is shared by `sidecar` and `cli` — extracted to avoid duplication.
- **`web/` is the production UI, `docs/mockups/` is the design prototype.** They're different artifacts: mockups are throwaway design references; `web/` is the real thing, `go:embed`-ed into the dashboard binary so it ships as one file. P4 ports the mockup's visuals into `web/`.
- **`hooks/` ships as glue, not code.** These are scripts/config installed into each agent CLI — versioned here, never compiled into `meshd`.
- **`test/e2e/` is first-class.** The audit's biggest lesson: green unit tests over mock stores hid a system that didn't run. Cross-process e2e tests (real NATS, real sockets, separate processes) are where "done" is proven.
- **`deploy/` is later.** Local-first runs as plain processes; Docker is the isolation/multi-host path, parked until needed.

## Conventions

- **Module path:** `github.com/georgenijo/agent-mesh`.
- **No logic in `cmd/`** — entrypoints wire flags → `internal/`.
- **Import direction:** `cmd → internal/<component> → internal/{envelope,bus,socket}`. The spine packages never import a component package (no cycles).
- **`internal/` only** until a package is deliberately made public; then move to `pkg/`.
- **One file owns the wire format** (`internal/envelope`) — encode and decode co-located (audit rule).
- **Tests beside code** (`foo_test.go`) for units; **`test/e2e/`** for cross-process flows.
- **Frontend embedded** via `go:embed web/*` so the binary is self-contained.

## Phase → directories that appear

- **P0** (presence): `cmd/{meshd,mesh}`, `internal/{envelope,bus,socket,sidecar,coordinator,cli,agentcard,config}`, minimal `web/`.
- **P1** (announce + blackboard): `internal/{claim}`; blackboard lives in `bus` (streams).
- **P2** (ask/answer): `internal/{ticket}`; `hooks/claude-code/`.
- **P3** (experts/caching/multi-CLI): `hooks/codex/` and more per-CLI adapters; cache/policy inside `coordinator`.
- **P4** (live dashboard): flesh out `web/`; `internal/dashboard` switches the mockup feed to the live tap.
- **later**: `deploy/`.
