# Agent Mesh — Architecture

> A local-first coordination fabric that lets heterogeneous coding agents
> (Claude Code, Codex CLI, Cursor CLI, Aider, …) discover each other, share
> status, ask each other questions, and avoid stepping on each other's work —
> through a single `mesh` command-line tool.

---

## 1. Vision

You run several coding agents at once. Today they're blind to each other: two
will edit the same file, a third re-derives a fact a fourth already figured out,
and you babysit all of them. The Agent Mesh gives them a **shared nervous
system** so they can:

- **announce** what they're working on (conflict avoidance),
- **ask** a question and get an answer from whichever agent (or human) knows,
- **read a shared blackboard** of decisions so knowledge spreads passively,
- be **observed** from one live dashboard.

The whole thing is opt-in *per message*, not forced *per turn*. Agents work
solo 95% of the time and reach into the mesh only when work overlaps or they're
stuck.

---

## 2. Design principles

1. **CLI at the edge.** Agents interact through one `mesh` binary, not an MCP
   tool surface. Cheaper on context (no schemas injected every turn), universal
   (every agent can run bash), composable (pipe/script it).
2. **The bus is the wire.** Agents address *subjects*, never each other by
   hostname. NATS fans out. Adding an agent rewires nothing.
3. **Async by default.** Asking costs an LLM turn on the responder's side. So an
   `ask` returns a ticket immediately; the asker keeps working and collects the
   answer later. Never block a turn waiting.
4. **Control plane ≠ data plane.** The coordinator decides *who* should answer
   and enforces policy; the actual question/answer payload travels *directly*
   between agents. The hub is never a throughput bottleneck.
5. **Isolation by container.** Each agent + its sidecar run in (or alongside) a
   small container: resource caps, clean lifecycle, crash recovery, language
   independence.
6. **Local-first.** Everything runs on one machine over a unix socket + local
   NATS. No cloud dependency. Scales to multi-host later by pointing sidecars at
   a remote NATS.

---

## 3. Topology

```
                         ┌──────────────────────────────┐
                         │   NATS  (JetStream)  = BUS    │
                         │   subjects: mesh.>            │
                         └──┬───────┬───────┬───────┬────┘
        control plane ......│.......│.......│.......│..... data plane
                            │       │       │       │
                    ┌───────┴─┐ ┌───┴───┐ ┌─┴─────┐ │  ┌──────────┐
                    │ sidecar │ │sidecar│ │sidecar│ └──┤coordinator│
                    └────┬────┘ └───┬───┘ └───┬───┘    │ (control) │
                 unix    │          │         │        └────┬─────┘
                 socket  │          │         │             │
                    ┌────┴────┐ ┌───┴───┐ ┌───┴───┐    ┌────┴─────┐
                    │ Claude  │ │ Codex │ │Cursor │    │dashboard │
                    │  Code   │ │  CLI  │ │  CLI  │    │(observer)│
                    └─────────┘ └───────┘ └───────┘    └──────────┘
                       runs `mesh` CLI ───────────────► taps mesh.> stream
```

- **Sidecar daemon** — one per agent. Registers the agent, emits heartbeats,
  owns the unix socket the `mesh` CLI talks to, bridges to NATS.
- **NATS bus** — the fabric. Pub/sub + request/reply + durable streams.
- **Coordinator** — *control plane only*. Maintains the registry, makes routing
  decisions for role-addressed asks, enforces policy (rate limits, dedup), runs
  the tie-breaker for consensus. Not in the data path.
- **Expert pool** — on-demand responder agents the coordinator can spawn when no
  live agent owns a topic (warm Agent SDK sessions preferred over cold spawns).
- **Dashboard** — pure observer, subscribes `mesh.>`, renders the live view.

---

## 4. The `mesh` CLI (the whole agent-facing API)

```
mesh join   --name <id> --role <role> --caps a,b,c   # usually auto-run by sidecar at boot
mesh leave                                            # graceful exit

mesh who                       [--json]               # roster + capability cards
mesh status "<text>"                                  # post what I'm doing now (also heartbeat)
mesh announce "editing EventForm.tsx" [--repo R]      # broadcast intent (conflict avoidance)

mesh ask --role auth "RLS recursion fix?"             # async → prints a ticket id, returns now
mesh ask --to codex-7 "..." [--wait]                  # direct; --wait blocks until answered
mesh poll <ticket>             [--json]               # did my ask get answered yet?

mesh inbox                     [--json]               # questions addressed to me
mesh inbox --watch                                    # stream pending questions (long-poll)
mesh answer <ticket> "use is_admin() SECURITY DEFINER"

mesh note "chose is_admin() for RLS"   [--repo R]     # write a decision to the blackboard
mesh context                   [--repo R] [--json]    # read the blackboard / shared decisions
```

Conventions:
- Every command supports `--json` for machine parsing; default is human text.
- Exit codes: `0` ok, `3` no-answer-yet (poll), `4` no-such-ticket, `5` not-joined,
  `6` dirty (`ops doctor`/`ops down`: drift found / teardown incomplete).
- The CLI is a *thin* client. It opens the unix socket, sends one request, prints
  the reply, exits. All state lives in the sidecar + NATS.

---

## 5. Subjects & message schema

| Subject | Type | Payload | Who listens |
|---|---|---|---|
| `mesh.register` | event | agent card `{id,name,role,caps,repo,cwd,model}` | coordinator |
| `mesh.heartbeat.<id>` | event | `{id, ts, status}` | coordinator (presence/TTL) |
| `mesh.leave` | event | `{id, reason}` | coordinator, dashboard |
| `mesh.status.<id>` | event | `{id, text, ts}` | dashboard |
| `mesh.announce.<repo>` | event | `{id, intent, paths[]}` | all agents on that repo |
| `mesh.ask.role.<role>` | req | `{ticket, from, q, ctx}` | coordinator → routes |
| `mesh.ask.id.<id>` | req | `{ticket, from, q, ctx}` | that agent's inbox |
| `mesh.inbox.<id>` | queue | pending asks for `<id>` | the owning agent |
| `mesh.answer.<ticket>` | reply | `{ticket, from, answer, ts}` | the original asker's sidecar |
| `mesh.note.<repo>` | **stream** | `{from, decision, ts}` (JetStream, durable) | `mesh context` readers |
| `mesh.>` | wildcard | everything | dashboard / observers |

`mesh.note.<repo>` is a **JetStream durable stream** — late-joining agents replay
the whole decision history with `mesh context`. Everything else is ephemeral
pub/sub.

---

## 6. Discovery & presence

- **Join:** sidecar boots → `PUB mesh.register` with the agent card → coordinator
  adds to registry. The agent itself does nothing; the daemon registers it.
- **Liveness:** sidecar `PUB mesh.heartbeat.<id>` every 5s. Coordinator TTLs each
  entry; miss 3 beats → mark dead → `PUB mesh.leave`. Crash = silent heartbeat
  stop = auto-detected.
- **Lookup:** any agent runs `mesh who` → request/reply to coordinator → current
  roster + capability cards. This is how Claude learns "Codex owns the auth
  subsystem" without any static config.

New container starts → auto-joins. Container dies → auto-removed. Zero manual
wiring.

---

## 7. Core message flows

### 7a. Status (heartbeat + "what I'm doing")
```
agent: mesh status "building RRULE builder"
  → sidecar PUB mesh.status.<id> + refreshes heartbeat
  → dashboard renders it. Fire-and-forget. ~microseconds.
```

### 7b. Async ask / answer (the important one)
```
1. claude: mesh ask --role auth "RLS recursion fix?"
     → sidecar mints ticket T1, PUB mesh.ask.role.auth {T1,...}
     → CLI prints "T1" and EXITS. Claude keeps working.            (no block)

2. coordinator routes T1 → codex (owns auth) → mesh.inbox.codex

3. codex's Stop-hook runs `mesh inbox` between turns → sees T1
     → codex: mesh answer T1 "use is_admin() SECURITY DEFINER"
     → sidecar PUB mesh.answer.T1                                  (one LLM turn — the real cost)

4. claude's sidecar cached the answer. Next turn Claude runs
     mesh poll T1 → gets the answer (or a hook injects it).
```
The only latency is step 3 (an LLM turn). Transport on 1/2/4 is sub-millisecond.

### 7c. Conflict avoidance (highest-value, cheapest)
```
codex: mesh announce "editing EventForm.tsx" --repo stbasils
  → PUB mesh.announce.stbasils
  → cursor's hook sees it before opening the same file → defers or coordinates.
No merge hell. No LLM turn required — pure pub/sub.
```

### 7d. Blackboard / stigmergy (passive collaboration)
```
claude: mesh note "events store UTC; convert at display via event-time.ts"
  → appended to JetStream mesh.note.stbasils
later:
cursor: mesh context --repo stbasils
  → replays all notes → conforms to the convention without asking anyone.
```

---

## 8. Performance model

```
ask published → bus transit → responder THINKS → answer back
   ~µs            ~µs            5–60 s             ~µs
   free           free           THE ENTIRE COST    free
```

NATS sustains millions of msgs/sec, sub-ms. **Transport is never the
bottleneck.** The cost is the responder's LLM turn. Therefore:

- **Async fire-and-continue** keeps that cost off the asker's critical path.
- **Warm experts** (persistent Agent SDK sessions) avoid cold context reloads.
- **Cheap router** — routing decisions are rules or a Haiku-tier call, not Opus.
- **Semantic cache** — repeat questions return cached answers, zero LLM cost.
- **Asks are the exception** — most turns touch the mesh only for a fire-and-forget
  `status`/`announce`, which is free.

Conclusion: normal agent work is **not** slowed. The mesh adds load only when an
agent genuinely needs help, and even then off the hot path.

---

## 9. Collaboration policy (when to use the mesh)

**Encourage** via:
- `mesh ask`/`announce` as prominent, well-described CLI verbs.
- **Hooks** that detect struggle (test-fail loop, repeated edits to one file,
  low-confidence language) and nudge: "ask the mesh?"
- **Blackboard reads** on task start so knowledge spreads without explicit asks.
- **Role addressing** (`--role auth`) — lower friction than naming a target.

**Guard against:**
- Chatty over-asking (token + latency cost) → rate-limit per agent, dedup asks.
- Consensus loops / bikeshedding → coordinator is the tie-breaker.
- Mesh overhead on independent tasks → it's opt-in; solo is the default.

**Rule of thumb:** collaboration pays when work is *interdependent* (shared repo,
shared conventions, overlapping files) and is dead weight when it isn't.

---

## 10. Isolation & security

- Each agent + sidecar in a small container (or a process group with cgroup
  limits). CPU/mem caps, filesystem scoped to its repo, clean teardown.
- `mesh` ↔ sidecar over a **unix socket** with file permissions — not a TCP port.
- NATS secured with per-sidecar credentials; subjects authorized per agent
  (an agent may publish `mesh.answer.*` only for tickets in its inbox).
- Coordinator enforces rate limits and an audit log (every `ask`/`answer`/`note`).
- Secrets never cross the bus; payloads carry references, not credentials.

---

## 11. Tech choices

| Concern | Choice | Why |
|---|---|---|
| Agent interface | **`mesh` CLI** | context-cheap, universal, composable |
| Transport / bus | **NATS + JetStream** | tiny binary, pub/sub + req/reply + durable streams |
| Local IPC | **unix socket** | fast, permissioned, no open port |
| Isolation | **Docker / cgroups** | resource caps, lifecycle, crash recovery |
| Agent semantics | **A2A-style agent cards** | capability advertisement + task delegation |
| Warm responders | **Claude Agent SDK** sessions | skip cold-spawn context reload |
| Sidecar + CLI lang | **Go or Rust** | single static binary, easy to ship in a container |

---

## 12. Build phases

> **Revised 2026-06-05 (cheap-core-first).** The original order built async
> ask/answer (P1) before the collaboration primitives (P2). Reversed: the cheap,
> no-LLM-in-the-loop, guaranteed-value primitives ship first; the expensive,
> hook-dependent, hallucination-compounding ask/answer vertical is deferred until
> the cheap core proves the thesis. Built in **Go** with an embedded NATS server.
> See `docs/decisions/DECISIONS.md` (2026-06-05).

- **P0 — Walking skeleton (presence).** Go `meshd` with embedded JetStream, one
  sidecar, `mesh join/who/status` + heartbeat lease, dashboard tails `mesh.>`.
  Prove registration + presence across a real process boundary.
- **P1 — Conflict avoidance + blackboard (cheap core).** `announce` (advisory
  pub/sub) **+ CAS file-claims** (real single-winner locks), `note/context`
  (JetStream blackboard). TTL leases + reclaim-on-death. Pure pub/sub + durable
  stream — zero LLM turns.
- **P2 — Async ask/answer (expensive vertical).** `ask/inbox/answer/poll`,
  ticket FSM validated in the write path, coordinator role-routing (pull/CAS
  self-claim). Wire one Claude Code hook to auto-`inbox`. **Start homogeneous**
  (Claude Code only) to solve hook parity once.
- **P3 — Experts, caching & multi-CLI.** Warm expert pool, semantic answer
  cache, rate limits, dedup, audit log, per-CLI hook adapters (Codex/Cursor/Aider).
- **P4 — Live dashboard.** Replace the scripted mockup feed with a real
  WebSocket tap on `mesh.>`. The mockup becomes the production UI.

---

## 13. Open questions

- Mid-turn interruption: can we push an urgent answer *into* a running agent
  turn, or only between turns (hooks/poll)? Per-agent capability — varies by CLI.
- Routing intelligence: rules vs. a small classifier model for `--role` → agent.
- Multi-host: when (if) to graduate from one machine to a shared NATS cluster.
- Answer trust: do we verify a peer's answer (second opinion) before the asker
  acts on it? Adversarial-verify option for high-stakes asks.

**Resolved** (see `docs/decisions/DECISIONS.md`, 2026-06-05):
- *Advisory `announce` vs real lock* → both: `announce` stays advisory pub/sub,
  but actual file edits take a **JetStream KV revision-CAS** lock. CAS is the one
  claim primitive for both file-claims and ask-tickets.
- *Crashed agent holds a claim forever* → every claim/presence record is a
  **TTL lease** renewed by heartbeat, with reclaim-on-death.
- *Language* → **Go**, embedded NATS.
