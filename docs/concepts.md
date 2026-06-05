# Agent Mesh — Concepts & Glossary

Plain-language reference for the building blocks. Read top-to-bottom: each term builds on the ones above it.

---

## Daemon

A program that runs **continuously in the background**, waiting to do work, instead of running once and exiting. "Daemon" is the Unix word for a long-running background service — the `d` suffix means daemon (`dockerd`, `sshd`).

In agent-mesh the **sidecar** and **coordinator** are daemons (alive for hours). The **`mesh` CLI is not** — it runs for milliseconds and exits.

---

## NATS

A **message broker** — software whose only job is: someone publishes a message to a *subject*, everyone subscribed to that subject receives a copy. Like a post office: drop a letter addressed to `mesh.ask.auth`, NATS delivers it to every listener on that address.

Plain NATS is **fire-and-forget**: if nobody is listening when you publish, the message is gone. We use it for ephemeral traffic — `status`, `heartbeat`, `announce`.

We run NATS off-the-shelf (it's one small binary), or **embed it** inside our own binary (NATS is a Go library). We do **not** build a broker.

## Subject

The "address" a NATS message is published to. Hierarchical, dot-separated, supports wildcards:

- `mesh.status.codex` — exact.
- `mesh.ask.role.auth` — role-addressed ask.
- `mesh.>` — wildcard, "everything under mesh" (how the dashboard taps the whole bus).

Agents address **subjects, never each other by hostname**. Adding an agent rewires nothing.

## JetStream

A feature **inside** NATS that adds durability. Where plain NATS forgets, JetStream **writes messages to disk and keeps them**, so you can:

- **replay** history ("show me every `note` ever written" → `mesh context`),
- survive restarts,
- get acknowledgements (did it actually persist?).

Mental model: **NATS = the live wire. JetStream = the wire + a tape recorder + a filing cabinet.**

Durable things go on JetStream (`note`s, ticket lifecycle events, audit log). Ephemeral things stay on plain NATS.

## KV bucket

JetStream also provides a **key-value store** — a tiny built-in database. A **bucket** is one named collection of key→value pairs (like a folder, a table, or a JS object).

```
bucket "claims":   "EventForm.tsx" → { agent: "codex", ts: … }
bucket "registry": "codex"         → { role: "auth", caps: […] }
bucket "tickets":  "T1"            → { state: "accepted", from: "claude" }
```

Two features we lean on:
- **Atomic compare-and-set (CAS)** — the single-winner claim/lock primitive (see *CAS*).
- **Per-key TTL** — a key auto-expires unless renewed (see *Lease / TTL*).

**This is why agent-mesh needs no separate Postgres/Redis** — JetStream KV is the database part, in the same binary as the bus.

---

## CAS (compare-and-set)

An atomic write that only succeeds if the value hasn't changed since you read it ("set key X to Y **only if** its revision is still R"). It's how two racing agents get a single winner with no separate lock service: whoever's CAS succeeds owns it; the loser's CAS fails and they back off. We return a **typed** result — `claimed | lost | error` — so a sidecar retries on a transport error but not on a legitimate loss.

## Lease / TTL

A claim or presence record that **expires on its own** unless actively renewed. TTL = "time to live." Heartbeats renew the lease; if an agent crashes, it stops renewing, the lease expires, and the system auto-cleans up. This fixes the audit's worst bug: a crashed agent that holds a file claim **forever** because release only happened on a graceful "I'm leaving" message that a crash never sends.

---

## Control plane vs data plane

- **Control plane** = decisions and bookkeeping: who's registered, which role should answer, policy/rate-limits. This is the **coordinator**.
- **Data plane** = the actual payloads moving between agents (the question text, the answer).

The law: **the coordinator decides *who*, never carries the *what*.** It computes a subject and updates the registry; the question/answer travels agent-to-agent over the bus directly. Keeps the coordinator from becoming a bottleneck.

## Local-first

Everything runs on **one machine** over loopback + unix sockets. No cloud dependency. Multi-host is a later option (point sidecars at a remote NATS), not a requirement.

---

## Sidecar

A **helper daemon that runs right next to each agent** and handles all the "talk to the mesh" work, so the agent CLI doesn't have to. (Name: a motorcycle sidecar — bolted on, carries a passenger, isn't the bike. The bike = the agent CLI.)

**What it does for us:**
- Holds the persistent **NATS connection** (the CLI can't — it exits too fast).
- Sends **heartbeats** to renew the agent's presence lease.
- Owns the **unix socket** the `mesh` CLI connects to.
- Remembers **this agent's tickets and file-claims** — state the short-lived CLI cannot hold.

**One sidecar per agent.** It is the persistent memory + connection that the millisecond-lived CLI lacks.

## Coordinator

The single **control-plane daemon**. Maintains the registry, routes role-addressed asks (computes the subject), enforces policy (rate-limit, dedup), runs lease-eviction, writes the audit log. **Not in the data path.** One per mesh.

## Dashboard

A **read-only observer**: a small web server that subscribes `mesh.>`, bridges events to a browser over WebSocket, and serves the UI. Renders roster, presence, status, claims, ticket lifecycle, notes. Pure view — it never writes to the mesh.

## Hooks

Small scripts wired into each agent CLI's config that **call `mesh` automatically** at the right moments:
- **pre-edit hook** — before the agent edits a file, check/claim it (conflict avoidance).
- **between-turn hook** — drain the inbox so the agent answers pending asks.

Hooks are **config/glue, not a daemon.** Cross-CLI hook parity (Claude Code vs Codex vs Cursor vs Aider) is the project's #1 integration risk.

---

## meshd

The name for **our compiled binary** ("mesh daemon"). One binary, several **modes** chosen by a flag:

```
meshd --sidecar      → run as a sidecar (one per agent)
meshd --coordinator  → run as the coordinator (one total)
meshd --dashboard    → run as the dashboard server (one total)
mesh  <command>      → the short-lived CLI (same binary, CLI mode)
```

Same executable, different code path per launch (like `busybox`). We ship **one thing**, not four.

## mesh (the CLI)

The **agent-facing API** and the whole reason the project is context-cheap. A short-lived client: open the unix socket → send one request → print the reply → exit. Every command supports `--json`; exit codes carry meaning (`0` ok, `3` no-answer-yet, `4` no-such-ticket, `5` not-joined). It holds **no state** — all state lives in the sidecar + JetStream.

---

## Agent card

The capability advertisement an agent publishes on `join`: `{ id, name, role, caps[], repo, cwd, model }`. `role` is coarse (for subject addressing, `--role auth`); `caps[]` is fine-grained, **open data** (any string), exact-token matched — **not** a closed compile-time enum (a third-party CLI must be able to register a role it ships). Advertised caps are **claims to verify**, not ground truth.

## Ticket

The unit of an async ask. `mesh ask` mints a ticket id and **returns immediately**; the answer arrives later via `poll`/`inbox`. A ticket moves through a small **state machine** — `open → routed → accepted → answered → closed | expired` — validated **before** any publish, so a peer physically can't answer an already-expired or already-answered ticket. The JetStream event stream *is* the ticket's history.

---

## The runtime picture (3 agents)

```
NATS + JetStream          ← bus + store (or embedded in coordinator)
meshd --coordinator       ← control plane
meshd --dashboard         ← observer
meshd --sidecar  × 3      ← one per agent
the 3 agent CLIs          ← external; we integrate via hooks
mesh <cmd>                ← transient, on demand
```

~6 long-running processes, all on one machine, over localhost + unix sockets. No containers required for MVP; Docker is the later path to isolation and multi-host.

## How it becomes real

A folder of source files (Go recommended) → compiled to the `meshd`/`mesh` binary → launched several times with different flags. "Make a coordinator" literally = run `meshd --coordinator`; its code subscribes to subjects and loops reacting to messages. No cloud, no magic — "making it live" = starting those processes (which `mesh up` will automate).
