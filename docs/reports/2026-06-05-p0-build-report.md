# P0 Build Report — Agent Mesh walking skeleton (presence)

**Date:** 2026-06-05
**Scope:** GitHub issues #3–#11 (P0 milestone), with #32 (runtime proxy spike) preserved untouched as evidence for P3.
**Result:** Working P0 spine. All acceptance commands pass; 9/9 test packages green (including race detector and cross-process e2e); gofmt/vet clean; `make ci` mirrors the GitHub Actions workflow.

---

## 1. What was built

A presence walking skeleton across **real process boundaries**: the `mesh` CLI talks to a per-agent **sidecar** over a permissioned unix socket; the sidecar talks to a **coordinator-embedded bus** over a second unix socket; the coordinator reduces presence events into an authoritative **registry KV**; a read-only **dashboard** taps `mesh.>` and serves a live page.

```
mesh CLI ──unix socket──▶ sidecar (1/agent) ──unix socket──▶ bus (in coordinator)
                                                                 │
                                              coordinator reducer ⇒ registry KV (authority)
                                                                 │
                                              dashboard ──taps mesh.> + polls KV──▶ browser (SSE)
```

### Acceptance flow (verified literally)

```sh
make test                                                  # all packages ok
mesh join --name test --role builder --caps go,backend    # autostarts sidecar + coordinator
mesh status "working"                                      # exit 0
mesh who --json                                            # shows agent live with lastStatus "working"
```

Killing a sidecar with SIGKILL marks the agent `away` after `MESH_AWAY_AFTER`, evicts it after `MESH_EVICT_AFTER`, announces the eviction as a `mesh.leave`, and leaves surviving peers untouched — proven end-to-end in `test/e2e`.

---

## 2. Decision resolved before coding (issue #5 gate)

Issue #5 required settling the reopened transport/language fork first. Logged in `docs/decisions/DECISIONS.md` (2026-06-05, supersedes "Transport reopened"):

> **P0 transport = Go + coordinator-embedded star bus over a unix socket.** No embedded NATS, no external broker. `internal/bus` provides pub/sub with NATS-style subjects (`*`, `>`), KV buckets with revision-CAS and per-key TTL leases, and bounded streams. The JetStream-contingent parts of earlier locked decisions (CAS claim primitive, TTL leases, one-authority-per-fact) map onto bus equivalents unchanged. `bus.Client` is the seam if NATS ever returns (lateral peer comms / multi-host).

Transport-independent invariants preserved throughout: one versioned envelope, one authority per fact, typed result enums (never fake-success), async-never-block.

---

## 3. Packages delivered (~6,200 LOC, zero external dependencies)

| Package | What it is |
|---|---|
| `internal/envelope` | The wire contract: versioned envelope (`schemaVersion`, `kind`, UUIDv7 `id`, `from`/`to`/`subject`/`ts`, typed payload), all 9 core kinds, encode+decode co-located, typed `DecodeError` codes (`unparseable`, `unsupported_version`, `unknown_kind`, `missing_field`, `kind_mismatch`, `invalid_payload`, `payload_too_large`), result enums (`claimed\|lost\|error`, `answered\|pending\|timed_out\|expired\|no_such_ticket`), subject taxonomy + bucket/stream names, 1 MiB payload bound at the publish edge. |
| `internal/agentcard` | Agent card (open-data role/caps, exact-token `HasCap`, `ValidName` guard for subject/path safety) + `RegistryRecord` + two-tier `PresenceState`. |
| `internal/config` | `$MESH_DIR` and timing knobs (`MESH_HEARTBEAT_INTERVAL`, `MESH_AWAY_AFTER`, `MESH_EVICT_AFTER`, `MESH_REGISTRATION_GRACE`), validated; socket path helpers. |
| `internal/bus` | The star bus/store: server (embedded in coordinator) + client. Pub/sub with wildcard matching; KV with revision-CAS (`CreateOnly()`/`Rev(n)`/unconditional) and TTL leases (lazy expiry + janitor); bounded streams (in-place trim); client auto-reconnect with resubscribe + `OnReconnect`; bounded outbound queues (slow consumer = disconnect, never unbounded RAM); store-name validation + distinct-name cap. |
| `internal/socket` | One-shot CLI↔sidecar IPC: one request → one reply → close; 0600 socket under 0700 dir; typed codes (`bad_request`, `not_joined`, `unavailable`, `internal`) and client errors (`ErrNoSocket`, `ErrProtocol`). |
| `internal/meshapi` | Shared verb names + typed args/results for the CLI↔sidecar hop (`MaxStatusLen` 4096). |
| `internal/sidecar` | Per-agent daemon: long-lived bus connection, boot registration, 5s heartbeat lease renewal (carries latest status), verb handlers, graceful `leave` → publish + daemon exit + socket removal, re-register on bus reconnect (the documented coordinator-restart recovery). |
| `internal/coordinator` | Control plane: embeds the bus server; single ordered subscription reduces register/leave/heartbeat/status into the registry KV (single writer); two-tier janitor (live → away → evict) with registration grace; evictions published as `mesh.leave`; transitions appended to the bounded `audit` stream; TTL lease backstop on every record. |
| `internal/dashboard` | Read-only observer: `mesh.>` tap + 1s registry snapshot → SSE bridge → embedded single-file page (roster + live event tail); `/api/roster` for machines. Never publishes. |
| `internal/cli` | `join`/`leave`/`who`/`status` (+ `version`/`help`), `--json` everywhere, position-independent flags, socket discovery (`--socket` → `$MESH_SOCKET` → single socket), exit codes 0/1/2/5 (3/4 reserved for P2). |
| `internal/autostart` | `mesh join` spawns a detached sidecar; a sidecar spawns the coordinator under an exclusive flock (exactly one winner among racers); daemon logs to `$MESH_DIR/logs/`; meshd resolved from `$MESH_MESHD` or sibling-of-binary only. |
| `internal/testsock` | Short socket paths for tests (darwin 104-byte unix path limit). |
| `cmd/meshd`, `cmd/mesh` | Thin entrypoints; `meshd --mode sidecar\|coordinator\|dashboard`. |
| `test/e2e` | Cross-process acceptance: real binaries, real daemons, the full join→status→who→tap→dashboard→SIGKILL→away→evict→leave story plus the exit-code contract. |

Also updated: `Makefile` (`build`, `test`, `test-race`, `e2e`, `fmt`, `fmt-check`, `ci`), `AGENTS.md` + `CLAUDE.md` (current state + commands), `docs/decisions/DECISIONS.md`.

---

## 4. Adversarial review (multi-agent workflow)

A 27-agent review workflow (4 dimension reviewers — concurrency, protocol/lifecycle, spec-compliance, robustness/security — each finding adversarially verified by an independent agent instructed to refute it) confirmed **12 findings**. All addressed:

### Critical (2, both fixed)
1. **Cross-subject event reordering.** Four separate subscriptions gave register/leave/heartbeat/status each their own delivery goroutine; a leave could be reduced before its own register, stranding a phantom "live" record. → Single ordered `mesh.>` subscription dispatched by kind. Regression test: 50× rapid register→leave always converges to gone.
2. **Forged presence mutations.** `handleLeave`/`handleHeartbeat`/`handleStatus` trusted the payload ID, so any bus client could evict or resurrect a peer. → All mutating reducers now require envelope `from` == payload id. Regression tests for forged leave/status/heartbeat.

### Major (4, all fixed)
3. Sidecar `handlePing` held the state mutex across a blocking bus round-trip (could freeze heartbeats + all verbs up to 5s) → snapshot under lock, I/O outside.
4. Re-register reset `RegisteredAt`, re-arming the grace window so a flapping agent could evade eviction → original `RegisteredAt` preserved.
5. Registry records had no store-level TTL despite the locked "presence is a TTL lease" decision → every record now carries a `2×(EvictAfter+RegistrationGrace)` lease backstop, renewed per heartbeat.
6. An oversized status/value could blow the 4 MB frame cap and silently kill connections → typed bounds at the edges: 4 KB status cap (sidecar), 1 MiB envelope payload cap (envelope), both typed errors.

### Minor (6 — 5 fixed, 1 accepted)
7. Direct live→evict jump skipped the `away` audit event → two-tier sequence now always emitted.
8. Heartbeat carried status text the coordinator ignored (status lost across coordinator restart) → heartbeats now refresh `lastStatus`.
9. Lazily-created KV buckets/streams unbounded by name count → validated names + cap of 64 each.
10. `meshd` resolution fell back to `$PATH` (search-path trust hole for a detached, session-leading spawn) → PATH fallback removed; `$MESH_MESHD` or sibling-of-binary only.
11. Stream trim reallocated the full backing array per append at capacity → in-place slide.
12. **Accepted:** `unavailable` (sidecar up, bus down) maps to generic exit 1 — within the documented 0/2/3/4/5 contract; `--json` carries the typed code. Documented in `internal/cli/cli.go`.

---

## 5. Test inventory

| Layer | Coverage |
|---|---|
| `envelope` | Round-trip all 9 kinds; malformed-input table (typed, never panics); explicit unknown version/kind; forward-compat unknown fields; UUIDv7 ordering/uniqueness; JSON shape; payload-too-large. |
| `bus` | Wildcard pub/sub; CAS race (20 racers, 4 conns → exactly 1 winner); revision-guard; TTL expire+renew; bounded stream retention; deterministic stop; reconnect+resubscribe; unsubscribe; invalid envelope rejected at publish edge; store-name validation/cap. |
| `socket` | Round-trip; typed errors; 0600 perms; protocol garbage; version rejection; goroutine-leak-free stop. |
| `coordinator` | 3-agent registration; status updates; two-tier eviction with evict announcement; grace window; away-recovery; graceful leave; audit sequence; **hardening:** forged leave/heartbeat/status rejected; register→leave ordering; `RegisteredAt` preservation; heartbeat-carries-status. |
| `sidecar` | Boot registration; heartbeats outlive away window; idempotent rejoin with role update; wrong-agent join rejected; oversized status rejected; leave deregisters + signals exit; coordinator-restart recovery via re-register. |
| `dashboard` | Roster endpoint; live SSE status event; read-only (stop does not disturb registry). |
| `cli` | Usage/exit-code matrix without daemons. |
| `e2e` (cross-process) | Full acceptance flow + exit-code contract; binaries built in TestMain; daemon logs dumped on failure. |

Final state: `make ci` green, `go test -race ./internal/...` green, `gofmt -l .` empty.

---

## 6. GitHub issue status

| Issue | Status | Note |
|---|---|---|
| #3 CI | **Partial** | Workflow pre-existed; Makefile parity + docs added. Needs one green remote run after push to close. |
| #4 envelope | **Satisfied** | Note posted. |
| #5 bus/store | **Satisfied** | Fork settled + logged first, per the issue's gate. Note posted. |
| #6 socket | **Satisfied** | Note posted. |
| #7 sidecar | **Satisfied** | Note posted. |
| #8 coordinator | **Satisfied** | Note posted (incl. review hardening). |
| #9 CLI | **Satisfied** | Note posted. |
| #10 dashboard | **Satisfied with deviation** | SSE instead of WebSocket (documented on the issue; P4/#31 revisits). |
| #11 e2e | **Satisfied** | Note posted. |
| #32 runtime proxy | **Closed previously (GO)** | Spike untouched: `docs/spikes/runtime_proxy_spike.py` + `runtime-proxy.md` preserved as the P3 substrate. |

No issues were closed (per instruction: completion notes only).

---

## 7. Deviations from the docs (all deliberate)

- **SSE instead of WebSocket** for the P0 dashboard bridge (zero-dep, same observer semantics).
- **Two packages beyond `docs/repo-layout.md`:** `internal/meshapi` (shared CLI↔sidecar verb types — prevents drift) and `internal/autostart` (spawn/election logic shared by CLI and meshd).
- **No `$PATH` fallback** for meshd autostart (security: detached session-leading spawn from a writable PATH entry).
- **`web/` not created yet** — the P0 observer page is embedded in `internal/dashboard`; `web/` stays reserved for the P4 production UI.

## 8. Constraints honored

No workers, no experts, no scheduler/DAG, no ask/answer, no claims/blackboard, no LLM calls, no TUI scraping, no OAuth-to-API-key conversion. Runtime proxy spike untouched.

---

## 9. What's next (P1+ is now mechanical)

1. **Commit + push** — nothing is committed yet; pushing also gets #3 its green CI run. (Working tree also holds pre-existing uncommitted docs/spikes/mockups.)
2. **P1 — announce + claims + blackboard:** claims = `KVPut` with `CreateOnly()` + TTL on a `claims` bucket (primitive exists and is race-tested); blackboard = `notes` stream + disk persistence in `internal/bus`; new verbs follow the `meshapi` pattern; reclaim-on-death joins the coordinator sweep.
3. **P2 — ask/answer:** `internal/ticket` FSM validated in the write path; `inbox` bucket; exit codes 3/4 already reserved; Claude Code Stop-hook drains `mesh inbox`.
4. **P3 — worker/expert runtime:** port `runtime_proxy_spike.py` semantics into `internal/agentruntime` (resident stream-json supervisor with typed crash events, `--resume` recovery); coordinator gains spawn.
5. **P4 — production dashboard:** promote `docs/mockups/dashboard-bus.html` into `web/`, fed by the existing tap (decide SSE vs WS then).
