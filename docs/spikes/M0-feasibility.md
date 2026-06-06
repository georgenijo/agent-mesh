# M0 — Feasibility spike findings

Resolves GitHub issues #1 (subscription headless-fleet feasibility) and #2 (verify
headless + structured-output flags). Scope locked to **Claude Code only** for M0 and
the early phases (start homogeneous; generalize to other CLIs at P3, issue #30).

Run date: 2026-06-05. Environment: macOS, `claude` 2.1.165 (Claude Code), subscription
auth (no `ANTHROPIC_API_KEY`, no Bedrock/Vertex).

---

## #1 — Subscription headless-fleet feasibility → **GO**

**Question:** can the coordinator spawn many headless `claude -p` workers/experts on the
user's existing subscription, without an API key, without hitting rate limits or ToS walls?

**Method:** burst waves of N parallel `claude -p --output-format json --model haiku`
(tiny prompt, run from `/tmp` to avoid loading the repo's CLAUDE.md). Measured per-call
success/error/`api_error_status`, wall-clock, and latency distribution.

| wave | N (parallel) | ok | errored | rate-limited | wall | dur min/med/max (ms) |
|---|---|---|---|---|---|---|
| 1 | 10 | 10 | 0 | 0 | 6.0s | 2049 / 2238 / 3002 |
| 2 | 20 | 20 | 0 | 0 | 10.0s | 1931 / 2553 / 5017 |
| 3 | 30 | 30 | 0 | 0 | 12.3s | 2042 / 3241 / 4620 |

**Findings:**
- **30 concurrent headless calls succeeded with zero rate-limiting** (`is_error:false`,
  `api_error_status:null`, no 429/overload lines in stderr) on this account/plan.
- **Real parallelism**, not serialization: 30 calls finished in 12.3s vs ~2.4s for a single
  call (serial would be ~72s).
- **The binding constraint is the local machine, not the subscription API.** Latency
  inflates ~2x beyond ~10 concurrent because each `claude -p` is a full Node process
  (process-spawn + cold-start cost), not because the server throttles.
- Auth confirmed subscription/OAuth: `ANTHROPIC_API_KEY` unset, Bedrock/Vertex off.

**Decision:** subscription-only headless fleet is viable. **Recommended initial worker
concurrency cap ≈ number of CPU cores (start 8–10)** — tune to the host, not the API.

**Residual risk (track, don't block):** the test used Haiku + tiny prompts, exercising the
*request-concurrency* dimension. Real workers use Opus/Sonnet, long multi-turn sessions, and
large context — that exercises *tokens-per-minute* and the subscription *usage cap*, which
were not stressed here. Re-measure under realistic Opus worker load before raising the cap.
Fallback if usage caps bite: per-agent throttle + optional `ANTHROPIC_API_KEY` opt-in for
overflow (the cost would then leave the subscription — make it explicit, never silent).

---

## #2 — Claude Code adapter contract (verified, not from memory)

How the worker/expert runtime drives Claude Code in headless + structured-output mode.

### Invocation

```
claude -p \
  --output-format json \           # typed envelope (also: stream-json, text)
  --model <haiku|sonnet|opus|id> \ # pin a model; default is opus-4-8[1m] (expensive)
  [--append-system-prompt "<role/expertise>"] \  # expert vs worker = prompt + model
  [--system-prompt "<full override>"] \
  [--permission-mode <mode>] \     # tool-permission posture (see worker note)
  [--max-turns <N>] \              # bound a headless run
  [--session-id <uuid>] \          # stable id; pairs with resume for warm experts
  "<prompt>"
```

- **Auth:** subscription works when `ANTHROPIC_API_KEY` is unset (OAuth/keychain).
- **⚠️ Do NOT use `--bare`.** `--bare` forces auth to `ANTHROPIC_API_KEY` / apiKeyHelper
  only — *OAuth and keychain are never read*. It would break subscription-only workers.
  Reduce per-spawn context cost instead via a clean cwd (no CLAUDE.md) and/or
  `--exclude-dynamic-system-prompt-sections`.
- **Worker tool permissions (defer to #26):** a worker that edits files needs write tools.
  Since workers run in an isolated git worktree, `--permission-mode acceptEdits` (or
  `--dangerously-skip-permissions` inside the sandbox) is the likely posture — decide in the
  worker-runtime issue, not here.
- **Warm experts (#27):** `--session-id` + `--resume`/`--continue` reuse a session to skip
  cold context reload — the persistent-expert mechanism.
- **Status streaming (P4):** `--output-format stream-json --include-partial-messages` emits
  incremental chunks for a live dashboard tap.

### Result envelope (`--output-format json`)

Single JSON object. Fields the adapter maps onto our typed envelope:

| field | meaning |
|---|---|
| `type` | `result` |
| `subtype` | `success` \| error variants |
| `is_error` | boolean — **note: jq `.is_error // x` misfires** (jq treats `false` as empty) |
| `result` | the model's text output (the answer/work) |
| `session_id` | for resume / warm-expert reuse |
| `modelUsage` | per-model `{inputTokens, outputTokens, cacheRead/CreationInputTokens, costUSD, contextWindow, maxOutputTokens}` |
| `usage` | aggregate token usage |
| `num_turns`, `duration_ms`, `duration_api_ms`, `ttft_ms` | timing |
| `total_cost_usd` | subscription accounting (not real $ on a plan) |
| `stop_reason`, `terminal_reason` | `end_turn` / `completed` etc. |
| `api_error_status` | non-null ⇒ transport/API error (rate-limit signal) |
| `permission_denials` | tools the run was blocked from using |
| `uuid` | call id |

**Adapter rule (audit: never fake-success):** map `is_error:true` OR `subtype != success`
OR `api_error_status != null` to a typed error result — do not treat a non-success envelope
as an answer.

### Observed costs (subscription accounting)
- Opus-4-8[1m], trivial prompt **in the repo dir**: ~$0.16 (18.9k cache-creation tokens from
  CLAUDE.md auto-load) → run workers in a clean cwd to shed this.
- Haiku, trivial prompt, `/tmp`: ~$0.003–0.013 per call.

---

## Out of M0 scope (deferred by the Claude-only decision)
- Codex (`codex exec --json` / `--output-schema`) and cursor-agent adapters → **P3 #30**.
  Both are installed here (`codex` 0.137.0, `cursor-agent`) but parity is not assumed.
- Aider: not installed.
