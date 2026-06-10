# Fleet feasibility — subscription rate-limits & ToS for concurrent `claude -p`

Resolves the **fleet-feasibility spike gate** (DECISIONS.md 2026-06-08: "P3 execution plan —
cheap-end-first, fleet-gated") that gates #25 (scheduler) and #26 (worker runtime). Follows up
the M0 spike (`docs/spikes/M0-feasibility.md`, run 2026-06-05), which answered the
*request-concurrency* question; this spike adds the ToS/policy dimension and re-verifies
concurrency on the current CLI.

Run date: **2026-06-09**. Environment: macOS, `claude` 2.1.170 (Claude Code), subscription
auth (no `ANTHROPIC_API_KEY`; `~/.claude.json` `billingType: stripe_subscription`). Account
plan: **Max 5x** (`organizationRateLimitTier: default_claude_max_5x`), **extra usage credits
NOT enabled** (`hasExtraUsageEnabled: false`).

---

## VERDICT: FEASIBLE-WITH-LIMITS

**Concurrency is not the constraint. Money is — starting 2026-06-15.**

- ToS/policy: spawning headless `claude -p` fleets on a subscription is **explicitly
  sanctioned** — it is a documented, first-party usage mode.
- Empirics: 16/16 one-shot invocations succeeded with **zero rate-limiting at up to 8
  concurrent** (M0 previously showed 30 concurrent clean). Near-perfect parallelism.
- **But:** on **June 15, 2026** (six days after this spike ran), `claude -p` / Agent SDK usage
  on subscription plans moves off the subscription's usage limits onto a **separate monthly
  Agent SDK credit, metered at standard API rates** — $100/month on this account's Max 5x
  plan. When the credit is exhausted and usage credits are not enabled (they are not, on this
  account), **programmatic requests stop until the monthly refresh**.

Safe operating envelope: **4–8 parallel workers** (host-bound, not API-bound), with the
scheduler treating the **monthly dollar budget as the scarce resource** — see Implications.

---

## Part 1 — ToS & usage-policy findings (cited)

### Hard facts (primary sources, fetched 2026-06-09)

1. **Headless `claude -p` is an official, documented usage mode.** The Claude Code docs page
   "Run Claude Code programmatically" (https://code.claude.com/docs/en/headless) documents
   `claude -p` with `--output-format json`, CI usage, scripted invocation, model pinning, and
   typed retry events. Programmatic use of the official CLI is not a ToS gray area — it is the
   product surface. (Third-party harnesses piping subscription OAuth tokens *outside* the
   official CLI/SDK were the thing Anthropic banned in Feb 2026, later reinstated under the
   credit-pool model; the mesh spawns the official `claude` binary, so this does not apply.)

2. **June 15, 2026 billing change (the load-bearing fact).** Anthropic Help Center, "Use the
   Claude Agent SDK with your Claude plan"
   (https://support.claude.com/en/articles/15036540-use-the-claude-agent-sdk-with-your-claude-plan,
   fetched 2026-06-09):
   - "Starting **June 15, 2026**, Claude Agent SDK and `claude -p` usage no longer counts
     toward your Claude plan's usage limits."
   - The credit covers: "Claude Agent SDK usage in your own projects (Python or TypeScript)",
     "The `claude -p` command in Claude Code (non-interactive mode)", "The Claude Code GitHub
     Actions integration", and "Third-party apps that authenticate with your Claude
     subscription through the Agent SDK."
   - Monthly credit by plan: **Pro $20 | Max 5x $100 | Max 20x $200** (Team Standard $20,
     Team Premium $100, Enterprise Premium $200/seat). Metered at **standard API rates**, no
     rollover.
   - "When your monthly credit runs out, additional Agent SDK usage flows to usage credits at
     standard API rates—but only if you've enabled usage credits. **If usage credits aren't
     enabled, Agent SDK requests stop until your credit refreshes.**"
   - "Using Claude Code in the terminal or your IDE continues to use your subscription usage
     limits exactly as before." (Interactive sessions are unaffected.)
   The headless docs page carries the same notice inline.

3. **Subscription usage limits (pre-June-15 regime, still governs interactive use).** Usage is
   shared across Claude and Claude Code on Pro/Max
   (https://support.claude.com/en/articles/11145838-use-claude-code-with-your-pro-or-max-plan);
   limits operate on a 5-hour rolling window plus weekly caps that scale by plan
   (https://support.claude.com/en/articles/11647753-how-do-usage-and-length-limits-work — the
   article confirms windowed limits exist but does not publish exact numbers).

4. **`--bare` trap (confirmed in official docs).** The headless docs now state "`--bare` is
   the recommended mode for scripted and SDK calls, **and will become the default for `-p` in
   a future release**" — and "Bare mode skips OAuth and keychain reads. Anthropic
   authentication must come from `ANTHROPIC_API_KEY` or an `apiKeyHelper`." This re-confirms
   the M0 warning (never pass `--bare` on subscription auth) and upgrades it to a tracked
   risk: when the default flips, our subscription-auth invocations could silently break
   unless we pin non-bare behavior.

### Inference / unpublished (flagged, not asserted)

- **Concurrency/rate-limit mechanics of the credit pool are unpublished.** Neither the help
  article nor the docs state an RPM/concurrent-session cap for credit-metered programmatic
  use. Secondary coverage (e.g. the community canonical-reference gist on the May 13
  announcement, https://gist.github.com/MagnaCapax/d9177e35b355853f03c730dfcaa693ef) flags
  "no published cooldown, queueing, or overage behavior" as an open edge case. Our empirical
  results below were measured **under the pre-June-15 regime** and may not predict throttling
  behavior after the cutover — re-measure after June 15.
- Per-prompt counts circulating for the 5-hour window (~10–45 prompts Pro, up to ~900 Max 20x)
  come from third-party write-ups (truefoundry.com, allthings.how, 2026 editions), not
  Anthropic; treat as indicative only. After June 15 they stop mattering for the fleet anyway.

---

## Part 2 — Empirical concurrency ladder (run 2026-06-09)

**Method:** one-shot `claude -p "Reply with exactly: ok" --output-format json --model haiku`
from a clean `/tmp` cwd (no CLAUDE.md auto-load), subscription auth, **no `--bare`**.
Sequential baseline ×2, then waves of 2, 4, 8 launched simultaneously. 16 invocations total.
Recorded per call: exit code, wall time, `subtype`, `is_error`, `api_error_status`,
`duration_ms`, `total_cost_usd`, and stderr verbatim.

| wave | N (parallel) | ok | errored | rate-limited | wave wall | per-call wall min/max (ms) | API dur min/max (ms) |
|---|---|---|---|---|---|---|---|
| seq1 | 1 | 1 | 0 | 0 | 5.2s | 5142 | 2875 |
| seq2 | 1 | 1 | 0 | 0 | 4.0s | 3973 | 2510 |
| c2   | 2 | 2 | 0 | 0 | 6.5s | 4658 / 6445 | 2411 / 4368 |
| c4   | 4 | 4 | 0 | 0 | 6.8s | 5143 / 6741 | 2610 / 4211 |
| c8   | 8 | 8 | 0 | 0 | 6.9s | 5354 / 6869 | 2120 / 3411 |

**Findings:**

- **16/16 succeeded.** Every envelope: `subtype: "success"`, `is_error: false`,
  `api_error_status: null`, result text exactly `ok`, exit code 0. **No stderr output on any
  call** — there is no rate-limit/429/quota error text to quote because none occurred.
- **Near-perfect parallelism:** 8 concurrent calls completed in 6.9s wall vs ~32s if
  serialized (~4s baseline × 8). Per-call wall inflates only ~25–40% at 8-way (process
  spawn + local contention), consistent with M0's finding that the local machine, not the
  API, is the binding latency factor.
- **Cost accounting (the post-June-15 currency):** per-call `total_cost_usd` ranged
  **$0.0030–$0.0156** (variance = prompt-cache hits); the whole 16-call ladder totaled
  **≈ $0.19** at API rates. M0 measured ~$0.16 for a single trivial **Opus** call made from a
  repo dir with CLAUDE.md auto-load — two orders of magnitude per-call difference depending
  on model + loaded context.

**Consistency with M0:** M0 (2026-06-05) ran 10/20/30 concurrent with zero rate-limiting.
Today's 8-way ladder confirms nothing regressed for request-concurrency on CLI 2.1.170.

---

## Part 3 — Failure modes

**Observed in this run:** none — all 16 invocations clean.

**Identified (from policy + envelope contract), to be handled by #25/#26:**

| # | Failure mode | Signal | When |
|---|---|---|---|
| 1 | **Credit exhausted → hard stop** (usage credits not enabled on this account) | stream `system/api_retry` / final envelope error; headless docs enumerate error category `billing_error` | Any time after June 15 once $100/month is burned |
| 2 | Transient rate-limit / overload | error categories `rate_limit`, `overloaded`; `api_error_status` non-null; `retry_delay_ms` on `system/api_retry` events | Possible under the unpublished credit-pool mechanics |
| 3 | **`--bare` becomes the `-p` default** → OAuth/keychain skipped → auth failure (`authentication_failed`) on subscription accounts | Docs: "will become the default for `-p` in a future release" | Unknown future CLI release — pin/monitor |
| 4 | Default-model cost blowout (un-pinned `claude -p` defaults to Opus-tier ≈ 50–100× Haiku per call) | `total_cost_usd` in every JSON envelope | Immediately — always pin `--model` |

---

## Implications for #25 (scheduler) and #26 (worker runtime)

1. **Max parallel workers: 4–8, host-bound.** Both spikes show the API does not throttle at
   this scale; latency inflection is local process cost. Keep M0's cap ≈ CPU cores; no
   API-side reason to go lower. Re-validate concurrency once after June 15 (unpublished
   credit-pool mechanics).
2. **Budget is the first-class scheduling resource.** Every `--output-format json` envelope
   reports `total_cost_usd`. The scheduler should: accumulate spend per job/task/month,
   expose a configurable budget cap (e.g. `MESH_BUDGET_USD`), and stop dispatching (jobs stay
   queued, not failed) when the cap or a `billing_error` is hit. On this account the
   effective fleet budget is **$100/month, hard-stop** — at realistic worker-task costs
   (Sonnet/Opus, real context: $0.10–$2.00/task) that is roughly **50–1,000 worker tasks per
   month**, not unlimited.
3. **Backoff policy:** map error categories, never substring-match prose — `rate_limit` /
   `overloaded` → exponential backoff + retry (honor `retry_delay_ms` from `system/api_retry`
   events); `billing_error` → pause the whole fleet until credit refresh (do **not** retry);
   `authentication_failed` → halt and surface to the human (likely the `--bare` flip or
   logged-out keychain).
4. **Queue pacing:** no inter-launch delay needed for bursts ≤ 8 (this run) and ≤ 30 (M0);
   a simple semaphore at the worker cap suffices. Pacing exists to smooth *spend*, not RPS.
5. **Cost hygiene (mandatory, cheap):** always pin `--model` (haiku/sonnet for triage and
   small tasks; opus only where justified); run workers from clean cwds or with minimal
   CLAUDE.md to avoid M0's 18.9k-token context tax; never pass `--bare` (breaks subscription
   OAuth) — and pin CLI behavior/version when the bare-by-default release lands.
6. **Question for the user (not assertable by this spike):** whether to enable **usage
   credits** (pay-as-you-go overflow past the $100 credit — `hasExtraUsageEnabled` is
   currently false) and/or whether the locked "no API key in the core" decision should be
   revisited *economically* (post-June-15, programmatic subscription use is metered at the
   same API rates anyway; the remaining difference is key management and the $100 included
   credit, not price). The mesh design itself needs no change either way — only the
   scheduler's budget config does.
