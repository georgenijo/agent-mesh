# Aider mesh hooks (STUB — STRUCTURALLY BLOCKED)

**Status: STRUCTURALLY BLOCKED — hooks not implemented as of 2026-06-10.**

This directory is a placeholder for mesh hooks for Aider. The adapter
abstraction exists in `internal/cliexec` (AiderAdapter), but Aider's
`--json` output format is structurally incompatible with the adapter
interface as currently defined.

## Why Aider is structurally blocked

The `Adapter.Invoke` interface requires a **single JSON result envelope** on
stdout, parseable by `internal/runtime.ParseEvent` (the never-fake-success
discriminators). Aider's `--json` flag (github.com/Aider-AI/aider) emits
**streaming status-line JSON objects** — one per action — not a single
terminal envelope. A shim layer is required to collect Aider's output and
synthesise a compatible result, but this shim has not been designed or tested.

Additional gaps:
- Aider typically requires an API key for the underlying LLM provider (OpenAI,
  Anthropic, etc.), which differs from the subscription-OAuth pattern used for
  Claude Code workers. A key would represent cost outside the operator's
  subscription.
- No hook system analogous to Claude Code hooks exists.

## Hook parity table

| Hook equivalent | Status |
| ---- | ----- |
| Session join (SessionStart) | Not available — no hook system |
| Pre-edit claim guard (PreToolUse) | Not available — no hook system; guard must be pre/post-exec wrapper only |
| Inbox drain between turns (Stop) | Not available — Aider is single-turn headless; no Stop-equivalent |
| Session leave (SessionEnd) | Not available — drive via post-exec wrapper |

## Path to implementation

1. Design and test the Aider output-shim layer that translates streaming
   status JSON into a single result envelope.
2. Decide on the auth/API-key story (opt-in explicit API key, separate from
   subscription-OAuth path).
3. Implement pre/post-exec wrappers for join/leave and document the
   capability gaps honestly.
4. Update `internal/cliexec.AiderAdapter` from STUB to verified.

See GitHub issue #30.
