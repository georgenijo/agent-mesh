# Codex CLI mesh hooks (STUB)

**Status: UNVERIFIED — hooks not implemented as of 2026-06-10.**

This directory is a placeholder for mesh hooks for the OpenAI Codex CLI
(`codex exec`). The adapter abstraction exists in `internal/cliexec`
(CodexAdapter), but the CLI contract has not been verified against a real
binary and the structured-output mode is unknown.

## What is known

- Codex CLI supports `codex exec` for non-interactive execution.
- A `--json` / `--output-format` flag for a result envelope has not been
  documented or verified.
- No hook system equivalent to Claude Code's PreToolUse / Stop /
  SessionStart / SessionEnd hooks has been identified.

## Hook parity table

| Hook equivalent | Status |
| ---- | ----- |
| Session join (SessionStart) | Not available — drive via wrapper script around `codex exec` |
| Pre-edit claim guard (PreToolUse) | Not available — no hook system; guard must be pre/post-exec only |
| Inbox drain between turns (Stop) | Not available — no Stop hook; multi-turn flow is not documented |
| Session leave (SessionEnd) | Not available — drive via wrapper script post-`codex exec` |

## Path to implementation

1. Verify `codex exec` structured-output flags and result envelope shape.
2. Map the output to `internal/runtime.ParseEvent` discriminators (or write a
   shim if the envelope differs).
3. Implement mesh claim-guard as a pre/post-exec wrapper (since there is no
   PreToolUse hook) and document the capability gap honestly.
4. Update `internal/cliexec.CodexAdapter` from STUB to verified.

See GitHub issue #30.
