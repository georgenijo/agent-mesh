# Cursor CLI mesh hooks (STUB)

**Status: UNVERIFIED — hooks not implemented as of 2026-06-10.**

This directory is a placeholder for mesh hooks for the Cursor CLI. The adapter
abstraction exists in `internal/cliexec` (CursorAdapter), but Cursor's
headless CLI mode is undocumented for programmatic use, and no structured-output
contract has been verified.

## What is known

- Cursor has a background-agent mode and cursor rules.
- A scriptable headless CLI invocation with a structured JSON output mode has
  not been publicly documented.
- No hook system analogous to Claude Code's PreToolUse / Stop / SessionStart /
  SessionEnd hooks has been identified for programmatic integration.

## Hook parity table

| Hook equivalent | Status |
| ---- | ----- |
| Session join (SessionStart) | Not available — Cursor manages its own session lifecycle |
| Pre-edit claim guard (PreToolUse) | Not available — no programmatic hook protocol identified |
| Inbox drain between turns (Stop) | Not available — inter-turn hook protocol not documented |
| Session leave (SessionEnd) | Not available — same as SessionStart |

## Path to implementation

1. Verify whether `cursor` has a headless invocation mode that emits a parseable
   result envelope.
2. Map the output to `internal/runtime.ParseEvent` discriminators.
3. Document any hook equivalents where they exist.
4. Update `internal/cliexec.CursorAdapter` from STUB to verified.

See GitHub issue #30.
