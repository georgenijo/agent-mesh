// Package cliexec defines the per-CLI adapter abstraction that maps the
// agent-mesh one-shot exec contract onto each supported CLI's actual flags and
// output format.
//
// # Contract
//
// Every CLI the mesh can drive for headless worker / planner / expert work must
// satisfy the Adapter interface: accept a prompt string, run headlessly, and
// return structured bytes that internal/runtime can parse (a single JSON result
// envelope with the spike's never-fake-success discriminators).
//
// # Verified vs stubbed
//
//   - ClaudeAdapter — fully verified (docs/spikes/M0-feasibility.md, spike date
//     2026-06-05): `claude -p --output-format json [--model M] <prompt>`.
//     The structured result envelope shape is pinned in internal/runtime.
//
//   - CodexAdapter  — STUB. The public Codex CLI (`codex exec`) flags and
//     structured-output contract are unverified as of 2026-06-10. The adapter
//     satisfies the interface so a future implementer only fills in InvokeArgs;
//     calling it returns ErrNotImplemented with a pointer to follow-up issue #30.
//
//   - CursorAdapter — STUB. Cursor CLI headless mode unverified. Same pattern.
//
//   - AiderAdapter  — STUB. Aider's `--json` flag produces status lines, not a
//     single result envelope. Verified enough to know it cannot satisfy the
//     interface today without a shim layer; returns ErrNotImplemented with notes.
//
// # Hook parity
//
// Claude Code has four hooks (hooks/claude-code/): SessionStart, PreToolUse
// claim-guard, Stop inbox-drain, SessionEnd leave. Other CLIs vary:
//
//   - Codex CLI: no published hook system as of the stub date. Claim-guard and
//     inbox-drain must be driven by the worker frame (pre/post exec), not hooks.
//   - Cursor CLI: cursor rules / background agents exist but their hook protocol
//     (if any) is undocumented for programmatic invocation.
//   - Aider: no hook system; pre/post exec wrappers are the only option.
//
// The Capabilities struct surfaces what each adapter supports so callers can
// choose degraded-but-honest behaviour rather than silently skipping features.
package cliexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

// ErrNotImplemented is returned by stub adapters whose CLI contract has not
// been verified. It carries a human-readable message pointing at the tracking
// issue. Use errors.Is to detect it.
var ErrNotImplemented = errors.New("cliexec: adapter not implemented")

// Capabilities describes which mesh features an adapter supports. An adapter
// that cannot deliver a feature sets the relevant field to false/empty; callers
// degrade gracefully rather than treating absence as failure.
type Capabilities struct {
	// StructuredOutput is true when the CLI emits a single JSON result envelope
	// compatible with internal/runtime.ParseEvent (the never-fake-success
	// discriminators). False means the adapter cannot be used for
	// never-scrape-prose compliance.
	StructuredOutput bool

	// ModelPin is true when the CLI accepts a --model flag (or equivalent) that
	// the adapter honours. The locked fleet decision requires model pinning in
	// production; an adapter with ModelPin=false produces a log warning.
	ModelPin bool

	// HookPreToolUse is true when the CLI exposes a PreToolUse hook that
	// mesh-claim-guard can be wired into, enabling per-file claim checks before
	// any mutating tool call.
	HookPreToolUse bool

	// HookStop is true when the CLI exposes a Stop hook for the inbox-drain
	// pattern (answer pending asks between turns).
	HookStop bool

	// HookSessionLifecycle is true when the CLI fires SessionStart / SessionEnd
	// hooks, enabling automatic mesh join / leave.
	HookSessionLifecycle bool

	// Notes is a human-readable string explaining any capability gaps or
	// verification status. Intended for operator documentation and log output,
	// not for programmatic branching.
	Notes string
}

// InvokeOptions carries per-call options shared across all adapters.
type InvokeOptions struct {
	// Model is the model identifier to pin (e.g. "sonnet", "haiku"). Empty
	// means use the CLI's configured default. The locked fleet decision requires
	// a non-empty value in production.
	Model string

	// WorkDir is the child working directory. Empty inherits the parent.
	WorkDir string

	// Env is the child environment. Nil inherits the parent.
	Env []string

	// WaitDelay is the grace period after context cancellation before the
	// stdout pipe is forcibly closed. 0 defaults to 3 seconds (the same
	// hardening applied in the triage planner and CLIDriver).
	WaitDelay time.Duration

	// OnStart is called once the child process has started, with its OS PID.
	// Nil is a no-op. Adapters that actually start a child process (e.g.
	// ClaudeAdapter) MUST call this immediately after cmd.Start() succeeds so
	// callers can surface the PID to the ops plane (TrackChild). Stub adapters
	// that never start a process simply never invoke it.
	OnStart func(pid int)

	// OnExit is called after the child process has exited (after cmd.Wait()
	// returns), with the same PID that was passed to OnStart. Nil is a no-op.
	// Callers use this to mark the child as exited on the ops plane
	// (MarkChildExited).
	OnExit func(pid int)
}

// Adapter is the per-CLI interface every supported agent CLI must satisfy to
// be used as a worker, planner, or expert driver.
//
// Invoke MUST:
//   - run the CLI headlessly (no interactive UI) against the given prompt;
//   - return a single JSON object on stdout parseable by
//     internal/runtime.ParseEvent (the structured result envelope);
//   - honour ctx cancellation / deadline and kill the child process on context
//     expiry — never hang after ctx.Done();
//   - never scrape prose (if the CLI has no structured-output mode, it cannot
//     satisfy this interface and must report ErrNotImplemented);
//   - never fake-success: a non-zero exit or a non-success result envelope must
//     return a non-nil error.
//
// Capabilities returns a stable, non-blocking description of what this adapter
// supports. It is called at configuration time, not per-Invoke.
type Adapter interface {
	Invoke(ctx context.Context, prompt string, opts InvokeOptions) ([]byte, error)
	Capabilities() Capabilities
}

// CLIName is a well-known CLI identifier. The mesh uses these as the canonical
// names in config and log output; adapters self-report their name.
type CLIName string

const (
	CLINameClaude CLIName = "claude"
	CLINameCodex  CLIName = "codex"
	CLINameCursor CLIName = "cursor"
	CLINameAider  CLIName = "aider"
)

// ClaudeAdapter drives `claude -p --output-format json [--model M] <prompt>`.
//
// Verified contract (docs/spikes/M0-feasibility.md, 2026-06-05):
//   - binary: claude (Claude Code CLI)
//   - flags: -p --output-format json
//   - model: --model <value> (optional; omit to use CLI default)
//   - auth: subscription OAuth (no ANTHROPIC_API_KEY required; do NOT pass --bare)
//   - result: single JSON object with type=result, subtype, is_error,
//     api_error_status, result (text), session_id, total_cost_usd
//   - never-fake-success: Succeeded() = subtype=="success" && !is_error && api_error_status==null
//
// Hook parity: full — all four Claude Code hooks are implemented in
// hooks/claude-code/ (SessionStart, PreToolUse claim-guard, Stop inbox-drain,
// SessionEnd leave).
type ClaudeAdapter struct {
	// Binary is the claude executable. Default "claude".
	Binary string
}

// Capabilities reports Claude's full capability set.
func (a ClaudeAdapter) Capabilities() Capabilities {
	return Capabilities{
		StructuredOutput:     true,
		ModelPin:             true,
		HookPreToolUse:       true,
		HookStop:             true,
		HookSessionLifecycle: true,
		Notes:                "Fully verified (docs/spikes/M0-feasibility.md, 2026-06-05). hooks/claude-code/ provides full hook parity.",
	}
}

// Invoke runs one headless claude invocation and returns its stdout bytes.
// The child is killed on ctx cancellation; WaitDelay (default 3s) bounds a
// grandchild that holds the stdout pipe after the parent dies.
func (a ClaudeAdapter) Invoke(ctx context.Context, prompt string, opts InvokeOptions) ([]byte, error) {
	bin := a.Binary
	if bin == "" {
		bin = "claude"
	}
	wd := opts.WaitDelay
	if wd <= 0 {
		wd = 3 * time.Second
	}

	// --dangerously-skip-permissions: a headless worker cannot answer an
	// interactive permission prompt, so without this claude is blocked from
	// using Edit/Write/Bash and makes zero file changes while still returning
	// is_error:false success — a silent no-op. The worker runs in an isolated
	// per-task git worktree, so granting tool access is bounded to that tree.
	args := []string{"-p", "--output-format", "json", "--dangerously-skip-permissions"}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, bin, args...)
	if opts.WorkDir != "" {
		cmd.Dir = opts.WorkDir
	}
	if opts.Env != nil {
		cmd.Env = opts.Env
	}
	cmd.WaitDelay = wd

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("claude: failed to start: %w", err)
	}
	pid := cmd.Process.Pid
	if opts.OnStart != nil {
		opts.OnStart(pid)
	}

	waitErr := cmd.Wait()
	if opts.OnExit != nil {
		opts.OnExit(pid)
	}

	if waitErr != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("claude: timed out or cancelled: %w", ctx.Err())
		}
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("claude: exited: %w: %s", waitErr, truncate(stderr.String(), 2048))
		}
		return nil, fmt.Errorf("claude: failed to run: %w", waitErr)
	}
	return stdout.Bytes(), nil
}

// CodexAdapter is a STUB for OpenAI Codex CLI support.
//
// Status: UNVERIFIED as of 2026-06-10.
//
// What is known:
//   - The Codex CLI project (github.com/openai/codex) supports `codex exec`
//     for non-interactive execution.
//   - A --json / --output-format flag for structured output has not been
//     verified against a live binary.
//   - The result envelope shape (if any) has not been mapped to
//     internal/runtime's ParseEvent discriminators.
//   - Model pinning flag is unknown.
//
// Follow-up: verify with a real codex binary and map the output to
// internal/runtime.ParseEvent, or write a shim if the envelope differs.
// Track in issue #30.
type CodexAdapter struct{}

// Capabilities reports Codex as unverified.
func (a CodexAdapter) Capabilities() Capabilities {
	return Capabilities{
		StructuredOutput:     false,
		ModelPin:             false,
		HookPreToolUse:       false,
		HookStop:             false,
		HookSessionLifecycle: false,
		Notes: "STUB — unverified as of 2026-06-10. codex exec structured-output flags " +
			"and result envelope shape not yet mapped to internal/runtime.ParseEvent. " +
			"No hook system equivalent documented. See issue #30.",
	}
}

// Invoke always returns ErrNotImplemented for the Codex stub.
func (a CodexAdapter) Invoke(_ context.Context, _ string, _ InvokeOptions) ([]byte, error) {
	return nil, fmt.Errorf("%w: CodexAdapter — codex CLI structured-output contract "+
		"not yet verified; see issue #30", ErrNotImplemented)
}

// CursorAdapter is a STUB for Cursor CLI support.
//
// Status: UNVERIFIED as of 2026-06-10.
//
// What is known:
//   - Cursor has a background-agent mode and cursor rules, but a scriptable
//     headless CLI with a structured JSON output mode has not been documented
//     or verified.
//   - There is no public specification for a hook system analogous to Claude
//     Code's PreToolUse / Stop / SessionStart / SessionEnd hooks.
//
// Follow-up: verify whether `cursor` has a headless invocation mode that
// emits a parseable result envelope, and document hook equivalents if any.
// Track in issue #30.
type CursorAdapter struct{}

// Capabilities reports Cursor as unverified.
func (a CursorAdapter) Capabilities() Capabilities {
	return Capabilities{
		StructuredOutput:     false,
		ModelPin:             false,
		HookPreToolUse:       false,
		HookStop:             false,
		HookSessionLifecycle: false,
		Notes: "STUB — unverified as of 2026-06-10. Cursor CLI headless/structured-output " +
			"mode not documented for programmatic use. No hook system equivalent. " +
			"See issue #30.",
	}
}

// Invoke always returns ErrNotImplemented for the Cursor stub.
func (a CursorAdapter) Invoke(_ context.Context, _ string, _ InvokeOptions) ([]byte, error) {
	return nil, fmt.Errorf("%w: CursorAdapter — cursor CLI structured-output contract "+
		"not yet verified; see issue #30", ErrNotImplemented)
}

// AiderAdapter is a STUB for Aider CLI support.
//
// Status: UNVERIFIED / STRUCTURALLY BLOCKED as of 2026-06-10.
//
// What is known:
//   - Aider supports --json output (github.com/Aider-AI/aider), but the flag
//     produces status-line JSON objects (one per action), NOT a single terminal
//     result envelope compatible with internal/runtime.ParseEvent.
//   - Mapping Aider's streaming status JSON to the never-fake-success result
//     discriminators requires a shim layer (not yet designed or implemented).
//   - Aider has no hook system analogous to Claude Code hooks.
//   - Model pinning is possible via --model but auth/provider flags differ
//     from the subscription-OAuth pattern (Aider typically requires an API key
//     for the underlying LLM provider).
//
// Structural gap: the adapter interface requires a single JSON result envelope
// on stdout (internal/runtime.ParseEvent contract). Aider's --json output
// does not satisfy this. A shim that collects Aider's streaming JSON and
// synthesises a compatible result envelope is the path forward, but it has
// not been designed or tested.
//
// Follow-up: design the Aider shim, verify with a real binary, document the
// model/auth story. Track in issue #30.
type AiderAdapter struct{}

// Capabilities reports Aider as structurally blocked pending a shim.
func (a AiderAdapter) Capabilities() Capabilities {
	return Capabilities{
		StructuredOutput:     false,
		ModelPin:             true, // --model flag exists but auth differs
		HookPreToolUse:       false,
		HookStop:             false,
		HookSessionLifecycle: false,
		Notes: "STUB — structurally blocked as of 2026-06-10. Aider --json produces " +
			"streaming status lines, not a single result envelope; a shim is needed " +
			"to satisfy internal/runtime.ParseEvent. No hook system. API-key auth " +
			"differs from Claude subscription-OAuth. See issue #30.",
	}
}

// Invoke always returns ErrNotImplemented for the Aider stub.
func (a AiderAdapter) Invoke(_ context.Context, _ string, _ InvokeOptions) ([]byte, error) {
	return nil, fmt.Errorf("%w: AiderAdapter — aider --json output is streaming status "+
		"lines, not a result envelope; a shim layer is required before this adapter "+
		"can be used; see issue #30", ErrNotImplemented)
}

// truncate is a convenience helper shared between adapter Invoke methods.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
