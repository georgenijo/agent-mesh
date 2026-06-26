// Package runtime owns the resident-agent process boundary: one long-lived
// stream-json `claude` child per warm expert, with stdin held open, one user
// message written per ask, and typed JSON events decoded line-by-line from
// stdout.
//
// The wire contract was verified by the runtime-proxy spike
// (docs/spikes/runtime-proxy.md, issue #32). The child is
//
//	claude -p --input-format stream-json --output-format stream-json --verbose
//
// and each ask is exactly one stdin line of the shape
//
//	{"type":"user","message":{"role":"user","content":"..."}}
//
// answered by a terminal `result` event on stdout. Warmth is RAM: the running
// process holds the conversation context. `--resume <session-id>` is the
// crash-recovery / cold-start path ONLY, never the steady-state warm path
// (decision 2026-06-05 "Persistent experts = a resident stream-json claude
// process; --resume is recovery-only"). No PTY is used anywhere — structured
// stdout is the protocol, prose is never scraped.
//
// This package deliberately does not import internal/envelope (it sits below
// the mesh wire layer) but mirrors its discipline: typed result enums, lost
// distinct from error, never fake-success. The package name shadows the
// stdlib runtime; importers that also need the stdlib package should alias
// this one (e.g. agentruntime ".../internal/runtime").
package runtime

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// DefaultBinary is the agent CLI the proxy supervises by default.
const DefaultBinary = "claude"

// DefaultArgs returns the spike-verified resident stream-json invocation.
// --verbose is required: without it `-p` does not emit stream-json events.
func DefaultArgs() []string {
	return []string{
		"-p",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--verbose",
		// A resident expert runs headless and cannot answer interactive
		// permission prompts, so without this it is blocked from using tools
		// (e.g. reading the repo to answer a question). The flag grants
		// non-interactive tool use so the expert can actually do its job.
		"--dangerously-skip-permissions",
	}
}

const (
	// defaultStartTimeout is the liveness grace on spawn: how long Start watches
	// for an immediate crash (bad binary/auth) before treating an alive child as
	// ready. It is NOT a session-id deadline — this claude build emits its init
	// (and session id) only after the first input, so session id is captured
	// lazily on the first turn, not waited for at startup (#93).
	defaultStartTimeout = 3 * time.Second
	defaultCloseTimeout = 5 * time.Second
)

// Options configure a Proxy. Zero values take defaults. Binary/Args/Dir/Env
// are configurable precisely so tests can substitute a fake child (the
// re-exec helper-process pattern) for the real CLI.
type Options struct {
	// Binary is the child executable. Default DefaultBinary.
	Binary string
	// Args is the base argv (without the binary). Default DefaultArgs().
	// Restart appends "--resume <session-id>" after these; never put a
	// --resume here yourself.
	Args []string
	// Dir is the child working directory ("" = inherit).
	Dir string
	// Env is the child environment (nil = inherit the parent's).
	Env []string
	// Stderr receives the child's stderr (nil = discarded).
	Stderr io.Writer
	// StartTimeout bounds how long Start/Restart wait for the init event's
	// session id. Default 30s.
	StartTimeout time.Duration
	// AskTimeout caps a single Ask in addition to its ctx. 0 = no cap; the
	// ask is then bounded only by ctx and by child death (which always
	// unblocks it). Callers should pass a ctx with a deadline.
	AskTimeout time.Duration
	// CloseTimeout is the grace period Close waits after closing stdin
	// before escalating to SIGKILL (and again after SIGKILL). It also bounds
	// how long a blocked Ask waits for the exit state once the child's
	// stdout has closed. Default 5s.
	CloseTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.Binary == "" {
		o.Binary = DefaultBinary
	}
	if o.Args == nil {
		o.Args = DefaultArgs()
	}
	if o.Stderr == nil {
		o.Stderr = io.Discard
	}
	if o.StartTimeout <= 0 {
		o.StartTimeout = defaultStartTimeout
	}
	if o.CloseTimeout <= 0 {
		o.CloseTimeout = defaultCloseTimeout
	}
	return o
}

// TurnStatus is the typed outcome of one ask, mirroring the claim enum
// discipline (claimed|lost|error): answered, error, and lost are distinct
// states and a non-success is never reported as an answer.
type TurnStatus string

const (
	// TurnAnswered means the child completed the turn with a structured
	// success result (subtype "success", is_error false, null
	// api_error_status — the spike's never-fake-success mapping rule).
	TurnAnswered TurnStatus = "answered"
	// TurnError means the child itself reported a structured non-success
	// result. The turn completed at the protocol level; the Result event
	// carries the details.
	TurnError TurnStatus = "error"
	// TurnLost means no result arrived: the child died, the ask was
	// cancelled or timed out, or the message could not be delivered. The
	// turn's true outcome is unknown.
	TurnLost TurnStatus = "lost"
)

// Turn is the typed outcome of one Ask.
type Turn struct {
	Status    TurnStatus
	Text      string       // the result text for answered turns
	SessionID string       // the Claude session id the turn ran under
	Result    *ResultEvent // full terminal result event; nil if the turn was lost before one arrived
}

// Typed errors. Callers must be able to distinguish "the child died" from
// "the child answered with a structured failure" from "the proxy was misused"
// — never a bare prose error for a state the supervisor loop branches on.
var (
	// ErrProcessExited means the resident child is gone. Errors carrying an
	// exit state wrap this sentinel via *ProcessExitedError.
	ErrProcessExited = errors.New("runtime: child process exited")
	// ErrClosed means the proxy was shut down by its owner.
	ErrClosed = errors.New("runtime: proxy closed")
	// ErrNotStarted means Start has not (successfully) run yet.
	ErrNotStarted = errors.New("runtime: proxy not started")
	// ErrAlreadyStarted means Start ran twice without an intervening failure.
	ErrAlreadyStarted = errors.New("runtime: proxy already started")
	// ErrNoSession means no session id was ever captured, so a --resume
	// recovery is impossible.
	ErrNoSession = errors.New("runtime: no session id captured")
	// ErrMalformedEvent means a stdout line was not a JSON object. The read
	// loop skips such lines; this surfaces only from ParseEvent directly.
	ErrMalformedEvent = errors.New("runtime: malformed stream event")
)

// ProcessExitedError is the typed child-death error delivered to a blocked
// Ask and to every call made after the child is gone. It wraps
// ErrProcessExited, so errors.Is(err, ErrProcessExited) matches.
type ProcessExitedError struct {
	// State is the reaped exit state. It is nil when the failure surfaced
	// before the child was reaped (e.g. stdout closed but the process
	// lingers, or a stdin write broke first).
	State *os.ProcessState
	// Detail says where the death was observed (e.g. "stdout closed").
	Detail string
	// Cause is an underlying I/O error, if one triggered the detection.
	Cause error
}

func (e *ProcessExitedError) Error() string {
	var b strings.Builder
	b.WriteString("runtime: child process exited")
	if e.State != nil {
		b.WriteString(": ")
		b.WriteString(e.State.String())
	}
	if e.Detail != "" {
		b.WriteString(" (")
		b.WriteString(e.Detail)
		b.WriteString(")")
	}
	if e.Cause != nil {
		b.WriteString(": ")
		b.WriteString(e.Cause.Error())
	}
	return b.String()
}

func (e *ProcessExitedError) Unwrap() error { return ErrProcessExited }

// ResultError is returned when the child completed a turn at the protocol
// level but flagged it non-success (subtype != "success", is_error true, or
// a non-null api_error_status). Distinct from ProcessExitedError: the child
// is still alive and the session continues.
type ResultError struct {
	Result *ResultEvent
}

func (e *ResultError) Error() string {
	status := "null"
	if e.Result.HasAPIError() {
		status = strings.TrimSpace(string(e.Result.APIErrorStatus))
	}
	return fmt.Sprintf("runtime: child reported non-success result (subtype=%q, is_error=%v, api_error_status=%s)",
		e.Result.Subtype, e.Result.IsError, status)
}
