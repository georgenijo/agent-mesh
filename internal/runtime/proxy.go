package runtime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"
)

// maxLineBytes bounds one stdout line, matching the bus frame cap. A child
// emitting a longer line ends the read loop, which the proxy treats as the
// stream closing (typed failure, recoverable via Restart) — never a hang.
const maxLineBytes = 4 * 1024 * 1024

// Proxy supervises ONE resident stream-json child process. It owns the
// child's stdin (held open across asks — that is what keeps the session
// warm), decodes stdout line-by-line into typed events, and surfaces child
// death as a typed error to any blocked Ask and to all subsequent calls.
//
// Concurrency: one in-flight ask at a time, enforced by askMu. This is a
// documented prep-scope simplification — and also what the wire allows:
// stream-json result events carry no correlation id, so a second concurrent
// ask could not be matched to its result anyway. Concurrent Ask calls simply
// queue on the mutex.
type Proxy struct {
	opts Options

	// askMu serializes Ask and Restart (one in-flight turn; no restart under
	// a live turn).
	askMu sync.Mutex

	mu          sync.Mutex
	closed      bool
	cur         *child
	sessionID   string          // last session id seen on the stream
	waiter      chan askOutcome // the blocked ask's delivery channel, if any
	orphanTurns int             // turns abandoned (ctx cancel/timeout) whose result must be dropped
}

// child is one spawned process generation. Restart replaces the whole struct
// so a dead generation's goroutines can never touch its successor's state.
type child struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	initOnce sync.Once
	init     chan struct{} // closed when the stream first yields a session id
	exited   chan struct{} // closed after Wait has reaped the child (exitErr is set by then)

	// Guarded by Proxy.mu:
	streamClosed bool                // stdout hit EOF; no result can ever arrive
	exitErr      *ProcessExitedError // the reaped exit state
}

type askOutcome struct {
	result *ResultEvent
	err    error
}

// New builds a Proxy. Call Start before Ask.
func New(opts Options) *Proxy {
	return &Proxy{opts: opts.withDefaults()}
}

// Start spawns the resident child and blocks until its init event yields a
// session id (bounded by ctx and Options.StartTimeout). On failure the child
// is killed and reaped and the proxy returns to the not-started state.
func (p *Proxy) Start(ctx context.Context) error {
	p.askMu.Lock()
	defer p.askMu.Unlock()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	if p.cur != nil {
		p.mu.Unlock()
		return ErrAlreadyStarted
	}
	p.mu.Unlock()

	return p.spawn(ctx, nil)
}

// Restart re-spawns the child with "--resume <session-id>" appended to the
// base argv. This is the crash-recovery / cold-start path ONLY — never the
// steady-state warm path (decision 2026-06-05): warmth is the running
// process's RAM; --resume reloads the session from disk and pays the
// prompt-cache miss. Callers should follow it with blackboard rehydration
// (`mesh context`) once that wiring exists.
func (p *Proxy) Restart(ctx context.Context) error {
	p.askMu.Lock()
	defer p.askMu.Unlock()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	c := p.cur
	sid := p.sessionID
	p.mu.Unlock()

	if sid == "" {
		return ErrNoSession
	}

	// Make sure the old generation is fully dead and reaped before replacing
	// it — two resident children for one session must never coexist.
	if c != nil {
		select {
		case <-c.exited:
		default:
			p.killAndReap(c)
		}
		select {
		case <-c.exited:
		default:
			return fmt.Errorf("runtime: old child pid %d did not exit within %s; cannot restart",
				c.cmd.Process.Pid, p.opts.CloseTimeout)
		}
	}

	return p.spawn(ctx, []string{"--resume", sid})
}

// Close shuts the resident child down: close stdin (the child's normal exit
// signal in -p stream-json mode), wait CloseTimeout, escalate to SIGKILL,
// wait again. Idempotent; a blocked Ask is unblocked with a typed
// ProcessExitedError by the supervisor as the child dies.
func (p *Proxy) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	c := p.cur
	p.mu.Unlock()

	if c == nil {
		return nil
	}

	c.stdin.Close() //nolint:errcheck // may already be closed or broken
	select {
	case <-c.exited:
		return nil
	case <-time.After(p.opts.CloseTimeout):
	}

	c.cmd.Process.Kill() //nolint:errcheck // racing the exit is fine
	select {
	case <-c.exited:
		return nil
	case <-time.After(p.opts.CloseTimeout):
		return fmt.Errorf("runtime: child pid %d did not exit after SIGKILL", c.cmd.Process.Pid)
	}
}

// Ask writes one stream-json user message to the held-open stdin and blocks
// until the matching result event (or ctx/AskTimeout, or child death — which
// ALWAYS unblocks it with a typed error, never a hang). The stdin write
// itself is bounded the same way: a child that is alive but not draining
// stdin can block a write past the OS pipe buffer indefinitely, so the write
// runs on its own goroutine and an ask abandoned mid-write poisons the
// generation's stdin (see abortWrite) instead of hanging. One ask is in
// flight at a time; see the Proxy doc. A cancelled or timed-out ask whose
// message WAS delivered leaves the session mid-turn: its eventual result is
// dropped so it cannot be misrouted to the next ask.
func (p *Proxy) Ask(ctx context.Context, content string) (Turn, error) {
	p.askMu.Lock()
	defer p.askMu.Unlock()

	line, err := EncodeUserMessage(content)
	if err != nil {
		return Turn{Status: TurnLost}, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return Turn{Status: TurnLost}, ErrClosed
	}
	c := p.cur
	if c == nil {
		p.mu.Unlock()
		return Turn{Status: TurnLost}, ErrNotStarted
	}
	sid := p.sessionID
	if c.exitErr != nil {
		exitErr := c.exitErr
		p.mu.Unlock()
		return Turn{Status: TurnLost, SessionID: sid}, exitErr
	}
	if c.streamClosed {
		p.mu.Unlock()
		return Turn{Status: TurnLost, SessionID: sid},
			&ProcessExitedError{Detail: "stdout closed"}
	}
	ch := make(chan askOutcome, 1)
	p.waiter = ch
	p.mu.Unlock()

	// The write runs on its own goroutine: if the child is alive but not
	// draining stdin and the message exceeds the OS pipe buffer, a direct
	// write would block past any deadline — unbounded by ctx or AskTimeout.
	writeDone := make(chan error, 1)
	go func() {
		_, werr := c.stdin.Write(line)
		writeDone <- werr // buffered(1); never blocks
	}()

	var timeout <-chan time.Time
	if p.opts.AskTimeout > 0 {
		t := time.NewTimer(p.opts.AskTimeout)
		defer t.Stop()
		timeout = t.C
	}

	select {
	case werr := <-writeDone:
		if werr != nil {
			p.clearWaiter(ch)
			p.mu.Lock()
			exitErr := c.exitErr
			p.mu.Unlock()
			if exitErr != nil {
				return Turn{Status: TurnLost, SessionID: sid}, exitErr
			}
			return Turn{Status: TurnLost, SessionID: sid},
				&ProcessExitedError{Detail: "stdin write failed", Cause: werr}
		}
	case out := <-ch:
		// The supervisor failed the waiter (e.g. stdout closed while the
		// write was still blocked). Close stdin so the writer goroutine is
		// released; the generation is dead either way.
		if out.err != nil {
			c.stdin.Close() //nolint:errcheck // may already be closed
			return Turn{Status: TurnLost, SessionID: sid}, out.err
		}
		return p.turnFromResult(out.result)
	case <-ctx.Done():
		p.abortWrite(c, ch, writeDone)
		return Turn{Status: TurnLost, SessionID: sid}, ctx.Err()
	case <-timeout:
		p.abortWrite(c, ch, writeDone)
		return Turn{Status: TurnLost, SessionID: sid},
			fmt.Errorf("runtime: ask timed out after %s", p.opts.AskTimeout)
	}

	select {
	case out := <-ch:
		if out.err != nil {
			return Turn{Status: TurnLost, SessionID: sid}, out.err
		}
		return p.turnFromResult(out.result)
	case <-ctx.Done():
		p.abandonTurn(ch)
		return Turn{Status: TurnLost, SessionID: sid}, ctx.Err()
	case <-timeout:
		p.abandonTurn(ch)
		return Turn{Status: TurnLost, SessionID: sid},
			fmt.Errorf("runtime: ask timed out after %s", p.opts.AskTimeout)
	}
}

// SessionID returns the Claude session id captured from the init event, or
// "" if none has been seen yet.
func (p *Proxy) SessionID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sessionID
}

// Pid returns the current child's OS pid, or 0 if no child is spawned. It is
// what a sidecar will hand to TrackChild for the ops plane.
func (p *Proxy) Pid() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cur == nil {
		return 0
	}
	return p.cur.cmd.Process.Pid
}

// --- internals -----------------------------------------------------------------

// spawn starts one child generation and waits for its session id.
func (p *Proxy) spawn(ctx context.Context, extraArgs []string) error {
	args := make([]string, 0, len(p.opts.Args)+len(extraArgs))
	args = append(args, p.opts.Args...)
	args = append(args, extraArgs...)

	cmd := exec.Command(p.opts.Binary, args...)
	cmd.Dir = p.opts.Dir
	cmd.Env = p.opts.Env
	cmd.Stderr = p.opts.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("runtime: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("runtime: stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("runtime: start %s: %w", p.opts.Binary, err)
	}

	c := &child{
		cmd:    cmd,
		stdin:  stdin,
		init:   make(chan struct{}),
		exited: make(chan struct{}),
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		cmd.Process.Kill() //nolint:errcheck
		go cmd.Wait()      //nolint:errcheck // reap; don't leak a zombie
		return ErrClosed
	}
	p.cur = c
	p.waiter = nil
	p.orphanTurns = 0 // a fresh child has a fresh turn ledger
	p.mu.Unlock()

	go p.supervise(c, stdout)

	timer := time.NewTimer(p.opts.StartTimeout)
	defer timer.Stop()
	select {
	case <-c.init:
		return nil
	case <-c.exited:
		err := p.childExitError(c)
		p.detach(c)
		return fmt.Errorf("runtime: child exited before reporting a session id: %w", err)
	case <-ctx.Done():
		p.killAndReap(c)
		p.detach(c)
		return fmt.Errorf("runtime: start cancelled: %w", ctx.Err())
	case <-timer.C:
		p.killAndReap(c)
		p.detach(c)
		return fmt.Errorf("runtime: child reported no session id within %s", p.opts.StartTimeout)
	}
}

// supervise owns the child's whole lifetime on one goroutine: drain stdout
// to EOF (a result can only arrive while the pipe is open), then reap with
// Wait. Sequential read-then-wait is required by the os/exec StdoutPipe
// contract — Wait closes the pipe, so it must not run while reads are in
// flight.
func (p *Proxy) supervise(c *child, stdout io.Reader) {
	p.readLoop(c, stdout)

	// stdout is gone: no result can ever arrive from this generation. New
	// asks fail fast from here on.
	p.mu.Lock()
	c.streamClosed = true
	p.mu.Unlock()

	// Prefer handing a blocked ask the real exit state from Wait, but a
	// child that closed stdout yet lingers must not hang the asker: bound
	// the reap before failing the waiter without a state.
	reaped := make(chan struct{})
	go func() {
		select {
		case <-reaped:
		case <-time.After(p.opts.CloseTimeout):
			p.mu.Lock()
			if p.cur == c {
				p.failWaiterLocked(&ProcessExitedError{Detail: "stdout closed; child not yet reaped"})
			}
			p.mu.Unlock()
		}
	}()

	waitErr := c.cmd.Wait()
	close(reaped)

	pe := &ProcessExitedError{State: c.cmd.ProcessState}
	var exitErr *exec.ExitError
	if waitErr != nil && !errors.As(waitErr, &exitErr) {
		pe.Cause = waitErr // an I/O-level wait failure, not a normal exit status
	}

	p.mu.Lock()
	c.exitErr = pe
	if p.cur == c {
		p.failWaiterLocked(pe)
	}
	p.mu.Unlock()
	close(c.exited)
}

// readLoop decodes stdout line-by-line. Malformed lines and unknown event
// types are skipped, never fatal (degrade-don't-throw at the boundary — the
// stream format will evolve). It returns when stdout closes or a line
// exceeds maxLineBytes.
func (p *Proxy) readLoop(c *child, stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), maxLineBytes)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		ev, err := ParseEvent(line)
		if err != nil {
			continue // not a JSON object: skip the line, keep the child
		}
		if ev.SessionID != "" {
			p.mu.Lock()
			if p.cur == c {
				p.sessionID = ev.SessionID
			}
			p.mu.Unlock()
			c.initOnce.Do(func() { close(c.init) })
		}
		if ev.Result == nil {
			continue // system/assistant/rate_limit/unknown: tolerated, not routed
		}
		p.mu.Lock()
		if p.cur == c {
			if p.orphanTurns > 0 {
				// This result answers an abandoned (cancelled/timed-out)
				// turn. Results carry no correlation id — matching is
				// positional — so it must be dropped, not delivered to the
				// next ask.
				p.orphanTurns--
			} else if p.waiter != nil {
				p.waiter <- askOutcome{result: ev.Result} // buffered(1); never blocks
				p.waiter = nil
			}
		}
		p.mu.Unlock()
	}
}

// turnFromResult maps a terminal result event onto the typed Turn enum.
// Never fake-success: only the spike's success rule yields TurnAnswered.
func (p *Proxy) turnFromResult(ev *ResultEvent) (Turn, error) {
	turn := Turn{Text: ev.Result, SessionID: ev.SessionID, Result: ev}
	if turn.SessionID == "" {
		turn.SessionID = p.SessionID()
	}
	if ev.Succeeded() {
		turn.Status = TurnAnswered
		return turn, nil
	}
	turn.Status = TurnError
	return turn, &ResultError{Result: ev}
}

// failWaiterLocked delivers a typed child-death error to the blocked ask, if
// any. Callers hold p.mu.
func (p *Proxy) failWaiterLocked(err *ProcessExitedError) {
	if p.waiter != nil {
		p.waiter <- askOutcome{err: err} // buffered(1); never blocks
		p.waiter = nil
	}
}

// clearWaiter unregisters an ask that is bailing out before delivery.
func (p *Proxy) clearWaiter(ch chan askOutcome) {
	p.mu.Lock()
	if p.waiter == ch {
		p.waiter = nil
	}
	p.mu.Unlock()
}

// abandonTurn unregisters a cancelled/timed-out ask whose message WAS
// delivered: the child will still answer it eventually, so the next result
// belongs to this dead turn and must be dropped (see readLoop).
func (p *Proxy) abandonTurn(ch chan askOutcome) {
	p.mu.Lock()
	if p.waiter == ch {
		p.waiter = nil
		p.orphanTurns++
	}
	p.mu.Unlock()
}

// abortWrite resolves an ask abandoned (ctx cancel / AskTimeout) while its
// stdin write may still be in flight. If the write in fact completed, the
// message was delivered and the turn is abandoned like any other (its late
// result must be dropped). If the write is still blocked, the pipe holds an
// unknown prefix of the message: a second write could interleave into that
// half-written line, so the generation's stdin is closed instead — which
// unblocks the writer goroutine and ends the generation (subsequent asks
// fail fast with a typed error; Restart recovers via --resume).
func (p *Proxy) abortWrite(c *child, ch chan askOutcome, writeDone <-chan error) {
	select {
	case werr := <-writeDone:
		if werr == nil {
			p.abandonTurn(ch)
			return
		}
		// The write failed outright: nothing was delivered, no result is
		// owed, and the child is (almost certainly) already dead.
		p.clearWaiter(ch)
	default:
		p.clearWaiter(ch)
		c.stdin.Close() //nolint:errcheck // poisoning a half-written pipe; may already be closed
	}
}

// killAndReap force-kills a generation and waits (bounded) for its
// supervisor to reap it.
func (p *Proxy) killAndReap(c *child) {
	c.stdin.Close()      //nolint:errcheck
	c.cmd.Process.Kill() //nolint:errcheck
	select {
	case <-c.exited:
	case <-time.After(p.opts.CloseTimeout):
	}
}

// childExitError returns the generation's reaped exit error (guaranteed set
// once c.exited is closed) or a stateless placeholder.
func (p *Proxy) childExitError(c *child) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c.exitErr != nil {
		return c.exitErr
	}
	return &ProcessExitedError{Detail: "exit state unavailable"}
}

// detach rolls the proxy back to not-started after a failed spawn so Start
// can be retried (the session id, if any, is kept for a --resume).
func (p *Proxy) detach(c *child) {
	p.mu.Lock()
	if p.cur == c {
		p.cur = nil
	}
	p.mu.Unlock()
}
