package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The fake resident child is this test binary re-exec'd (the os/exec
// TestHelperProcess pattern): no real claude invocation anywhere. It speaks
// the spike-verified stream-json contract — an init event carrying a
// session id, then one result event per stdin user message.
const (
	helperEnv         = "GO_RUNTIME_HELPER"           // "1" activates the fake
	helperSessionEnv  = "GO_RUNTIME_HELPER_SESSION"   // session id to report (default sess-fake-1)
	helperArgsFileEnv = "GO_RUNTIME_HELPER_ARGS_FILE" // append argv (after "--") as a JSON line per spawn
	helperNoiseEnv    = "GO_RUNTIME_HELPER_NOISE"     // "1": emit junk/unknown lines before each result
	helperStallEnv    = "GO_RUNTIME_HELPER_STALL"     // "1": emit init, then stay alive without ever reading stdin
)

// TestHelperProcess is not a real test: it is the fake child. It only runs
// when re-exec'd with helperEnv set.
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		return
	}
	defer os.Exit(0)
	helperMain()
}

func helperMain() {
	args := helperArgs()
	if path := os.Getenv(helperArgsFileEnv); path != "" {
		recordArgs(path, args)
	}

	session := os.Getenv(helperSessionEnv)
	if session == "" {
		session = "sess-fake-1"
	}
	// Mimic the real CLI: a --resume'd child reports the resumed session id.
	for i, a := range args {
		if a == "--resume" && i+1 < len(args) {
			session = args[i+1]
		}
	}

	out := bufio.NewWriter(os.Stdout)
	emit := func(v any) {
		b, err := json.Marshal(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "helper: marshal: %v\n", err)
			os.Exit(2)
		}
		out.Write(append(b, '\n'))
		out.Flush()
	}
	emitRaw := func(s string) {
		out.WriteString(s + "\n")
		out.Flush()
	}

	// Startup mirrors the spike: hook noise first (no session_id), then init.
	emit(map[string]any{"type": "system", "subtype": "hook_started", "hook": "fake-startup"})
	emit(map[string]any{"type": "system", "subtype": "hook_response", "hook": "fake-startup"})
	emit(map[string]any{
		"type": "system", "subtype": "init", "session_id": session,
		"model": "fake-model", "apiKeySource": "none", "pid": os.Getpid(),
	})

	if os.Getenv(helperStallEnv) == "1" {
		// Alive but never draining stdin: a mid-turn child. Writes past the
		// OS pipe buffer block until the writer gives up or we are killed.
		for {
			time.Sleep(time.Hour)
		}
	}

	in := bufio.NewScanner(os.Stdin)
	turn := 0
	for in.Scan() {
		var msg struct {
			Type    string `json:"type"`
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(in.Bytes(), &msg); err != nil ||
			msg.Type != "user" || msg.Message.Role != "user" {
			// The real CLI rejects stdin lines without message.role.
			fmt.Fprintln(os.Stderr, "helper: bad stdin line")
			os.Exit(2)
		}
		turn++
		content := msg.Message.Content

		if strings.HasPrefix(content, "DIE") {
			os.Exit(3) // crash mid-ask: no result event, nonzero exit
		}
		if strings.HasPrefix(content, "FAILRESULT") {
			emit(map[string]any{
				"type": "result", "subtype": "error_during_execution",
				"is_error": true, "session_id": session, "num_turns": turn,
			})
			continue
		}
		if strings.HasPrefix(content, "SLOW") {
			time.Sleep(300 * time.Millisecond)
		}

		if os.Getenv(helperNoiseEnv) == "1" {
			emitRaw("this is not json at all {{{")
			emitRaw(`["a","top-level","array"]`)
			emit(map[string]any{"type": 42, "weird": true})
			emit(map[string]any{"type": "wormhole_event", "subtype": "exotic", "payload": []int{1, 2, 3}})
			emit(map[string]any{"type": "rate_limit_event", "status": "allowed"})
		}

		emit(map[string]any{"type": "assistant", "message": map[string]any{"content": "thinking..."}})
		emit(map[string]any{
			"type": "result", "subtype": "success", "is_error": false,
			"result":     fmt.Sprintf("echo[pid=%d turn=%d]: %s", os.Getpid(), turn, content),
			"session_id": session, "num_turns": 1, "duration_ms": 5,
		})
	}
}

func helperArgs() []string {
	for i, a := range os.Args {
		if a == "--" {
			return os.Args[i+1:]
		}
	}
	return nil
}

func recordArgs(path string, args []string) {
	b, err := json.Marshal(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: marshal args: %v\n", err)
		os.Exit(2)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: open args file: %v\n", err)
		os.Exit(2)
	}
	defer f.Close()
	f.Write(append(b, '\n'))
}

// --- test harness ----------------------------------------------------------------

func newTestProxy(t *testing.T, extraEnv ...string) *Proxy {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	env := append(os.Environ(), helperEnv+"=1")
	env = append(env, extraEnv...)
	p := New(Options{
		Binary:       exe,
		Args:         []string{"-test.run=^TestHelperProcess$", "--"},
		Env:          env,
		Stderr:       os.Stderr,
		StartTimeout: 10 * time.Second,
		CloseTimeout: 3 * time.Second,
	})
	t.Cleanup(func() { p.Close() })
	return p
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func mustStart(t *testing.T, p *Proxy) {
	t.Helper()
	if err := p.Start(testCtx(t)); err != nil {
		t.Fatalf("start: %v", err)
	}
}

func mustAsk(t *testing.T, p *Proxy, content string) Turn {
	t.Helper()
	turn, err := p.Ask(testCtx(t), content)
	if err != nil {
		t.Fatalf("ask %q: %v", content, err)
	}
	if turn.Status != TurnAnswered {
		t.Fatalf("ask %q: status = %q, want %q", content, turn.Status, TurnAnswered)
	}
	return turn
}

func readSpawns(t *testing.T, path string) [][]string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read args file: %v", err)
	}
	var spawns [][]string
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var args []string
		if err := json.Unmarshal([]byte(line), &args); err != nil {
			t.Fatalf("unmarshal args line %q: %v", line, err)
		}
		spawns = append(spawns, args)
	}
	return spawns
}

// helperPid extracts the pid the fake stamps into each result text, proving
// which OS process answered.
func helperPid(t *testing.T, text string) string {
	t.Helper()
	start := strings.Index(text, "pid=")
	if start < 0 {
		t.Fatalf("no pid stamp in %q", text)
	}
	rest := text[start+len("pid="):]
	end := strings.IndexByte(rest, ' ')
	if end < 0 {
		t.Fatalf("malformed pid stamp in %q", text)
	}
	return rest[:end]
}

// --- the required scenarios --------------------------------------------------------

// (1) Warm path: two sequential asks answered by ONE resident process —
// same session id, same child pid, no respawn.
func TestWarmPathTwoAsksOneProcess(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args.jsonl")
	p := newTestProxy(t, helperArgsFileEnv+"="+argsFile)
	mustStart(t, p)

	if got := p.SessionID(); got != "sess-fake-1" {
		t.Fatalf("session id = %q, want sess-fake-1", got)
	}
	startPid := p.Pid()
	if startPid == 0 {
		t.Fatal("pid = 0 after start")
	}

	t1 := mustAsk(t, p, "first question")
	t2 := mustAsk(t, p, "second question")

	if !strings.Contains(t1.Text, "first question") || !strings.Contains(t2.Text, "second question") {
		t.Fatalf("answers did not echo their asks: %q / %q", t1.Text, t2.Text)
	}
	if t1.SessionID != "sess-fake-1" || t2.SessionID != "sess-fake-1" {
		t.Fatalf("session ids = %q / %q, want sess-fake-1 for both", t1.SessionID, t2.SessionID)
	}
	if pid1, pid2 := helperPid(t, t1.Text), helperPid(t, t2.Text); pid1 != pid2 {
		t.Fatalf("answers came from different processes: pid %s vs %s", pid1, pid2)
	}
	if p.Pid() != startPid {
		t.Fatalf("child pid changed: %d -> %d", startPid, p.Pid())
	}
	// turn=2 in the second answer proves RAM state carried across the asks
	// (the spike's nonce-recall proof, in miniature).
	if !strings.Contains(t2.Text, "turn=2") {
		t.Fatalf("second answer lost the resident turn counter: %q", t2.Text)
	}
	if spawns := readSpawns(t, argsFile); len(spawns) != 1 {
		t.Fatalf("recorded %d spawns, want exactly 1 (no respawn between asks): %v", len(spawns), spawns)
	}
}

// (2) Child killed mid-ask: the blocked Ask gets a typed ErrProcessExited
// (with the exit state), never hangs, and subsequent asks fail fast.
func TestChildDeathMidAskSurfacesTypedError(t *testing.T) {
	p := newTestProxy(t)
	mustStart(t, p)

	done := make(chan struct{})
	var turn Turn
	var err error
	go func() {
		defer close(done)
		turn, err = p.Ask(testCtx(t), "DIE now")
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Ask hung after child death")
	}

	if err == nil {
		t.Fatalf("ask after DIE succeeded: %+v", turn)
	}
	if !errors.Is(err, ErrProcessExited) {
		t.Fatalf("error = %v, want ErrProcessExited", err)
	}
	var pe *ProcessExitedError
	if !errors.As(err, &pe) {
		t.Fatalf("error %v is not a *ProcessExitedError", err)
	}
	if pe.State == nil || pe.State.ExitCode() != 3 {
		t.Fatalf("exit state = %v, want exit code 3", pe.State)
	}
	if turn.Status != TurnLost {
		t.Fatalf("turn status = %q, want %q", turn.Status, TurnLost)
	}

	// Subsequent asks must fail fast with the same typed error.
	turn2, err2 := p.Ask(testCtx(t), "after death")
	if !errors.Is(err2, ErrProcessExited) {
		t.Fatalf("post-death ask error = %v, want ErrProcessExited", err2)
	}
	if turn2.Status != TurnLost {
		t.Fatalf("post-death turn status = %q, want %q", turn2.Status, TurnLost)
	}
}

// (3) Close is clean: stdin close lets the child exit on its own, Close
// returns nil, is idempotent, and post-close calls return ErrClosed.
func TestCloseClean(t *testing.T) {
	p := newTestProxy(t)
	mustStart(t, p)
	mustAsk(t, p, "warm me up")

	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := p.Ask(testCtx(t), "too late"); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close ask error = %v, want ErrClosed", err)
	}
	if err := p.Restart(testCtx(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close restart error = %v, want ErrClosed", err)
	}
}

// (4) Restart passes --resume <session-id> in argv (recovery-only path),
// verified through the fake's recorded args, and the proxy answers again.
func TestRestartPassesResume(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args.jsonl")
	p := newTestProxy(t, helperArgsFileEnv+"="+argsFile)
	mustStart(t, p)

	first := p.SessionID()
	if first != "sess-fake-1" {
		t.Fatalf("session id = %q, want sess-fake-1", first)
	}

	// Crash the child, then recover.
	if _, err := p.Ask(testCtx(t), "DIE"); !errors.Is(err, ErrProcessExited) {
		t.Fatalf("DIE ask error = %v, want ErrProcessExited", err)
	}
	if err := p.Restart(testCtx(t)); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := p.SessionID(); got != first {
		t.Fatalf("resumed session id = %q, want %q", got, first)
	}

	turn := mustAsk(t, p, "hello again")
	if turn.SessionID != first {
		t.Fatalf("post-restart turn session = %q, want %q", turn.SessionID, first)
	}

	spawns := readSpawns(t, argsFile)
	if len(spawns) != 2 {
		t.Fatalf("recorded %d spawns, want 2: %v", len(spawns), spawns)
	}
	for _, a := range spawns[0] {
		if a == "--resume" {
			t.Fatalf("first spawn must not carry --resume: %v", spawns[0])
		}
	}
	second := spawns[1]
	if len(second) < 2 ||
		second[len(second)-2] != "--resume" || second[len(second)-1] != first {
		t.Fatalf("restart argv = %v, want trailing [--resume %s]", second, first)
	}
}

// (5) Malformed and unknown stdout lines are skipped without killing the
// proxy: asks still complete around the noise.
func TestMalformedAndUnknownLinesSkipped(t *testing.T) {
	p := newTestProxy(t, helperNoiseEnv+"=1")
	mustStart(t, p)

	t1 := mustAsk(t, p, "alpha")
	t2 := mustAsk(t, p, "beta")
	if !strings.Contains(t1.Text, "alpha") || !strings.Contains(t2.Text, "beta") {
		t.Fatalf("answers misrouted around noise: %q / %q", t1.Text, t2.Text)
	}
	if t1.SessionID != t2.SessionID {
		t.Fatalf("session changed across noisy asks: %q vs %q", t1.SessionID, t2.SessionID)
	}
}

// --- additional discipline ---------------------------------------------------------

// A structured non-success result is a typed *ResultError (TurnError),
// distinct from child death (TurnLost) — and the session survives it.
func TestNonSuccessResultIsTypedError(t *testing.T) {
	p := newTestProxy(t)
	mustStart(t, p)

	turn, err := p.Ask(testCtx(t), "FAILRESULT please")
	if err == nil {
		t.Fatalf("FAILRESULT ask succeeded: %+v", turn)
	}
	var re *ResultError
	if !errors.As(err, &re) {
		t.Fatalf("error = %v, want *ResultError", err)
	}
	if errors.Is(err, ErrProcessExited) {
		t.Fatal("a non-success result must not look like child death")
	}
	if turn.Status != TurnError {
		t.Fatalf("turn status = %q, want %q", turn.Status, TurnError)
	}
	if turn.Result == nil || turn.Result.Subtype != "error_during_execution" {
		t.Fatalf("turn.Result = %+v, want the non-success result event", turn.Result)
	}

	// The child is still alive and warm.
	mustAsk(t, p, "still alive?")
}

// A cancelled ask's late result must be dropped, not misrouted to the next
// ask (results carry no correlation id; matching is positional).
func TestCancelledAskDoesNotMisrouteNextResult(t *testing.T) {
	p := newTestProxy(t)
	mustStart(t, p)

	cctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	turn, err := p.Ask(cctx, "SLOW first")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled ask error = %v, want DeadlineExceeded", err)
	}
	if turn.Status != TurnLost {
		t.Fatalf("cancelled turn status = %q, want %q", turn.Status, TurnLost)
	}

	t2 := mustAsk(t, p, "second")
	if !strings.Contains(t2.Text, "second") || strings.Contains(t2.Text, "SLOW first") {
		t.Fatalf("second ask got the orphaned first result: %q", t2.Text)
	}
}

// The stdin write itself must be bounded by ctx: a child that is alive but
// not draining stdin blocks a write larger than the OS pipe buffer forever,
// which must not hang Ask past its deadline — nor wedge Restart (the
// crash-recovery path) behind the held askMu.
func TestStdinWriteBlockedHonoursContext(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	p := New(Options{
		Binary:       exe,
		Args:         []string{"-test.run=^TestHelperProcess$", "--"},
		Env:          append(os.Environ(), helperEnv+"=1", helperStallEnv+"=1"),
		Stderr:       os.Stderr,
		StartTimeout: 10 * time.Second,
		CloseTimeout: time.Second,
	})
	t.Cleanup(func() { p.Close() })
	mustStart(t, p)

	// Far past any OS pipe buffer (~64KB), so the write itself blocks.
	big := strings.Repeat("x", 4<<20)
	cctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	var turn Turn
	var askErr error
	go func() {
		defer close(done)
		turn, askErr = p.Ask(cctx, big)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Ask blocked on the stdin write past its ctx deadline")
	}
	if !errors.Is(askErr, context.DeadlineExceeded) {
		t.Fatalf("blocked-write ask error = %v, want DeadlineExceeded", askErr)
	}
	if turn.Status != TurnLost {
		t.Fatalf("blocked-write turn status = %q, want %q", turn.Status, TurnLost)
	}

	// The crash-recovery path must not be wedged behind the aborted write.
	if err := p.Restart(testCtx(t)); err != nil {
		t.Fatalf("restart after aborted write: %v", err)
	}
	if got := p.SessionID(); got != "sess-fake-1" {
		t.Fatalf("resumed session id = %q, want sess-fake-1", got)
	}
}

// Lifecycle misuse returns typed errors, never panics or hangs.
func TestLifecycleGuards(t *testing.T) {
	p := newTestProxy(t)

	if _, err := p.Ask(testCtx(t), "x"); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("pre-start ask error = %v, want ErrNotStarted", err)
	}
	if err := p.Restart(testCtx(t)); !errors.Is(err, ErrNoSession) {
		t.Fatalf("pre-start restart error = %v, want ErrNoSession", err)
	}
	mustStart(t, p)
	if err := p.Start(testCtx(t)); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("double start error = %v, want ErrAlreadyStarted", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := p.Start(testCtx(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close start error = %v, want ErrClosed", err)
	}
}
