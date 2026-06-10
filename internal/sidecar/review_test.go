package sidecar

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// reviewTap subscribes mesh.review.> on a fresh bus client and collects the
// decoded review payloads, so a test can assert the published verdict event.
type reviewTap struct {
	mu   sync.Mutex
	recv []envelope.ReviewPayload
	cli  *bus.Client
}

func newReviewTap(t *testing.T, cfg interface{ BusSocket() string }) *reviewTap {
	t.Helper()
	cli, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatalf("dial bus tap: %v", err)
	}
	tap := &reviewTap{cli: cli}
	if _, err := cli.Subscribe(envelope.PatternReviews, func(env envelope.Envelope) {
		var p envelope.ReviewPayload
		if envelope.DecodeInto(env, &p) != nil {
			return
		}
		tap.mu.Lock()
		tap.recv = append(tap.recv, p)
		tap.mu.Unlock()
	}); err != nil {
		t.Fatalf("subscribe reviews: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return tap
}

func (tp *reviewTap) await(t *testing.T, task string, timeout time.Duration) envelope.ReviewPayload {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tp.mu.Lock()
		for _, p := range tp.recv {
			if p.Task == task {
				tp.mu.Unlock()
				return p
			}
		}
		tp.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no review event for task %q within %s", task, timeout)
	return envelope.ReviewPayload{}
}

// TestReviewPublishesTypedVerdict proves the sidecar review path records a real
// expert verdict as a mesh.review.<task> event with the typed verdict, code,
// session id, and turn count — the observability tap, never a second authority.
// The "brain" is a fake ReviewFunc; the runtime child is exercised by the
// runtime package's own tests.
func TestReviewPublishesTypedVerdict(t *testing.T) {
	cfg := fastConfig(t)
	sc := startMesh(t, cfg, "auth-expert")
	tap := newReviewTap(t, cfg)

	fn := func(_ context.Context, req ReviewRequest) (ReviewResult, error) {
		return ReviewResult{
			Verdict:   envelope.ReviewRequestChanges,
			Notes:     "missing error handling on " + req.ChangedFiles[0],
			SessionID: "sess-auth-1",
			NumTurns:  4,
		}, nil
	}

	res, err := sc.Review(context.Background(), fn, ReviewRequest{
		Task: "task-1", Job: "job-1", Branch: "mesh/worker/task-1",
		HeadSHA: "abc123", Instruction: "add RLS", Diff: "@@ diff @@",
		ChangedFiles: []string{"db/policy.sql"},
	})
	if err != nil {
		t.Fatalf("Review: %v", err)
	}
	if res.Verdict != envelope.ReviewRequestChanges {
		t.Fatalf("verdict = %q, want request_changes", res.Verdict)
	}
	if res.Code != "" {
		t.Fatalf("decided verdict carried a code %q", res.Code)
	}

	ev := tap.await(t, "task-1", 2*time.Second)
	if ev.Verdict != envelope.ReviewRequestChanges {
		t.Fatalf("event verdict = %q, want request_changes", ev.Verdict)
	}
	if ev.Branch != "mesh/worker/task-1" || ev.HeadSHA != "abc123" || ev.Job != "job-1" {
		t.Fatalf("event diff metadata wrong: %+v", ev)
	}
	if ev.SessionID != "sess-auth-1" || ev.NumTurns != 4 {
		t.Fatalf("event session metadata wrong: session=%q turns=%d", ev.SessionID, ev.NumTurns)
	}
	if ev.Code != "" {
		t.Fatalf("decided event carried a code %q", ev.Code)
	}
}

// TestReviewErrorVerdictIsNotApprove proves never-fake-success at the sidecar
// layer: a ReviewFunc that hands back a ReviewError (e.g. the runtime turn was
// lost) is recorded as a typed error verdict with its code — never coerced to
// approve — and the event carries the code.
func TestReviewErrorVerdictIsNotApprove(t *testing.T) {
	cfg := fastConfig(t)
	sc := startMesh(t, cfg, "auth-expert")
	tap := newReviewTap(t, cfg)

	fn := func(_ context.Context, _ ReviewRequest) (ReviewResult, error) {
		return ReviewResult{Verdict: envelope.ReviewError, Code: envelope.ReviewRuntimeLost,
			Notes: "child died mid-review"}, nil
	}
	res, err := sc.Review(context.Background(), fn, ReviewRequest{Task: "task-2", Diff: "x"})
	if err != nil {
		t.Fatalf("Review: %v", err)
	}
	if res.Verdict != envelope.ReviewError || res.Code != envelope.ReviewRuntimeLost {
		t.Fatalf("result = %+v, want error/runtime_lost", res)
	}
	ev := tap.await(t, "task-2", 2*time.Second)
	if ev.Verdict != envelope.ReviewError || ev.Code != envelope.ReviewRuntimeLost {
		t.Fatalf("event = %+v, want error/runtime_lost", ev)
	}
}

// TestReviewInvalidVerdictIsCoercedToError proves defense-in-depth: a
// ReviewFunc that hands back an unset/invalid verdict (a buggy brain) is
// recorded as a typed error with bad_verdict, never silently as approve.
func TestReviewInvalidVerdictIsCoercedToError(t *testing.T) {
	cfg := fastConfig(t)
	sc := startMesh(t, cfg, "auth-expert")
	tap := newReviewTap(t, cfg)

	fn := func(_ context.Context, _ ReviewRequest) (ReviewResult, error) {
		return ReviewResult{Verdict: envelope.ReviewVerdict("ship-it")}, nil
	}
	res, _ := sc.Review(context.Background(), fn, ReviewRequest{Task: "task-3", Diff: "x"})
	if res.Verdict != envelope.ReviewError || res.Code != envelope.ReviewBadVerdict {
		t.Fatalf("result = %+v, want error/bad_verdict", res)
	}
	ev := tap.await(t, "task-3", 2*time.Second)
	if ev.Verdict != envelope.ReviewError || ev.Code != envelope.ReviewBadVerdict {
		t.Fatalf("event = %+v, want error/bad_verdict", ev)
	}
}

// TestReviewSeamFaultIsTypedError proves a true internal seam fault (the
// ReviewFunc returned a Go error) is surfaced AND recorded as a typed internal
// error verdict — the tap still sees a result, the caller still gets the error.
func TestReviewSeamFaultIsTypedError(t *testing.T) {
	cfg := fastConfig(t)
	sc := startMesh(t, cfg, "auth-expert")
	tap := newReviewTap(t, cfg)

	boom := context.DeadlineExceeded
	fn := func(_ context.Context, _ ReviewRequest) (ReviewResult, error) {
		return ReviewResult{}, boom
	}
	res, err := sc.Review(context.Background(), fn, ReviewRequest{Task: "task-4", Diff: "x"})
	if err != boom {
		t.Fatalf("err = %v, want the seam fault surfaced", err)
	}
	if res.Verdict != envelope.ReviewError || res.Code != envelope.ReviewInternal {
		t.Fatalf("result = %+v, want error/internal", res)
	}
	ev := tap.await(t, "task-4", 2*time.Second)
	if ev.Verdict != envelope.ReviewError || ev.Code != envelope.ReviewInternal {
		t.Fatalf("event = %+v, want error/internal", ev)
	}
}

// TestReviewUsageErrors proves the usage guards (nil fn, empty task) are Go
// errors distinct from a produced verdict, and publish nothing.
func TestReviewUsageErrors(t *testing.T) {
	cfg := fastConfig(t)
	sc := startMesh(t, cfg, "auth-expert")

	if _, err := sc.Review(context.Background(), nil, ReviewRequest{Task: "t"}); err == nil {
		t.Fatal("nil ReviewFunc accepted")
	}
	fn := func(_ context.Context, _ ReviewRequest) (ReviewResult, error) {
		return ReviewResult{Verdict: envelope.ReviewApprove}, nil
	}
	if _, err := sc.Review(context.Background(), fn, ReviewRequest{Task: ""}); err == nil {
		t.Fatal("empty task accepted")
	}
}
