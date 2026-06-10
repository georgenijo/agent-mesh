package scheduler

// Unit acceptance for the #80 review gate, against FAKE driver + reviewer (no
// bus round trip, no LLM): the verdict→terminal-state policy matrix, the
// opt-out (nil Reviewer = pre-#80 behavior), and budget accounting of review
// cost on the same meter as worker runs.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// fakeReviewer resolves every review with one canned decision (or error).
type fakeReviewer struct {
	mu      sync.Mutex
	dec     ReviewDecision
	err     error
	targets []ReviewTarget
}

func (r *fakeReviewer) Review(_ context.Context, target ReviewTarget) (ReviewDecision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets = append(r.targets, target)
	return r.dec, r.err
}

func (r *fakeReviewer) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.targets)
}

func singlePlan() task.Plan {
	return task.Plan{Version: 1, Nodes: []task.Node{
		{ID: "A", Title: "A", Role: "builder"},
	}}
}

func chainPlan() task.Plan {
	return task.Plan{Version: 1, Nodes: []task.Node{
		{ID: "A", Title: "A", Role: "builder"},
		{ID: "B", Title: "B", Role: "builder", DependsOn: []string{"A"}},
	}}
}

// approve → the task reaches done and the job completes (the only reviewed
// path to done).
func TestReviewApproveTransitionsTaskDone(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, singlePlan())

	rev := &fakeReviewer{dec: ReviewDecision{Verdict: envelope.ReviewApprove, Notes: "looks fine"}}
	startScheduler(t, f.cli, newFakeDriver(), func(o *Options) { o.Reviewer = rev })

	f.waitJob(t, j.ID, envelope.JobDone)
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskDone {
		t.Fatalf("task A = %s, want done", got)
	}
	if rev.calls() != 1 {
		t.Fatalf("reviewer called %d times, want 1", rev.calls())
	}
}

// reject → the task fails and the existing fail-fast path cancels dependents;
// the diff never silently passes.
func TestReviewRejectFailsTaskAndCancelsDependents(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, chainPlan())

	rev := &fakeReviewer{dec: ReviewDecision{Verdict: envelope.ReviewReject, Notes: "fundamentally wrong"}}
	startScheduler(t, f.cli, newFakeDriver(), func(o *Options) { o.Reviewer = rev })

	f.waitJob(t, j.ID, envelope.JobFailed)
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskFailed {
		t.Fatalf("task A = %s, want failed", got)
	}
	if got := f.taskState(t, byNode["B"].ID); got != envelope.TaskCancelled {
		t.Fatalf("dependent B = %s, want cancelled", got)
	}
}

// request_changes → the task fails (typed, with the verdict in the reason);
// re-dispatch-with-feedback is the documented deferral, never a silent done.
func TestReviewRequestChangesFailsTask(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, singlePlan())

	rev := &fakeReviewer{dec: ReviewDecision{Verdict: envelope.ReviewRequestChanges, Notes: "missing tests"}}
	startScheduler(t, f.cli, newFakeDriver(), func(o *Options) { o.Reviewer = rev })

	f.waitJob(t, j.ID, envelope.JobFailed)
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskFailed {
		t.Fatalf("task A = %s, want failed", got)
	}
}

// Every ReviewError code is a typed non-success: the task fails, never done.
func TestReviewErrorNeverApproves(t *testing.T) {
	for _, code := range []envelope.ReviewErrorCode{
		envelope.ReviewRuntimeLost,
		envelope.ReviewRuntimeError,
		envelope.ReviewBadVerdict,
		envelope.ReviewEmptyDiff,
		envelope.ReviewInternal,
	} {
		t.Run(string(code), func(t *testing.T) {
			f := newFixture(t)
			j, byNode := f.triagedJob(t, singlePlan())

			rev := &fakeReviewer{dec: ReviewDecision{Verdict: envelope.ReviewError, Code: code}}
			startScheduler(t, f.cli, newFakeDriver(), func(o *Options) { o.Reviewer = rev })

			f.waitJob(t, j.ID, envelope.JobFailed)
			if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskFailed {
				t.Fatalf("task A = %s, want failed (code %s must never approve)", got, code)
			}
		})
	}
}

// A reviewer Go error (unclassifiable fault) maps to a typed failure too.
func TestReviewerErrorMapsToTypedFailure(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, singlePlan())

	rev := &fakeReviewer{err: context.DeadlineExceeded}
	startScheduler(t, f.cli, newFakeDriver(), func(o *Options) { o.Reviewer = rev })

	f.waitJob(t, j.ID, envelope.JobFailed)
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskFailed {
		t.Fatalf("task A = %s, want failed", got)
	}
}

// A typed success with nothing to review (head == base) reaches done without
// a verdict — the gate gates diffs, not no-ops.
func TestReviewNoDiffSucceedsWithoutVerdict(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, singlePlan())

	rev := &fakeReviewer{dec: ReviewDecision{NoDiff: true}}
	startScheduler(t, f.cli, newFakeDriver(), func(o *Options) { o.Reviewer = rev })

	f.waitJob(t, j.ID, envelope.JobDone)
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskDone {
		t.Fatalf("task A = %s, want done", got)
	}
}

// Opt-out: with no Reviewer configured the scheduler behaves exactly as
// before #80 — worker success → done, no review round trip.
func TestNoReviewerConfiguredSuccessIsDone(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, singlePlan())

	startScheduler(t, f.cli, newFakeDriver(), nil) // Options.Reviewer stays nil

	f.waitJob(t, j.ID, envelope.JobDone)
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskDone {
		t.Fatalf("task A = %s, want done", got)
	}
}

// Review cost counts toward the same MESH_BUDGET_USD meter as worker runs:
// worker (0.3) + review (0.8) crosses the 1.0 cap, so the fleet pauses and
// the dependent task never spawns — queued, never failed (locked decision).
func TestReviewCostCountsTowardBudget(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, chainPlan())

	d := newFakeDriver()
	d.behave = func(task.Record, int) (Result, error) {
		return Result{Summary: "done", CostUSD: 0.3}, nil
	}
	rev := &fakeReviewer{dec: ReviewDecision{Verdict: envelope.ReviewApprove, CostUSD: 0.8}}
	startScheduler(t, f.cli, d, func(o *Options) { o.Reviewer = rev; o.BudgetUSD = 1.0 })

	// A is approved (done); the budget is then exhausted, so B never runs.
	waitFor(t, 5*time.Second, "task A done after approved review", func() bool {
		return f.taskState(t, byNode["A"].ID) == envelope.TaskDone
	})
	waitFor(t, 5*time.Second, "fleet pause event published", func() bool {
		for _, env := range f.events() {
			if env.Kind != envelope.KindFleet {
				continue
			}
			var p envelope.FleetPayload
			if envelope.DecodeInto(env, &p) == nil &&
				p.State == envelope.FleetPaused && p.Code == envelope.FleetBudgetExhausted {
				return true
			}
		}
		return false
	})
	// Paused means queued, never failed: B stays pending, the job stays open.
	time.Sleep(50 * time.Millisecond) // a few sweep intervals
	if got := f.taskState(t, byNode["B"].ID); got != envelope.TaskPending {
		t.Fatalf("task B = %s, want pending (paused fleet must not run or fail it)", got)
	}
	if got := d.attemptCount("B"); got != 0 {
		t.Fatalf("task B ran %d times under a paused fleet, want 0", got)
	}
	if got := f.jobState(t, j.ID); got == envelope.JobDone || got == envelope.JobFailed {
		t.Fatalf("job = %s, want non-terminal under a paused fleet", got)
	}
}

// When the worker run itself exhausts the budget, the gate spends no review
// turn: the fleet pauses first and the task stays persisted running for the
// next coordinator lifetime — a review is an LLM turn and obeys the hard cap.
func TestPausedFleetDefersReview(t *testing.T) {
	f := newFixture(t)
	_, byNode := f.triagedJob(t, singlePlan())

	d := newFakeDriver()
	d.behave = func(task.Record, int) (Result, error) {
		return Result{Summary: "done", CostUSD: 2.0}, nil
	}
	rev := &fakeReviewer{dec: ReviewDecision{Verdict: envelope.ReviewApprove}}
	startScheduler(t, f.cli, d, func(o *Options) { o.Reviewer = rev; o.BudgetUSD = 1.0 })

	waitFor(t, 5*time.Second, "fleet pauses on the worker cost", func() bool {
		for _, env := range f.events() {
			if env.Kind == envelope.KindFleet {
				return true
			}
		}
		return false
	})
	time.Sleep(50 * time.Millisecond)
	if got := rev.calls(); got != 0 {
		t.Fatalf("reviewer called %d times under a paused fleet, want 0", got)
	}
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskRunning {
		t.Fatalf("task A = %s, want running (deferred to the next lifetime, never failed)", got)
	}
}
