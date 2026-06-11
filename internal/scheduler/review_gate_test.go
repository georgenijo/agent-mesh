package scheduler

// Unit acceptance for the #80 review gate, against FAKE driver + reviewer (no
// bus round trip, no LLM): the verdict→terminal-state policy matrix, the
// opt-out (nil Reviewer = pre-#80 behavior), and budget accounting of review
// cost on the same meter as worker runs.

import (
	"context"
	"strings"
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

// seqReviewer resolves reviews in sequence: call N gets decs[N] (the last
// decision repeats once the script is exhausted).
type seqReviewer struct {
	mu      sync.Mutex
	decs    []ReviewDecision
	targets []ReviewTarget
}

func (r *seqReviewer) Review(_ context.Context, target ReviewTarget) (ReviewDecision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets = append(r.targets, target)
	i := len(r.targets) - 1
	if i >= len(r.decs) {
		i = len(r.decs) - 1
	}
	return r.decs[i], nil
}

func (r *seqReviewer) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.targets)
}

// feedbackNotes replays the repo blackboard and returns the coordinator's
// durable feedback notes (sender-bound, so the worker primer will carry them).
func (f fixture) feedbackNotes(t *testing.T, repo string) []string {
	t.Helper()
	entries, err := f.cli.StreamRead(envelope.StreamNotes(repo), 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		env, err := envelope.Decode(e.Data)
		if err != nil || env.Kind != envelope.KindNote {
			continue
		}
		var p envelope.NotePayload
		if envelope.DecodeInto(env, &p) != nil {
			continue
		}
		if env.From != schedulerID || p.ID != schedulerID {
			continue // sender binding: a note the primer would drop is no feedback channel
		}
		out = append(out, p.Decision)
	}
	return out
}

// request_changes with retries left (#85): the task stays running, is
// re-dispatched with the reviewer's notes recorded on the durable blackboard
// (the worker primer's source), and the new diff is re-reviewed — approve on
// the retry reaches done.
func TestReviewRequestChangesRetriesWithFeedback(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, singlePlan())

	rev := &seqReviewer{decs: []ReviewDecision{
		{Verdict: envelope.ReviewRequestChanges, Notes: "missing tests"},
		{Verdict: envelope.ReviewApprove, Notes: "fixed"},
	}}
	d := newFakeDriver()
	startScheduler(t, f.cli, d, func(o *Options) { o.Reviewer = rev; o.ReviewRetries = 1 })

	f.waitJob(t, j.ID, envelope.JobDone)
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskDone {
		t.Fatalf("task A = %s, want done after the retried diff is approved", got)
	}
	if got := d.attemptCount("A"); got != 2 {
		t.Fatalf("worker ran %d times, want 2 (original + one feedback retry)", got)
	}
	if got := rev.calls(); got != 2 {
		t.Fatalf("reviewer called %d times, want 2 (the retried diff is re-reviewed)", got)
	}
	notes := f.feedbackNotes(t, "demo")
	found := false
	for _, n := range notes {
		if strings.Contains(n, byNode["A"].ID) && strings.Contains(n, "missing tests") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no durable feedback note carrying the task id and verdict notes; got %q", notes)
	}
}

// Retries are bounded: once exhausted, request_changes fails the task exactly
// as the pre-#85 policy did.
func TestReviewRequestChangesExhaustsRetriesAndFails(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, singlePlan())

	rev := &seqReviewer{decs: []ReviewDecision{
		{Verdict: envelope.ReviewRequestChanges, Notes: "still missing tests"},
	}}
	d := newFakeDriver()
	startScheduler(t, f.cli, d, func(o *Options) { o.Reviewer = rev; o.ReviewRetries = 1 })

	f.waitJob(t, j.ID, envelope.JobFailed)
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskFailed {
		t.Fatalf("task A = %s, want failed after retries exhausted", got)
	}
	if got := d.attemptCount("A"); got != 2 {
		t.Fatalf("worker ran %d times, want 2 (original + one bounded retry)", got)
	}
}

// reject conserves budget: it fails immediately, never a retry — feedback
// cannot fix a fundamentally wrong approach.
func TestReviewRejectNeverRetries(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, singlePlan())

	rev := &fakeReviewer{dec: ReviewDecision{Verdict: envelope.ReviewReject, Notes: "wrong approach"}}
	d := newFakeDriver()
	startScheduler(t, f.cli, d, func(o *Options) { o.Reviewer = rev; o.ReviewRetries = 3 })

	f.waitJob(t, j.ID, envelope.JobFailed)
	if got := d.attemptCount("A"); got != 1 {
		t.Fatalf("worker ran %d times after reject, want 1 (never retried)", got)
	}
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskFailed {
		t.Fatalf("task A = %s, want failed", got)
	}
}

// Review errors never retry either: the verdict was never produced, so there
// is no feedback to act on — the typed failure stands.
func TestReviewErrorNeverRetries(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, singlePlan())

	rev := &fakeReviewer{dec: ReviewDecision{Verdict: envelope.ReviewError, Code: envelope.ReviewRuntimeLost}}
	d := newFakeDriver()
	startScheduler(t, f.cli, d, func(o *Options) { o.Reviewer = rev; o.ReviewRetries = 3 })

	f.waitJob(t, j.ID, envelope.JobFailed)
	if got := d.attemptCount("A"); got != 1 {
		t.Fatalf("worker ran %d times after a review error, want 1", got)
	}
	if got := f.taskState(t, byNode["A"].ID); got != envelope.TaskFailed {
		t.Fatalf("task A = %s, want failed", got)
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
