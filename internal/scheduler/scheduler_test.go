package scheduler

// Unit acceptance for #25, against a FAKE worker driver (no real CLI, no
// LLM): dependency gating over a 3-node DAG, typed skip of dependents on
// failure, exactly-once teardown, budget/billing fleet pause (queued, never
// failed), rate-limit backoff, and resume across scheduler lifetimes.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/task"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// --- fake driver -----------------------------------------------------------------

// fakeDriver implements the Driver seam in-process. Behavior is per-node and
// per-attempt; everything observable (runs, teardowns, order) is counted.
type fakeDriver struct {
	mu        sync.Mutex
	behave    func(rec task.Record, attempt int) (Result, error)
	spawn     func(rec task.Record) error
	block     map[string]chan struct{} // node → Run waits for close (or ctx)
	started   []string                 // node ids in Run order
	attempts  map[string]int
	teardowns map[string]int
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{
		block:     make(map[string]chan struct{}),
		attempts:  make(map[string]int),
		teardowns: make(map[string]int),
	}
}

func (d *fakeDriver) Spawn(_ context.Context, rec task.Record) (Worker, error) {
	d.mu.Lock()
	spawn := d.spawn
	d.mu.Unlock()
	if spawn != nil {
		if err := spawn(rec); err != nil {
			return nil, err
		}
	}
	return &fakeWorker{d: d, rec: rec}, nil
}

type fakeWorker struct {
	d   *fakeDriver
	rec task.Record
}

func (w *fakeWorker) Run(ctx context.Context) (Result, error) {
	d := w.d
	d.mu.Lock()
	d.attempts[w.rec.Node]++
	attempt := d.attempts[w.rec.Node]
	d.started = append(d.started, w.rec.Node)
	blocker := d.block[w.rec.Node]
	behave := d.behave
	d.mu.Unlock()
	if blocker != nil {
		select {
		case <-blocker:
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
	if behave != nil {
		return behave(w.rec, attempt)
	}
	return Result{Summary: "done"}, nil
}

func (w *fakeWorker) Teardown() error {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	w.d.teardowns[w.rec.Node]++
	return nil
}

func (d *fakeDriver) startedNodes() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.started...)
}

func (d *fakeDriver) attemptCount(node string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.attempts[node]
}

func (d *fakeDriver) teardownCount(node string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.teardowns[node]
}

// --- fixture ---------------------------------------------------------------------

type fixture struct {
	cli    *bus.Client
	jobs   job.Store
	tasks  task.Store
	events func() []envelope.Envelope
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	path := testsock.Path(t, "bus.sock")
	srv := bus.NewServer(path, bus.Options{})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	cli, err := bus.Dial(path, bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cli.Close()
		srv.Stop()
	})

	var (
		mu   sync.Mutex
		seen []envelope.Envelope
	)
	if _, err := cli.Subscribe(envelope.PatternAll, func(env envelope.Envelope) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, env)
	}); err != nil {
		t.Fatal(err)
	}
	return fixture{
		cli:   cli,
		jobs:  job.NewStore(cli),
		tasks: task.NewStore(cli),
		events: func() []envelope.Envelope {
			mu.Lock()
			defer mu.Unlock()
			return append([]envelope.Envelope(nil), seen...)
		},
	}
}

// fanoutPlan is the acceptance DAG: A → B and A → C.
func fanoutPlan() task.Plan {
	return task.Plan{Version: 1, Nodes: []task.Node{
		{ID: "A", Title: "A", Role: "builder"},
		{ID: "B", Title: "B", Role: "builder", DependsOn: []string{"A"}},
		{ID: "C", Title: "C", Role: "builder", DependsOn: []string{"A"}},
	}}
}

// triagedJob persists a job + its DAG exactly the way triage commits them
// (tasks first, then open→triaged), and returns the node→record index.
func (f fixture) triagedJob(t *testing.T, p task.Plan) (job.Record, map[string]task.Record) {
	t.Helper()
	rec, err := f.jobs.Create(job.Record{Repo: "demo", Source: job.SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}
	recs := task.FromPlan(rec.ID, p, time.Now().UTC())
	if err := f.tasks.CreateAll(recs); err != nil {
		t.Fatal(err)
	}
	moved, err := f.jobs.Transition(rec.ID, envelope.JobOpen, envelope.JobTriaged, "test", "")
	if err != nil {
		t.Fatal(err)
	}
	byNode := make(map[string]task.Record, len(recs))
	for _, r := range recs {
		byNode[r.Node] = r
	}
	return moved, byNode
}

func startScheduler(t *testing.T, cli *bus.Client, d Driver, mutate func(*Options)) *Scheduler {
	t.Helper()
	opts := Options{Driver: d, Interval: 10 * time.Millisecond, Backoff: 20 * time.Millisecond}
	if mutate != nil {
		mutate(&opts)
	}
	s, err := New(cli, opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	t.Cleanup(s.Stop)
	return s
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("never happened: %s", what)
}

func (f fixture) jobState(t *testing.T, id string) envelope.JobState {
	t.Helper()
	rec, found, err := f.jobs.Get(id)
	if err != nil || !found {
		t.Fatalf("get job %s: found=%v err=%v", id, found, err)
	}
	return rec.State
}

func (f fixture) taskState(t *testing.T, id string) envelope.TaskState {
	t.Helper()
	rec, found, err := f.tasks.Get(id)
	if err != nil || !found {
		t.Fatalf("get task %s: found=%v err=%v", id, found, err)
	}
	return rec.State
}

func (f fixture) waitJob(t *testing.T, id string, want envelope.JobState) {
	t.Helper()
	waitFor(t, 5*time.Second, fmt.Sprintf("job %s reaches %s", id, want), func() bool {
		return f.jobState(t, id) == want
	})
}

// --- acceptance ------------------------------------------------------------------

// A 3-node DAG A→B, A→C: B and C run only after A succeeds.
func TestDependentsRunOnlyAfterDependencySucceeds(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, fanoutPlan())

	d := newFakeDriver()
	release := make(chan struct{})
	d.block["A"] = release
	startScheduler(t, f.cli, d, nil)

	waitFor(t, 5*time.Second, "A dispatched", func() bool { return d.attemptCount("A") == 1 })
	// Many sweeps pass while A is still running: B and C must stay queued.
	time.Sleep(60 * time.Millisecond)
	if got := d.startedNodes(); len(got) != 1 || got[0] != "A" {
		t.Fatalf("started %v while A still running, want [A] only", got)
	}
	if st := f.taskState(t, byNode["B"].ID); st != envelope.TaskPending {
		t.Fatalf("B state = %s while A running, want pending", st)
	}
	if st := f.taskState(t, byNode["C"].ID); st != envelope.TaskPending {
		t.Fatalf("C state = %s while A running, want pending", st)
	}

	close(release)
	f.waitJob(t, j.ID, envelope.JobDone)

	for _, node := range []string{"A", "B", "C"} {
		if st := f.taskState(t, byNode[node].ID); st != envelope.TaskDone {
			t.Errorf("%s state = %s, want done", node, st)
		}
		if n := d.teardownCount(node); n != 1 {
			t.Errorf("%s teardowns = %d, want exactly 1", node, n)
		}
	}
	if got := d.startedNodes(); got[0] != "A" {
		t.Errorf("run order %v, want A first", got)
	}
}

// Failure of A blocks/skips B and C with the typed terminal cancelled state.
func TestFailedDependencySkipsDependentsTyped(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, fanoutPlan())

	d := newFakeDriver()
	d.behave = func(rec task.Record, _ int) (Result, error) {
		if rec.Node == "A" {
			return Result{Code: envelope.WorkerFailed, Summary: "boom"}, nil
		}
		return Result{Summary: "done"}, nil
	}
	startScheduler(t, f.cli, d, nil)

	f.waitJob(t, j.ID, envelope.JobFailed)
	if st := f.taskState(t, byNode["A"].ID); st != envelope.TaskFailed {
		t.Fatalf("A state = %s, want failed", st)
	}
	for _, node := range []string{"B", "C"} {
		if st := f.taskState(t, byNode[node].ID); st != envelope.TaskCancelled {
			t.Errorf("%s state = %s, want cancelled (skipped)", node, st)
		}
		if n := d.attemptCount(node); n != 0 {
			t.Errorf("%s ran %d times, want never", node, n)
		}
	}
	if n := d.teardownCount("A"); n != 1 {
		t.Errorf("A teardowns = %d, want exactly 1", n)
	}

	// The typed worker outcome reached the tap.
	waitFor(t, 2*time.Second, "KindWorker error envelope", func() bool {
		for _, env := range f.events() {
			if env.Kind != envelope.KindWorker {
				continue
			}
			var p envelope.WorkerPayload
			if envelope.DecodeInto(env, &p) != nil {
				continue
			}
			if p.Task == byNode["A"].ID && p.Result == envelope.WorkerError && p.Code == envelope.WorkerFailed {
				return true
			}
		}
		return false
	})
}

// A panicking driver still tears down exactly once and fails the task typed —
// never crashes the coordinator.
func TestPanickingWorkerFailsTypedAndTearsDownOnce(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, fanoutPlan())

	d := newFakeDriver()
	d.behave = func(rec task.Record, _ int) (Result, error) {
		if rec.Node == "A" {
			panic("driver bug")
		}
		return Result{}, nil
	}
	startScheduler(t, f.cli, d, nil)

	f.waitJob(t, j.ID, envelope.JobFailed)
	if st := f.taskState(t, byNode["A"].ID); st != envelope.TaskFailed {
		t.Fatalf("A state = %s, want failed", st)
	}
	if n := d.teardownCount("A"); n != 1 {
		t.Fatalf("A teardowns = %d, want exactly 1 despite the panic", n)
	}
}

// Hitting the budget cap pauses the fleet: remaining tasks stay pending and
// the job is NOT failed (locked decision: queued, never failed).
func TestBudgetCapPausesFleetQueuedNotFailed(t *testing.T) {
	f := newFixture(t)
	plan := task.Plan{Version: 1, Nodes: []task.Node{
		{ID: "N1", Title: "N1", Role: "builder"},
		{ID: "N2", Title: "N2", Role: "builder"},
		{ID: "N3", Title: "N3", Role: "builder"},
	}}
	j, byNode := f.triagedJob(t, plan)

	d := newFakeDriver()
	d.behave = func(task.Record, int) (Result, error) {
		return Result{Summary: "done", CostUSD: 0.6}, nil
	}
	startScheduler(t, f.cli, d, func(o *Options) {
		o.MaxParallel = 1
		o.BudgetUSD = 1.0
	})

	var fleet envelope.FleetPayload
	waitFor(t, 5*time.Second, "KindFleet paused envelope", func() bool {
		for _, env := range f.events() {
			if env.Kind != envelope.KindFleet {
				continue
			}
			if envelope.DecodeInto(env, &fleet) == nil && fleet.State == envelope.FleetPaused {
				return true
			}
		}
		return false
	})
	if fleet.Code != envelope.FleetBudgetExhausted {
		t.Fatalf("pause code = %s, want budget_exhausted", fleet.Code)
	}
	if fleet.SpentUSD < 1.0 || fleet.BudgetUSD != 1.0 {
		t.Fatalf("fleet payload spent=%v budget=%v, want spent>=1 budget=1", fleet.SpentUSD, fleet.BudgetUSD)
	}

	// Two runs spent 1.2 >= 1.0; the third never spawns and is NOT failed.
	time.Sleep(60 * time.Millisecond) // several would-be sweeps
	total := 0
	pending := 0
	for node := range byNode {
		total += d.attemptCount(node)
		if f.taskState(t, byNode[node].ID) == envelope.TaskPending {
			pending++
		}
	}
	if total != 2 {
		t.Fatalf("total attempts = %d, want exactly 2 before the cap", total)
	}
	if pending != 1 {
		t.Fatalf("pending tasks = %d, want 1 queued (never failed)", pending)
	}
	if st := f.jobState(t, j.ID); st != envelope.JobRunning {
		t.Fatalf("job state = %s, want running (paused fleet must not fail jobs)", st)
	}
}

// A billing_error pauses the fleet; the task stays persisted running for the
// next lifetime to resume — never failed.
func TestBillingErrorPausesFleetTaskStaysRunning(t *testing.T) {
	f := newFixture(t)
	plan := task.Plan{Version: 1, Nodes: []task.Node{{ID: "A", Title: "A", Role: "builder"}}}
	j, byNode := f.triagedJob(t, plan)

	d := newFakeDriver()
	d.behave = func(task.Record, int) (Result, error) {
		return Result{Code: envelope.WorkerBillingError, Summary: "credit exhausted"}, nil
	}
	startScheduler(t, f.cli, d, nil)

	waitFor(t, 5*time.Second, "KindFleet billing_error pause", func() bool {
		for _, env := range f.events() {
			if env.Kind != envelope.KindFleet {
				continue
			}
			var p envelope.FleetPayload
			if envelope.DecodeInto(env, &p) == nil && p.State == envelope.FleetPaused &&
				p.Code == envelope.FleetBillingError {
				return true
			}
		}
		return false
	})
	time.Sleep(60 * time.Millisecond)
	if st := f.taskState(t, byNode["A"].ID); st != envelope.TaskRunning {
		t.Fatalf("A state = %s, want running (queued for resume, never failed)", st)
	}
	if st := f.jobState(t, j.ID); st == envelope.JobFailed {
		t.Fatal("job failed on billing error; want it kept alive for resume")
	}
	if n := d.attemptCount("A"); n != 1 {
		t.Fatalf("A attempts = %d, want 1 (paused fleet must not retry)", n)
	}
}

// rate_limited backs off and re-dispatches; the task never fails and each
// spawned worker is torn down exactly once.
func TestRateLimitBacksOffAndRetries(t *testing.T) {
	f := newFixture(t)
	plan := task.Plan{Version: 1, Nodes: []task.Node{{ID: "A", Title: "A", Role: "builder"}}}
	j, byNode := f.triagedJob(t, plan)

	d := newFakeDriver()
	d.behave = func(_ task.Record, attempt int) (Result, error) {
		if attempt == 1 {
			return Result{Code: envelope.WorkerRateLimited, Summary: "429"}, nil
		}
		return Result{Summary: "done"}, nil
	}
	startScheduler(t, f.cli, d, nil)

	f.waitJob(t, j.ID, envelope.JobDone)
	if n := d.attemptCount("A"); n != 2 {
		t.Fatalf("A attempts = %d, want 2 (one rate-limited, one retry)", n)
	}
	if n := d.teardownCount("A"); n != 2 {
		t.Fatalf("A teardowns = %d, want exactly 1 per spawned worker (2 workers)", n)
	}
	if st := f.taskState(t, byNode["A"].ID); st != envelope.TaskDone {
		t.Fatalf("A state = %s, want done", st)
	}
	// A rate-limited run must never surface as a failed task transition.
	for _, env := range f.events() {
		if env.Kind != envelope.KindTask {
			continue
		}
		var p envelope.TaskPayload
		if envelope.DecodeInto(env, &p) == nil && p.ID == byNode["A"].ID && p.State == envelope.TaskFailed {
			t.Fatal("rate-limited task published a failed transition")
		}
	}
}

// A task left persisted running by a previous lifetime (crash/stop) is
// re-dispatched by the next scheduler over the same durable state.
func TestResumesOrphanedRunningTaskAcrossLifetimes(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, fanoutPlan())

	first := newFakeDriver()
	first.block["A"] = make(chan struct{}) // never released: A wedges mid-run
	s1, err := New(f.cli, Options{Driver: first, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	s1.Start()
	waitFor(t, 5*time.Second, "A dispatched by first lifetime", func() bool {
		return first.attemptCount("A") == 1
	})
	s1.Stop() // ctx-cancels the wedged worker; its outcome is dropped

	if st := f.taskState(t, byNode["A"].ID); st != envelope.TaskRunning {
		t.Fatalf("A state after stop = %s, want running (orphaned)", st)
	}

	second := newFakeDriver()
	startScheduler(t, f.cli, second, nil)
	f.waitJob(t, j.ID, envelope.JobDone)
	if n := second.attemptCount("A"); n != 1 {
		t.Fatalf("second lifetime A attempts = %d, want 1 (orphan resumed)", n)
	}
	for _, node := range []string{"A", "B", "C"} {
		if st := f.taskState(t, byNode[node].ID); st != envelope.TaskDone {
			t.Errorf("%s state = %s, want done", node, st)
		}
	}
}

// A worker that returns WorkerEscalated drives its task to TaskEscalated (not
// done/failed), records the reason, and the scheduler does NOT retry it.
func TestWorkerEscalatedTransitionsTaskAndRecordsReason(t *testing.T) {
	f := newFixture(t)
	const escalationReason = "no concrete acceptance criteria: what does 'make it nicer' mean?"
	plan := task.Plan{Version: 1, Nodes: []task.Node{{ID: "A", Title: "A", Role: "builder"}}}
	j, byNode := f.triagedJob(t, plan)

	d := newFakeDriver()
	d.behave = func(task.Record, int) (Result, error) {
		return Result{Code: envelope.WorkerEscalated, Summary: escalationReason}, nil
	}
	startScheduler(t, f.cli, d, nil)

	waitFor(t, 5*time.Second, "A reaches escalated", func() bool {
		return f.taskState(t, byNode["A"].ID) == envelope.TaskEscalated
	})

	// Task must be escalated, not done/failed.
	if st := f.taskState(t, byNode["A"].ID); st != envelope.TaskEscalated {
		t.Fatalf("A state = %s, want escalated", st)
	}

	// Escalation reason must be recorded on the task record.
	rec, found, err := f.tasks.Get(byNode["A"].ID)
	if err != nil || !found {
		t.Fatalf("get task: found=%v err=%v", found, err)
	}
	if rec.EscalationReason != escalationReason {
		t.Fatalf("EscalationReason = %q, want %q", rec.EscalationReason, escalationReason)
	}

	// Scheduler must treat escalated as terminal: only one attempt, no retry.
	time.Sleep(60 * time.Millisecond) // several would-be sweeps
	if n := d.attemptCount("A"); n != 1 {
		t.Fatalf("A attempts = %d, want 1 (no retry on escalated)", n)
	}
	if n := d.teardownCount("A"); n != 1 {
		t.Fatalf("A teardowns = %d, want 1", n)
	}

	// Job should reach a terminal state (failed — not all tasks done).
	f.waitJob(t, j.ID, envelope.JobFailed)
}

// An escalated task blocks its dependents (they cannot proceed without the
// escalated task's output) and does NOT doom unrelated sibling tasks.
func TestEscalatedTaskBlocksDependentsNotSiblings(t *testing.T) {
	f := newFixture(t)
	// Linear chain A → B; C is independent of both.
	plan := task.Plan{Version: 1, Nodes: []task.Node{
		{ID: "A", Title: "A", Role: "builder"},
		{ID: "B", Title: "B", Role: "builder", DependsOn: []string{"A"}},
		{ID: "C", Title: "C", Role: "builder"},
	}}
	_, byNode := f.triagedJob(t, plan)

	d := newFakeDriver()
	d.behave = func(rec task.Record, _ int) (Result, error) {
		if rec.Node == "A" {
			return Result{Code: envelope.WorkerEscalated, Summary: "ambiguous"}, nil
		}
		return Result{Summary: "done"}, nil
	}
	startScheduler(t, f.cli, d, nil)

	waitFor(t, 5*time.Second, "A escalated", func() bool {
		return f.taskState(t, byNode["A"].ID) == envelope.TaskEscalated
	})
	// B depends on A; A escalated → B must be cancelled, not run.
	waitFor(t, 5*time.Second, "B cancelled (blocked by escalated dep)", func() bool {
		return f.taskState(t, byNode["B"].ID) == envelope.TaskCancelled
	})
	// C is independent; escalation of A must not cancel C.
	waitFor(t, 5*time.Second, "C done (unaffected by A escalation)", func() bool {
		return f.taskState(t, byNode["C"].ID) == envelope.TaskDone
	})

	if n := d.attemptCount("B"); n != 0 {
		t.Fatalf("B attempts = %d, want 0 (blocked by escalated dep)", n)
	}
}

// A spawn failure is a typed spawn_failed outcome — task failed, no teardown
// (nothing was spawned), dependents skipped.
func TestSpawnFailureFailsTaskTyped(t *testing.T) {
	f := newFixture(t)
	j, byNode := f.triagedJob(t, fanoutPlan())

	d := newFakeDriver()
	d.spawn = func(rec task.Record) error {
		if rec.Node == "A" {
			return fmt.Errorf("no worktree")
		}
		return nil
	}
	startScheduler(t, f.cli, d, nil)

	f.waitJob(t, j.ID, envelope.JobFailed)
	if st := f.taskState(t, byNode["A"].ID); st != envelope.TaskFailed {
		t.Fatalf("A state = %s, want failed", st)
	}
	if n := d.teardownCount("A"); n != 0 {
		t.Fatalf("A teardowns = %d, want 0 (never spawned)", n)
	}
	waitFor(t, 2*time.Second, "typed spawn_failed worker envelope", func() bool {
		for _, env := range f.events() {
			if env.Kind != envelope.KindWorker {
				continue
			}
			var p envelope.WorkerPayload
			if envelope.DecodeInto(env, &p) == nil && p.Task == byNode["A"].ID &&
				p.Code == envelope.WorkerSpawnFailed {
				return true
			}
		}
		return false
	})
}
