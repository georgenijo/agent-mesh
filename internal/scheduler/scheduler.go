// Package scheduler turns a triaged job's persisted task DAG into worker
// lifecycle decisions (#25): dispatch a task only when every dependency is
// done, skip dependents of a failed task with a typed terminal state, tear
// each worker down exactly once, and drive the job lifecycle
// (triaged→scheduled→running→done|failed) through the CAS legality tables.
//
// It mirrors the #24 triage loop: a coordinator-embedded sweep over the KV
// authorities (jobs + tasks buckets), self-healing by construction — every
// sweep recomputes the whole picture from persisted state, so a coordinator
// restart resumes mid-job (the buckets are durable, #65) and a task that was
// running when the old coordinator died is simply re-dispatched.
//
// Budget posture (locked decision, DECISIONS.md 2026-06-09): the monthly
// credit is the scarce resource. Every worker result's CostUSD accumulates;
// reaching the configured cap — or any billing_error — PAUSES the fleet:
// nothing new spawns, jobs and tasks stay queued/pending in their KV records,
// never failed. The meter is in-memory per coordinator lifetime; restarting
// the coordinator (e.g. after the monthly credit refresh) is the reset.
// rate_limited results back off and retry; they never fail the task.
package scheduler

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"context"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// schedulerID is the From id scheduler events carry on the bus. The
// scheduler runs inside the coordinator process, so it speaks as the
// coordinator.
const schedulerID = "coordinator"

// Options configure a Scheduler. Zero values get safe defaults except
// Driver, which is required (the coordinator only constructs a Scheduler
// when the operator configured a worker CLI).
type Options struct {
	Driver      Driver
	BudgetUSD   float64       // fleet budget cap; 0 = unlimited
	MaxParallel int           // max concurrent workers (default 4)
	Interval    time.Duration // sweep cadence (default 5s)
	Backoff     time.Duration // rate-limit re-dispatch delay (default 30s)
	Log         *slog.Logger

	// Reviewer gates worker successes on an expert review (#80). Nil = review
	// gating off: a worker success transitions the task to done exactly as
	// before. Set (the coordinator wires a BusReviewer when MESH_REVIEW_ROLE
	// is configured), every typed worker success is routed for review and the
	// task's terminal state follows the gate policy (review.go): only approve
	// — or a success with no diff to review — reaches done; request_changes,
	// reject, and every review error fail the task, never silently pass it.
	// Review cost accrues against the same budget meter as worker runs.
	Reviewer Reviewer
}

// outcome is one finished worker run, delivered from a worker goroutine to
// the loop goroutine — the only goroutine that touches scheduler state or
// writes the KV.
type outcome struct {
	task     task.Record
	res      Result
	err      error // spawn/run error; maps to a typed code
	spawnErr bool
}

// Scheduler sweeps the jobs bucket for triaged/scheduled/running jobs and
// dispatches their runnable tasks through the worker driver. One loop
// goroutine owns all state and KV writes; worker goroutines only run the
// driver and report back.
type Scheduler struct {
	opts Options
	cli  *bus.Client
	log  *slog.Logger

	jobs  job.Store
	tasks task.Store

	results chan outcome
	reviews chan reviewOutcome

	// Loop-goroutine-only state.
	inflight map[string]string    // task id → job id, workers in flight this lifetime
	retryAt  map[string]time.Time // task id → earliest re-dispatch (rate-limit backoff)
	spent    float64              // accumulated CostUSD this coordinator lifetime
	paused   bool                 // fleet paused: nothing new spawns until restart

	ctx     context.Context // cancels in-flight workers on Stop
	cancel  context.CancelFunc
	stop    chan struct{}
	loopWG  sync.WaitGroup
	workWG  sync.WaitGroup
	started bool
}

// New builds a Scheduler over the given bus client.
func New(cli *bus.Client, opts Options) (*Scheduler, error) {
	if opts.Driver == nil {
		return nil, errors.New("scheduler: Driver is required")
	}
	if opts.MaxParallel <= 0 {
		opts.MaxParallel = 4
	}
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 30 * time.Second
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		opts:     opts,
		cli:      cli,
		log:      opts.Log,
		jobs:     job.NewStore(cli),
		tasks:    task.NewStore(cli),
		results:  make(chan outcome),
		reviews:  make(chan reviewOutcome),
		inflight: make(map[string]string),
		retryAt:  make(map[string]time.Time),
		ctx:      ctx,
		cancel:   cancel,
		stop:     make(chan struct{}),
	}, nil
}

// Start launches the sweep loop.
func (s *Scheduler) Start() {
	s.started = true
	s.loopWG.Add(1)
	go s.loop()
}

// Stop halts the loop, cancels in-flight workers, and waits for both. Each
// spawned worker's Teardown still runs exactly once (in its own goroutine's
// defer); a result that arrives during shutdown is dropped — its task stays
// persisted running and the next coordinator lifetime re-dispatches it.
func (s *Scheduler) Stop() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.cancel()
	s.workWG.Wait()
	if s.started {
		s.loopWG.Wait()
	}
}

func (s *Scheduler) loop() {
	defer s.loopWG.Done()
	ticker := time.NewTicker(s.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case o := <-s.results:
			s.handleOutcome(o)
			s.sweepOnce() // chain: a finished dependency may unlock dependents now
		case ro := <-s.reviews:
			s.handleReview(ro)
			s.sweepOnce() // an approved dependency may unlock dependents now
		case <-ticker.C:
			s.sweepOnce()
		}
	}
}

// sweepOnce walks every non-terminal job: adopts freshly-triaged ones and
// advances the DAG of each scheduled/running one. Stateless over the KV by
// design — restart-safe and self-healing.
func (s *Scheduler) sweepOnce() {
	if s.paused {
		return
	}
	jobs, err := s.jobs.List()
	if err != nil {
		s.log.Warn("scheduler: list jobs failed", "err", err)
		return
	}
	for _, j := range jobs {
		select {
		case <-s.stop:
			return
		default:
		}
		switch j.State {
		case envelope.JobTriaged:
			adopted, err := s.jobs.Transition(j.ID, envelope.JobTriaged, envelope.JobScheduled,
				schedulerID, "adopted by scheduler")
			if err != nil {
				s.log.Warn("scheduler: adopt job failed", "job", j.ID, "err", err)
				continue
			}
			s.publishJob(adopted)
			s.sweepJob(adopted)
		case envelope.JobScheduled, envelope.JobRunning:
			s.sweepJob(j)
		}
		if s.paused {
			return
		}
	}
}

// sweepJob advances one job's DAG: cancels blocked/doomed nodes, dispatches
// runnable ones, resumes orphaned running ones, and finalizes the job when
// every node is terminal.
func (s *Scheduler) sweepJob(j job.Record) {
	recs, err := s.tasks.ListByJob(j.ID)
	if err != nil {
		s.log.Warn("scheduler: list tasks failed", "job", j.ID, "err", err)
		return
	}
	if len(recs) == 0 {
		// A triaged job always has tasks (tasks-first commit order, #24).
		// An empty DAG here is a corrupt store, not a schedulable job.
		s.log.Warn("scheduler: triaged job has no tasks; leaving as-is", "job", j.ID)
		return
	}
	byID := make(map[string]task.Record, len(recs))
	for _, r := range recs {
		byID[r.ID] = r
	}
	doomed := anyFailed(recs)
	anyRunning := false

	for i := range recs {
		rec := recs[i]
		switch GateOf(rec, byID) {
		case GateBlocked:
			dep, _ := blockingDep(rec, byID)
			s.cancelTask(rec, byID, fmt.Sprintf("dependency %s did not succeed", dep))
		case GateQueued, GateRunnable:
			if doomed {
				// Fail-fast: a failed sibling dooms the job; spending worker
				// turns on it wastes the budget (the scarce resource).
				s.cancelTask(rec, byID, "job doomed by a failed task")
				continue
			}
			if GateOf(rec, byID) == GateRunnable && s.dispatch(j, rec, byID) {
				anyRunning = true
			}
		case GateRunning:
			if _, busy := s.inflight[rec.ID]; busy {
				anyRunning = true
				continue
			}
			if doomed {
				// Orphan (restart / dropped shutdown result) in a doomed job:
				// re-running it cannot save the job.
				s.cancelTask(rec, byID, "job doomed by a failed task")
				continue
			}
			// Orphaned by a previous lifetime, or backing off after a
			// rate-limit: re-dispatch without a state transition (the record
			// is already running — the KV authority is correct).
			if time.Now().Before(s.retryAt[rec.ID]) {
				anyRunning = true
				continue
			}
			if s.checkBudgetBeforeSpawn() && len(s.inflight) < s.opts.MaxParallel {
				s.spawn(j, rec)
				anyRunning = true
			}
		}
		if s.paused {
			return
		}
	}

	// Re-read the job's tasks only through our in-memory view: cancelTask and
	// dispatch keep byID current, so terminal detection sees this sweep's moves.
	if allTerminal(byID) {
		s.finalize(j, byID)
		return
	}
	if j.State == envelope.JobScheduled && anyRunning {
		moved, err := s.jobs.Transition(j.ID, envelope.JobScheduled, envelope.JobRunning,
			schedulerID, "first task dispatched")
		if err != nil {
			s.log.Warn("scheduler: job scheduled→running failed", "job", j.ID, "err", err)
			return
		}
		s.publishJob(moved)
	}
}

// dispatch moves a runnable task pending→running and spawns its worker.
// Returns true when a worker is now in flight for it.
func (s *Scheduler) dispatch(j job.Record, rec task.Record, byID map[string]task.Record) bool {
	if len(s.inflight) >= s.opts.MaxParallel {
		return false
	}
	if !s.checkBudgetBeforeSpawn() {
		return false
	}
	updated, err := s.tasks.Transition(rec.ID, envelope.TaskPending, envelope.TaskRunning,
		schedulerID, "dependencies satisfied")
	if err != nil {
		// Lost a CAS race or stale read: the next sweep sees the truth.
		s.log.Warn("scheduler: dispatch transition failed", "task", rec.ID, "err", err)
		return false
	}
	byID[rec.ID] = updated
	s.publishTask(updated)
	s.spawn(j, updated)
	return true
}

// spawn launches the worker goroutine for a task already persisted running.
func (s *Scheduler) spawn(j job.Record, rec task.Record) {
	s.inflight[rec.ID] = j.ID
	delete(s.retryAt, rec.ID)
	s.workWG.Add(1)
	go s.runWorker(rec)
}

// runWorker runs on its own goroutine: spawn → run → teardown (exactly once,
// structurally — the single deferred call below is the only teardown site),
// then reports the outcome to the loop. A panicking driver is converted into
// a typed failure, never a coordinator crash.
func (s *Scheduler) runWorker(rec task.Record) {
	defer s.workWG.Done()
	o := outcome{task: rec}
	w, err := s.opts.Driver.Spawn(s.ctx, rec)
	if err != nil {
		o.err, o.spawnErr = err, true
	} else {
		func() {
			defer func() {
				if terr := w.Teardown(); terr != nil {
					s.log.Warn("scheduler: worker teardown failed", "task", rec.ID, "err", terr)
				}
			}()
			defer func() {
				if p := recover(); p != nil {
					o.err = fmt.Errorf("worker panicked: %v", p)
				}
			}()
			o.res, o.err = w.Run(s.ctx)
		}()
	}
	select {
	case s.results <- o:
	case <-s.stop:
		// Shutting down: drop the outcome. The task stays persisted running
		// and the next coordinator lifetime re-dispatches it.
	}
}

// handleOutcome records one finished run: cost accounting, the typed
// KindWorker event, the task transition per the locked policy, and the
// budget check. Loop goroutine only.
func (s *Scheduler) handleOutcome(o outcome) {
	delete(s.inflight, o.task.ID)
	res := o.res
	if o.err != nil {
		code := envelope.WorkerFailed
		if o.spawnErr {
			code = envelope.WorkerSpawnFailed
		}
		res = Result{Code: code, Summary: o.err.Error(), CostUSD: o.res.CostUSD}
	}
	s.spent += res.CostUSD
	s.publishWorker(o.task, res)

	switch {
	case res.Succeeded():
		// Record the output branch before gating, so a dependent task
		// dispatched after this one reaches done can base its worktree on it
		// (#26 dependency inheritance). Done unconditionally and best-effort:
		// the branch is recorded whether or not review gating is on, so a
		// later approve→done needs no second write. A missing branch only
		// costs a dependent the inherited diff, never state-machine correctness.
		if res.Branch != "" {
			if err := s.tasks.SetBranch(o.task.ID, res.Branch); err != nil {
				s.log.Warn("scheduler: record task output branch failed", "task", o.task.ID, "err", err)
			}
		}
		if s.opts.Reviewer == nil {
			// Review gating off (the pre-#80 contract): success is done.
			s.transitionTask(o.task.ID, envelope.TaskRunning, envelope.TaskDone, "worker succeeded")
			break
		}
		s.maybeStartReview(o.task, res)
	case res.Code == envelope.WorkerRateLimited:
		// Back off, never fail: the task stays persisted running and is
		// re-dispatched once the backoff elapses.
		s.retryAt[o.task.ID] = time.Now().Add(s.opts.Backoff)
		s.log.Warn("scheduler: rate limited; backing off", "task", o.task.ID, "backoff", s.opts.Backoff)
	case res.Code == envelope.WorkerBillingError:
		// Pause the fleet, never fail: the task stays persisted running and
		// the next lifetime (after the operator resets) resumes it.
		s.pause(envelope.FleetBillingError, truncate(res.Summary, 512))
	default:
		s.transitionTask(o.task.ID, envelope.TaskRunning, envelope.TaskFailed,
			truncate(fmt.Sprintf("%s: %s", res.Code, res.Summary), 512))
	}

	if s.opts.BudgetUSD > 0 && s.spent >= s.opts.BudgetUSD {
		s.pause(envelope.FleetBudgetExhausted,
			fmt.Sprintf("spent %.4f of %.4f USD", s.spent, s.opts.BudgetUSD))
	}
}

// --- review gating (#80) -----------------------------------------------------------

// reviewOutcome is one resolved review, delivered from a review goroutine to
// the loop goroutine — the same single-writer discipline as worker outcomes.
type reviewOutcome struct {
	task task.Record
	dec  ReviewDecision
}

// maybeStartReview routes a typed worker success to the reviewer. The task
// stays persisted running and stays in s.inflight while the review is in
// flight, so sweeps neither re-dispatch nor cancel it; a coordinator restart
// loses only the in-memory review state and honestly re-runs the task (the
// same posture as an orphaned running worker). When the fleet is paused (the
// run that just landed may have exhausted the budget), no review is requested
// — a review is an expert LLM turn and must respect the same hard cap; the
// task stays running and the next coordinator lifetime resumes it.
// Loop goroutine only.
func (s *Scheduler) maybeStartReview(rec task.Record, res Result) {
	if !s.checkBudgetBeforeSpawn() {
		s.log.Warn("scheduler: fleet paused; review deferred, task stays running", "task", rec.ID)
		return
	}
	repo := ""
	if j, found, err := s.jobs.Get(rec.Job); err == nil && found {
		repo = j.Repo
	}
	s.inflight[rec.ID] = rec.Job
	s.workWG.Add(1)
	go s.runReview(rec, repo, res.Summary)
}

// runReview runs on its own goroutine: one Reviewer round trip, then report
// to the loop. A reviewer Go error (a fault it could not classify) maps to a
// typed ReviewError — never an approval; a panicking reviewer likewise.
func (s *Scheduler) runReview(rec task.Record, repo, summary string) {
	defer s.workWG.Done()
	var dec ReviewDecision
	var err error
	func() {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("reviewer panicked: %v", p)
			}
		}()
		dec, err = s.opts.Reviewer.Review(s.ctx, ReviewTarget{Task: rec, Repo: repo, Summary: summary})
	}()
	if err != nil {
		dec = ReviewDecision{Verdict: envelope.ReviewError, Code: envelope.ReviewInternal,
			Notes: err.Error(), CostUSD: dec.CostUSD}
	}
	select {
	case s.reviews <- reviewOutcome{task: rec, dec: dec}:
	case <-s.stop:
		// Shutting down: drop the outcome. The task stays persisted running
		// and the next coordinator lifetime re-runs and re-reviews it.
	}
}

// handleReview applies the gate policy (review.go) to one resolved review:
// cost accounting against the same budget meter as worker runs, then the task
// transition. Only approve — or a success with no diff to review — reaches
// done; everything else, including every review error, fails the task (the
// existing fail-fast sweep then cancels dependents). Never a silent approve.
// Loop goroutine only.
func (s *Scheduler) handleReview(o reviewOutcome) {
	delete(s.inflight, o.task.ID)
	dec := o.dec
	s.spent += dec.CostUSD

	switch {
	case dec.NoDiff:
		s.transitionTask(o.task.ID, envelope.TaskRunning, envelope.TaskDone,
			"worker succeeded (no diff to review)")
	case dec.Verdict == envelope.ReviewApprove:
		s.transitionTask(o.task.ID, envelope.TaskRunning, envelope.TaskDone,
			truncate("review approved: "+dec.Notes, 512))
	case dec.Verdict == envelope.ReviewRequestChanges, dec.Verdict == envelope.ReviewReject:
		s.transitionTask(o.task.ID, envelope.TaskRunning, envelope.TaskFailed,
			truncate(fmt.Sprintf("review %s: %s", dec.Verdict, dec.Notes), 512))
	default:
		// ReviewError — or anything outside the closed verdict set (defense in
		// depth). The absence of a clean verdict is never an approval.
		code := dec.Code
		if code == "" {
			code = envelope.ReviewInternal
		}
		s.transitionTask(o.task.ID, envelope.TaskRunning, envelope.TaskFailed,
			truncate(fmt.Sprintf("review error (%s): %s", code, dec.Notes), 512))
	}

	if s.opts.BudgetUSD > 0 && s.spent >= s.opts.BudgetUSD {
		s.pause(envelope.FleetBudgetExhausted,
			fmt.Sprintf("spent %.4f of %.4f USD", s.spent, s.opts.BudgetUSD))
	}
}

// finalize moves a job whose every task is terminal into done or failed. A
// job with any failed task fails; cancelled-but-never-failed cannot arise
// from this scheduler's own moves, but is treated as failed too — done must
// mean every node succeeded.
func (s *Scheduler) finalize(j job.Record, byID map[string]task.Record) {
	allDone := true
	for _, rec := range byID {
		if rec.State != envelope.TaskDone {
			allDone = false
			break
		}
	}
	cur := j.State
	if cur == envelope.JobScheduled && allDone {
		// done is only reachable from running in the legality table; a job
		// that raced a restart between task completion and its own
		// scheduled→running move passes through running first.
		moved, err := s.jobs.Transition(j.ID, envelope.JobScheduled, envelope.JobRunning,
			schedulerID, "finalizing")
		if err != nil {
			s.log.Warn("scheduler: finalize pre-transition failed", "job", j.ID, "err", err)
			return
		}
		s.publishJob(moved)
		cur = envelope.JobRunning
	}
	to, reason := envelope.JobDone, "all tasks done"
	if !allDone {
		to, reason = envelope.JobFailed, "one or more tasks did not succeed"
	}
	moved, err := s.jobs.Transition(j.ID, cur, to, schedulerID, reason)
	if err != nil {
		s.log.Warn("scheduler: finalize transition failed", "job", j.ID, "to", string(to), "err", err)
		return
	}
	s.publishJob(moved)
	s.log.Info("scheduler: job finalized", "job", j.ID, "state", string(to))
}

// cancelTask skips a pending task — or reaps an orphaned running one in a
// doomed job — with the typed terminal cancelled state, keeping byID current.
func (s *Scheduler) cancelTask(rec task.Record, byID map[string]task.Record, reason string) {
	if _, busy := s.inflight[rec.ID]; busy {
		return // never yank a task a live worker is executing
	}
	from := rec.State
	if from != envelope.TaskPending && from != envelope.TaskRunning {
		return
	}
	updated, err := s.tasks.Transition(rec.ID, from, envelope.TaskCancelled, schedulerID, reason)
	if err != nil {
		s.log.Warn("scheduler: cancel transition failed", "task", rec.ID, "err", err)
		return
	}
	delete(s.retryAt, rec.ID)
	byID[rec.ID] = updated
	s.publishTask(updated)
}

// transitionTask is the outcome-path task move + derived event.
func (s *Scheduler) transitionTask(id string, from, to envelope.TaskState, reason string) {
	updated, err := s.tasks.Transition(id, from, to, schedulerID, reason)
	if err != nil {
		s.log.Warn("scheduler: task transition failed",
			"task", id, "from", string(from), "to", string(to), "err", err)
		return
	}
	s.publishTask(updated)
}

// checkBudgetBeforeSpawn reports whether spawning is allowed, pausing the
// fleet when the cap is already reached.
func (s *Scheduler) checkBudgetBeforeSpawn() bool {
	if s.paused {
		return false
	}
	if s.opts.BudgetUSD > 0 && s.spent >= s.opts.BudgetUSD {
		s.pause(envelope.FleetBudgetExhausted,
			fmt.Sprintf("spent %.4f of %.4f USD", s.spent, s.opts.BudgetUSD))
		return false
	}
	return true
}

// pause stops all future spawning until the coordinator restarts. Jobs and
// tasks keep their KV states — queued, never failed (locked decision).
func (s *Scheduler) pause(code envelope.FleetPauseCode, reason string) {
	if s.paused {
		return
	}
	s.paused = true
	s.log.Warn("scheduler: fleet paused", "code", string(code), "reason", reason,
		"spentUSD", s.spent, "budgetUSD", s.opts.BudgetUSD)
	env, err := envelope.New(envelope.KindFleet, schedulerID, envelope.SubjectFleet, &envelope.FleetPayload{
		State: envelope.FleetPaused, Code: code, Reason: reason,
		SpentUSD: s.spent, BudgetUSD: s.opts.BudgetUSD,
	})
	if err == nil {
		err = s.cli.Publish(env)
	}
	if err != nil {
		s.log.Warn("scheduler: publish fleet event failed", "err", err)
	}
}

// --- derived observability events (the KV records stay the authorities) ---

func (s *Scheduler) publishJob(rec job.Record) {
	env, err := envelope.New(envelope.KindJob, schedulerID, envelope.SubjectJob(rec.ID), &envelope.JobPayload{
		ID: rec.ID, Repo: rec.Repo, Source: rec.Source, Title: rec.Title, State: rec.State,
	})
	if err == nil {
		err = s.cli.Publish(env)
	}
	if err != nil {
		s.log.Warn("scheduler: publish job event failed", "job", rec.ID, "err", err)
	}
}

func (s *Scheduler) publishTask(rec task.Record) {
	env, err := envelope.New(envelope.KindTask, schedulerID, envelope.SubjectTask(rec.ID), &envelope.TaskPayload{
		ID: rec.ID, Job: rec.Job, Role: rec.Role, Title: rec.Title, State: rec.State,
	})
	if err == nil {
		err = s.cli.Publish(env)
	}
	if err != nil {
		s.log.Warn("scheduler: publish task event failed", "task", rec.ID, "err", err)
	}
}

func (s *Scheduler) publishWorker(rec task.Record, res Result) {
	p := envelope.WorkerPayload{
		Task: rec.ID, Job: rec.Job, Result: envelope.WorkerOK, CostUSD: res.CostUSD,
	}
	if !res.Succeeded() {
		p.Result, p.Code, p.Reason = envelope.WorkerError, res.Code, truncate(res.Summary, 512)
	}
	env, err := envelope.New(envelope.KindWorker, schedulerID, envelope.SubjectWorker(rec.ID), &p)
	if err == nil {
		err = s.cli.Publish(env)
	}
	if err != nil {
		s.log.Warn("scheduler: publish worker event failed", "task", rec.ID, "err", err)
	}
}

// --- pure helpers ----------------------------------------------------------------

func anyFailed(recs []task.Record) bool {
	for _, r := range recs {
		if r.State == envelope.TaskFailed {
			return true
		}
	}
	return false
}

func allTerminal(byID map[string]task.Record) bool {
	for _, r := range byID {
		switch r.State {
		case envelope.TaskDone, envelope.TaskFailed, envelope.TaskCancelled:
		default:
			return false
		}
	}
	return true
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
