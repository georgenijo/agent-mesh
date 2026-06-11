// Package triage turns an open Job into a validated, persisted task DAG
// (#24). It owns everything planner-shaped — the prompt, the one-shot
// headless CLI invocation, strict output parsing, and the orchestration that
// commits the result — so the planner stays replaceable in one place.
//
// The planner is a single `<cli> -p --output-format json` invocation (the
// M0-verified contract, docs/spikes/M0-feasibility.md), never an LLM API
// call. Its stdout is parsed with internal/runtime's proven never-fake-
// success result discriminators; the result text must be a strict JSON plan
// document (internal/task.DecodePlan) — malformed output of any shape becomes
// a typed triage error, and the coordinator keeps running.
//
// Commit order is tasks-first: persist the task records, then CAS the job
// open→triaged. A job that never reaches triaged cannot expose half a DAG,
// because the scheduler (#25) only reads tasks of triaged jobs.
//
// Failure handling is the #64 retry/backoff policy (see attempts.go). A typed
// failure is classified TRANSIENT (may not recur: planner_unavailable, a
// planner_failed carrying an api_error_status, or internal) or PERMANENT (a
// retry of the same prompt would reproduce it: bad_plan, invalid_dag, or a
// planner_failed from prose/non-result stdout with no api_error_status).
// PERMANENT fails the job open→failed immediately. TRANSIENT increments a
// DURABLE per-job attempt count and either schedules a backed-off retry (the
// job stays open; the loop skips it until its nextRetryAt) or, once the
// configured attempt cap is reached, fails it open→failed with the last typed
// code. The cap honors the hard-cap billing posture: each attempt is one
// planner LLM turn, so transient failures are never retried infinitely.
package triage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	meshruntime "github.com/georgenijo/agent-mesh/internal/runtime"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// triagerID is the From id triage events carry on the bus. Triage runs inside
// the coordinator process, so it speaks as the coordinator.
const triagerID = "coordinator"

// DefaultRoles is the role vocabulary offered to the planner and enforced by
// DAG validation when Options.Roles is empty. One authority: the same slice
// drives the prompt and the validator, so the planner can never be told a
// role the validator rejects. Roles stay open data mesh-wide; this is only
// the triage offering, not a closed mesh enum.
var DefaultRoles = []string{"builder", "reviewer", "tester"}

// MaxResultBytes bounds the planner result text we are willing to parse.
const MaxResultBytes = 1 << 20 // 1 MiB

// maxPromptNotes bounds how many trailing blackboard notes are injected into
// the planner prompt.
const maxPromptNotes = 20

// Error is the typed triage failure. Code is the wire-level classification
// (envelope.TriageErrorCode); Err carries detail.
//
// apiError marks a planner_failed that carries a non-null api_error_status — a
// TRANSIENT API blip (rate-limit / overload / 5xx) rather than deterministic
// malformed output. It refines the #64 retry classification: a planner_failed
// WITH an api_error_status is retried; a planner_failed from prose/non-result
// stdout or a plain non-success subtype is deterministic and fails fast (see
// the transientErr classifier in attempts.go). The flag is set only where
// resultText observes the discriminator; it is meaningless for other codes.
type Error struct {
	Code     envelope.TriageErrorCode
	Err      error
	apiError bool
}

func (e *Error) Error() string { return fmt.Sprintf("triage: %s: %v", e.Code, e.Err) }
func (e *Error) Unwrap() error { return e.Err }

// CodeOf extracts the typed code from a triage error (TriageInternal for
// anything untyped).
func CodeOf(err error) envelope.TriageErrorCode {
	var te *Error
	if errors.As(err, &te) {
		return te.Code
	}
	return envelope.TriageInternal
}

// Options configure a Triager. Zero values get safe defaults except
// PlannerCLI, which is required (the coordinator only constructs a Triager
// when the operator set one).
type Options struct {
	PlannerCLI  string        // required: planner binary (claude | fake)
	Model       string        // optional --model for the planner CLI
	Timeout     time.Duration // wall-clock bound per invocation (default 2m)
	Interval    time.Duration // sweep cadence (default 5s)
	Roles       []string      // allowed roles (default DefaultRoles)
	WorkDir     string        // planner working dir (M0: clean cwd sheds CLAUDE.md cost)
	MaxAttempts int           // #64: max planner attempts per job for TRANSIENT failures (default 4)
	Backoff     time.Duration // #64: base delay of the exponential retry schedule (default 30s)
	Log         *slog.Logger

	// now is the clock the retry schedule reads. Swappable seam for tests so a
	// backoff window can be asserted without sleeping; nil = time.Now (UTC).
	now func() time.Time
}

// Triager sweeps the jobs bucket for open jobs and triages each under the #64
// retry/backoff policy: a job is attempted up to MaxAttempts times for TRANSIENT
// planner failures, with bounded exponential backoff between attempts, then
// fails terminally; a PERMANENT failure fails it on the first attempt. One
// goroutine, one planner invocation in flight — triage is a single planner
// call, not a fan-out (locked P3 plan decision).
type Triager struct {
	opts Options
	cli  *bus.Client
	log  *slog.Logger
	now  func() time.Time

	jobs     job.Store
	tasks    task.Store
	attempts attemptStore

	// invoke runs the planner and returns its raw stdout. Swappable seam for
	// unit tests; the default execs Options.PlannerCLI one-shot.
	invoke func(ctx context.Context, prompt string) ([]byte, error)

	stop chan struct{}
	wg   sync.WaitGroup
}

// New builds a Triager over the given bus client.
func New(cli *bus.Client, opts Options) (*Triager, error) {
	if opts.PlannerCLI == "" {
		return nil, errors.New("triage: PlannerCLI is required")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Minute
	}
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if len(opts.Roles) == 0 {
		opts.Roles = DefaultRoles
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 4
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 30 * time.Second
	}
	if opts.now == nil {
		opts.now = func() time.Time { return time.Now().UTC() }
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	t := &Triager{
		opts:     opts,
		cli:      cli,
		log:      opts.Log,
		now:      opts.now,
		jobs:     job.NewStore(cli),
		tasks:    task.NewStore(cli),
		attempts: newAttemptStore(cli),
		stop:     make(chan struct{}),
	}
	t.invoke = t.execPlanner
	return t, nil
}

// Start launches the sweep loop.
func (t *Triager) Start() {
	t.wg.Add(1)
	go t.loop()
}

// Stop halts the loop and waits for any in-flight triage to finish (the
// planner child is bounded by Options.Timeout and killed on context cancel).
func (t *Triager) Stop() {
	select {
	case <-t.stop:
	default:
		close(t.stop)
	}
	t.wg.Wait()
}

func (t *Triager) loop() {
	defer t.wg.Done()
	ticker := time.NewTicker(t.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			t.sweepOnce()
		}
	}
}

// sweepOnce triages every open job whose retry schedule is due, sequentially.
// A job in backoff (nextRetryAt in the future) is skipped until its deadline,
// so a down planner is never hammered every tick. The schedule is read from the
// durable attempt bucket, so it resumes across a coordinator restart rather than
// restarting from attempt 0.
func (t *Triager) sweepOnce() {
	jobs, err := t.jobs.List()
	if err != nil {
		t.log.Warn("triage: list jobs failed", "err", err)
		return
	}
	for _, rec := range jobs {
		select {
		case <-t.stop:
			return
		default:
		}
		if rec.State != envelope.JobOpen {
			continue
		}
		att, _, err := t.attempts.get(rec.ID)
		if err != nil {
			t.log.Warn("triage: read attempt record failed", "job", rec.ID, "err", err)
			continue
		}
		if !att.NextRetryAt.IsZero() && t.now().Before(att.NextRetryAt) {
			continue // still backing off
		}
		t.runOne(rec, att)
	}
}

// runOne triages a single job: plan, validate, persist, transition, publish.
// att is the job's durable attempt record (Attempts is the count BEFORE this
// invocation). Every failure path is typed and degrades — it must never take
// the coordinator down.
func (t *Triager) runOne(rec job.Record, att attemptRecord) {
	// This invocation consumes one planner turn: record it durably first, so a
	// crash mid-plan still counts the attempt against the cap (a planner turn
	// that ran is money spent whether or not we observed the result).
	att.Attempts++
	if err := t.attempts.put(att); err != nil {
		t.log.Warn("triage: persist attempt failed", "job", rec.ID, "err", err)
	}

	recs, err := t.plan(rec)
	if err != nil {
		t.onFailure(rec, att, err)
		return
	}
	updated, err := t.jobs.Transition(rec.ID, envelope.JobOpen, envelope.JobTriaged, triagerID,
		fmt.Sprintf("tasks: %d", len(recs)))
	if err != nil {
		// Tasks were persisted but the job never reached triaged: inert for
		// the scheduler (it only reads tasks of triaged jobs).
		t.onFailure(rec, att, &Error{Code: envelope.TriageInternal, Err: fmt.Errorf("transition: %w", err)})
		return
	}
	// Triaged: the job left the open state, so its retry bookkeeping is done.
	if err := t.attempts.clear(rec.ID); err != nil {
		t.log.Warn("triage: clear attempt record failed", "job", rec.ID, "err", err)
	}
	for _, tr := range recs {
		t.publishTask(tr)
	}
	t.publishJob(updated)
	t.publishTriage(envelope.TriagePayload{
		Job: rec.ID, Result: envelope.TriageOK, Tasks: len(recs),
	})
	t.log.Info("triage: job triaged", "job", rec.ID, "tasks", len(recs))
}

// onFailure applies the #64 retry/backoff policy to a typed triage failure.
//
//   - PERMANENT (bad_plan, invalid_dag): fail the job immediately. The planner
//     ran and produced garbage; retrying the same prompt burns a planner turn
//     for nothing.
//   - TRANSIENT (planner_unavailable, planner_failed, internal): if the attempt
//     cap is reached, fail terminally with the last typed code; otherwise keep
//     the job open and schedule a backed-off retry (durable, so it survives a
//     restart). The loop skips the job until nextRetryAt — a down planner is
//     not hammered every tick.
func (t *Triager) onFailure(rec job.Record, att attemptRecord, cause error) {
	code := CodeOf(cause)
	att.LastCode = code

	if !transientErr(cause) {
		t.log.Warn("triage: permanent failure; failing fast",
			"job", rec.ID, "code", string(code), "attempt", att.Attempts, "err", cause)
		t.failTerminal(rec, code, cause)
		return
	}
	if att.Attempts >= t.opts.MaxAttempts {
		t.log.Warn("triage: transient failure; attempts exhausted",
			"job", rec.ID, "code", string(code), "attempts", att.Attempts, "max", t.opts.MaxAttempts, "err", cause)
		t.failTerminal(rec, code, cause)
		return
	}
	// Schedule a backed-off retry; the job stays open.
	delay := backoffFor(t.opts.Backoff, att.Attempts)
	att.NextRetryAt = t.now().Add(delay)
	if err := t.attempts.put(att); err != nil {
		t.log.Warn("triage: persist retry schedule failed", "job", rec.ID, "err", err)
	}
	t.log.Info("triage: transient failure; backing off",
		"job", rec.ID, "code", string(code), "attempt", att.Attempts, "max", t.opts.MaxAttempts,
		"retryIn", delay, "err", cause)
	// Emit the typed error event so the failure (and its retry) is observable,
	// but DO NOT transition the job — it stays open for the next attempt.
	t.publishTriage(envelope.TriagePayload{
		Job: rec.ID, Result: envelope.TriageError, Code: code, Reason: truncate(cause.Error(), 512),
	})
}

// plan runs the planner and returns validated, persisted task records.
func (t *Triager) plan(rec job.Record) ([]task.Record, error) {
	ctx, cancel := context.WithTimeout(context.Background(), t.opts.Timeout)
	defer cancel()

	prompt := buildPrompt(rec, t.opts.Roles, t.recentNotes(rec.Repo))
	stdout, err := t.invoke(ctx, prompt)
	if err != nil {
		return nil, &Error{Code: envelope.TriagePlannerUnavailable, Err: err}
	}

	text, err := resultText(stdout)
	if err != nil {
		return nil, err
	}
	plan, err := task.DecodePlan(text)
	if err != nil {
		return nil, &Error{Code: envelope.TriageBadPlan, Err: err}
	}
	if err := plan.Validate(t.opts.Roles); err != nil {
		return nil, &Error{Code: envelope.TriageInvalidDAG, Err: err}
	}
	recs := task.FromPlan(rec.ID, plan, time.Now().UTC())
	if err := t.tasks.CreateAll(recs); err != nil {
		return nil, &Error{Code: envelope.TriageInternal, Err: fmt.Errorf("persist tasks: %w", err)}
	}
	return recs, nil
}

// resultText extracts the model's text output from the planner's one-shot
// stdout, applying the spike's never-fake-success rule via the same
// discriminator parsing the expert runtime uses (runtime.ParseEvent): a
// non-result object, a non-success subtype, is_error, api_error_status, or a
// type-degraded discriminator is a typed planner_failed — never an answer.
func resultText(stdout []byte) (string, error) {
	if len(stdout) > MaxResultBytes {
		return "", &Error{Code: envelope.TriagePlannerFailed,
			Err: fmt.Errorf("planner stdout %d bytes exceeds %d", len(stdout), MaxResultBytes)}
	}
	ev, err := meshruntime.ParseEvent(stdout)
	if err != nil {
		return "", &Error{Code: envelope.TriagePlannerFailed, Err: err}
	}
	if ev.Result == nil {
		return "", &Error{Code: envelope.TriagePlannerFailed,
			Err: fmt.Errorf("planner stdout is %q, not a result envelope", ev.Type)}
	}
	if !ev.Result.Succeeded() {
		// A non-null api_error_status is a TRANSIENT API blip (rate-limit /
		// overload / 5xx) — mark it retryable (#64). A plain non-success
		// subtype or is_error is deterministic and fails fast.
		return "", &Error{Code: envelope.TriagePlannerFailed, apiError: ev.Result.HasAPIError(),
			Err: fmt.Errorf("planner result not a success (subtype=%q is_error=%v api_error=%v)",
				ev.Result.Subtype, ev.Result.IsError, ev.Result.HasAPIError())}
	}
	return ev.Result.Result, nil
}

// execPlanner is the default invoke: one-shot headless structured output,
// exactly the M0-verified contract. The context bound kills a wedged child.
func (t *Triager) execPlanner(ctx context.Context, prompt string) ([]byte, error) {
	args := []string{"-p", "--output-format", "json"}
	if t.opts.Model != "" {
		args = append(args, "--model", t.opts.Model)
	}
	args = append(args, prompt)
	cmd := exec.CommandContext(ctx, t.opts.PlannerCLI, args...)
	if t.opts.WorkDir != "" {
		cmd.Dir = t.opts.WorkDir
	}
	// On timeout the child is killed, but a grandchild that inherited the
	// stdout pipe would keep Output() blocked indefinitely. WaitDelay bounds
	// that wait: after it, the pipe is forcibly closed and Wait returns.
	cmd.WaitDelay = 3 * time.Second
	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("planner timed out after %s: %w", t.opts.Timeout, ctx.Err())
		}
		if ee, ok := err.(*exec.ExitError); ok && len(ee.Stderr) > 0 {
			return nil, fmt.Errorf("planner exited: %w: %s", err, ee.Stderr)
		}
		return nil, fmt.Errorf("planner failed to run: %w", err)
	}
	return out, nil
}

// failTerminal records a terminal triage failure: job open→failed, the durable
// attempt record cleared, a KindTriage error event, and the derived KindJob
// event. Reached for a PERMANENT code on the first attempt or a TRANSIENT code
// once the attempt cap is exhausted. Best-effort by design — a failure to
// record the failure degrades to a log line, never a crash.
func (t *Triager) failTerminal(rec job.Record, code envelope.TriageErrorCode, cause error) {
	t.log.Warn("triage: job failed", "job", rec.ID, "code", string(code), "err", cause)
	updated, err := t.jobs.Transition(rec.ID, envelope.JobOpen, envelope.JobFailed, triagerID,
		fmt.Sprintf("%s: %v", code, cause))
	if err != nil {
		t.log.Warn("triage: record failure transition failed", "job", rec.ID, "err", err)
	} else {
		// The job left open; its retry bookkeeping is done.
		if cerr := t.attempts.clear(rec.ID); cerr != nil {
			t.log.Warn("triage: clear attempt record failed", "job", rec.ID, "err", cerr)
		}
		t.publishJob(updated)
	}
	t.publishTriage(envelope.TriagePayload{
		Job: rec.ID, Result: envelope.TriageError, Code: code, Reason: truncate(cause.Error(), 512),
	})
}

// --- derived observability events (the KV records stay the authorities) ---

func (t *Triager) publishJob(rec job.Record) {
	env, err := envelope.New(envelope.KindJob, triagerID, envelope.SubjectJob(rec.ID), &envelope.JobPayload{
		ID: rec.ID, Repo: rec.Repo, Source: rec.Source, Title: rec.Title, State: rec.State,
	})
	if err == nil {
		err = t.cli.Publish(env)
	}
	if err != nil {
		t.log.Warn("triage: publish job event failed", "job", rec.ID, "err", err)
	}
}

func (t *Triager) publishTask(rec task.Record) {
	env, err := envelope.New(envelope.KindTask, triagerID, envelope.SubjectTask(rec.ID), &envelope.TaskPayload{
		ID: rec.ID, Job: rec.Job, Role: rec.Role, Title: rec.Title, State: rec.State,
	})
	if err == nil {
		err = t.cli.Publish(env)
	}
	if err != nil {
		t.log.Warn("triage: publish task event failed", "task", rec.ID, "err", err)
	}
}

func (t *Triager) publishTriage(p envelope.TriagePayload) {
	env, err := envelope.New(envelope.KindTriage, triagerID, envelope.SubjectTriage(p.Job), &p)
	if err == nil {
		err = t.cli.Publish(env)
	}
	if err != nil {
		t.log.Warn("triage: publish triage event failed", "job", p.Job, "err", err)
	}
}

// recentNotes reads the tail of the repo's blackboard stream for prompt
// context (#15 injection). Best-effort: a missing or unreadable stream just
// means an emptier prompt.
func (t *Triager) recentNotes(repo string) []string {
	if !envelope.ValidRepo(repo) {
		return nil
	}
	entries, err := t.cli.StreamRead(envelope.StreamNotes(repo), 0)
	if err != nil {
		return nil
	}
	var notes []string
	for _, e := range entries {
		env, err := envelope.Decode(e.Data)
		if err != nil || env.Kind != envelope.KindNote {
			continue
		}
		var p envelope.NotePayload
		if envelope.DecodeInto(env, &p) != nil {
			continue
		}
		notes = append(notes, p.Decision)
	}
	if len(notes) > maxPromptNotes {
		notes = notes[len(notes)-maxPromptNotes:]
	}
	return notes
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
