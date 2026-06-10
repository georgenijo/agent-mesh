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
// because the scheduler (#25) only reads tasks of triaged jobs. Any failure
// transitions the job open→failed with a typed code (retry/backoff policy is
// deliberately out of scope here — see #29).
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
type Error struct {
	Code envelope.TriageErrorCode
	Err  error
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
	PlannerCLI string        // required: planner binary (claude | fake)
	Model      string        // optional --model for the planner CLI
	Timeout    time.Duration // wall-clock bound per invocation (default 2m)
	Interval   time.Duration // sweep cadence (default 5s)
	Roles      []string      // allowed roles (default DefaultRoles)
	WorkDir    string        // planner working dir (M0: clean cwd sheds CLAUDE.md cost)
	Log        *slog.Logger
}

// Triager sweeps the jobs bucket for open jobs and triages each at most once
// per coordinator lifetime. One goroutine, one planner invocation in flight —
// triage is a single planner call, not a fan-out (locked P3 plan decision).
type Triager struct {
	opts Options
	cli  *bus.Client
	log  *slog.Logger

	jobs  job.Store
	tasks task.Store

	// invoke runs the planner and returns its raw stdout. Swappable seam for
	// unit tests; the default execs Options.PlannerCLI one-shot.
	invoke func(ctx context.Context, prompt string) ([]byte, error)

	// attempted tracks jobs this coordinator already tried, successful or
	// not, so a failed job is never planner-hammered by every sweep. Only
	// touched from the loop goroutine.
	attempted map[string]bool

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
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	t := &Triager{
		opts:      opts,
		cli:       cli,
		log:       opts.Log,
		jobs:      job.NewStore(cli),
		tasks:     task.NewStore(cli),
		attempted: make(map[string]bool),
		stop:      make(chan struct{}),
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

// sweepOnce triages every open, not-yet-attempted job sequentially.
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
		if rec.State != envelope.JobOpen || t.attempted[rec.ID] {
			continue
		}
		t.attempted[rec.ID] = true
		t.runOne(rec)
	}
}

// runOne triages a single job: plan, validate, persist, transition, publish.
// Every failure path is typed and degrades — it must never take the
// coordinator down.
func (t *Triager) runOne(rec job.Record) {
	recs, err := t.plan(rec)
	if err != nil {
		t.fail(rec, err)
		return
	}
	updated, err := t.jobs.Transition(rec.ID, envelope.JobOpen, envelope.JobTriaged, triagerID,
		fmt.Sprintf("tasks: %d", len(recs)))
	if err != nil {
		// Tasks were persisted but the job never reached triaged: inert for
		// the scheduler (it only reads tasks of triaged jobs).
		t.fail(rec, &Error{Code: envelope.TriageInternal, Err: fmt.Errorf("transition: %w", err)})
		return
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
		return "", &Error{Code: envelope.TriagePlannerFailed,
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

// fail records a typed triage failure: job open→failed, a KindTriage error
// event, and the derived KindJob event. Best-effort by design — a failure to
// record the failure degrades to a log line, never a crash.
func (t *Triager) fail(rec job.Record, cause error) {
	code := CodeOf(cause)
	t.log.Warn("triage: job failed", "job", rec.ID, "code", string(code), "err", cause)
	updated, err := t.jobs.Transition(rec.ID, envelope.JobOpen, envelope.JobFailed, triagerID,
		fmt.Sprintf("%s: %v", code, cause))
	if err != nil {
		t.log.Warn("triage: record failure transition failed", "job", rec.ID, "err", err)
	} else {
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
