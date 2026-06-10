package triage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/task"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// validPlanJSON is a well-formed plan inside DefaultRoles.
const validPlanJSON = `{"version":1,"nodes":[` +
	`{"id":"impl","title":"implement","role":"builder","files":["src/x.go"],"acceptance":["tests pass"]},` +
	`{"id":"review","title":"review","role":"reviewer","dependsOn":["impl"]}]}`

// successEnvelope wraps text in the M0 one-shot result contract.
func successEnvelope(text string) string {
	b, _ := json.Marshal(map[string]any{ //nolint:errcheck
		"type": "result", "subtype": "success", "is_error": false,
		"result": text, "session_id": "sess-1", "num_turns": 1, "duration_ms": 5,
	})
	return string(b)
}

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

func newTriager(t *testing.T, cli *bus.Client, invoke func(context.Context, string) ([]byte, error)) *Triager {
	t.Helper()
	return newTriagerOpts(t, cli, Options{PlannerCLI: "stub"}, invoke)
}

// newTriagerOpts builds a triager with explicit retry/clock options, for the
// #64 backoff tests. PlannerCLI defaults to "stub" if unset.
func newTriagerOpts(t *testing.T, cli *bus.Client, opts Options, invoke func(context.Context, string) ([]byte, error)) *Triager {
	t.Helper()
	if opts.PlannerCLI == "" {
		opts.PlannerCLI = "stub"
	}
	tr, err := New(cli, opts)
	if err != nil {
		t.Fatal(err)
	}
	if invoke != nil {
		tr.invoke = invoke
	}
	return tr
}

func (f fixture) openJob(t *testing.T) job.Record {
	t.Helper()
	rec, err := f.jobs.Create(job.Record{Repo: "demo", Source: job.SourceManual,
		Title: "add RRULE builder", Body: "events repeat weekly"})
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func (f fixture) waitKinds(t *testing.T, want map[envelope.Kind]int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := map[envelope.Kind]int{}
		for _, env := range f.events() {
			got[env.Kind]++
		}
		ok := true
		for k, n := range want {
			if got[k] < n {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("events never arrived: want %v, got %v", want, kindsOf(f.events()))
}

func kindsOf(envs []envelope.Envelope) map[envelope.Kind]int {
	got := map[envelope.Kind]int{}
	for _, env := range envs {
		got[env.Kind]++
	}
	return got
}

func TestTriageHappyPathPersistsDAGAndTransitionsJob(t *testing.T) {
	f := newFixture(t)
	rec := f.openJob(t)

	var prompts []string
	tr := newTriager(t, f.cli, func(_ context.Context, prompt string) ([]byte, error) {
		prompts = append(prompts, prompt)
		return []byte(successEnvelope(validPlanJSON)), nil
	})
	tr.sweepOnce()

	// The job is triaged (KV authority).
	got, found, err := f.jobs.Get(rec.ID)
	if err != nil || !found {
		t.Fatalf("get job: found=%v err=%v", found, err)
	}
	if got.State != envelope.JobTriaged {
		t.Fatalf("job state = %s, want triaged", got.State)
	}

	// The persisted DAG is readable the way the scheduler will read it.
	tasks, err := f.tasks.ListByJob(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(tasks))
	}
	byNode := map[string]task.Record{}
	for _, tk := range tasks {
		if tk.State != envelope.TaskPending {
			t.Fatalf("task %s state = %s, want pending", tk.Node, tk.State)
		}
		byNode[tk.Node] = tk
	}
	if deps := byNode["review"].DependsOn; len(deps) != 1 || deps[0] != byNode["impl"].ID {
		t.Fatalf("review deps = %v, want [%s]", deps, byNode["impl"].ID)
	}

	// Derived observability: one task event per node, a triaged job event,
	// and an ok triage event.
	f.waitKinds(t, map[envelope.Kind]int{envelope.KindTask: 2, envelope.KindTriage: 1})
	for _, env := range f.events() {
		if env.Kind == envelope.KindTriage {
			var p envelope.TriagePayload
			if err := envelope.DecodeInto(env, &p); err != nil {
				t.Fatal(err)
			}
			if p.Result != envelope.TriageOK || p.Tasks != 2 || p.Job != rec.ID {
				t.Fatalf("triage event = %+v", p)
			}
		}
	}

	// The prompt carried the job and the role vocabulary.
	if len(prompts) != 1 {
		t.Fatalf("planner invoked %d times, want 1", len(prompts))
	}
	for _, must := range []string{"add RRULE builder", "events repeat weekly", "builder, reviewer, tester", "demo"} {
		if !strings.Contains(prompts[0], must) {
			t.Errorf("prompt missing %q", must)
		}
	}
}

// TestTriageTypedFailures pins the typed code each malformed-planner shape maps
// to, the #64 transient/permanent classification, and that every one degrades
// without touching the process. PERMANENT codes (bad_plan/invalid_dag) fail the
// job on the first sweep; TRANSIENT codes (planner_unavailable/planner_failed)
// keep the job open for a backed-off retry. Either way a typed KindTriage error
// event carries the code.
func TestTriageTypedFailures(t *testing.T) {
	cases := []struct {
		name      string
		stdout    string
		err       error
		code      envelope.TriageErrorCode
		permanent bool
	}{
		// planner_unavailable is always transient (the CLI may recover).
		{"planner cannot run", "", errors.New("exec: no such file"), envelope.TriagePlannerUnavailable, false},
		// planner_failed WITHOUT an api_error_status is deterministic → permanent.
		{"stdout not JSON", "I had a thought about your repo...", nil, envelope.TriagePlannerFailed, true},
		{"stdout not a result", `{"type":"system","subtype":"init"}`, nil, envelope.TriagePlannerFailed, true},
		{"result is_error", `{"type":"result","subtype":"success","is_error":true,"result":"x"}`, nil, envelope.TriagePlannerFailed, true},
		{"result error subtype", `{"type":"result","subtype":"error_during_execution","is_error":false,"result":""}`, nil, envelope.TriagePlannerFailed, true},
		// planner_failed WITH an api_error_status is a transient API blip → retry.
		{"result api_error_status", `{"type":"result","subtype":"success","is_error":false,"result":"x","api_error_status":429}`, nil, envelope.TriagePlannerFailed, false},
		{"result text is prose", successEnvelope("Here is your plan: 1) do it"), nil, envelope.TriageBadPlan, true},
		{"plan wrong version", successEnvelope(`{"version":9,"nodes":[{"id":"a","title":"t","role":"builder"}]}`), nil, envelope.TriageInvalidDAG, true},
		{"plan unknown role", successEnvelope(`{"version":1,"nodes":[{"id":"a","title":"t","role":"wizard"}]}`), nil, envelope.TriageInvalidDAG, true},
		{"plan cycle", successEnvelope(`{"version":1,"nodes":[` +
			`{"id":"a","title":"t","role":"builder","dependsOn":["b"]},` +
			`{"id":"b","title":"t","role":"builder","dependsOn":["a"]}]}`), nil, envelope.TriageInvalidDAG, true},
		{"plan missing node id", successEnvelope(`{"version":1,"nodes":[{"title":"t","role":"builder"}]}`), nil, envelope.TriageInvalidDAG, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			rec := f.openJob(t)
			tr := newTriager(t, f.cli, func(context.Context, string) ([]byte, error) {
				return []byte(tc.stdout), tc.err
			})
			tr.sweepOnce()

			got, found, err := f.jobs.Get(rec.ID)
			if err != nil || !found {
				t.Fatalf("get job: found=%v err=%v", found, err)
			}
			wantState := envelope.JobOpen // transient: stays open, backing off
			if tc.permanent {
				wantState = envelope.JobFailed // permanent: fails fast
			}
			if got.State != wantState {
				t.Fatalf("job state = %s, want %s (permanent=%v)", got.State, wantState, tc.permanent)
			}
			tasks, err := f.tasks.ListByJob(rec.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 0 {
				t.Fatalf("failed triage persisted %d tasks", len(tasks))
			}

			f.waitKinds(t, map[envelope.Kind]int{envelope.KindTriage: 1})
			for _, env := range f.events() {
				if env.Kind != envelope.KindTriage {
					continue
				}
				var p envelope.TriagePayload
				if err := envelope.DecodeInto(env, &p); err != nil {
					t.Fatal(err)
				}
				if p.Result != envelope.TriageError || p.Code != tc.code {
					t.Fatalf("triage event = %+v, want error code %s", p, tc.code)
				}
			}
		})
	}
}

// fakeClock is a settable monotonic-ish clock for the #64 backoff tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// TestTriageTransientRetriesWithBackoff: a transient failure keeps the job open,
// is NOT re-attempted while backing off, and IS re-attempted once the backoff
// window elapses — eventually succeeding.
func TestTriageTransientRetriesWithBackoff(t *testing.T) {
	f := newFixture(t)
	rec := f.openJob(t)

	clk := &fakeClock{now: time.Now().UTC()}
	calls := 0
	tr := newTriagerOpts(t, f.cli, Options{
		MaxAttempts: 5,
		Backoff:     30 * time.Second,
		now:         clk.Now,
	}, func(context.Context, string) ([]byte, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("planner down") // transient: planner_unavailable
		}
		return []byte(successEnvelope(validPlanJSON)), nil // recovers
	})

	// Attempt 1 (transient): job stays open, schedule a 30s backoff.
	tr.sweepOnce()
	if calls != 1 {
		t.Fatalf("after first sweep calls=%d, want 1", calls)
	}
	if got, _, _ := f.jobs.Get(rec.ID); got.State != envelope.JobOpen {
		t.Fatalf("after transient failure job is %s, want open", got.State)
	}

	// Still backing off: a sweep before the deadline must NOT re-invoke.
	clk.advance(10 * time.Second)
	tr.sweepOnce()
	if calls != 1 {
		t.Fatalf("planner re-invoked during backoff window (calls=%d)", calls)
	}

	// Backoff elapsed: attempt 2 (still transient), 30s window again.
	clk.advance(25 * time.Second)
	tr.sweepOnce()
	if calls != 2 {
		t.Fatalf("after backoff calls=%d, want 2", calls)
	}
	if got, _, _ := f.jobs.Get(rec.ID); got.State != envelope.JobOpen {
		t.Fatalf("after second transient failure job is %s, want open", got.State)
	}

	// Second backoff is 60s (exponential): a 30s advance is not yet due.
	clk.advance(30 * time.Second)
	tr.sweepOnce()
	if calls != 2 {
		t.Fatalf("planner re-invoked before exponential backoff elapsed (calls=%d)", calls)
	}

	// Past the 60s window: attempt 3 succeeds, job triaged.
	clk.advance(35 * time.Second)
	tr.sweepOnce()
	if calls != 3 {
		t.Fatalf("after exponential backoff calls=%d, want 3", calls)
	}
	if got, _, _ := f.jobs.Get(rec.ID); got.State != envelope.JobTriaged {
		t.Fatalf("after recovery job is %s, want triaged", got.State)
	}
	// The attempt record is cleared once the job leaves open.
	if _, found, _ := tr.attempts.get(rec.ID); found {
		t.Fatal("attempt record survived a successful triage")
	}
}

// TestTriagePermanentFailsFast: a permanent failure (bad_plan/invalid_dag) fails
// the job on the first attempt with no retry — retrying garbage burns money.
func TestTriagePermanentFailsFast(t *testing.T) {
	f := newFixture(t)
	rec := f.openJob(t)

	calls := 0
	tr := newTriagerOpts(t, f.cli, Options{MaxAttempts: 5}, func(context.Context, string) ([]byte, error) {
		calls++
		return []byte(successEnvelope(`{"version":1,"nodes":[{"id":"a","title":"t","role":"wizard"}]}`)), nil
	})
	tr.sweepOnce()
	tr.sweepOnce() // a failed job is terminal; no further attempts
	if calls != 1 {
		t.Fatalf("permanent failure was retried (calls=%d, want 1)", calls)
	}
	got, _, _ := f.jobs.Get(rec.ID)
	if got.State != envelope.JobFailed {
		t.Fatalf("permanent failure left job %s, want failed", got.State)
	}
	if _, found, _ := tr.attempts.get(rec.ID); found {
		t.Fatal("attempt record survived a terminal failure")
	}
}

// TestTriageMaxAttemptsThenFail: a transient failure that never recovers is
// retried up to the cap, then fails terminally with the typed code.
func TestTriageMaxAttemptsThenFail(t *testing.T) {
	f := newFixture(t)
	rec := f.openJob(t)

	clk := &fakeClock{now: time.Now().UTC()}
	calls := 0
	tr := newTriagerOpts(t, f.cli, Options{
		MaxAttempts: 3,
		Backoff:     1 * time.Second,
		now:         clk.Now,
	}, func(context.Context, string) ([]byte, error) {
		calls++
		return nil, errors.New("planner permanently down")
	})

	// Drive enough due sweeps to exhaust the cap. Generous advance each time
	// so the (exponential) backoff window is always elapsed.
	for i := 0; i < 10; i++ {
		tr.sweepOnce()
		clk.advance(10 * time.Minute)
	}
	if calls != 3 {
		t.Fatalf("planner invoked %d times, want exactly MaxAttempts=3", calls)
	}
	got, _, _ := f.jobs.Get(rec.ID)
	if got.State != envelope.JobFailed {
		t.Fatalf("after exhausting attempts job is %s, want failed", got.State)
	}
	f.waitKinds(t, map[envelope.Kind]int{envelope.KindTriage: 1})
	sawTerminal := false
	for _, env := range f.events() {
		if env.Kind != envelope.KindJob {
			continue
		}
		var p envelope.JobPayload
		if err := envelope.DecodeInto(env, &p); err != nil {
			t.Fatal(err)
		}
		if p.ID == rec.ID && p.State == envelope.JobFailed {
			sawTerminal = true
		}
	}
	if !sawTerminal {
		t.Fatal("no terminal KindJob failed event after exhausting attempts")
	}
	if _, found, _ := tr.attempts.get(rec.ID); found {
		t.Fatal("attempt record survived terminal failure")
	}
}

// TestTriageBackoffSurvivesRestart: the durable attempt state means a NEW
// triager (a fresh coordinator lifetime) over the SAME bus resumes the job's
// schedule — it does not restart from attempt 0 and does not re-hammer a job
// that is still mid-backoff.
func TestTriageBackoffSurvivesRestart(t *testing.T) {
	f := newFixture(t)
	rec := f.openJob(t)

	clk := &fakeClock{now: time.Now().UTC()}
	calls := 0
	invoke := func(context.Context, string) ([]byte, error) {
		calls++
		return nil, errors.New("planner down")
	}

	// Lifetime 1: one transient attempt, then a 30s backoff scheduled durably.
	tr1 := newTriagerOpts(t, f.cli, Options{MaxAttempts: 3, Backoff: 30 * time.Second, now: clk.Now}, invoke)
	tr1.sweepOnce()
	if calls != 1 {
		t.Fatalf("lifetime-1 calls=%d, want 1", calls)
	}

	// Lifetime 2 (fresh triager, same durable bucket): mid-backoff, so it must
	// NOT re-attempt — proving attempt state survived and was not reset to 0.
	tr2 := newTriagerOpts(t, f.cli, Options{MaxAttempts: 3, Backoff: 30 * time.Second, now: clk.Now}, invoke)
	clk.advance(10 * time.Second) // still inside the 30s window
	tr2.sweepOnce()
	if calls != 1 {
		t.Fatalf("restart re-hammered a job mid-backoff (calls=%d, want 1)", calls)
	}

	// Confirm the persisted record carried the attempt count across lifetimes.
	att, found, err := tr2.attempts.get(rec.ID)
	if err != nil || !found {
		t.Fatalf("attempt record not durable: found=%v err=%v", found, err)
	}
	if att.Attempts != 1 {
		t.Fatalf("persisted attempts=%d, want 1 (restart reset the counter)", att.Attempts)
	}

	// Once the window elapses, lifetime 2 resumes from attempt 2 (not 1).
	clk.advance(25 * time.Second)
	tr2.sweepOnce()
	if calls != 2 {
		t.Fatalf("after backoff lifetime-2 calls=%d, want 2", calls)
	}
	att, _, _ = tr2.attempts.get(rec.ID)
	if att.Attempts != 2 {
		t.Fatalf("after resumed attempt persisted attempts=%d, want 2", att.Attempts)
	}
}

func TestTriageSkipsNonOpenJobs(t *testing.T) {
	f := newFixture(t)
	rec := f.openJob(t)
	if _, err := f.jobs.Transition(rec.ID, envelope.JobOpen, envelope.JobCancelled, "test", ""); err != nil {
		t.Fatal(err)
	}
	tr := newTriager(t, f.cli, func(context.Context, string) ([]byte, error) {
		t.Fatal("planner invoked for a non-open job")
		return nil, nil
	})
	tr.sweepOnce()
}

// TestExecPlannerRealProcess exercises the default invoke through a real
// child process: a script that emits the documented one-shot result envelope.
func TestExecPlannerRealProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script planner stub")
	}
	f := newFixture(t)
	rec := f.openJob(t)

	script := filepath.Join(t.TempDir(), "fakeplanner.sh")
	out := successEnvelope(validPlanJSON)
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' "+shellQuote(out)+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	tr, err := New(f.cli, Options{PlannerCLI: script, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	tr.sweepOnce()

	got, found, err := f.jobs.Get(rec.ID)
	if err != nil || !found {
		t.Fatalf("get job: found=%v err=%v", found, err)
	}
	if got.State != envelope.JobTriaged {
		t.Fatalf("job state = %s, want triaged", got.State)
	}
}

// TestExecPlannerTimeout proves a wedged planner child is killed and typed
// planner_unavailable, not waited on forever.
func TestExecPlannerTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script planner stub")
	}
	f := newFixture(t)
	rec := f.openJob(t)

	script := filepath.Join(t.TempDir(), "sleeper.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	tr, err := New(f.cli, Options{PlannerCLI: script, Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	tr.sweepOnce()
	// Bound = Timeout + WaitDelay (the grandchild holding the pipe), with
	// slack; what must NOT happen is waiting out the child's full 30s sleep.
	if time.Since(start) > 10*time.Second {
		t.Fatal("timeout did not bound the planner")
	}

	got, _, err := f.jobs.Get(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	// A timeout is planner_unavailable — TRANSIENT (#64): the job stays open
	// with a scheduled backoff rather than failing on the first try.
	if got.State != envelope.JobOpen {
		t.Fatalf("job state = %s, want open (transient backoff)", got.State)
	}
	f.waitKinds(t, map[envelope.Kind]int{envelope.KindTriage: 1})
	for _, env := range f.events() {
		if env.Kind != envelope.KindTriage {
			continue
		}
		var p envelope.TriagePayload
		if err := envelope.DecodeInto(env, &p); err != nil {
			t.Fatal(err)
		}
		if p.Code != envelope.TriagePlannerUnavailable {
			t.Fatalf("code = %s, want planner_unavailable", p.Code)
		}
	}
}

func TestPromptIncludesBlackboardNotes(t *testing.T) {
	f := newFixture(t)

	// Append two notes to the repo blackboard the way the sidecar does.
	for _, decision := range []string{"events store UTC", "exporter owns TZID"} {
		env, err := envelope.New(envelope.KindNote, "alice", envelope.SubjectNote("demo"),
			&envelope.NotePayload{ID: "alice", Decision: decision, Repo: "demo"})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := envelope.Encode(env)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.cli.StreamAppend(envelope.StreamNotes("demo"), json.RawMessage(raw)); err != nil {
			t.Fatal(err)
		}
	}
	f.openJob(t)

	var prompt string
	tr := newTriager(t, f.cli, func(_ context.Context, p string) ([]byte, error) {
		prompt = p
		return []byte(successEnvelope(validPlanJSON)), nil
	})
	tr.sweepOnce()

	for _, must := range []string{"events store UTC", "exporter owns TZID"} {
		if !strings.Contains(prompt, must) {
			t.Errorf("prompt missing blackboard note %q", must)
		}
	}
}

// shellQuote single-quotes s for a POSIX shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func TestNewRequiresPlannerCLI(t *testing.T) {
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("New without PlannerCLI accepted")
	}
}
