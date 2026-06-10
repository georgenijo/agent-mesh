package coordinator

// Coordinator-level triage wiring (#24): a coordinator started with a
// PlannerCLI sweeps open jobs through a REAL child process and commits the
// outcome; one started without a PlannerCLI never triages. The planner here
// is a script speaking the documented one-shot result contract — never a
// real LLM.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/task"
)

const coordPlanJSON = `{"version":1,"nodes":[` +
	`{"id":"impl","title":"implement","role":"builder"},` +
	`{"id":"review","title":"review","role":"reviewer","dependsOn":["impl"]}]}`

// plannerScript writes a fake planner that emits one result envelope whose
// result text is the given plan document.
func plannerScript(t *testing.T, resultText string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell script planner stub")
	}
	payload := fmt.Sprintf(
		`{"type":"result","subtype":"success","is_error":false,"result":%q,"session_id":"s","num_turns":1,"duration_ms":1}`,
		resultText)
	path := filepath.Join(t.TempDir(), "fakeplanner.sh")
	script := "#!/bin/sh\ncat <<'PLANNER_EOF'\n" + payload + "\nPLANNER_EOF\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("never happened: %s", what)
}

func waitJobState(t *testing.T, jobs job.Store, id string, want envelope.JobState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last envelope.JobState
	for time.Now().Before(deadline) {
		rec, found, err := jobs.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			last = rec.State
			if rec.State == want {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s never reached %s (last %s)", id, want, last)
}

func TestCoordinatorTriagesOpenJobThroughRealPlannerProcess(t *testing.T) {
	cfg := fastConfig(t)
	cfg.PlannerCLI = plannerScript(t, coordPlanJSON)
	cfg.TriageTimeout = 10 * time.Second
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	jobs := job.NewStore(cli)
	rec, err := jobs.Create(job.Record{Repo: "demo", Source: job.SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}
	waitJobState(t, jobs, rec.ID, envelope.JobTriaged)

	// The persisted DAG is readable exactly the way the scheduler (#25) will
	// read it.
	tasks, err := task.NewStore(cli).ListByJob(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(tasks))
	}
}

func TestCoordinatorSurvivesMalformedPlannerOutput(t *testing.T) {
	cfg := fastConfig(t)
	cfg.PlannerCLI = plannerScript(t, "Sorry, I can only answer in prose.")
	cfg.TriageTimeout = 10 * time.Second
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	jobs := job.NewStore(cli)
	rec, err := jobs.Create(job.Record{Repo: "demo", Source: job.SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}
	waitJobState(t, jobs, rec.ID, envelope.JobFailed)

	// The coordinator is still alive and reducing: a fresh agent registers
	// and lands in the registry.
	register(t, cli, "survivor")
	waitFor(t, 2*time.Second, "survivor registered after triage failure", func() bool {
		_, found := getRecord(t, cli, "survivor")
		return found
	})
}

func TestCoordinatorWithoutPlannerNeverTriages(t *testing.T) {
	cfg := fastConfig(t) // PlannerCLI empty = triage disabled
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	jobs := job.NewStore(cli)
	rec, err := jobs.Create(job.Record{Repo: "demo", Source: job.SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}
	// Several sweep intervals pass; the job must still be open and task-free.
	time.Sleep(10 * sweepInterval(cfg.HeartbeatInterval))
	got, found, err := jobs.Get(rec.ID)
	if err != nil || !found {
		t.Fatalf("get job: found=%v err=%v", found, err)
	}
	if got.State != envelope.JobOpen {
		t.Fatalf("job state = %s, want open", got.State)
	}
	tasks, err := task.NewStore(cli).ListByJob(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 0 {
		t.Fatalf("triage-disabled coordinator persisted %d tasks", len(tasks))
	}
}
