package coordinator

// Coordinator-level scheduler wiring (#25): a coordinator started with a
// WorkerCLI drives a triaged job's DAG through REAL worker child processes to
// done; one started without a WorkerCLI never schedules — a bare `mesh join`
// coordinator must not start spawning workers. The worker here is a script
// speaking the documented one-shot result contract — never a real LLM.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/sidecar"
	"github.com/georgenijo/agent-mesh/internal/task"
	"github.com/georgenijo/agent-mesh/internal/worker"
)

// testWorkerJoin mirrors cmd/meshd's production wiring: a real per-worker
// sidecar adapted to the worker.Session seam. (This package cannot import
// sidecar itself — sidecar's tests import the coordinator — but its TESTS
// can: sidecar does not import coordinator.)
func testWorkerJoin(cfg config.Config) worker.JoinFunc {
	return func(card agentcard.Card) (worker.Session, error) {
		sc, err := sidecar.New(cfg, card, nil)
		if err != nil {
			return nil, err
		}
		if err := sc.Start(); err != nil {
			return nil, err
		}
		return testWorkerSession{sc}, nil
	}
}

type testWorkerSession struct{ *sidecar.Sidecar }

func (s testWorkerSession) BuildPrimer(repo string, budget int) (string, error) {
	p, err := s.Sidecar.BuildMemoryPrimer(repo, budget)
	if err != nil {
		return "", err
	}
	return p.Text, nil
}

// schedRepoFixture creates a git checkout for repo "demo" under a repos dir
// (the #26 worker driver maps a job's repo name to <ReposDir>/<name> and
// refuses to run without the mapping).
func schedRepoFixture(t *testing.T) string {
	t.Helper()
	reposDir := t.TempDir()
	dir := filepath.Join(reposDir, "demo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-q")
	run("config", "user.name", "test")
	run("config", "user.email", "test@localhost")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "-A")
	run("commit", "-q", "--no-gpg-sign", "-m", "init")
	return reposDir
}

// schedWorkerScript writes a fake worker emitting one success result with a
// reported cost.
func schedWorkerScript(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell script worker stub")
	}
	payload := fmt.Sprintf(
		`{"type":"result","subtype":"success","is_error":false,"result":%q,"session_id":"w","num_turns":1,"duration_ms":1,"total_cost_usd":0.001}`,
		"did the work")
	path := filepath.Join(t.TempDir(), "fakeworker.sh")
	script := "#!/bin/sh\ncat <<'WORKER_EOF'\n" + payload + "\nWORKER_EOF\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCoordinatorSchedulesTriagedJobThroughRealWorkerProcess(t *testing.T) {
	cfg := fastConfig(t)
	cfg.PlannerCLI = plannerScript(t, coordPlanJSON)
	cfg.WorkerCLI = schedWorkerScript(t)
	cfg.ReposDir = schedRepoFixture(t)
	cfg.TriageTimeout = 10 * time.Second
	cfg.WorkerTimeout = 10 * time.Second
	cfg.MaxWorkers = 4
	c := New(cfg, nil)
	c.WorkerJoin = testWorkerJoin(cfg)
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	if c.scheduler == nil {
		t.Fatal("scheduler not constructed despite WorkerCLI")
	}
	cli := dialBus(t, cfg)

	jobs := job.NewStore(cli)
	rec, err := jobs.Create(job.Record{Repo: "demo", Source: job.SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}

	// intake → triage (planner child) → schedule (worker children) → done.
	waitJobState(t, jobs, rec.ID, envelope.JobDone)

	tasks, err := task.NewStore(cli).ListByJob(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(tasks))
	}
	for _, tr := range tasks {
		if tr.State != envelope.TaskDone {
			t.Errorf("task %s (%s) state = %s, want done", tr.ID, tr.Node, tr.State)
		}
	}
}

func TestCoordinatorRefusesWorkerCLIWithoutReposDir(t *testing.T) {
	cfg := fastConfig(t)
	cfg.WorkerCLI = schedWorkerScript(t)
	// ReposDir deliberately unset: a worker must never guess which directory
	// tree it may rewrite, so this is a startup error, not a per-task one.
	c := New(cfg, nil)
	err := c.Start()
	if err == nil {
		c.Stop()
		t.Fatal("coordinator started with WorkerCLI but no ReposDir")
	}
	if !strings.Contains(err.Error(), config.EnvReposDir) {
		t.Fatalf("error %q does not name %s", err, config.EnvReposDir)
	}
}

func TestCoordinatorWithoutWorkerCLINeverSchedules(t *testing.T) {
	cfg := fastConfig(t)
	cfg.PlannerCLI = plannerScript(t, coordPlanJSON)
	cfg.TriageTimeout = 10 * time.Second
	// WorkerCLI deliberately unset: triage runs, scheduling must not.
	c := startCoordinator(t, cfg)
	if c.scheduler != nil {
		t.Fatal("scheduler constructed without WorkerCLI opt-in")
	}
	cli := dialBus(t, cfg)

	jobs := job.NewStore(cli)
	rec, err := jobs.Create(job.Record{Repo: "demo", Source: job.SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}
	waitJobState(t, jobs, rec.ID, envelope.JobTriaged)

	// Many sweep intervals later the job is still triaged and every task is
	// still pending — nothing adopted it, nothing spawned.
	time.Sleep(10 * cfg.HeartbeatInterval)
	got, found, err := jobs.Get(rec.ID)
	if err != nil || !found {
		t.Fatalf("get job: found=%v err=%v", found, err)
	}
	if got.State != envelope.JobTriaged {
		t.Fatalf("job state = %s, want still triaged", got.State)
	}
	tasks, err := task.NewStore(cli).ListByJob(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range tasks {
		if tr.State != envelope.TaskPending {
			t.Errorf("task %s state = %s, want still pending", tr.Node, tr.State)
		}
	}
}
