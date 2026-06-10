package e2e

// Cross-process scheduler acceptance (#25): a submitted job is triaged into a
// DAG by a coordinator-spawned planner CHILD PROCESS (test/e2e/fakeplanner)
// and then executed node-by-node by coordinator-spawned worker CHILD
// PROCESSES (test/e2e/fakeworker speaking the same one-shot contract through
// the provisional CLIDriver), and the whole lifecycle is observable on the
// mesh.> tap. Assertions are over typed JSON only — never prose. Additive to
// the shared harness: the fake binaries are built here, not in TestMain.

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// buildFakeWorker compiles the fakeworker binary into this mesh's dir.
func buildFakeWorker(t *testing.T, m *mesh) string {
	t.Helper()
	bin := filepath.Join(m.dir, "fakeworker")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/georgenijo/agent-mesh/test/e2e/fakeworker")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build fakeworker: %v\n%s", err, out)
	}
	return bin
}

// submitJob submits one job through the joined agent and returns its id.
func submitSchedulerJob(t *testing.T, m *mesh) string {
	t.Helper()
	code, stdout, stderr := m.run("submit", "do X", "--repo", "demo", "--json", "--socket", m.agentSocket("intake"))
	if code != 0 {
		t.Fatalf("submit exit %d: stderr %s stdout %s", code, stderr, stdout)
	}
	var res struct {
		Job string `json:"job"`
	}
	if err := json.Unmarshal([]byte(stdout), &res); err != nil || res.Job == "" {
		t.Fatalf("submit json: %v\n%s", err, stdout)
	}
	return res.Job
}

func TestSchedulerRunsTriagedDAGAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	m.env = append(m.env,
		"MESH_PLANNER_CLI="+buildFakePlanner(t, m),
		"MESH_WORKER_CLI="+buildFakeWorker(t, m),
		// The #26 worktree driver needs the repo-name → checkout mapping.
		"MESH_REPOS_DIR="+makeWorkerRepoFixture(t, m),
	)
	m.startCoordinator()
	base := m.startDashboard()

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}
	jobEnvelopes := tapJobEnvelopes(t, base+"/events")
	taps := tapTriageEnvelopes(t, base+"/events")

	jobID := submitSchedulerJob(t, m)

	// The full autonomous spine: open → triaged → scheduled → running → done,
	// with both DAG nodes (fakeplanner's impl → review) executed by real
	// worker child processes.
	m.eventually(15*time.Second, "KindJob done envelope on the mesh.> tap", func() bool {
		for _, p := range jobEnvelopes() {
			if p.ID == jobID && p.State == "done" {
				return true
			}
		}
		return false
	})
	m.eventually(10*time.Second, "both task done envelopes on the mesh.> tap", func() bool {
		tasks, _ := taps()
		done := map[string]bool{}
		for _, p := range tasks {
			if p.Job == jobID && p.State == "done" {
				done[p.ID] = true
			}
		}
		return len(done) == 2
	})
}

func TestSchedulerFailedWorkerFailsJobAndSkipsDependent(t *testing.T) {
	m := newMesh(t)
	m.env = append(m.env,
		"MESH_PLANNER_CLI="+buildFakePlanner(t, m),
		"MESH_WORKER_CLI="+buildFakeWorker(t, m),
		"MESH_REPOS_DIR="+makeWorkerRepoFixture(t, m),
		"FAKEWORKER_MODE=fail",
	)
	m.startCoordinator()
	base := m.startDashboard()

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}
	jobEnvelopes := tapJobEnvelopes(t, base+"/events")
	taps := tapTriageEnvelopes(t, base+"/events")

	jobID := submitSchedulerJob(t, m)

	// impl fails typed; review (dependsOn impl) is skipped with the typed
	// terminal cancelled state; the job fails.
	m.eventually(15*time.Second, "KindJob failed envelope on the mesh.> tap", func() bool {
		for _, p := range jobEnvelopes() {
			if p.ID == jobID && p.State == "failed" {
				return true
			}
		}
		return false
	})
	m.eventually(10*time.Second, "one failed and one cancelled task envelope", func() bool {
		tasks, _ := taps()
		failed, cancelled := 0, 0
		seen := map[string]string{}
		for _, p := range tasks {
			if p.Job != jobID {
				continue
			}
			seen[p.ID] = p.State // last state wins per task
		}
		for _, st := range seen {
			switch st {
			case "failed":
				failed++
			case "cancelled":
				cancelled++
			}
		}
		return failed == 1 && cancelled == 1
	})

	// The coordinator survived: presence still works end-to-end.
	if code, _, stderr := m.run("join", "--name", "after", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join after scheduler failure exit %d: %s", code, stderr)
	}
}
