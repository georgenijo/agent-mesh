package e2e

// Cross-process integration proof for the Atlas planning-tool ↔ mesh wire:
// a PLAN written to a repo's blackboard reaches the worker that implements the
// ticket. The plan-step (triage.runPlanStep) runs fakeplancli before
// decomposition and writes its stdout to the repo blackboard as a context note,
// so decomposition (recentNotes) and every worker (mesh context + primer) read
// the plan — no manual note write required.
//
//	triage plan-step (fakeplancli)  →  plan note on blackboard
//	  →  triage decomposes          →  worker reads it via
//	                                   `mesh context` from
//	                                   inside its own run
//
// fakeworker "context" mode persists what the worker SAW to a marker file in
// its isolated worktree, so the assertion is over a real filesystem fact, not
// prose: the distinctive plan token must appear in what the worker received.
//
// $0, deterministic, no LLM/API key — additive to the shared harness.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildFakePlanCLI compiles the fakeplancli binary into this mesh's dir.
func buildFakePlanCLI(t *testing.T, m *mesh) string {
	t.Helper()
	bin := filepath.Join(m.dir, "fakeplancli")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/georgenijo/agent-mesh/test/e2e/fakeplancli")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build fakeplancli: %v\n%s", err, out)
	}
	return bin
}

func TestWorkerReceivesPlanFromBlackboard(t *testing.T) {
	const planToken = "PLANTOKEN-7F3A2C" // distinctive marker to grep out of what the worker saw

	m := newMesh(t)
	reposDir := makeWorkerRepoFixture(t, m)
	m.env = append(m.env,
		"MESH_PLANNER_CLI="+buildFakePlanner(t, m),
		"FAKEPLANNER_MODE=single", // one builder node → one worker, one worktree
		"MESH_WORKER_CLI="+buildFakeWorker(t, m),
		"FAKEWORKER_MODE=context", // capture what `mesh context` returns inside the run
		"FAKEWORKER_MESH_BIN="+meshBin,
		"MESH_REPOS_DIR="+reposDir,
		"MESH_KEEP_WORKTREES=always",            // preserve the worktree so we can read the marker
		"MESH_PLAN_CLI="+buildFakePlanCLI(t, m), // plan-step writes the note automatically
		"FAKEPLANCLI_TOKEN="+planToken,
	)
	m.startCoordinator()
	base := m.startDashboard()

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}

	// 1. Submit the ticket as a job; the plan-step runs fakeplancli and writes the
	//    plan note to the blackboard before decomposition; triage then decomposes
	//    it; a worker implements it.
	jobEnvelopes := tapJobEnvelopes(t, base+"/events")
	jobID := submitSchedulerJob(t, m)
	t.Logf("submitted job %s; waiting for the worker to run", jobID)

	m.eventually(20*time.Second, "KindJob done envelope on the mesh.> tap", func() bool {
		for _, p := range jobEnvelopes() {
			if p.ID == jobID && p.State == "done" {
				return true
			}
		}
		return false
	})

	// 2. The worker, from inside its isolated run, saw the plan on the blackboard.
	//    fakeworker context-mode persisted exactly what `mesh context` returned.
	workersDir := filepath.Join(m.dir, "workers")
	entries, err := os.ReadDir(workersDir)
	if err != nil {
		t.Fatalf("read workers dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d worker worktrees, want 1: %v", len(entries), entries)
	}
	markers, err := filepath.Glob(filepath.Join(workersDir, entries[0].Name(), "worker-saw-context-*.txt"))
	if err != nil || len(markers) != 1 {
		t.Fatalf("worktree %s context markers = %v (err %v), want exactly 1", entries[0].Name(), markers, err)
	}
	saw, err := os.ReadFile(markers[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(saw), planToken) {
		t.Fatalf("the worker did NOT receive the plan: %q not found in what `mesh context` returned:\n%s",
			planToken, saw)
	}
	t.Logf("PROVEN: the worker received the plan from the blackboard (token %s present in its `mesh context`)", planToken)
}
