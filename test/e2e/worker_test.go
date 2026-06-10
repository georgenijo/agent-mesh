package e2e

// Cross-process worker-runtime acceptance (#26): coordinator-spawned worker
// CHILD PROCESSES (test/e2e/fakeworker driving the REAL worktree driver) run
// in per-task isolated git worktrees, reach the mesh from inside their run
// through their own embedded sidecar (`mesh context` / `mesh claim` /
// `mesh ask --wait`), and the worktree retention policy is deterministic.
// Assertions are over typed JSON, KV-backed states, and filesystem/git facts
// — never prose. Additive to the shared harness.

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// makeWorkerRepoFixture creates the git checkout the job's repo name "demo"
// resolves to, under <mesh-dir>/repos (MESH_REPOS_DIR). Returns the repos dir.
func makeWorkerRepoFixture(t *testing.T, m *mesh) string {
	t.Helper()
	reposDir := filepath.Join(m.dir, "repos")
	repo := filepath.Join(reposDir, "demo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	gitInWorkerRepo(t, repo, "init", "-q")
	gitInWorkerRepo(t, repo, "config", "user.name", "test")
	gitInWorkerRepo(t, repo, "config", "user.email", "test@localhost")
	if err := os.WriteFile(filepath.Join(repo, "README.md"), []byte("hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitInWorkerRepo(t, repo, "add", "-A")
	gitInWorkerRepo(t, repo, "commit", "-q", "--no-gpg-sign", "-m", "init")
	return reposDir
}

func gitInWorkerRepo(t *testing.T, repo string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestWorkerWorktreeIsolationAndMeshAccessAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	reposDir := makeWorkerRepoFixture(t, m)
	m.env = append(m.env,
		"MESH_PLANNER_CLI="+buildFakePlanner(t, m),
		"FAKEPLANNER_MODE=parallel", // two INDEPENDENT builder nodes
		"MESH_WORKER_CLI="+buildFakeWorker(t, m),
		"FAKEWORKER_MODE=mesh", // context + edit + claim from inside the run
		"FAKEWORKER_MESH_BIN="+meshBin,
		"MESH_REPOS_DIR="+reposDir,
		"MESH_KEEP_WORKTREES=always", // keep the evidence for inspection below
	)
	m.startCoordinator()
	base := m.startDashboard()

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "intake-bot", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}
	jobEnvelopes := tapJobEnvelopes(t, base+"/events")
	taps := tapTriageEnvelopes(t, base+"/events")

	jobID := submitSchedulerJob(t, m)

	// Both independent tasks executed by real worker children to done — and a
	// fakeworker in mesh mode emits a typed ERROR result if `mesh context` or
	// `mesh claim` fails, so a done job IS the in-run mesh-access proof.
	m.eventually(20*time.Second, "KindJob done envelope on the mesh.> tap", func() bool {
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

	// Two workers, two DISTINCT worktrees (keep=always preserved them).
	workersDir := filepath.Join(m.dir, "workers")
	entries, err := os.ReadDir(workersDir)
	if err != nil {
		t.Fatalf("read workers dir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d worker worktrees, want 2: %v", len(entries), entries)
	}

	repo := filepath.Join(reposDir, "demo")
	for _, e := range entries {
		dir := filepath.Join(workersDir, e.Name())
		// Each worker's edit landed in its OWN tree and recorded its own cwd.
		markers, err := filepath.Glob(filepath.Join(dir, "worker-edit-*.txt"))
		if err != nil || len(markers) != 1 {
			t.Fatalf("worktree %s markers = %v (err %v), want exactly 1", e.Name(), markers, err)
		}
		content, err := os.ReadFile(markers[0])
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(strings.TrimSpace(string(content)), e.Name()) {
			t.Fatalf("marker in %s recorded cwd %q — not contained to its worktree", e.Name(), content)
		}
		// The diff was committed onto the per-task branch in the main repo:
		// init + the worker's commit.
		branch := "mesh/worker/" + e.Name()
		if got := gitInWorkerRepo(t, repo, "rev-list", "--count", branch); got != "2" {
			t.Fatalf("branch %s has %s commits, want 2 (init + worker)", branch, got)
		}
	}

	// No cross-contamination: the shared checkout's working tree is untouched.
	if leaked, _ := filepath.Glob(filepath.Join(repo, "worker-edit-*.txt")); len(leaked) != 0 {
		t.Fatalf("worker edits leaked into the shared checkout: %v", leaked)
	}
	if status := gitInWorkerRepo(t, repo, "status", "--porcelain"); status != "" {
		t.Fatalf("shared checkout dirty after worker runs:\n%s", status)
	}
}

func TestDependentWorkerInheritsPredecessorDiffAcrossProcesses(t *testing.T) {
	// The DAG carries code, not just order (#26): the default plan is a chain —
	// impl (builder) then review (reviewer, dependsOn impl). Each worker child
	// commits a worker-edit marker. With dependency inheritance the dependent's
	// worktree merges impl's committed marker before it runs, so it holds TWO
	// markers; the root holds one. Without inheritance both would hold one.
	m := newMesh(t)
	reposDir := makeWorkerRepoFixture(t, m)
	m.env = append(m.env,
		"MESH_PLANNER_CLI="+buildFakePlanner(t, m), // default mode: impl -> review chain
		"MESH_WORKER_CLI="+buildFakeWorker(t, m),
		"FAKEWORKER_MODE=mesh", // each worker commits worker-edit-<pid>.txt = its cwd
		"FAKEWORKER_MESH_BIN="+meshBin,
		"MESH_REPOS_DIR="+reposDir,
		"MESH_KEEP_WORKTREES=always", // keep both trees for inspection
	)
	m.startCoordinator()
	base := m.startDashboard()

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "intake-bot", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}
	jobEnvelopes := tapJobEnvelopes(t, base+"/events")
	jobID := submitSchedulerJob(t, m)

	// A done job means BOTH chained workers succeeded — and review can only
	// succeed if its spawn-time merge of impl's branch was clean.
	m.eventually(25*time.Second, "KindJob done after the impl->review chain", func() bool {
		for _, p := range jobEnvelopes() {
			if p.ID == jobID && p.State == "done" {
				return true
			}
		}
		return false
	})

	workersDir := filepath.Join(m.dir, "workers")
	entries, err := os.ReadDir(workersDir)
	if err != nil {
		t.Fatalf("read workers dir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d worker worktrees, want 2: %v", len(entries), entries)
	}
	counts := make([]int, 0, 2)
	for _, e := range entries {
		markers, _ := filepath.Glob(filepath.Join(workersDir, e.Name(), "worker-edit-*.txt"))
		counts = append(counts, len(markers))
	}
	sort.Ints(counts)
	// [1 2]: the root committed one marker; the dependent inherited it and added
	// its own. [1 1] would mean the dependent branched off bare base — the bug.
	if counts[0] != 1 || counts[1] != 2 {
		t.Fatalf("worker-edit marker counts across worktrees = %v, want [1 2] (dependent inherits predecessor's diff)", counts)
	}
}

func TestWorkerBlockedOnAskResumesWithAnswerAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	reposDir := makeWorkerRepoFixture(t, m)
	m.env = append(m.env,
		"MESH_PLANNER_CLI="+buildFakePlanner(t, m),
		"FAKEPLANNER_MODE=single", // one builder node
		"MESH_WORKER_CLI="+buildFakeWorker(t, m),
		"FAKEWORKER_MODE=ask", // block on `mesh ask --role expert --wait`
		"FAKEWORKER_MESH_BIN="+meshBin,
		"MESH_REPOS_DIR="+reposDir,
		// Default retention policy (on-failure): a successful worker's
		// worktree is removed at teardown — asserted below.
	)
	m.startCoordinator()
	base := m.startDashboard()

	// The expert that unblocks the worker: a resident responder loop over the
	// fake claude runtime, auto-answering role-routed asks.
	m.startExpert("guru", "expert")

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "intake-bot", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}
	jobEnvelopes := tapJobEnvelopes(t, base+"/events")
	taps := tapTriageEnvelopes(t, base+"/events")

	jobID := submitSchedulerJob(t, m)

	// The worker blocks mid-run on `mesh ask --wait`; the expert answers; the
	// worker resumes and reports a typed success — a wait failure would emit a
	// typed error result and fail the job instead.
	m.eventually(25*time.Second, "KindJob done envelope after blocked-on-ask resume", func() bool {
		for _, p := range jobEnvelopes() {
			if p.ID == jobID && p.State == "done" {
				return true
			}
		}
		return false
	})
	m.eventually(10*time.Second, "the task done envelope on the mesh.> tap", func() bool {
		tasks, _ := taps()
		for _, p := range tasks {
			if p.Job == jobID && p.State == "done" {
				return true
			}
		}
		return false
	})

	// Deterministic cleanup: default policy removes a successful worker's
	// worktree (the branch keeps any work product).
	m.eventually(5*time.Second, "successful worker's worktree removed", func() bool {
		entries, err := os.ReadDir(filepath.Join(m.dir, "workers"))
		return err == nil && len(entries) == 0
	})
}
