package e2e

// Cross-process cold-start review acceptance (#125 hardening / Feature 7):
// with MESH_AUTO_EXPERTS + MESH_REVIEW_ROLE armed, the coordinator must
// auto-spawn a reviewer expert when NO live agent owns the role, re-deliver
// the first KindReviewRequest after the expert is listening, and gate the
// task to done on a real KindReview verdict — without a human pre-starting
// `mesh expert serve`. Proves the cold first review lands (not a 20m hang).

import (
	"testing"
	"time"
)

// TestAutoExpertColdStartReviewAcrossProcesses is the acceptance gate for
// auto-spawned review on a cold fleet: no reviewer is started by the test.
// The coordinator observes the scheduler's KindReviewRequest, launches a
// resident expert for MESH_REVIEW_ROLE, re-delivers the buffered request, and
// the fakeclaude verdict (approve) drives the job to done.
func TestAutoExpertColdStartReviewAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	m.env = append(m.env,
		"MESH_AUTO_EXPERTS=on",
		"MESH_REVIEW_ROLE=reviewer",
		"MESH_REVIEW_TIMEOUT=45s",
		"MESH_PLANNER_CLI="+buildFakePlanner(t, m),
		"FAKEPLANNER_MODE=single", // one builder node → one review
		"MESH_WORKER_CLI="+buildFakeWorker(t, m),
		"FAKEWORKER_MODE=mesh", // real worktree edit → a diff to review
		"FAKEWORKER_MESH_BIN="+meshBin,
		"MESH_REPOS_DIR="+makeWorkerRepoFixture(t, m),
		`FAKECLAUDE_VERDICT={"verdict":"approve","notes":"cold-start review ok"}`,
	)
	m.startCoordinator()
	base := m.startDashboard()

	// Intake only — deliberately NO startExpert. Ground truth: no reviewer
	// exists before the job, so any KindReview must come from an auto-spawn.
	if code, _, stderr := m.run("join", "--name", "intake", "--role", "intake-bot", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}
	agents, _ := m.who("intake")
	if rec, ok := findAgentByRole(agents, "reviewer"); ok {
		t.Fatalf("a reviewer (%s) already exists; cannot prove autonomous cold-start spawn", rec.Card.Name)
	}

	jobs := tapJobEnvelopes(t, base+"/events")
	reviews := tapReviewEnvelopes(t, base+"/events")

	jobID := submitSchedulerJob(t, m)

	// Coordinator brings up a live reviewer expert on its own.
	m.eventually(30*time.Second, "coordinator auto-spawns a live reviewer expert", func() bool {
		agents, exit := m.who("intake")
		if exit != 0 {
			return false
		}
		rec, ok := findAgentByRole(agents, "reviewer")
		return ok && string(rec.State) == "live"
	})

	// Cold first review lands as a typed KindReview (not a timeout / silent pass).
	m.eventually(45*time.Second, "KindReview approve envelope on the mesh.> tap", func() bool {
		for _, p := range reviews() {
			if p.Job == jobID && p.Verdict == "approve" {
				return true
			}
		}
		return false
	})

	// And the gate drives the job to done.
	m.eventually(15*time.Second, "KindJob done after cold-start approved review", func() bool {
		for _, p := range jobs() {
			if p.ID == jobID && p.State == "done" {
				return true
			}
		}
		return false
	})
}
