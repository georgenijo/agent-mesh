package e2e

// Cross-process review-gating acceptance (#80): a coordinator with
// MESH_REVIEW_ROLE set routes every successful worker diff to the resident
// expert serving that role (a real `mesh expert serve` process over the fake
// claude runtime) and gates the task's terminal state on the typed verdict —
// approve reaches done, reject fails the task and the job, and the verdict is
// observable as a KindReview envelope on the mesh.> tap. Assertions are over
// typed JSON payloads and KV-backed states, never prose. Additive to the
// shared harness.

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

type reviewTapPayload struct {
	Task    string  `json:"task"`
	Job     string  `json:"job"`
	Verdict string  `json:"verdict"`
	Code    string  `json:"code"`
	Notes   string  `json:"notes"`
	CostUSD float64 `json:"costUSD"`
}

// tapReviewEnvelopes opens the dashboard SSE stream and collects the
// ReviewPayloads of every KindReview envelope it sees. Local to this test
// file (the shared harness is not modified), mirroring tapJobEnvelopes.
func tapReviewEnvelopes(t *testing.T, url string) func() []reviewTapPayload {
	t.Helper()
	resp, err := http.Get(url) //nolint:bodyclose // closed in cleanup below
	if err != nil {
		t.Fatalf("open SSE tap: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	var (
		mu   = make(chan struct{}, 1)
		seen []reviewTapPayload
	)
	mu <- struct{}{}
	go func() {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			data, ok := strings.CutPrefix(sc.Text(), "data: ")
			if !ok {
				continue
			}
			var ev struct {
				Type     string `json:"type"`
				Envelope struct {
					Kind    string           `json:"kind"`
					Payload reviewTapPayload `json:"payload"`
				} `json:"envelope"`
			}
			if json.Unmarshal([]byte(data), &ev) != nil {
				continue
			}
			if ev.Type != "event" || ev.Envelope.Kind != "review" {
				continue
			}
			<-mu
			seen = append(seen, ev.Envelope.Payload)
			mu <- struct{}{}
		}
	}()
	return func() []reviewTapPayload {
		<-mu
		out := append([]reviewTapPayload(nil), seen...)
		mu <- struct{}{}
		return out
	}
}

// startReviewGateMesh boots the gated fleet: triage + worker (fakes) + the
// review gate addressed at role "reviewer", plus the dashboard taps and an
// intake agent. The expert is started and confirmed live BEFORE any job is
// submitted, so a review request can never race its subscription.
func startReviewGateMesh(t *testing.T, m *mesh) (jobs func() []jobPayload, reviews func() []reviewTapPayload) {
	t.Helper()
	m.env = append(m.env,
		"MESH_PLANNER_CLI="+buildFakePlanner(t, m),
		"FAKEPLANNER_MODE=single", // one builder node
		"MESH_WORKER_CLI="+buildFakeWorker(t, m),
		"FAKEWORKER_MODE=mesh", // edits a file in the worktree → a real diff to review
		"FAKEWORKER_MESH_BIN="+meshBin,
		"MESH_REPOS_DIR="+makeWorkerRepoFixture(t, m),
		"MESH_REVIEW_ROLE=reviewer",
	)
	m.startCoordinator()
	base := m.startDashboard()

	m.startExpert("rev", "reviewer")
	m.eventually(5*time.Second, "rev is a live reviewer agent", func() bool {
		agents, exit := m.who("rev")
		if exit != 0 {
			return false
		}
		rec, ok := findAgent(agents, "rev")
		return ok && rec.Card.Role == "reviewer" && string(rec.State) == "live"
	})

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "intake-bot", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}
	return tapJobEnvelopes(t, base+"/events"), tapReviewEnvelopes(t, base+"/events")
}

// An approved review is the only path from a reviewed worker success to done:
// the job completes AND the typed approve verdict is observable on the tap.
func TestReviewGateApprovesDiffAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	// fakeclaude's default review verdict is approve; pin it for explicitness.
	m.env = append(m.env, `FAKECLAUDE_VERDICT={"verdict":"approve","notes":"diff is sound"}`)
	jobs, reviews := startReviewGateMesh(t, m)

	jobID := submitSchedulerJob(t, m)

	m.eventually(30*time.Second, "KindJob done envelope after the approved review", func() bool {
		for _, p := range jobs() {
			if p.ID == jobID && p.State == "done" {
				return true
			}
		}
		return false
	})
	m.eventually(10*time.Second, "KindReview approve envelope on the mesh.> tap", func() bool {
		for _, p := range reviews() {
			if p.Job == jobID && p.Verdict == "approve" {
				return true
			}
		}
		return false
	})
}

// A rejected review NEVER becomes a silent done: the worker run succeeds, the
// expert rejects the diff, and the task — and with it the job — fails typed.
func TestReviewGateRejectFailsTaskAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	m.env = append(m.env, `FAKECLAUDE_VERDICT={"verdict":"reject","notes":"wrong approach entirely"}`)
	jobs, reviews := startReviewGateMesh(t, m)

	jobID := submitSchedulerJob(t, m)

	m.eventually(30*time.Second, "KindJob failed envelope after the rejected review", func() bool {
		for _, p := range jobs() {
			if p.ID == jobID && p.State == "failed" {
				return true
			}
		}
		return false
	})
	m.eventually(10*time.Second, "KindReview reject envelope on the mesh.> tap", func() bool {
		for _, p := range reviews() {
			if p.Job == jobID && p.Verdict == "reject" {
				return true
			}
		}
		return false
	})
	// The board stays truthful: the job never reached done at any point.
	for _, p := range jobs() {
		if p.ID == jobID && p.State == "done" {
			t.Fatalf("rejected job emitted a done envelope")
		}
	}
}
