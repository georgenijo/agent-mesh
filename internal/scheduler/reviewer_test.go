package scheduler

// Unit acceptance for the #80 BusReviewer transport: worker-metadata parsing,
// the request→verdict bus round trip against a FAKE in-process expert, the
// timeout synthesis (never an approve), and the no-diff / unreviewable paths.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/task"
)

func TestParseWorkerMeta(t *testing.T) {
	block := "[mesh worker] task=t-1 branch=mesh/worker/t-1\n" +
		"base=aaa head=bbb\n" +
		"changed files (2):\n  src/a.go\n  src/b.go"
	noChanges := "[mesh worker] task=t-1 branch=mesh/worker/t-1\n" +
		"base=aaa head=aaa\nno file changes"

	cases := []struct {
		name    string
		summary string
		ok      bool
		head    string
		files   int
	}{
		{"plain block", block, true, "bbb", 2},
		{"model prose before the block", "I did the work.\n\n" + block, true, "bbb", 2},
		{"no changes block", noChanges, true, "aaa", 0},
		{"no metadata at all", "I did the work, trust me.", false, "", 0},
		{"empty summary", "", false, "", 0},
		{"marker but torn block", "[mesh worker] task=t-1 branch=b", false, "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := parseWorkerMeta(tc.summary)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if m.HeadSHA != tc.head {
				t.Fatalf("head = %q, want %q", m.HeadSHA, tc.head)
			}
			if len(m.Files) != tc.files {
				t.Fatalf("files = %v, want %d entries", m.Files, tc.files)
			}
			if m.TaskID != "t-1" || m.Branch != "mesh/worker/t-1" || m.BaseSHA == "" {
				t.Fatalf("meta fields = %+v", m)
			}
		})
	}
}

// reviewRepoFixture builds a <reposDir>/demo git checkout with one base commit
// and one change committed on a worker-style branch, returning the two SHAs.
func reviewRepoFixture(t *testing.T) (reposDir, baseSHA, headSHA string) {
	t.Helper()
	reposDir = t.TempDir()
	repo := filepath.Join(reposDir, "demo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("config", "user.name", "test")
	git("config", "user.email", "test@localhost")
	if err := os.WriteFile(filepath.Join(repo, "main.go"), []byte("old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "-A")
	git("commit", "-q", "--no-gpg-sign", "-m", "base")
	baseSHA = git("rev-parse", "HEAD")
	git("checkout", "-q", "-b", "mesh/worker/t-1")
	if err := os.WriteFile(filepath.Join(repo, "main.go"), []byte("new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", "-A")
	git("commit", "-q", "--no-gpg-sign", "-m", "worker change")
	headSHA = git("rev-parse", "HEAD")
	return reposDir, baseSHA, headSHA
}

func workerSummary(baseSHA, headSHA string) string {
	return fmt.Sprintf("did the work\n\n[mesh worker] task=t-1 branch=mesh/worker/t-1\nbase=%s head=%s\nchanged files (1):\n  main.go",
		baseSHA, headSHA)
}

func reviewTarget(summary string) ReviewTarget {
	return ReviewTarget{
		Task:    task.Record{ID: "t-1", Job: "j-1", Title: "change main.go", Role: "builder"},
		Repo:    "demo",
		Summary: summary,
	}
}

// The full round trip: the reviewer computes the real diff from the shared
// checkout, publishes a role-addressed request, and resolves to the typed
// verdict the (fake) expert publishes on mesh.review.<task> — cost included.
func TestBusReviewerRoundTripDeliversVerdict(t *testing.T) {
	f := newFixture(t)
	reposDir, baseSHA, headSHA := reviewRepoFixture(t)

	// The fake expert: serve mesh.review-req.rev, assert the request carries
	// the real diff, answer with a typed verdict event.
	if _, err := f.cli.Subscribe(envelope.SubjectReviewRequest("rev"), func(env envelope.Envelope) {
		var p envelope.ReviewRequestPayload
		if err := envelope.DecodeInto(env, &p); err != nil {
			t.Errorf("decode review request: %v", err)
			return
		}
		if !strings.Contains(p.Diff, "+new") || !strings.Contains(p.Diff, "-old") {
			t.Errorf("request diff does not carry the worker change:\n%s", p.Diff)
		}
		if p.HeadSHA != headSHA || len(p.ChangedFiles) != 1 || p.ChangedFiles[0] != "main.go" {
			t.Errorf("request metadata = %+v", p)
		}
		verdict, err := envelope.New(envelope.KindReview, "rev-expert",
			envelope.SubjectReview(p.Task), &envelope.ReviewPayload{
				Task: p.Task, Job: p.Job, Branch: p.Branch, HeadSHA: p.HeadSHA,
				Verdict: envelope.ReviewRequestChanges, Notes: "needs tests", CostUSD: 0.5,
			})
		if err != nil {
			t.Errorf("build verdict: %v", err)
			return
		}
		if err := f.cli.Publish(verdict); err != nil {
			t.Errorf("publish verdict: %v", err)
		}
	}); err != nil {
		t.Fatal(err)
	}

	r, err := NewBusReviewer(f.cli, ReviewerOptions{Role: "rev", ReposDir: reposDir, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)

	dec, err := r.Review(context.Background(), reviewTarget(workerSummary(baseSHA, headSHA)))
	if err != nil {
		t.Fatal(err)
	}
	if dec.Verdict != envelope.ReviewRequestChanges || dec.Notes != "needs tests" {
		t.Fatalf("decision = %+v, want request_changes/needs tests", dec)
	}
	if dec.CostUSD != 0.5 {
		t.Fatalf("decision cost = %v, want 0.5 (review cost must reach the budget meter)", dec.CostUSD)
	}
}

// No expert serving the role: the round trip times out and resolves to a
// typed runtime_lost error — never an approve — and a synthesized KindReview
// event still reaches the taps.
func TestBusReviewerTimeoutSynthesizesLostVerdict(t *testing.T) {
	f := newFixture(t)
	reposDir, baseSHA, headSHA := reviewRepoFixture(t)

	r, err := NewBusReviewer(f.cli, ReviewerOptions{Role: "rev", ReposDir: reposDir, Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)

	dec, err := r.Review(context.Background(), reviewTarget(workerSummary(baseSHA, headSHA)))
	if err != nil {
		t.Fatal(err)
	}
	if dec.Verdict != envelope.ReviewError || dec.Code != envelope.ReviewRuntimeLost {
		t.Fatalf("decision = %+v, want error/runtime_lost", dec)
	}
	waitFor(t, 2*time.Second, "synthesized review event on the tap", func() bool {
		for _, env := range f.events() {
			if env.Kind != envelope.KindReview {
				continue
			}
			var p envelope.ReviewPayload
			if envelope.DecodeInto(env, &p) == nil && p.Task == "t-1" &&
				p.Verdict == envelope.ReviewError && p.Code == envelope.ReviewRuntimeLost {
				return true
			}
		}
		return false
	})
}

// head == base: nothing to review exists; no request is published.
func TestBusReviewerNoDiffSkipsRequest(t *testing.T) {
	f := newFixture(t)
	reposDir, baseSHA, _ := reviewRepoFixture(t)

	r, err := NewBusReviewer(f.cli, ReviewerOptions{Role: "rev", ReposDir: reposDir, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)

	dec, err := r.Review(context.Background(), reviewTarget(workerSummary(baseSHA, baseSHA)))
	if err != nil {
		t.Fatal(err)
	}
	if !dec.NoDiff {
		t.Fatalf("decision = %+v, want NoDiff", dec)
	}
	for _, env := range f.events() {
		if env.Kind == envelope.KindReviewRequest {
			t.Fatalf("a review request was published for a no-change success")
		}
	}
}

// A success without the worker's diff metadata block is unreviewable: a typed
// internal error verdict, never an approve.
func TestBusReviewerMissingMetadataIsTypedError(t *testing.T) {
	f := newFixture(t)
	reposDir, _, _ := reviewRepoFixture(t)

	r, err := NewBusReviewer(f.cli, ReviewerOptions{Role: "rev", ReposDir: reposDir, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)

	dec, err := r.Review(context.Background(), reviewTarget("I did the work, no metadata here"))
	if err != nil {
		t.Fatal(err)
	}
	if dec.Verdict != envelope.ReviewError || dec.Code != envelope.ReviewInternal {
		t.Fatalf("decision = %+v, want error/internal", dec)
	}
}

// An illegal role token is a construction error, not a runtime surprise.
func TestBusReviewerRejectsIllegalRole(t *testing.T) {
	f := newFixture(t)
	if _, err := NewBusReviewer(f.cli, ReviewerOptions{Role: "bad role!", ReposDir: t.TempDir()}); err == nil {
		t.Fatal("NewBusReviewer accepted an illegal role token")
	}
}
