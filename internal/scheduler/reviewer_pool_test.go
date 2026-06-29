package scheduler

// Pool-parallelism acceptance for #123: MESH_REVIEW_POOL_SIZE > 1 distributes
// concurrent KindReviewRequests across distinct experts via round-robin on the
// per-expert direct subjects, so N reviews run in parallel instead of serialising
// on one expert. Pool size 1 is covered by the existing BusReviewer tests.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// workerSummaryForTask builds the #26 diff-metadata summary block for an
// arbitrary task ID (the existing workerSummary helper hard-codes t-1).
func workerSummaryForTask(taskID, baseSHA, headSHA string) string {
	return fmt.Sprintf(
		"did the work\n\n[mesh worker] task=%s branch=mesh/worker/%s\nbase=%s head=%s\nchanged files (1):\n  main.go",
		taskID, taskID, baseSHA, headSHA,
	)
}

// seedRegistryExpert writes a live RegistryRecord for name/role directly into
// the bus KV so the BusReviewer's poolExperts() call finds it.
func seedRegistryExpert(t *testing.T, cli *bus.Client, name, role string) {
	t.Helper()
	rec := agentcard.RegistryRecord{
		Card:  agentcard.Card{ID: name, Name: name, Role: role},
		State: agentcard.PresenceLive,
	}
	// KVPut marshals the value via json.Marshal internally — pass the struct.
	if _, err := cli.KVPut(envelope.BucketRegistry, name, rec, bus.PutOptions{}); err != nil {
		t.Fatalf("seed registry expert %q: %v", name, err)
	}
}

// fakePoolExpert subscribes to the direct slot subject for name/role and
// immediately publishes an approve verdict for every incoming review request.
// It returns an atomic counter of how many requests it handled and a done
// channel that is closed once the expert's handler has been called at least
// once (used for barrier synchronisation in parallel assertions).
func fakePoolExpert(t *testing.T, cli *bus.Client, name, role string) (handled *atomic.Int32, first chan struct{}) {
	t.Helper()
	h := new(atomic.Int32)
	ch := make(chan struct{})
	var once sync.Once

	subject := envelope.SubjectReviewRequestDirect(role, name)
	if _, err := cli.Subscribe(subject, func(env envelope.Envelope) {
		var p envelope.ReviewRequestPayload
		if err := envelope.DecodeInto(env, &p); err != nil {
			t.Errorf("fakePoolExpert %q: decode request: %v", name, err)
			return
		}
		h.Add(1)
		once.Do(func() { close(ch) })
		verdict, err := envelope.New(envelope.KindReview, name,
			envelope.SubjectReview(p.Task), &envelope.ReviewPayload{
				Task:    p.Task,
				Job:     p.Job,
				Verdict: envelope.ReviewApprove,
			})
		if err != nil {
			t.Errorf("fakePoolExpert %q: build verdict: %v", name, err)
			return
		}
		if err := cli.Publish(verdict); err != nil {
			t.Errorf("fakePoolExpert %q: publish verdict: %v", name, err)
		}
	}); err != nil {
		t.Fatalf("fakePoolExpert %q: subscribe %s: %v", name, subject, err)
	}
	return h, ch
}

// TestReviewPoolDistributesRequestsAcrossExperts proves that with PoolSize 2
// and two registered live experts, two concurrent Review() calls are routed to
// DISTINCT experts (not both to the same one) so they can run in parallel.
func TestReviewPoolDistributesRequestsAcrossExperts(t *testing.T) {
	f := newFixture(t)
	reposDir, baseSHA, headSHA := reviewRepoFixture(t)

	const role = "rev"
	const expertA = "expert-rev-0"
	const expertB = "expert-rev-1"

	// Seed the registry so poolExperts() discovers both experts.
	seedRegistryExpert(t, f.cli, expertA, role)
	seedRegistryExpert(t, f.cli, expertB, role)

	// Fake experts: each subscribes to its own direct subject and instantly
	// approves. Both must handle exactly one review each.
	handledA, firstA := fakePoolExpert(t, f.cli, expertA, role)
	handledB, firstB := fakePoolExpert(t, f.cli, expertB, role)

	r, err := NewBusReviewer(f.cli, ReviewerOptions{
		Role:     role,
		ReposDir: reposDir,
		PoolSize: 2,
		Timeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)

	target1 := ReviewTarget{
		Task:    task.Record{ID: "t-pool-1", Job: "j-1"},
		Repo:    "demo",
		Summary: workerSummaryForTask("t-pool-1", baseSHA, headSHA),
	}
	target2 := ReviewTarget{
		Task:    task.Record{ID: "t-pool-2", Job: "j-1"},
		Repo:    "demo",
		Summary: workerSummaryForTask("t-pool-2", baseSHA, headSHA),
	}

	// Issue both reviews concurrently.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		dec, err := r.Review(context.Background(), target1)
		if err != nil {
			t.Errorf("review t-pool-1: %v", err)
			return
		}
		if dec.Verdict != envelope.ReviewApprove {
			t.Errorf("t-pool-1 verdict = %v, want approve", dec.Verdict)
		}
	}()
	go func() {
		defer wg.Done()
		dec, err := r.Review(context.Background(), target2)
		if err != nil {
			t.Errorf("review t-pool-2: %v", err)
			return
		}
		if dec.Verdict != envelope.ReviewApprove {
			t.Errorf("t-pool-2 verdict = %v, want approve", dec.Verdict)
		}
	}()
	wg.Wait()

	// Each expert must have handled exactly one review — the pool distributed
	// the two requests to distinct experts, not both to one.
	if handledA.Load() != 1 || handledB.Load() != 1 {
		t.Fatalf("pool distribution: expertA handled %d, expertB handled %d; want 1 each (pool size 2 must route to distinct experts)",
			handledA.Load(), handledB.Load())
	}

	// Wait for both "first handled" signals to confirm both experts were
	// actually invoked (not just that the verdict channel got the right task).
	timeout := time.After(2 * time.Second)
	select {
	case <-firstA:
	case <-timeout:
		t.Fatal("expertA never received its review request")
	}
	timeout = time.After(2 * time.Second)
	select {
	case <-firstB:
	case <-timeout:
		t.Fatal("expertB never received its review request")
	}
}

// TestReviewPoolRoundRobinIsStable verifies that successive calls cycle
// deterministically across the sorted expert list (alphabetical order).
// With 2 experts A and B (sorted: A < B), calls 1→A, 2→B, 3→A, 4→B, …
func TestReviewPoolRoundRobinIsStable(t *testing.T) {
	f := newFixture(t)
	reposDir, baseSHA, headSHA := reviewRepoFixture(t)

	const role = "rr"
	// Two experts whose sorted names produce a predictable order.
	const expertA = "expert-rr-alpha"
	const expertB = "expert-rr-beta"

	seedRegistryExpert(t, f.cli, expertA, role)
	seedRegistryExpert(t, f.cli, expertB, role)

	// Track which expert handled each task in order.
	var mu sync.Mutex
	order := []string{}

	makeExpert := func(name string) {
		subject := envelope.SubjectReviewRequestDirect(role, name)
		if _, err := f.cli.Subscribe(subject, func(env envelope.Envelope) {
			var p envelope.ReviewRequestPayload
			if err := envelope.DecodeInto(env, &p); err != nil {
				return
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			verdict, _ := envelope.New(envelope.KindReview, name,
				envelope.SubjectReview(p.Task), &envelope.ReviewPayload{
					Task: p.Task, Verdict: envelope.ReviewApprove,
				})
			f.cli.Publish(verdict) //nolint:errcheck
		}); err != nil {
			t.Fatalf("subscribe %s: %v", name, err)
		}
	}
	makeExpert(expertA)
	makeExpert(expertB)

	r, err := NewBusReviewer(f.cli, ReviewerOptions{
		Role:     role,
		ReposDir: reposDir,
		PoolSize: 2,
		Timeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)

	// Issue 4 sequential reviews; round-robin must alternate A, B, A, B.
	for i := 1; i <= 4; i++ {
		taskID := fmt.Sprintf("t-rr-%d", i)
		tgt := ReviewTarget{
			Task:    task.Record{ID: taskID, Job: "j-rr"},
			Repo:    "demo",
			Summary: workerSummaryForTask(taskID, baseSHA, headSHA),
		}
		dec, err := r.Review(context.Background(), tgt)
		if err != nil {
			t.Fatalf("review %s: %v", taskID, err)
		}
		if dec.Verdict != envelope.ReviewApprove {
			t.Fatalf("review %s: verdict = %v, want approve", taskID, dec.Verdict)
		}
	}

	mu.Lock()
	got := order
	mu.Unlock()

	// Sorted names: expertA < expertB → seq 0→A, 1→B, 2→A, 3→B
	want := []string{expertA, expertB, expertA, expertB}
	if len(got) != len(want) {
		t.Fatalf("round-robin order len = %d, want %d", len(got), len(want))
	}
	for i, g := range got {
		if g != want[i] {
			t.Errorf("round-robin[%d] = %q, want %q", i, g, want[i])
		}
	}
}
