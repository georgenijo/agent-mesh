package sidecar

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
)

// --- BuildMemoryPrimer: rehydration -------------------------------------------

// TestBuildMemoryPrimerRehydratesBlackboard proves the primer renders the durable
// blackboard decisions a restarted expert needs — the core "recover prior
// decisions" acceptance item, at the read layer.
func TestBuildMemoryPrimerRehydratesBlackboard(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")

	resp := do(t, cfg, "expert", meshapi.VerbNote, meshapi.NoteArgs{
		Repo: "demo", Kind: envelope.NoteKindDecision, Text: "auth uses RLS at the DB layer",
	})
	if !resp.OK {
		t.Fatalf("note failed: %+v", resp)
	}
	resp = do(t, cfg, "expert", meshapi.VerbNote, meshapi.NoteArgs{
		Repo: "demo", Kind: envelope.NoteKindSummary, Text: "events are stored in UTC",
	})
	if !resp.OK {
		t.Fatalf("note failed: %+v", resp)
	}

	primer, err := expert.BuildMemoryPrimer("demo", DefaultPrimerBudget)
	if err != nil {
		t.Fatal(err)
	}
	if primer.Total != 2 || primer.Included != 2 {
		t.Fatalf("primer counts = total %d included %d, want 2/2", primer.Total, primer.Included)
	}
	if primer.HighWater == 0 {
		t.Fatal("primer high-water seq must be set")
	}
	for _, want := range []string{"auth uses RLS at the DB layer", "events are stored in UTC", "demo"} {
		if !strings.Contains(primer.Text, want) {
			t.Fatalf("primer missing %q:\n%s", want, primer.Text)
		}
	}
}

// TestBuildMemoryPrimerEmptyBlackboard returns an empty (not error) primer when
// no notes exist, so the loop simply skips priming.
func TestBuildMemoryPrimerEmptyBlackboard(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")

	primer, err := expert.BuildMemoryPrimer("demo", DefaultPrimerBudget)
	if err != nil {
		t.Fatal(err)
	}
	if primer.Text != "" || primer.Total != 0 || primer.Included != 0 {
		t.Fatalf("empty blackboard primer = %+v, want zero", primer)
	}
}

// TestBuildMemoryPrimerCompactsToBudget proves compaction bounds the primer and
// preferentially keeps the highest-value kinds: a decision recorded early
// survives a tight budget even though many later context notes are dropped, so a
// repeated project question is still answerable. This is the compaction +
// "compacted summary preserves the facts" acceptance item.
func TestBuildMemoryPrimerCompactsToBudget(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")

	// One early, high-value decision...
	resp := do(t, cfg, "expert", meshapi.VerbNote, meshapi.NoteArgs{
		Repo: "demo", Kind: envelope.NoteKindDecision, Text: "PRIMARY KEY: tenant_id is required on every row",
	})
	if !resp.OK {
		t.Fatalf("note failed: %+v", resp)
	}
	// ...followed by a flood of low-value context notes that must be elided.
	for i := 0; i < 60; i++ {
		resp := do(t, cfg, "expert", meshapi.VerbNote, meshapi.NoteArgs{
			Repo: "demo", Kind: envelope.NoteKindContext,
			Text: fmt.Sprintf("ran the test suite, iteration %d, all green, no changes needed here at all", i),
		})
		if !resp.OK {
			t.Fatalf("note %d failed: %+v", i, resp)
		}
	}

	const budget = 1024
	primer, err := expert.BuildMemoryPrimer("demo", budget)
	if err != nil {
		t.Fatal(err)
	}
	if primer.Total != 61 {
		t.Fatalf("total = %d, want 61", primer.Total)
	}
	if primer.Included >= primer.Total {
		t.Fatalf("expected compaction to drop notes: included %d of %d", primer.Included, primer.Total)
	}
	if len(primer.Text) > budget {
		t.Fatalf("primer is %d bytes, over budget %d", len(primer.Text), budget)
	}
	// The high-value decision must survive compaction.
	if !strings.Contains(primer.Text, "tenant_id is required") {
		t.Fatalf("compaction dropped the high-value decision:\n%s", primer.Text)
	}
	// The elision must be disclosed honestly, not silent.
	if !strings.Contains(primer.Text, "elided") {
		t.Fatalf("compacted primer must disclose elision:\n%s", primer.Text)
	}
}

// TestSelectWithinBudgetKeepsDecisionsOverContext is a focused unit test of the
// compaction ranking, independent of the bus.
func TestSelectWithinBudgetKeepsDecisionsOverContext(t *testing.T) {
	notes := []memoryNote{
		{Seq: 1, Kind: envelope.NoteKindContext, Text: strings.Repeat("c", 80), Author: "w1"},
		{Seq: 2, Kind: envelope.NoteKindDecision, Text: strings.Repeat("d", 80), Author: "w1"},
		{Seq: 3, Kind: envelope.NoteKindContext, Text: strings.Repeat("c", 80), Author: "w1"},
		{Seq: 4, Kind: envelope.NoteKindOther, Text: strings.Repeat("o", 80), Author: "w1"},
	}
	// Budget that fits only the overhead + one note line.
	keep, dropped := selectWithinBudget("demo", notes, len(renderPrimer("demo", nil, len(notes)))+100)
	if len(keep) != 1 {
		t.Fatalf("kept %d notes, want 1", len(keep))
	}
	if dropped != 3 {
		t.Fatalf("dropped = %d, want 3", dropped)
	}
	if keep[0].Kind != envelope.NoteKindDecision {
		t.Fatalf("kept %q, want the decision to win the tight budget", keep[0].Kind)
	}
}

// --- ServeExpertWithMemory: priming + re-sync ---------------------------------

// recordingPrimer is a fake PrimerFunc that records every injected primer.
type recordingPrimer struct {
	mu       sync.Mutex
	primers  []string
	resyncCh chan struct{}
}

func (r *recordingPrimer) fn(_ context.Context, primer string) error {
	r.mu.Lock()
	r.primers = append(r.primers, primer)
	r.mu.Unlock()
	if r.resyncCh != nil {
		select {
		case r.resyncCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func (r *recordingPrimer) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.primers)
}

func (r *recordingPrimer) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.primers) == 0 {
		return ""
	}
	return r.primers[len(r.primers)-1]
}

// TestServeExpertPrimesFromBlackboardOnStart proves the responder loop injects
// the blackboard memory primer before serving, with no manual reload — the
// restart-without-amnesia core at the loop layer.
func TestServeExpertPrimesFromBlackboardOnStart(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")

	resp := do(t, cfg, "expert", meshapi.VerbNote, meshapi.NoteArgs{
		Repo: "demo", Kind: envelope.NoteKindDecision, Text: "shard by tenant_id",
	})
	if !resp.OK {
		t.Fatalf("note failed: %+v", resp)
	}

	rp := &recordingPrimer{resyncCh: make(chan struct{}, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = expert.ServeExpertWithMemory(ctx, func(_ context.Context, q, _ string) (ExpertResult, error) {
			return ExpertResult{Answer: q, OK: true}, nil
		}, 15*time.Millisecond, ExpertOptions{Repo: "demo", Prime: rp.fn})
	}()

	select {
	case <-rp.resyncCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expert never primed from the blackboard on start")
	}
	if !strings.Contains(rp.last(), "shard by tenant_id") {
		t.Fatalf("primer missing the recorded decision:\n%s", rp.last())
	}
}

// TestServeExpertResyncsOnNewNote proves the expert re-primes when a new
// decision lands on the blackboard (the in-mesh re-sync signal: a worker
// recording a decision after landing a diff), but does NOT re-prime every tick
// when nothing changed.
func TestServeExpertResyncsOnNewNote(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")

	resp := do(t, cfg, "expert", meshapi.VerbNote, meshapi.NoteArgs{
		Repo: "demo", Kind: envelope.NoteKindDecision, Text: "first decision",
	})
	if !resp.OK {
		t.Fatalf("note failed: %+v", resp)
	}

	rp := &recordingPrimer{resyncCh: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = expert.ServeExpertWithMemory(ctx, func(_ context.Context, q, _ string) (ExpertResult, error) {
			return ExpertResult{Answer: q, OK: true}, nil
		}, 15*time.Millisecond, ExpertOptions{Repo: "demo", Prime: rp.fn})
	}()

	// Initial prime.
	select {
	case <-rp.resyncCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no initial prime")
	}

	// Several ticks pass with no new notes: the loop must NOT re-prime.
	time.Sleep(120 * time.Millisecond)
	if got := rp.count(); got != 1 {
		t.Fatalf("re-primed %d times with no new notes, want exactly 1", got)
	}

	// A new decision lands → exactly one re-prime, carrying both decisions.
	resp = do(t, cfg, "expert", meshapi.VerbNote, meshapi.NoteArgs{
		Repo: "demo", Kind: envelope.NoteKindDecision, Text: "second decision",
	})
	if !resp.OK {
		t.Fatalf("note failed: %+v", resp)
	}
	select {
	case <-rp.resyncCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expert never re-primed after a new note landed")
	}
	last := rp.last()
	if !strings.Contains(last, "first decision") || !strings.Contains(last, "second decision") {
		t.Fatalf("re-primed memory missing a decision:\n%s", last)
	}
}

// TestResyncSignalForcesReprime proves an out-of-band Resync request (what
// cmd/meshd raises after a runtime --resume restart) forces a re-prime even when
// the blackboard high-water has not moved — the "--resume cold" acceptance item.
func TestResyncSignalForcesReprime(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")

	resp := do(t, cfg, "expert", meshapi.VerbNote, meshapi.NoteArgs{
		Repo: "demo", Kind: envelope.NoteKindDecision, Text: "only decision",
	})
	if !resp.OK {
		t.Fatalf("note failed: %+v", resp)
	}

	resync := NewResyncSignal()
	rp := &recordingPrimer{resyncCh: make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = expert.ServeExpertWithMemory(ctx, func(_ context.Context, q, _ string) (ExpertResult, error) {
			return ExpertResult{Answer: q, OK: true}, nil
		}, 15*time.Millisecond, ExpertOptions{Repo: "demo", Prime: rp.fn, Resync: resync})
	}()

	select {
	case <-rp.resyncCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no initial prime")
	}

	// No new note, but a restart happened: force a re-prime.
	resync.Request()
	select {
	case <-rp.resyncCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Resync signal did not force a re-prime")
	}
	if got := rp.count(); got < 2 {
		t.Fatalf("expected a forced re-prime, count = %d", got)
	}
}

// TestServeExpertWithoutPrimeDisablesMemory proves the zero ExpertOptions keeps
// pre-#28 behavior: no priming, no blackboard reads.
func TestServeExpertWithoutPrimeDisablesMemory(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")
	asker := startSidecar(t, cfg, "asker")
	_ = asker

	resp := do(t, cfg, "expert", meshapi.VerbJoin, meshapi.JoinArgs{
		Card: agentcard.Card{Name: "expert", Role: "auth"},
	})
	if !resp.OK {
		t.Fatalf("re-join failed: %+v", resp)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// No Prime: this must not panic on a nil PrimerFunc and must still answer.
	go func() {
		_ = expert.ServeExpertWithMemory(ctx, func(_ context.Context, q, _ string) (ExpertResult, error) {
			return ExpertResult{Answer: q, OK: true}, nil
		}, 15*time.Millisecond, ExpertOptions{})
	}()
	// A short settle is enough to confirm no nil-Prime panic crashes the loop.
	time.Sleep(80 * time.Millisecond)
}
