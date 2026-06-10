package job

import (
	"errors"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

func TestTransitionOpenToTriaged(t *testing.T) {
	store, cleanup := jobStore(t)
	defer cleanup()

	rec, err := store.Create(Record{Repo: "demo", Source: SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Transition(rec.ID, envelope.JobOpen, envelope.JobTriaged, "coordinator", "tasks: 3")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != envelope.JobTriaged {
		t.Fatalf("state = %s, want triaged", got.State)
	}
	persisted, found, err := store.Get(rec.ID)
	if err != nil || !found {
		t.Fatalf("get found=%v err=%v", found, err)
	}
	if persisted.State != envelope.JobTriaged {
		t.Fatalf("persisted state = %s, want triaged", persisted.State)
	}
}

func TestTransitionRejectsIllegalAndStaleMoves(t *testing.T) {
	store, cleanup := jobStore(t)
	defer cleanup()

	rec, err := store.Create(Record{Repo: "demo", Source: SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}

	// Not in the legality table at all.
	if _, err := store.Transition(rec.ID, envelope.JobOpen, envelope.JobDone, "x", ""); !errors.Is(err, ErrBadTransition) {
		t.Fatalf("open->done err = %v, want ErrBadTransition", err)
	}
	// Legal shape, but the record is not in the claimed from-state.
	if _, err := store.Transition(rec.ID, envelope.JobTriaged, envelope.JobScheduled, "x", ""); !errors.Is(err, ErrBadTransition) {
		t.Fatalf("stale from-state err = %v, want ErrBadTransition", err)
	}
	// Unknown job.
	if _, err := store.Transition("nope", envelope.JobOpen, envelope.JobTriaged, "x", ""); !errors.Is(err, ErrNoSuchJob) {
		t.Fatalf("missing job err = %v, want ErrNoSuchJob", err)
	}
	// Terminal states have no successors.
	if CanTransition(envelope.JobDone, envelope.JobRunning) ||
		CanTransition(envelope.JobFailed, envelope.JobOpen) ||
		CanTransition(envelope.JobCancelled, envelope.JobOpen) {
		t.Fatal("terminal state has successors")
	}
}

func TestTransitionAppendsEvent(t *testing.T) {
	store, cleanup := jobStore(t)
	defer cleanup()

	rec, err := store.Create(Record{Repo: "demo", Source: SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(rec.ID, envelope.JobOpen, envelope.JobFailed, "coordinator", "bad_plan: not json"); err != nil {
		t.Fatal(err)
	}
	entries, err := store.cli.StreamRead(envelope.StreamJobs, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Create appended one event, Transition the second.
	if len(entries) != 2 {
		t.Fatalf("stream has %d events, want 2", len(entries))
	}
}
