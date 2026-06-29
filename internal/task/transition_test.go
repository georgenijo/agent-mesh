package task

import (
	"errors"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func transitionFixture(t *testing.T) (Store, *bus.Client) {
	t.Helper()
	path := testsock.Path(t, "bus.sock")
	srv := bus.NewServer(path, bus.Options{})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	cli, err := bus.Dial(path, bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cli.Close()
		srv.Stop()
	})
	return NewStore(cli), cli
}

func mintTask(t *testing.T, s Store) Record {
	t.Helper()
	rec := Record{
		ID: envelope.NewID(), Job: envelope.NewID(), Node: "impl",
		Title: "implement", Role: "builder",
		State: envelope.TaskPending, CreatedAt: time.Now().UTC(),
	}
	if err := s.CreateAll([]Record{rec}); err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestCanTransitionTable(t *testing.T) {
	legal := []struct{ from, to envelope.TaskState }{
		{envelope.TaskPending, envelope.TaskRunning},
		{envelope.TaskPending, envelope.TaskFailed},
		{envelope.TaskPending, envelope.TaskCancelled},
		{envelope.TaskRunning, envelope.TaskDone},
		{envelope.TaskRunning, envelope.TaskFailed},
		{envelope.TaskRunning, envelope.TaskCancelled},
		{envelope.TaskRunning, envelope.TaskEscalated},
	}
	for _, tc := range legal {
		if !CanTransition(tc.from, tc.to) {
			t.Errorf("CanTransition(%s, %s) = false, want true", tc.from, tc.to)
		}
	}
	illegal := []struct{ from, to envelope.TaskState }{
		{envelope.TaskPending, envelope.TaskDone},      // never skip running
		{envelope.TaskPending, envelope.TaskEscalated}, // only valid from running
		{envelope.TaskDone, envelope.TaskRunning},
		{envelope.TaskDone, envelope.TaskFailed},
		{envelope.TaskFailed, envelope.TaskRunning},
		{envelope.TaskCancelled, envelope.TaskRunning},
		{envelope.TaskRunning, envelope.TaskPending},
		{envelope.TaskEscalated, envelope.TaskRunning}, // escalated is terminal
	}
	for _, tc := range illegal {
		if CanTransition(tc.from, tc.to) {
			t.Errorf("CanTransition(%s, %s) = true, want false", tc.from, tc.to)
		}
	}
}

func TestEscalateMovesRecordAndRecordsReason(t *testing.T) {
	s, _ := transitionFixture(t)
	rec := mintTask(t, s)

	// Advance to running first.
	if _, err := s.Transition(rec.ID, envelope.TaskPending, envelope.TaskRunning, "coordinator", ""); err != nil {
		t.Fatal(err)
	}

	const reason = "what does 'make it nicer' mean concretely? no acceptance criteria given"
	moved, err := s.Escalate(rec.ID, reason, "coordinator")
	if err != nil {
		t.Fatalf("Escalate: %v", err)
	}
	if moved.State != envelope.TaskEscalated {
		t.Fatalf("state = %s, want escalated", moved.State)
	}
	if moved.EscalationReason != reason {
		t.Fatalf("EscalationReason = %q, want %q", moved.EscalationReason, reason)
	}

	// Persisted record must reflect the escalation.
	got, found, err := s.Get(rec.ID)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.State != envelope.TaskEscalated {
		t.Fatalf("persisted state = %s, want escalated", got.State)
	}
	if got.EscalationReason != reason {
		t.Fatalf("persisted EscalationReason = %q, want %q", got.EscalationReason, reason)
	}
}

func TestEscalateRejectsNonRunningTask(t *testing.T) {
	s, _ := transitionFixture(t)
	rec := mintTask(t, s) // pending

	if _, err := s.Escalate(rec.ID, "ambiguous", "coordinator"); !errors.Is(err, ErrBadTransition) {
		t.Fatalf("Escalate(pending) err = %v, want ErrBadTransition", err)
	}
}

func TestTransitionMovesRecordAndAppendsEvent(t *testing.T) {
	s, cli := transitionFixture(t)
	rec := mintTask(t, s)

	moved, err := s.Transition(rec.ID, envelope.TaskPending, envelope.TaskRunning, "coordinator", "deps satisfied")
	if err != nil {
		t.Fatal(err)
	}
	if moved.State != envelope.TaskRunning {
		t.Fatalf("state = %s, want running", moved.State)
	}
	got, found, err := s.Get(rec.ID)
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if got.State != envelope.TaskRunning {
		t.Fatalf("persisted state = %s, want running", got.State)
	}

	entries, err := cli.StreamRead(envelope.StreamTasks, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 { // TaskPending from CreateAll + the transition
		t.Fatalf("got %d task events, want 2", len(entries))
	}
}

func TestTransitionRejectsIllegalAndStaleMoves(t *testing.T) {
	s, _ := transitionFixture(t)
	rec := mintTask(t, s)

	if _, err := s.Transition(rec.ID, envelope.TaskPending, envelope.TaskDone, "x", ""); !errors.Is(err, ErrBadTransition) {
		t.Fatalf("pending→done err = %v, want ErrBadTransition", err)
	}
	if _, err := s.Transition("no-such-id", envelope.TaskPending, envelope.TaskRunning, "x", ""); !errors.Is(err, ErrNoSuchTask) {
		t.Fatalf("missing task err = %v, want ErrNoSuchTask", err)
	}
	if _, err := s.Transition(rec.ID, envelope.TaskPending, envelope.TaskRunning, "x", ""); err != nil {
		t.Fatal(err)
	}
	// The record is running now: a second pending→running is a stale from-state.
	if _, err := s.Transition(rec.ID, envelope.TaskPending, envelope.TaskRunning, "x", ""); !errors.Is(err, ErrBadTransition) {
		t.Fatalf("stale from-state err = %v, want ErrBadTransition", err)
	}
}
