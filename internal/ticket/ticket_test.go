package ticket

import (
	"errors"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func ticketStore(t *testing.T) (Store, func()) {
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
	base := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	return NewStore(cli).withNow(func() time.Time { return base }), func() {
		cli.Close()
		srv.Stop()
	}
}

func TestFSMHappyPath(t *testing.T) {
	store, cleanup := ticketStore(t)
	defer cleanup()

	rec, err := store.Create(Record{Asker: "asker", Role: "auth", Q: "question"})
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != envelope.TicketOpen {
		t.Fatalf("create state = %s", rec.State)
	}
	rec, err = store.Route(rec.Ticket, "asker", "")
	if err != nil {
		t.Fatal(err)
	}
	rec, err = store.Accept(rec.Ticket, "expert")
	if err != nil {
		t.Fatal(err)
	}
	if rec.AcceptedBy != "expert" || rec.To != "expert" {
		t.Fatalf("accepted rec = %+v", rec)
	}
	rec, err = store.Answer(rec.Ticket, "expert", "answer")
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != envelope.TicketAnswered || rec.Answer != "answer" {
		t.Fatalf("answered rec = %+v", rec)
	}
	rec, err = store.Close(rec.Ticket, "asker")
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != envelope.TicketClosed {
		t.Fatalf("close state = %s", rec.State)
	}
}

func TestIllegalTransitions(t *testing.T) {
	store, cleanup := ticketStore(t)
	defer cleanup()

	rec, err := store.Create(Record{Asker: "asker", To: "expert", Q: "question"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Answer(rec.Ticket, "expert", "too soon"); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("answer from open err = %v, want illegal", err)
	}
	if _, err := store.Accept(rec.Ticket, "expert"); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("accept from open err = %v, want illegal", err)
	}
	rec, err = store.Route(rec.Ticket, "asker", "expert")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Accept(rec.Ticket, "other"); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("wrong acceptor err = %v, want illegal", err)
	}
	rec, err = store.Accept(rec.Ticket, "expert")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Answer(rec.Ticket, "other", "answer"); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("wrong answerer err = %v, want illegal", err)
	}
}

func TestExpiredCannotBeAcceptedOrAnswered(t *testing.T) {
	store, cleanup := ticketStore(t)
	defer cleanup()
	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	store = store.withNow(func() time.Time { return now })

	rec, err := store.Create(Record{Asker: "asker", Role: "auth", Q: "question", CreatedAt: now, ExpiresAt: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	rec, err = store.Route(rec.Ticket, "asker", "")
	if err != nil {
		t.Fatal(err)
	}
	store = store.withNow(func() time.Time { return now.Add(2 * time.Second) })
	rec, err = store.Accept(rec.Ticket, "expert")
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != envelope.TicketExpired {
		t.Fatalf("accept expired state = %s", rec.State)
	}
	if _, err := store.Answer(rec.Ticket, "expert", "answer"); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("answer expired err = %v, want illegal", err)
	}
}

func TestReduceReconstructsState(t *testing.T) {
	initial := Record{
		Ticket: "T1", State: envelope.TicketOpen, Asker: "asker", Role: "auth", Q: "question",
		CreatedAt: time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC),
		ExpiresAt: time.Date(2026, 6, 6, 12, 30, 0, 0, time.UTC),
	}
	events := []Event{
		{Ticket: "T1", From: envelope.TicketOpen, To: envelope.TicketRouted},
		{Ticket: "T1", From: envelope.TicketRouted, To: envelope.TicketAccepted, AcceptedBy: "expert"},
		{Ticket: "T1", From: envelope.TicketAccepted, To: envelope.TicketAnswered, AnsweredBy: "expert", Answer: "answer", At: initial.CreatedAt.Add(time.Minute)},
	}
	rec, err := Reduce(initial, events)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != envelope.TicketAnswered || rec.AcceptedBy != "expert" || rec.Answer != "answer" {
		t.Fatalf("reduced rec = %+v", rec)
	}
}
