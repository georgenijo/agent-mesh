package sidecar

import (
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/ticket"
)

// TestHandleIncomingAsk_SelfAskIgnored verifies the self-ask deadlock guard:
// when an agent receives a role-ask it published itself (From == own ID),
// handleIncomingAsk must return immediately without calling ticketStore.Accept
// and without transitioning the ticket to TicketAccepted.
func TestHandleIncomingAsk_SelfAskIgnored(t *testing.T) {
	cfg := fastConfig(t)
	sc := startMesh(t, cfg, "selfasker")

	sc.mu.Lock()
	id := sc.card.ID
	sc.mu.Unlock()

	store := sc.ticketStore()
	now := time.Now().UTC()
	rec, err := store.Create(ticket.Record{
		Asker:     id,
		Role:      "builder",
		Q:         "what is 2+2?",
		CreatedAt: now,
		ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("create ticket: %v", err)
	}
	if _, err := store.Route(rec.Ticket, id, ""); err != nil {
		t.Fatalf("route ticket: %v", err)
	}

	// Build a self-ask envelope: From == own sidecar ID.
	env, err := envelope.New(envelope.KindAsk, id, envelope.SubjectAskRole("builder"), &envelope.AskPayload{
		Ticket: rec.Ticket,
		Role:   "builder",
		Q:      "what is 2+2?",
	})
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}

	sc.handleIncomingAsk(env)

	got, found, err := store.Get(rec.Ticket)
	if err != nil {
		t.Fatalf("get ticket: %v", err)
	}
	if !found {
		t.Fatal("ticket not found after handleIncomingAsk")
	}
	if got.State == envelope.TicketAccepted {
		t.Fatalf("self-ask was accepted: state = %q, want %q (self-accept guard missing)", got.State, envelope.TicketRouted)
	}
	if got.State != envelope.TicketRouted {
		t.Fatalf("ticket state = %q, want %q", got.State, envelope.TicketRouted)
	}
}
