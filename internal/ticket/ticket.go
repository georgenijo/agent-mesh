// Package ticket owns the ask-ticket record — the KV shape behind the async
// ask/answer flow (P2). Record shapes live in their domain package as the
// single authority (the same role agentcard.RegistryRecord plays for presence
// and claim.Record plays for claims); the envelope package owns the rest of
// the ticket vocabulary: KindTicket, TicketPayload, TicketState, the
// mesh.ask/answer/inbox/ticket subjects, and BucketTickets.
//
// The FSM — the legal-transition table and reducer over these records — is
// #17's; only the record shape is pinned here so #17–#22 build against one
// seam.
package ticket

import (
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// Record is the authoritative tickets-bucket entry, keyed by ticket id in
// envelope.BucketTickets. Ticket ids are envelope.NewID() UUIDv7s, so they
// are valid mesh.answer.<ticket> subject tokens. The KV record is the one
// authority for ticket state; mesh.ticket.<ticket> envelopes (KindTicket)
// are derived observability events.
//
// Exactly one of Role and To is set at creation: a role-addressed ask is
// routed by the coordinator, a direct ask goes straight to that agent's
// inbox. AcceptedBy/AnsweredBy/AnsweredAt/Answer fill in as the ticket
// advances; they are empty until the corresponding transition.
type Record struct {
	Ticket     string               `json:"ticket"`
	State      envelope.TicketState `json:"state"`
	Asker      string               `json:"asker"`
	Role       string               `json:"role,omitempty"`
	To         string               `json:"to,omitempty"`
	Q          string               `json:"q"`
	Ctx        string               `json:"ctx,omitempty"`
	CreatedAt  time.Time            `json:"createdAt"`
	ExpiresAt  time.Time            `json:"expiresAt"`
	AcceptedBy string               `json:"acceptedBy,omitempty"`
	AnsweredBy string               `json:"answeredBy,omitempty"`
	AnsweredAt time.Time            `json:"answeredAt,omitzero"`
	Answer     string               `json:"answer,omitempty"`
}
