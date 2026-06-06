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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

var (
	ErrBadRecord         = errors.New("ticket: bad record")
	ErrNoSuchTicket      = errors.New("ticket: no such ticket")
	ErrIllegalTransition = errors.New("ticket: illegal transition")
	ErrCASLost           = errors.New("ticket: cas lost")
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

// Event records one ticket transition. The tickets KV record is still the
// current-state authority; events let tests and observers deterministically
// replay how a ticket reached that state.
type Event struct {
	Ticket     string               `json:"ticket"`
	From       envelope.TicketState `json:"from,omitempty"`
	To         envelope.TicketState `json:"to"`
	By         string               `json:"by,omitempty"`
	At         time.Time            `json:"at"`
	AcceptedBy string               `json:"acceptedBy,omitempty"`
	AnsweredBy string               `json:"answeredBy,omitempty"`
	Answer     string               `json:"answer,omitempty"`
	Reason     string               `json:"reason,omitempty"`
}

// Store applies the ticket FSM to the authoritative tickets KV bucket.
type Store struct {
	cli *bus.Client
	now func() time.Time
}

func NewStore(cli *bus.Client) Store {
	return Store{cli: cli, now: func() time.Time { return time.Now().UTC() }}
}

func (s Store) withNow(now func() time.Time) Store {
	s.now = now
	return s
}

func (s Store) Create(rec Record) (Record, error) {
	if s.now == nil {
		s.now = func() time.Time { return time.Now().UTC() }
	}
	if rec.Ticket == "" {
		rec.Ticket = envelope.NewID()
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = s.now()
	}
	if rec.ExpiresAt.IsZero() {
		rec.ExpiresAt = rec.CreatedAt.Add(30 * time.Minute)
	}
	rec.State = envelope.TicketOpen
	if err := validateNew(rec); err != nil {
		return Record{}, err
	}
	if _, err := s.cli.KVPut(envelope.BucketTickets, rec.Ticket, rec, bus.PutOptions{CAS: bus.CreateOnly()}); err != nil {
		if errors.Is(err, bus.ErrCASLost) {
			return Record{}, ErrCASLost
		}
		return Record{}, err
	}
	_ = s.append(Event{Ticket: rec.Ticket, To: envelope.TicketOpen, By: rec.Asker, At: rec.CreatedAt})
	return rec, nil
}

func (s Store) Route(ticket, by, to string) (Record, error) {
	return s.update(ticket, func(rec Record) (Record, Event, error) {
		if rec.State != envelope.TicketOpen {
			return rec, Event{}, illegal(rec.State, envelope.TicketRouted)
		}
		if expired(rec, s.now()) {
			rec.State = envelope.TicketExpired
			return rec, Event{Ticket: rec.Ticket, From: envelope.TicketOpen, To: envelope.TicketExpired, By: by, At: s.now(), Reason: "expired"}, nil
		}
		rec.State = envelope.TicketRouted
		if to != "" {
			rec.To = to
		}
		return rec, Event{Ticket: rec.Ticket, From: envelope.TicketOpen, To: envelope.TicketRouted, By: by, At: s.now()}, nil
	})
}

func (s Store) Accept(ticket, by string) (Record, error) {
	return s.update(ticket, func(rec Record) (Record, Event, error) {
		if rec.AcceptedBy == by && rec.State == envelope.TicketAccepted {
			return rec, Event{}, nil
		}
		if rec.State != envelope.TicketRouted {
			return rec, Event{}, illegal(rec.State, envelope.TicketAccepted)
		}
		if rec.To != "" && rec.To != by {
			return rec, Event{}, fmt.Errorf("%w: ticket routed to %q, not %q", ErrIllegalTransition, rec.To, by)
		}
		if expired(rec, s.now()) {
			rec.State = envelope.TicketExpired
			return rec, Event{Ticket: rec.Ticket, From: envelope.TicketRouted, To: envelope.TicketExpired, By: by, At: s.now(), Reason: "expired"}, nil
		}
		rec.State = envelope.TicketAccepted
		rec.AcceptedBy = by
		if rec.To == "" {
			rec.To = by
		}
		return rec, Event{Ticket: rec.Ticket, From: envelope.TicketRouted, To: envelope.TicketAccepted, By: by, At: s.now(), AcceptedBy: by}, nil
	})
}

func (s Store) Answer(ticket, by, answer string) (Record, error) {
	return s.update(ticket, func(rec Record) (Record, Event, error) {
		if strings.TrimSpace(answer) == "" {
			return rec, Event{}, fmt.Errorf("%w: empty answer", ErrBadRecord)
		}
		if rec.State != envelope.TicketAccepted {
			return rec, Event{}, illegal(rec.State, envelope.TicketAnswered)
		}
		if rec.AcceptedBy != by {
			return rec, Event{}, fmt.Errorf("%w: accepted by %q, not %q", ErrIllegalTransition, rec.AcceptedBy, by)
		}
		if expired(rec, s.now()) {
			rec.State = envelope.TicketExpired
			return rec, Event{Ticket: rec.Ticket, From: envelope.TicketAccepted, To: envelope.TicketExpired, By: by, At: s.now(), Reason: "expired"}, nil
		}
		rec.State = envelope.TicketAnswered
		rec.AnsweredBy = by
		rec.AnsweredAt = s.now()
		rec.Answer = answer
		return rec, Event{Ticket: rec.Ticket, From: envelope.TicketAccepted, To: envelope.TicketAnswered, By: by, At: rec.AnsweredAt, AnsweredBy: by, Answer: answer}, nil
	})
}

func (s Store) Close(ticket, by string) (Record, error) {
	return s.update(ticket, func(rec Record) (Record, Event, error) {
		if rec.State == envelope.TicketClosed {
			return rec, Event{}, nil
		}
		if rec.State != envelope.TicketAnswered {
			return rec, Event{}, illegal(rec.State, envelope.TicketClosed)
		}
		if rec.Asker != by {
			return rec, Event{}, fmt.Errorf("%w: asker is %q, not %q", ErrIllegalTransition, rec.Asker, by)
		}
		rec.State = envelope.TicketClosed
		return rec, Event{Ticket: rec.Ticket, From: envelope.TicketAnswered, To: envelope.TicketClosed, By: by, At: s.now()}, nil
	})
}

func (s Store) Get(ticket string) (Record, bool, error) {
	kv, found, err := s.cli.KVGet(envelope.BucketTickets, ticket)
	if err != nil || !found {
		return Record{}, found, err
	}
	var rec Record
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return Record{}, false, err
	}
	return rec, true, nil
}

func (s Store) ListInbox(agent string, limit int) ([]Record, bool, error) {
	keys, err := s.cli.KVList(envelope.BucketTickets)
	if err != nil {
		return nil, false, err
	}
	records := make([]Record, 0, len(keys))
	for _, kv := range keys {
		var rec Record
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		if rec.AcceptedBy == agent && rec.State == envelope.TicketAccepted {
			records = append(records, rec)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].CreatedAt.Before(records[j].CreatedAt) })
	overflow := limit > 0 && len(records) > limit
	if overflow {
		records = records[:limit]
	}
	return records, overflow, nil
}

func (s Store) update(ticket string, mutate func(Record) (Record, Event, error)) (Record, error) {
	for i := 0; i < 3; i++ {
		kv, found, err := s.cli.KVGet(envelope.BucketTickets, ticket)
		if err != nil {
			return Record{}, err
		}
		if !found {
			return Record{}, ErrNoSuchTicket
		}
		var rec Record
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			return Record{}, err
		}
		next, ev, err := mutate(rec)
		if err != nil {
			return Record{}, err
		}
		if next == rec {
			return next, nil
		}
		if _, err := s.cli.KVPut(envelope.BucketTickets, ticket, next, bus.PutOptions{CAS: bus.Rev(kv.Rev)}); err != nil {
			if errors.Is(err, bus.ErrCASLost) {
				continue
			}
			return Record{}, err
		}
		if ev.Ticket != "" {
			_ = s.append(ev)
		}
		return next, nil
	}
	return Record{}, ErrCASLost
}

func (s Store) append(ev Event) error {
	if ev.At.IsZero() {
		ev.At = s.now()
	}
	_, err := s.cli.StreamAppend(envelope.StreamTickets, ev)
	return err
}

func validateNew(rec Record) error {
	if strings.TrimSpace(rec.Ticket) == "" || strings.TrimSpace(rec.Asker) == "" || strings.TrimSpace(rec.Q) == "" {
		return fmt.Errorf("%w: missing ticket, asker, or question", ErrBadRecord)
	}
	if (rec.Role == "") == (rec.To == "") {
		return fmt.Errorf("%w: exactly one of role or to is required", ErrBadRecord)
	}
	if rec.State != envelope.TicketOpen {
		return fmt.Errorf("%w: new ticket state must be open", ErrBadRecord)
	}
	if !rec.ExpiresAt.After(rec.CreatedAt) {
		return fmt.Errorf("%w: expiresAt must be after createdAt", ErrBadRecord)
	}
	return nil
}

func expired(rec Record, now time.Time) bool {
	return !rec.ExpiresAt.IsZero() && !now.Before(rec.ExpiresAt)
}

func illegal(from, to envelope.TicketState) error {
	return fmt.Errorf("%w: %s -> %s", ErrIllegalTransition, from, to)
}

// Reduce reconstructs current state from an initial record and transition
// events. Events are applied in slice order; callers that replay a stream
// should pass oldest first.
func Reduce(initial Record, events []Event) (Record, error) {
	rec := initial
	for _, ev := range events {
		if ev.Ticket != rec.Ticket {
			return Record{}, fmt.Errorf("%w: event for %q on %q", ErrBadRecord, ev.Ticket, rec.Ticket)
		}
		if ev.From != "" && ev.From != rec.State {
			return Record{}, fmt.Errorf("%w: event from %s while record is %s", ErrIllegalTransition, ev.From, rec.State)
		}
		switch ev.To {
		case envelope.TicketOpen:
			rec.State = envelope.TicketOpen
		case envelope.TicketRouted:
			if rec.State != envelope.TicketOpen {
				return Record{}, illegal(rec.State, ev.To)
			}
			rec.State = ev.To
		case envelope.TicketAccepted:
			if rec.State != envelope.TicketRouted {
				return Record{}, illegal(rec.State, ev.To)
			}
			rec.State = ev.To
			rec.AcceptedBy = ev.AcceptedBy
			if rec.To == "" {
				rec.To = ev.AcceptedBy
			}
		case envelope.TicketAnswered:
			if rec.State != envelope.TicketAccepted {
				return Record{}, illegal(rec.State, ev.To)
			}
			rec.State = ev.To
			rec.AnsweredBy = ev.AnsweredBy
			rec.AnsweredAt = ev.At
			rec.Answer = ev.Answer
		case envelope.TicketClosed, envelope.TicketExpired, envelope.TicketCancelled:
			rec.State = ev.To
		default:
			return Record{}, fmt.Errorf("%w: unknown state %q", ErrBadRecord, ev.To)
		}
	}
	return rec, nil
}
