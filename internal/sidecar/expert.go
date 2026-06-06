package sidecar

import (
	"context"
	"errors"
	"time"

	"github.com/georgenijo/agent-mesh/internal/meshapi"
	"github.com/georgenijo/agent-mesh/internal/ticket"
)

// ExpertResult is the typed outcome of one expert turn. OK is true only when a
// real answer was produced — never fake-success. A turn that was lost (child
// death) or that the runtime flagged non-success leaves OK false and the
// ticket unanswered.
type ExpertResult struct {
	Answer string
	OK     bool
}

// ExpertFunc answers one accepted ask. It is the seam that keeps the runtime
// boundary (internal/runtime) out of this package: cmd/meshd implements it over
// a runtime.Proxy, tests implement it with an in-process fake. contextText is
// the ticket's optional ctx; question is the ask body.
type ExpertFunc func(ctx context.Context, question, contextText string) (ExpertResult, error)

// ServeExpert is the responder loop: poll this agent's accepted inbox, answer
// each ticket through fn, and record real answers in the tickets KV (the one
// authority) via the same path the `answer` verb uses. It blocks until ctx is
// cancelled or the sidecar stops.
//
// The expert is an ordinary role-owning sidecar — its ask subscription already
// CAS-accepts role-routed tickets into its own inbox (handleIncomingAsk), so
// this loop only drains what is already accepted. Tickets are processed strictly
// sequentially: the runtime proxy is single-in-flight, and one accepted ticket
// at a time keeps the turn ledger unambiguous.
//
// A ticket that errors or returns a non-success result is recorded in a
// per-agent skip set so the loop does not re-ask the runtime for it every tick
// (a poison ticket would otherwise hot-loop). Such tickets stay accepted until
// their TTL expires — honest: no fake answer is ever written.
func (s *Sidecar) ServeExpert(ctx context.Context, fn ExpertFunc, poll time.Duration) error {
	if poll <= 0 {
		poll = time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	skip := make(map[string]bool)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stop:
			return nil
		case <-ticker.C:
			s.drainInbox(ctx, fn, skip)
		}
	}
}

// drainInbox answers every currently-accepted, not-yet-skipped ticket once.
func (s *Sidecar) drainInbox(ctx context.Context, fn ExpertFunc, skip map[string]bool) {
	id, joined := s.joinedID()
	if !joined {
		return
	}
	records, _, err := s.ticketStore().ListInbox(id, meshapi.DefaultInboxLimit)
	if err != nil {
		s.log.Debug("expert: list inbox failed", "err", err)
		return
	}
	for _, rec := range records {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if skip[rec.Ticket] {
			continue
		}
		res, err := fn(ctx, rec.Q, rec.Ctx)
		if err != nil {
			// ctx cancellation is shutdown, not a poison ticket — let the next
			// run retry once a fresh runtime is up.
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("expert: runtime turn failed", "ticket", rec.Ticket, "err", err)
			skip[rec.Ticket] = true
			continue
		}
		if !res.OK {
			s.log.Warn("expert: runtime returned no answer", "ticket", rec.Ticket)
			skip[rec.Ticket] = true
			continue
		}
		if _, err := s.recordAndPublishAnswer(id, rec.Ticket, res.Answer); err != nil {
			// A lost CAS race (another transition won) or an already-answered
			// ticket is benign — drop it from future attempts.
			if errors.Is(err, ticket.ErrNoSuchTicket) || errors.Is(err, ticket.ErrIllegalTransition) {
				skip[rec.Ticket] = true
				continue
			}
			s.log.Warn("expert: record answer failed", "ticket", rec.Ticket, "err", err)
		}
	}
}
