package sidecar

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
	"github.com/georgenijo/agent-mesh/internal/socket"
	"github.com/georgenijo/agent-mesh/internal/ticket"
)

const defaultAskTTL = 30 * time.Minute
const maxClaimLosses = 32

func (s *Sidecar) refreshAskSubscriptions() error {
	s.mu.Lock()
	id, role, joined := s.card.ID, s.card.Role, s.joined
	current := s.askSubs
	want := map[string]bool{}
	if joined {
		want[envelope.SubjectAskID(id)] = true
		if role != "" {
			want[envelope.SubjectAskRole(role)] = true
		}
	}
	for pattern, sub := range current {
		if !want[pattern] {
			delete(current, pattern)
			go sub.Unsubscribe()
		}
	}
	var add []string
	for pattern := range want {
		if current[pattern] == nil {
			add = append(add, pattern)
		}
	}
	s.mu.Unlock()

	for _, pattern := range add {
		p := pattern
		sub, err := s.bus.Subscribe(p, func(env envelope.Envelope) { s.handleIncomingAsk(env) })
		if err != nil {
			return err
		}
		s.mu.Lock()
		if s.askSubs[p] == nil {
			s.askSubs[p] = sub
			sub = nil
		}
		s.mu.Unlock()
		if sub != nil {
			sub.Unsubscribe()
		}
	}
	return nil
}

func (s *Sidecar) handleIncomingAsk(env envelope.Envelope) {
	if env.Kind != envelope.KindAsk {
		return
	}
	var p envelope.AskPayload
	if envelope.DecodeInto(env, &p) != nil {
		return
	}
	id, joined := s.joinedID()
	if !joined {
		return
	}
	rec, err := s.ticketStore().Accept(p.Ticket, id)
	if err != nil {
		return
	}
	if rec.State != envelope.TicketAccepted || rec.AcceptedBy != id {
		return
	}
	s.publishTicket(rec.Ticket, envelope.TicketAccepted, id, "")
	inboxEnv, err := envelope.New(envelope.KindAsk, env.From, envelope.SubjectInbox(id), &p)
	if err != nil {
		return
	}
	inboxEnv.To = id
	_ = s.bus.Publish(inboxEnv)
}

func (s *Sidecar) handleAsk(req socket.Request) socket.Response {
	var args meshapi.AskArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("ask args: %v", err))
	}
	args.Question = strings.TrimSpace(args.Question)
	if args.Question == "" {
		return socket.Fail(socket.CodeBadRequest, "empty question")
	}
	if len(args.Question) > meshapi.MaxQuestionLen {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("question %d bytes exceeds limit %d", len(args.Question), meshapi.MaxQuestionLen))
	}
	if (args.Role == "") == (args.To == "") {
		return socket.Fail(socket.CodeBadRequest, "exactly one of --role or --to is required")
	}
	if args.Role != "" && !agentcard.ValidName(args.Role) {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("invalid role %q", args.Role))
	}
	if args.To != "" && !agentcard.ValidName(args.To) {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("invalid target %q", args.To))
	}
	id, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	ttl := defaultAskTTL
	if args.TTL != "" {
		parsed, err := time.ParseDuration(args.TTL)
		if err != nil || parsed <= 0 {
			return socket.Fail(socket.CodeBadRequest, "invalid --ttl duration")
		}
		ttl = parsed
	}
	if err := s.ensureResponder(args.Role, args.To); err != nil {
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}

	store := s.ticketStore()
	now := time.Now().UTC()
	rec, err := store.Create(ticket.Record{
		Asker: id, Role: args.Role, To: args.To, Q: args.Question, Ctx: args.Context,
		CreatedAt: now, ExpiresAt: now.Add(ttl),
	})
	if err != nil {
		return socket.Fail(socket.CodeUnavailable, err.Error())
	}
	routed, err := store.Route(rec.Ticket, id, args.To)
	if err != nil {
		return socket.Fail(socket.CodeUnavailable, err.Error())
	}
	s.publishTicket(rec.Ticket, envelope.TicketOpen, id, "")
	s.publishTicket(rec.Ticket, envelope.TicketRouted, id, "")

	subject := envelope.SubjectAskID(args.To)
	if args.Role != "" {
		subject = envelope.SubjectAskRole(args.Role)
	}
	env, err := envelope.New(envelope.KindAsk, id, subject, &envelope.AskPayload{
		Ticket: rec.Ticket, Role: args.Role, To: args.To, Q: args.Question, Ctx: args.Context,
	})
	if err != nil {
		return socket.Fail(socket.CodeInternal, err.Error())
	}
	env.To = args.To
	if err := s.bus.Publish(env); err != nil {
		return socket.Fail(socket.CodeUnavailable, fmt.Sprintf("bus publish: %v", err))
	}

	return socket.OKData(meshapi.AskVerbResult{
		Ticket: rec.Ticket, Result: envelope.AskPending, State: routed.State,
		Role: routed.Role, To: routed.To, ExpiresAt: routed.ExpiresAt,
	})
}

func (s *Sidecar) handlePoll(req socket.Request) socket.Response {
	var args meshapi.PollArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("poll args: %v", err))
	}
	id, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	rec, found, err := s.ticketStore().Get(args.Ticket)
	if err != nil {
		return socket.Fail(socket.CodeUnavailable, err.Error())
	}
	if !found {
		return socket.OKData(meshapi.PollResult{Ticket: args.Ticket, Result: envelope.AskNoSuchTicket})
	}
	if rec.Asker != id {
		return socket.Fail(socket.CodeBadRequest, "ticket belongs to a different asker")
	}
	switch rec.State {
	case envelope.TicketAnswered:
		closed, err := s.ticketStore().Close(rec.Ticket, id)
		if err == nil {
			rec = closed
			s.publishTicket(rec.Ticket, envelope.TicketClosed, id, "")
		}
		return socket.OKData(pollAnswered(rec))
	case envelope.TicketClosed:
		return socket.OKData(pollAnswered(rec))
	case envelope.TicketExpired:
		return socket.OKData(meshapi.PollResult{Ticket: rec.Ticket, Result: envelope.AskExpired, State: rec.State})
	default:
		return socket.OKData(meshapi.PollResult{Ticket: rec.Ticket, Result: envelope.AskPending, State: rec.State})
	}
}

func (s *Sidecar) handleInbox(req socket.Request) socket.Response {
	var args meshapi.InboxArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("inbox args: %v", err))
	}
	id, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	limit := args.Limit
	if limit <= 0 {
		limit = meshapi.DefaultInboxLimit
	}
	records, overflow, err := s.ticketStore().ListInbox(id, limit)
	if err != nil {
		return socket.Fail(socket.CodeUnavailable, err.Error())
	}
	items := make([]meshapi.InboxItem, 0, len(records))
	for _, rec := range records {
		items = append(items, meshapi.InboxItem{
			Ticket: rec.Ticket, From: rec.Asker, Role: rec.Role, To: rec.To,
			Question: rec.Q, Context: rec.Ctx, CreatedAt: rec.CreatedAt, ExpiresAt: rec.ExpiresAt,
		})
	}
	return socket.OKData(meshapi.InboxResult{Items: items, Overflow: overflow})
}

func (s *Sidecar) handleAnswer(req socket.Request) socket.Response {
	var args meshapi.AnswerArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("answer args: %v", err))
	}
	args.Answer = strings.TrimSpace(args.Answer)
	if args.Ticket == "" || args.Answer == "" {
		return socket.Fail(socket.CodeBadRequest, "ticket and answer are required")
	}
	if len(args.Answer) > meshapi.MaxAnswerLen {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("answer %d bytes exceeds limit %d", len(args.Answer), meshapi.MaxAnswerLen))
	}
	id, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	rec, err := s.recordAndPublishAnswer(id, args.Ticket, args.Answer)
	if err != nil {
		if errors.Is(err, ticket.ErrNoSuchTicket) {
			return socket.OKData(meshapi.AnswerVerbResult{Ticket: args.Ticket, Result: envelope.AskNoSuchTicket})
		}
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}
	return socket.OKData(meshapi.AnswerVerbResult{Ticket: rec.Ticket, Result: envelope.AskAnswered, State: rec.State})
}

// recordAndPublishAnswer commits an answer to the tickets KV (the one
// authority) and publishes the derived KindAnswer envelope back to the asker's
// sidecar. It is the single answer path shared by the `answer` verb and the
// expert responder loop (expert.go): a typed CAS-guarded transition, never a
// coordinator-mediated answer payload. ticket.Store.Answer enforces
// AcceptedBy==id, so an agent can only answer what it accepted.
func (s *Sidecar) recordAndPublishAnswer(id, ticketID, answer string) (ticket.Record, error) {
	rec, err := s.ticketStore().Answer(ticketID, id, answer)
	if err != nil {
		return ticket.Record{}, err
	}
	s.publishTicket(rec.Ticket, envelope.TicketAnswered, id, "")
	env, err := envelope.New(envelope.KindAnswer, id, envelope.SubjectAnswer(rec.Ticket),
		&envelope.AnswerPayload{Ticket: rec.Ticket, Answer: rec.Answer})
	if err == nil {
		env.To = rec.Asker
		_ = s.bus.Publish(env)
	}
	return rec, nil
}

func pollAnswered(rec ticket.Record) meshapi.PollResult {
	return meshapi.PollResult{
		Ticket: rec.Ticket, Result: envelope.AskAnswered, State: rec.State,
		Answer: rec.Answer, AnsweredBy: rec.AnsweredBy, AnsweredAt: rec.AnsweredAt,
	}
}

func (s *Sidecar) ensureResponder(role, to string) error {
	keys, err := s.bus.KVList(envelope.BucketRegistry)
	if err != nil {
		return err
	}
	for _, kv := range keys {
		var rec agentcard.RegistryRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		if rec.State != agentcard.PresenceLive {
			continue
		}
		if to != "" && rec.Card.ID == to {
			return nil
		}
		if role != "" && rec.Card.Role == role {
			return nil
		}
	}
	if to != "" {
		return fmt.Errorf("no live agent %q", to)
	}
	return fmt.Errorf("no live agent with role %q", role)
}

func (s *Sidecar) publishTicket(ticketID string, state envelope.TicketState, by, reason string) {
	env, err := envelope.New(envelope.KindTicket, by, envelope.SubjectTicket(ticketID),
		&envelope.TicketPayload{Ticket: ticketID, State: state, By: by, Reason: reason})
	if err == nil {
		_ = s.bus.Publish(env)
	}
}

func (s *Sidecar) recordClaimLoss(loss meshapi.ClaimLoss) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimLosses = append(s.claimLosses, loss)
	if over := len(s.claimLosses) - maxClaimLosses; over > 0 {
		copy(s.claimLosses, s.claimLosses[over:])
		s.claimLosses = s.claimLosses[:maxClaimLosses]
	}
}
