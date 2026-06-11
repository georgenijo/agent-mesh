package coordinator

import (
	"strings"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// auditObserved fans a non-presence lifecycle event tapped on mesh.> into the
// unified audit log (#29). The coordinator is the one component subscribed to
// every subject, which makes it the natural single writer of an audit trail
// that spans claims, asks, answers, tickets, jobs, tasks, triage, worker runs,
// and fleet pauses — so a single ordered read of envelope.StreamAudit can
// reconstruct how any ticket/job/task reached its current state (issue #29
// acceptance: "audit stream can reconstruct a ticket's major lifecycle events").
//
// It is observation only: the domain KV records stay the one authority for
// state, exactly as the mesh.<domain>.* events they mirror are. A malformed or
// uninteresting envelope is skipped, never logged as an error — the audit log
// must degrade, not wedge the reducer's single delivery goroutine.
//
// Gated by cfg.AuditFanout (MESH_AUDIT_FANOUT=off): the presence/claim audits
// emitted at their mutation sites are always on (existing paths depend on
// them); only this bus-observed fan-out is switchable, for test determinism.
func (c *Coordinator) auditObserved(env envelope.Envelope) {
	if !c.cfg.AuditFanout {
		return
	}
	entry, ok := c.auditEntryFor(env)
	if !ok {
		return
	}
	if entry.TS.IsZero() {
		entry.TS = time.Now().UTC()
	}
	if _, err := c.cli.StreamAppend(envelope.StreamAudit, entry); err != nil {
		c.log.Warn("audit fan-out append failed", "kind", env.Kind, "subject", env.Subject, "err", err)
	}
}

// auditEntryFor maps one observed envelope to its audit entry, or reports
// ok=false for events that carry no lifecycle meaning (heartbeats, status,
// announces — fire-and-forget chatter) or that would double-log (the routed
// inbox copy of an ask, which echoes an ask already audited on its origin
// subject). The envelope TS is carried through so the audit ordering matches
// publish ordering.
func (c *Coordinator) auditEntryFor(env envelope.Envelope) (AuditEntry, bool) {
	base := AuditEntry{ID: env.From, TS: env.TS}
	switch env.Kind {
	case envelope.KindClaim:
		// The KindClaim event is the observability tap for a CAS attempt; the
		// coordinator-side reclaim path already audits its own releases, so this
		// records the agent-initiated claim attempts (claimed|lost|error).
		var p envelope.ClaimPayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditClaim
		base.Event = "attempt"
		base.Path, base.Repo, base.Result = p.Path, p.Repo, string(p.Result)
		return base, true

	case envelope.KindAsk:
		// An ask is published twice: once on its origin subject
		// (mesh.ask.role.<role> | mesh.ask.id.<id>) and once re-routed to the
		// responder's mesh.inbox.<id>. Audit only the origin so the trail holds
		// exactly one "ask opened" per ticket.
		if strings.HasPrefix(env.Subject, "mesh.inbox.") {
			return AuditEntry{}, false
		}
		var p envelope.AskPayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditAsk
		base.Event = "opened"
		base.Ticket, base.Role = p.Ticket, p.Role
		if p.To != "" {
			base.Detail = "to " + p.To
		}
		return base, true

	case envelope.KindAnswer:
		var p envelope.AnswerPayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditAnswer
		base.Event = "answered"
		base.Ticket = p.Ticket
		return base, true

	case envelope.KindTicket:
		// The ticket FSM transition tap is the richest ticket signal: routed →
		// accepted → answered → closed (and expired|cancelled). By is the actor
		// that caused the transition; the FSM event's From is the publisher,
		// which may be the coordinator or the responder, so prefer By for ID.
		var p envelope.TicketPayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditTicket
		base.Event = string(p.State)
		base.Ticket, base.State, base.By, base.Detail = p.Ticket, string(p.State), p.By, p.Reason
		return base, true

	case envelope.KindJob:
		var p envelope.JobPayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditJob
		base.Event = string(p.State)
		base.Job, base.Repo, base.State, base.Detail = p.ID, p.Repo, string(p.State), p.Title
		return base, true

	case envelope.KindTask:
		var p envelope.TaskPayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditTask
		base.Event = string(p.State)
		base.Task, base.Job, base.Role, base.State, base.Detail = p.ID, p.Job, p.Role, string(p.State), p.Title
		return base, true

	case envelope.KindTriage:
		var p envelope.TriagePayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditTriage
		base.Event = string(p.Result)
		base.Job, base.Result, base.Detail = p.Job, string(p.Result), p.Reason
		if p.Code != "" {
			base.State = string(p.Code) // failure class, for discrimination without prose
		}
		return base, true

	case envelope.KindWorker:
		var p envelope.WorkerPayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditWorker
		base.Event = string(p.Result)
		base.Task, base.Job, base.Result, base.Detail = p.Task, p.Job, string(p.Result), p.Reason
		if p.Code != "" {
			base.State = string(p.Code)
		}
		return base, true

	case envelope.KindReview:
		// The expert-review verdict over a worker diff (#27/#80): the gate's
		// input. Event = the typed verdict; State carries the error class when
		// the review could not be produced.
		var p envelope.ReviewPayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditReview
		base.Event = string(p.Verdict)
		base.Task, base.Job, base.Result, base.Detail = p.Task, p.Job, string(p.Verdict), p.Notes
		if p.Code != "" {
			base.State = string(p.Code)
		}
		return base, true

	case envelope.KindFleet:
		var p envelope.FleetPayload
		if envelope.DecodeInto(env, &p) != nil {
			return AuditEntry{}, false
		}
		base.Kind = envelope.AuditFleet
		base.Event = string(p.State)
		base.State, base.Detail = string(p.State), p.Reason
		if p.Code != "" {
			base.Result = string(p.Code)
		}
		return base, true

	default:
		// KindAnnounce, KindNote (never published — stream-only),
		// KindReviewRequest (the verdict, not the request, is the audited
		// fact), and any future chatter kind: no audit entry.
		return AuditEntry{}, false
	}
}
