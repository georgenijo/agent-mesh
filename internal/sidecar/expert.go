package sidecar

import (
	"context"
	"errors"
	"sync"
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

// PrimerFunc injects a memory primer (the compacted blackboard, see memory.go)
// into the expert's warm runtime child as one user message, so a (re)started
// expert answers project questions from durable recorded decisions rather than
// from an empty session. cmd/meshd implements it over the runtime.Proxy; tests
// implement it in-process. A nil PrimerFunc disables priming (the loop then
// behaves exactly as before #28).
type PrimerFunc func(ctx context.Context, primer string) error

// ExpertOptions configure the responder loop's memory behavior (#28). The zero
// value is the pre-#28 loop: no priming, no re-sync.
type ExpertOptions struct {
	// Repo is the blackboard the expert rehydrates from. Empty falls back to the
	// agent card's repo, then envelope.DefaultRepo — the same resolution the
	// note/context verbs use.
	Repo string
	// Prime injects a memory primer into the runtime child. Nil disables priming.
	Prime PrimerFunc
	// PrimerBudget bounds the injected primer in bytes (0 = DefaultPrimerBudget).
	PrimerBudget int
	// Resync, if non-nil, lets the runtime layer (cmd/meshd) signal that the
	// warm child was rebuilt out-of-band — e.g. after a runtime Restart
	// (--resume), whose on-disk session may be cold or stale relative to the
	// blackboard. When it is signalled, the loop re-primes from the durable
	// record on its next tick. It is created with NewResyncSignal.
	Resync *ResyncSignal
	// IdleTTL is the idle reaper window (#105): when > 0, the expert exits
	// cleanly and deregisters if no ask or review has been handled for this
	// long. 0 disables the reaper (the expert runs until context cancellation
	// or a signal). Config knob: MESH_EXPERT_IDLE_TTL.
	IdleTTL time.Duration
}

// ResyncSignal is a concurrency-safe one-shot flag the runtime layer raises to
// ask the responder loop to re-prime from the blackboard. It decouples the
// per-ask runtime closure (which detects a restart) from the loop goroutine
// (which owns priming state), without sharing the loop's internal expertMemory.
type ResyncSignal struct {
	mu      sync.Mutex
	pending bool
}

// NewResyncSignal returns a ready signal.
func NewResyncSignal() *ResyncSignal { return &ResyncSignal{} }

// Request raises the flag. Safe to call from any goroutine; idempotent until
// the loop consumes it.
func (r *ResyncSignal) Request() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.pending = true
	r.mu.Unlock()
}

// take atomically reads and clears the flag.
func (r *ResyncSignal) take() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.pending
	r.pending = false
	return p
}

// ServeExpert is the responder loop without blackboard memory (pre-#28
// behavior). It is retained for callers and tests that do not exercise expert
// memory; ServeExpertWithMemory is the production entry point.
func (s *Sidecar) ServeExpert(ctx context.Context, fn ExpertFunc, poll time.Duration) error {
	return s.ServeExpertWithMemory(ctx, fn, poll, ExpertOptions{})
}

// ServeExpertWithMemory is the responder loop: poll this agent's accepted
// inbox, answer each ticket through fn, and record real answers in the tickets
// KV (the one authority) via the same path the `answer` verb uses. It blocks
// until ctx is cancelled or the sidecar stops.
//
// The expert is an ordinary role-owning sidecar — its ask subscription already
// CAS-accepts role-routed tickets into its own inbox (handleIncomingAsk), so
// this loop only drains what is already accepted. Tickets are processed strictly
// sequentially: the runtime proxy is single-in-flight, and one accepted ticket
// at a time keeps the turn ledger unambiguous.
//
// Memory (#28): when opts.Prime is set, the expert rehydrates from the durable
// blackboard. Before answering its FIRST ticket after (re)start it injects a
// compacted memory primer (the recorded project decisions) into the warm
// runtime child, so a cold or --resume-less restart recovers prior decisions
// with no manual reload. On each tick it then checks the blackboard high-water
// seq and re-primes when new notes have landed — the in-mesh re-sync signal
// (a worker recording a decision after landing a diff). Priming is best-effort:
// a failed primer never blocks or fakes an answer, and the loop retries on the
// next tick (priming did not advance the remembered high-water).
//
// A ticket that errors or returns a non-success result is recorded in a
// per-agent skip set so the loop does not re-ask the runtime for it every tick
// (a poison ticket would otherwise hot-loop). Such tickets stay accepted until
// their TTL expires — honest: no fake answer is ever written.
func (s *Sidecar) ServeExpertWithMemory(ctx context.Context, fn ExpertFunc, poll time.Duration, opts ExpertOptions) error {
	if poll <= 0 {
		poll = time.Second
	}
	repo := opts.Repo
	if repo == "" {
		if r, err := s.resolveRepo(""); err == nil {
			repo = r
		}
	}

	// Seed the idle clock so the reaper measures from serve-start, not from
	// the zero Unix epoch (which would fire immediately on the first tick).
	s.expertLastActivity.Store(time.Now().UnixNano())

	mem := &expertMemory{opts: opts, repo: repo, primed: false}
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
			s.syncMemory(ctx, mem)
			s.drainInbox(ctx, fn, skip)
			if opts.IdleTTL > 0 {
				lastNano := s.expertLastActivity.Load()
				if time.Since(time.Unix(0, lastNano)) > opts.IdleTTL {
					s.log.Info("expert: idle TTL exceeded, leaving mesh", "ttl", opts.IdleTTL)
					s.Leave("expert idle timeout")
					// Close Done() so cmd/meshd's main goroutine unblocks and
					// can clean up the runtime proxy (same path as the leave verb).
					s.doneOnce.Do(func() { close(s.done) })
					return nil
				}
			}
		}
	}
}

// expertMemory tracks the responder loop's rehydration state across ticks: the
// blackboard the expert reads, whether it has primed since the last (re)start,
// and the highest note seq already reflected in the warm child. It lives only
// for the lifetime of one ServeExpertWithMemory call.
type expertMemory struct {
	opts    ExpertOptions
	repo    string
	primed  bool   // true once an initial primer was successfully injected
	highSeq uint64 // last blackboard high-water reflected in the runtime child
}

// syncMemory primes (first run) or re-primes (new notes landed, or an
// out-of-band Resync was requested) the runtime child from the blackboard. It is
// best-effort: any failure leaves the loop unprimed/under-synced so the next
// tick retries, and never produces a fake answer. A no-op when priming is
// disabled (opts.Prime == nil).
func (s *Sidecar) syncMemory(ctx context.Context, mem *expertMemory) {
	if mem.opts.Prime == nil {
		return
	}
	// A runtime restart (--resume) rebuilds the child from disk, which may be
	// cold or stale relative to the blackboard. Consume any pending signal and
	// force a re-prime regardless of the remembered high-water.
	if mem.opts.Resync.take() {
		mem.primed = false
		mem.highSeq = 0
	}
	primer, err := s.BuildMemoryPrimer(mem.repo, mem.opts.PrimerBudget)
	if err != nil {
		s.log.Debug("expert: read blackboard for priming failed", "repo", mem.repo, "err", err)
		return
	}
	// Nothing to inject yet (empty blackboard), or already in sync with the
	// latest note: skip. The first prime always runs once primer.Text is
	// non-empty, even if highSeq has not moved (the warm child may be cold).
	if primer.Text == "" {
		return
	}
	if mem.primed && primer.HighWater <= mem.highSeq {
		return
	}
	if err := mem.opts.Prime(ctx, primer.Text); err != nil {
		if ctx.Err() == nil {
			s.log.Warn("expert: inject memory primer failed", "repo", mem.repo, "err", err)
		}
		return // leave mem.primed/highSeq unchanged so the next tick retries
	}
	mem.primed = true
	mem.highSeq = primer.HighWater
	s.log.Info("expert: rehydrated from blackboard",
		"repo", mem.repo, "notes", primer.Included, "total", primer.Total, "highSeq", primer.HighWater)
}

// touchExpertActivity records the current time as the last expert activity for
// the idle reaper (#105). Safe to call from any goroutine.
func (s *Sidecar) touchExpertActivity() {
	s.expertLastActivity.Store(time.Now().UnixNano())
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
		// Any non-skipped ticket we attempt counts as activity for the idle reaper.
		s.touchExpertActivity()
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
