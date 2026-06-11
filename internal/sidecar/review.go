package sidecar

import (
	"context"
	"errors"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// Usage errors for the Review entry point — distinct from a review the expert
// actually produced (which always lands as a typed verdict).
var (
	errNilReviewFunc = errors.New("sidecar: nil ReviewFunc")
	errReviewNoTask  = errors.New("sidecar: review request missing task id")
)

// Expert review capability (#27): an expert reviews a worker diff and produces
// a TYPED verdict (approve|request_changes|reject|error) through the SAME
// resident runtime child it answers asks with. This file owns the sidecar-side
// contract — the ReviewFunc seam (so internal/runtime stays out of this
// package, exactly like ExpertFunc), the typed ReviewResult, and the path that
// records the verdict as a mesh.review.<task> observability event.
//
// This is the review CAPABILITY only. The scheduler→expert review-GATING
// integration (auto-routing a worker's committed diff to the owning expert and
// blocking the task on the verdict) is a deliberate non-goal here to avoid
// colliding with the coordinator/scheduler lane — it is a tracked follow-up.
// Authority discipline matches the answer path: the verdict event is an
// observability tap (like mesh.worker.<task>), never a second authority over
// any KV record.

// ReviewRequest is the worker diff handed to an expert for review. It mirrors
// the diff metadata #26 commits onto the worker branch (base/head SHAs,
// changed files, branch) plus the diff text and the task's acceptance context
// so the expert judges the diff against intent. Task is required (it keys the
// review event); the rest is best-effort context.
type ReviewRequest struct {
	Task         string   // the task whose diff is under review (keys mesh.review.<task>)
	Job          string   // the owning job, for the event (optional)
	Instruction  string   // what the diff was meant to accomplish (acceptance context)
	Diff         string   // the unified diff / patch text
	ChangedFiles []string // files the worker touched
	BaseSHA      string   // the worktree base the diff was cut from
	HeadSHA      string   // the committed head of the worker branch
	Branch       string   // the worker branch (mesh/worker/<task>)
}

// ReviewResult is the typed outcome of one expert review. Verdict is always a
// valid envelope.ReviewVerdict — a clean judgement (approve|request_changes|
// reject) or ReviewError when the review could not be produced (Code says why).
// Never fake-success: a runtime turn that was lost, errored, or returned no
// parseable verdict yields ReviewError with VerdictNone's wire form, never a
// silent approve. SessionID/NumTurns come from the runtime turn so a caller can
// prove which resident session the review ran under.
type ReviewResult struct {
	Verdict   envelope.ReviewVerdict
	Code      envelope.ReviewErrorCode // set iff Verdict == ReviewError
	Notes     string
	SessionID string
	NumTurns  int
	// CostUSD is the review turn's reported total_cost_usd (#80): a review is
	// an expert LLM turn and its cost travels on the verdict event so the
	// scheduler's budget meter accounts it like a worker run.
	CostUSD float64
}

// ReviewFunc reviews one worker diff through the resident runtime child and
// returns a typed verdict. It is the seam that keeps internal/runtime out of
// this package (cmd/meshd implements it over a runtime.Proxy.Review; tests
// implement it in-process), mirroring ExpertFunc for the answer path.
//
// The contract: a ReviewFunc NEVER returns a Go error for a diff it judged —
// the verdict (including ReviewError + Code) carries the whole outcome, so the
// caller has one typed thing to record. cmd/meshd's implementation maps the
// runtime's typed errors (ProcessExited → runtime_lost, ResultError →
// runtime_error, ErrNoVerdict → bad_verdict, ErrEmptyReview → empty_diff) onto
// the verdict, attempting a best-effort --resume restart on child death exactly
// like the answer path. A returned Go error is reserved for a true internal
// fault the caller cannot classify.
type ReviewFunc func(ctx context.Context, req ReviewRequest) (ReviewResult, error)

// Review drives one worker-diff review through fn and publishes the typed
// verdict as a mesh.review.<task> event (the observability tap). It returns the
// typed result to the caller as well. The published event is best-effort: a
// publish failure is logged, never fatal, and never changes the verdict — the
// verdict is the contract, the event is the tap.
//
// Review is the expert-side capability entry point. It does not poll an inbox
// (review requests are not asks); a caller hands it a concrete diff. A nil fn
// or an empty Task is a usage error surfaced as a Go error, distinct from a
// review the expert produced.
func (s *Sidecar) Review(ctx context.Context, fn ReviewFunc, req ReviewRequest) (ReviewResult, error) {
	if fn == nil {
		return ReviewResult{}, errNilReviewFunc
	}
	if req.Task == "" {
		return ReviewResult{}, errReviewNoTask
	}

	res, err := fn(ctx, req)
	if err != nil {
		// A true internal fault (not a classified verdict). Record it as a typed
		// error verdict so the tap still sees a result, then surface the error.
		res = ReviewResult{Verdict: envelope.ReviewError, Code: envelope.ReviewInternal, Notes: err.Error()}
		s.publishReview(req, res)
		return res, err
	}

	// Defense in depth: a ReviewFunc must hand back a valid verdict. An
	// unset/invalid verdict is treated as an error result, never coerced to
	// approve (never-fake-success).
	if !envelope.ValidReviewVerdict(res.Verdict) {
		res = ReviewResult{Verdict: envelope.ReviewError, Code: envelope.ReviewBadVerdict,
			Notes: res.Notes, SessionID: res.SessionID, NumTurns: res.NumTurns}
	}
	if res.Verdict == envelope.ReviewError && !envelope.ValidReviewErrorCode(res.Code) {
		res.Code = envelope.ReviewBadVerdict
	}
	if res.Verdict != envelope.ReviewError {
		res.Code = "" // a decided verdict never carries an error code
	}

	s.publishReview(req, res)
	return res, nil
}

// ServeReviews subscribes this expert to its role's review-request subject
// (mesh.review-req.<role>, #80) and serves each inbound request through fn via
// Review — which publishes the typed verdict as the mesh.review.<task> event
// the review-gating scheduler awaits. Non-blocking: it returns once the
// subscription is up; the subscription is dropped when ctx is cancelled or the
// sidecar stops. Requests are handled inline on the subscription's delivery
// goroutine — reviews are rare and the runtime proxy serializes turns anyway,
// so queueing would only add state.
//
// This is the inbound transport for the #27 review capability: requests are
// NOT asks (no ticket, no inbox) — the request envelope is fire-and-forget and
// the verdict event is the reply, correlated by task id.
func (s *Sidecar) ServeReviews(ctx context.Context, fn ReviewFunc) error {
	if fn == nil {
		return errNilReviewFunc
	}
	subject := envelope.SubjectReviewRequest(s.card.Role)
	sub, err := s.bus.Subscribe(subject, func(env envelope.Envelope) {
		if ctx.Err() != nil {
			return
		}
		var p envelope.ReviewRequestPayload
		if err := envelope.DecodeInto(env, &p); err != nil {
			s.log.Warn("expert: drop malformed review request", "subject", env.Subject, "err", err)
			return
		}
		req := ReviewRequest{
			Task:         p.Task,
			Job:          p.Job,
			Instruction:  p.Instruction,
			Diff:         p.Diff,
			ChangedFiles: p.ChangedFiles,
			BaseSHA:      p.BaseSHA,
			HeadSHA:      p.HeadSHA,
			Branch:       p.Branch,
		}
		if _, err := s.Review(ctx, fn, req); err != nil {
			// Review already recorded the typed error verdict event; this is
			// the unclassifiable-internal-fault surface, log only.
			s.log.Warn("expert: review failed", "task", p.Task, "err", err)
		}
	})
	if err != nil {
		return err
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-s.stop:
		}
		sub.Unsubscribe()
	}()
	s.log.Info("expert: serving review requests", "subject", subject)
	return nil
}

// publishReview emits the typed verdict as a mesh.review.<task> observability
// event. Best-effort: a publish failure is logged, never fatal.
func (s *Sidecar) publishReview(req ReviewRequest, res ReviewResult) {
	id, _ := s.joinedID()
	payload := envelope.ReviewPayload{
		Task:      req.Task,
		Job:       req.Job,
		Branch:    req.Branch,
		HeadSHA:   req.HeadSHA,
		Verdict:   res.Verdict,
		Code:      res.Code,
		Notes:     truncateNotes(res.Notes, maxReviewNotesBytes),
		SessionID: res.SessionID,
		NumTurns:  res.NumTurns,
		CostUSD:   res.CostUSD,
	}
	env, err := envelope.New(envelope.KindReview, id, envelope.SubjectReview(req.Task), payload)
	if err != nil {
		s.log.Warn("expert: build review event failed", "task", req.Task, "err", err)
		return
	}
	if err := s.bus.Publish(env); err != nil {
		s.log.Debug("expert: review event publish failed", "task", req.Task, "err", err)
	}
}

// maxReviewNotesBytes bounds the verdict notes carried on the wire so an
// over-chatty model cannot bloat the review event past the bus frame cap.
const maxReviewNotesBytes = 4 * 1024

func truncateNotes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
