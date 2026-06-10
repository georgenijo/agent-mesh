package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// BusReviewer is the production Reviewer (#80): a bus round trip to the
// resident expert serving the configured role.
//
// Transport (the fork the issue left open, resolved as the role-addressed
// subject): the reviewer publishes one KindReviewRequest on
// mesh.review-req.<role> carrying the diff + its commit metadata, and waits
// for the expert's typed KindReview verdict event on mesh.review.<task> —
// the SAME event #27 defined for observability, so one publish serves both
// the gate and the dashboard/audit taps. The ask/ticket path was rejected:
// a ticket's Q/Answer fields are free text, and tunnelling a structured
// verdict through them would mean parsing contract data out of prose.
//
// The diff itself is computed here, from the shared repo checkout
// (<ReposDir>/<repo>, the same mapping the worker driver resolves), via
// `git diff base..head` — the worker's branch commits live in the shared
// object store even after its worktree is removed, so a review never needs
// the (possibly already torn down) worktree.
//
// Failure posture — never fake success, never block forever:
//   - missing/unparseable diff metadata, git failure, publish failure
//     → a synthesized ReviewError verdict (internal), published as a
//     KindReview event by THIS side (the expert never saw the request, so
//     the audit trail still records how the diff was judged);
//   - no verdict within Timeout → ReviewError/runtime_lost, synthesized and
//     published the same way;
//   - head == base → NoDiff (nothing to review; no request, no event — the
//     KindWorker ok event already records the no-change success).
//
// Duplicate experts on one role both review and both publish; the first
// verdict event wins and the rest are dropped (the waiter is gone). That
// costs a duplicate turn, not correctness — running one expert per review
// role is the documented operating shape.
type BusReviewer struct {
	cli      *bus.Client
	role     string
	reposDir string
	timeout  time.Duration
	log      *slog.Logger

	mu      sync.Mutex
	waiters map[string]chan envelope.ReviewPayload // task id → verdict delivery
	sub     *bus.Subscription
}

// ReviewerOptions configure a BusReviewer.
type ReviewerOptions struct {
	Role     string        // reviewing role the requests are addressed to (required)
	ReposDir string        // job repo name → git checkout mapping (required; same as the worker driver's)
	Timeout  time.Duration // wall-clock bound on one round trip (default config.DefaultReviewTimeout's value, 5m)
	Log      *slog.Logger
}

// NewBusReviewer validates the options and subscribes the verdict tap.
func NewBusReviewer(cli *bus.Client, opts ReviewerOptions) (*BusReviewer, error) {
	if cli == nil {
		return nil, errors.New("scheduler: reviewer needs a bus client")
	}
	if !envelope.ValidRole(opts.Role) {
		return nil, fmt.Errorf("scheduler: review role %q is not a legal subject token", opts.Role)
	}
	if opts.ReposDir == "" {
		return nil, errors.New("scheduler: reviewer needs ReposDir (it computes diffs from the shared checkout)")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Minute
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	r := &BusReviewer{
		cli:      cli,
		role:     opts.Role,
		reposDir: opts.ReposDir,
		timeout:  opts.Timeout,
		log:      opts.Log,
		waiters:  make(map[string]chan envelope.ReviewPayload),
	}
	// One subscription for every review this reviewer ever awaits: verdict
	// events are correlated to waiters by task id.
	sub, err := cli.Subscribe(envelope.PatternReviews, r.deliver)
	if err != nil {
		return nil, fmt.Errorf("scheduler: subscribe reviews: %w", err)
	}
	r.sub = sub
	return r, nil
}

// Close drops the verdict subscription. In-flight Reviews resolve via their
// timeout/ctx; the scheduler is being stopped anyway when this runs.
func (r *BusReviewer) Close() {
	if r.sub != nil {
		r.sub.Unsubscribe()
	}
}

// deliver routes a verdict event to the waiter for its task, if any.
func (r *BusReviewer) deliver(env envelope.Envelope) {
	if env.Kind != envelope.KindReview {
		return
	}
	var p envelope.ReviewPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		r.log.Debug("reviewer: drop malformed review event", "subject", env.Subject, "err", err)
		return
	}
	r.mu.Lock()
	ch := r.waiters[p.Task]
	delete(r.waiters, p.Task)
	r.mu.Unlock()
	if ch != nil {
		ch <- p // buffered(1); the waiter is deregistered, so exactly one send
	}
}

// register parks a waiter for the task's verdict; the returned cancel is
// idempotent and safe to call after delivery.
func (r *BusReviewer) register(task string) (<-chan envelope.ReviewPayload, func()) {
	ch := make(chan envelope.ReviewPayload, 1)
	r.mu.Lock()
	r.waiters[task] = ch
	r.mu.Unlock()
	return ch, func() {
		r.mu.Lock()
		delete(r.waiters, task)
		r.mu.Unlock()
	}
}

// Review implements Reviewer over the bus round trip.
func (r *BusReviewer) Review(ctx context.Context, target ReviewTarget) (ReviewDecision, error) {
	rec := target.Task

	meta, ok := parseWorkerMeta(target.Summary)
	if !ok {
		// Unreviewable success: the summary carries no diff metadata block
		// (a non-#26 driver). Never an approve.
		return r.synthesize(rec, meta, envelope.ReviewInternal,
			"worker summary carried no diff metadata block; cannot review"), nil
	}
	if meta.HeadSHA == meta.BaseSHA {
		// A typed success with zero file changes: nothing to review exists.
		return ReviewDecision{NoDiff: true}, nil
	}

	diff, err := r.gitDiff(ctx, target.Repo, meta.BaseSHA, meta.HeadSHA)
	if err != nil {
		return r.synthesize(rec, meta, envelope.ReviewInternal,
			fmt.Sprintf("computing the diff failed: %v", err)), nil
	}

	ch, cancel := r.register(rec.ID)
	defer cancel()

	payload := envelope.ReviewRequestPayload{
		Task:         rec.ID,
		Job:          rec.Job,
		Role:         r.role,
		Repo:         target.Repo,
		Branch:       meta.Branch,
		BaseSHA:      meta.BaseSHA,
		HeadSHA:      meta.HeadSHA,
		ChangedFiles: meta.Files,
		Instruction:  reviewInstruction(rec),
		Diff:         truncateDiff(diff, envelope.MaxReviewRequestDiffBytes),
	}
	env, err := envelope.New(envelope.KindReviewRequest, schedulerID,
		envelope.SubjectReviewRequest(r.role), &payload)
	if err == nil {
		err = r.cli.Publish(env)
	}
	if err != nil {
		cancel()
		return r.synthesize(rec, meta, envelope.ReviewInternal,
			fmt.Sprintf("publishing the review request failed: %v", err)), nil
	}

	timer := time.NewTimer(r.timeout)
	defer timer.Stop()
	select {
	case p := <-ch:
		// The expert's own KindReview event is the verdict — it already
		// reached every tap; nothing more to publish here.
		return ReviewDecision{Verdict: p.Verdict, Code: p.Code, Notes: p.Notes, CostUSD: p.CostUSD}, nil
	case <-timer.C:
		cancel()
		return r.synthesize(rec, meta, envelope.ReviewRuntimeLost,
			fmt.Sprintf("no review verdict within %s (is an expert serving role %q?)", r.timeout, r.role)), nil
	case <-ctx.Done():
		// Scheduler shutdown: no synthesized event — the task stays persisted
		// running and the next coordinator lifetime re-runs and re-reviews it.
		return ReviewDecision{Verdict: envelope.ReviewError, Code: envelope.ReviewRuntimeLost,
			Notes: ctx.Err().Error()}, nil
	}
}

// synthesize builds a typed ReviewError decision for a review the expert never
// produced (pre-flight failure or timeout) and publishes it as a KindReview
// event so the dashboard/audit taps still see how the diff was judged. One
// publisher per fact: the expert publishes verdicts it produced; this side
// publishes only the verdicts the expert never saw.
func (r *BusReviewer) synthesize(rec task.Record, meta workerMeta, code envelope.ReviewErrorCode, notes string) ReviewDecision {
	dec := ReviewDecision{Verdict: envelope.ReviewError, Code: code, Notes: notes}
	payload := envelope.ReviewPayload{
		Task:    rec.ID,
		Job:     rec.Job,
		Branch:  meta.Branch,
		HeadSHA: meta.HeadSHA,
		Verdict: dec.Verdict,
		Code:    dec.Code,
		Notes:   notes,
	}
	env, err := envelope.New(envelope.KindReview, schedulerID, envelope.SubjectReview(rec.ID), &payload)
	if err == nil {
		err = r.cli.Publish(env)
	}
	if err != nil {
		r.log.Warn("reviewer: publish synthesized review event failed", "task", rec.ID, "err", err)
	}
	return dec
}

// gitDiff renders base..head from the shared checkout the job's repo name
// maps to. Repo-name hygiene mirrors the worker driver: plain directory names
// only, nothing that can escape ReposDir.
func (r *BusReviewer) gitDiff(ctx context.Context, repo, base, head string) (string, error) {
	if repo == "" || repo != filepath.Base(repo) || repo == "." || repo == ".." {
		return "", fmt.Errorf("repo %q is not a plain directory name", repo)
	}
	dir := filepath.Join(r.reposDir, repo)
	ctx, cancelGit := context.WithTimeout(ctx, 30*time.Second)
	defer cancelGit()
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "diff", base+".."+head)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git diff %s..%s: %w: %s", base, head, err, truncate(stderr.String(), 512))
	}
	return stdout.String(), nil
}

// reviewInstruction renders the task's acceptance context — what the diff was
// meant to accomplish — for the expert's judgement.
func reviewInstruction(rec task.Record) string {
	var b strings.Builder
	b.WriteString(rec.Title)
	if rec.Description != "" {
		b.WriteString("\n" + rec.Description)
	}
	if len(rec.Acceptance) > 0 {
		b.WriteString("\nAcceptance criteria:")
		for _, a := range rec.Acceptance {
			b.WriteString("\n- " + a)
		}
	}
	return b.String()
}

// truncateDiff bounds the diff carried on the wire, marking the cut.
func truncateDiff(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "\n…[diff truncated]"
}
