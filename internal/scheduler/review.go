package scheduler

import (
	"context"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// The review-gating seam (#80). After a worker run lands a typed success, the
// scheduler optionally routes the committed diff to an expert and GATES the
// task's terminal state on the typed verdict — opt-in via Options.Reviewer
// (nil = pre-#80 behavior: worker success → done, no review).
//
// Gating policy (the design fork resolved for #80, see DECISIONS.md):
//
//   - approve          → task done (the only path from a reviewed success to done)
//   - request_changes  → task failed, typed reason carries the verdict + notes
//   - reject           → task failed, same shape
//   - error (any code) → task failed — NEVER a silent approve (never-fake-success)
//   - no diff to review (head == base per the worker's committed metadata)
//     → task done without spending a review turn: the gate gates DIFFS, and a
//     typed worker success with zero file changes has nothing to judge.
//
// request_changes does NOT re-dispatch in this slice: a bounded retry needs
// worker-runtime support that is out of #80's lane (branch-aware worktree
// re-allocation after a successful first run, plus a feedback channel into the
// worker prompt) — tracked as a follow-up issue. Failing typed beats burning a
// worker turn on an unchanged prompt, and the existing fail-fast path already
// cancels dependents of a failed task.
//
// One authority per fact: the tasks KV record stays the sole authority for
// task state. The KindReview event is the gate's INPUT (and the audit tap),
// never a second authority — the scheduler alone writes the task transition.

// ReviewTarget is one successful worker run handed to the reviewer: the task
// it executed, the owning job's repo name (for diff computation), and the
// worker Result.Summary, whose tail carries the #26 diff metadata block
// (branch, base/head SHAs, changed files).
type ReviewTarget struct {
	Task    task.Record
	Repo    string
	Summary string
}

// ReviewDecision is the typed gate input one review resolves to. Exactly one
// of two shapes: NoDiff true (nothing to review existed — head == base; the
// Verdict fields are unset), or a valid envelope.ReviewVerdict with Code set
// iff Verdict == ReviewError. CostUSD is the review turn's reported cost and
// counts against the same fleet budget meter as a worker run.
type ReviewDecision struct {
	Verdict envelope.ReviewVerdict
	Code    envelope.ReviewErrorCode
	Notes   string
	CostUSD float64
	NoDiff  bool
}

// Reviewer resolves one worker diff to a typed decision. Implementations must
// never fake success: an unreachable expert, a timed-out round trip, or an
// unparseable verdict resolves to Verdict == ReviewError with a typed code —
// never to approve. A returned Go error is reserved for a fault the reviewer
// could not classify; the scheduler maps it to ReviewError/internal.
//
// The production implementation is BusReviewer (reviewer.go): a bus round trip
// to the expert serving the configured role. Tests inject in-process fakes to
// exercise the gate policy without a bus or an LLM.
type Reviewer interface {
	Review(ctx context.Context, target ReviewTarget) (ReviewDecision, error)
}

// --- worker diff metadata ---------------------------------------------------------

// workerMeta is the diff metadata block the #26 worker commits into
// Result.Summary on every typed success (worker.commitAndDescribe):
//
//	[mesh worker] task=<id> branch=<branch>
//	base=<sha> head=<sha>
//	no file changes | changed files (N):\n  <path>...
//
// It is machine-written by the worker driver with a fixed grammar — parsing it
// here is decoding our own typed metadata, not scraping model prose (the
// model's free text precedes the block; the LAST block wins).
type workerMeta struct {
	TaskID  string
	Branch  string
	BaseSHA string
	HeadSHA string
	Files   []string
}

const workerMetaMarker = "[mesh worker] task="

// parseWorkerMeta extracts the LAST diff metadata block from a worker summary.
// ok is false when no block is present (a non-#26 driver, or a summary the
// worker never stamped) — the gate treats that as unreviewable, never as
// approved.
func parseWorkerMeta(summary string) (workerMeta, bool) {
	idx := strings.LastIndex(summary, workerMetaMarker)
	if idx < 0 {
		return workerMeta{}, false
	}
	lines := strings.Split(summary[idx:], "\n")
	if len(lines) < 2 {
		return workerMeta{}, false
	}

	var m workerMeta
	// Line 0: "[mesh worker] task=<id> branch=<branch>"
	for _, f := range strings.Fields(strings.TrimPrefix(lines[0], "[mesh worker] ")) {
		switch {
		case strings.HasPrefix(f, "task="):
			m.TaskID = strings.TrimPrefix(f, "task=")
		case strings.HasPrefix(f, "branch="):
			m.Branch = strings.TrimPrefix(f, "branch=")
		}
	}
	// Line 1: "base=<sha> head=<sha>"
	for _, f := range strings.Fields(lines[1]) {
		switch {
		case strings.HasPrefix(f, "base="):
			m.BaseSHA = strings.TrimPrefix(f, "base=")
		case strings.HasPrefix(f, "head="):
			m.HeadSHA = strings.TrimPrefix(f, "head=")
		}
	}
	if m.TaskID == "" || m.BaseSHA == "" || m.HeadSHA == "" {
		return workerMeta{}, false
	}
	// Optional: "changed files (N):" followed by two-space-indented paths.
	for i := 2; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "  ") {
			if f := strings.TrimSpace(lines[i]); f != "" {
				m.Files = append(m.Files, f)
			}
		}
	}
	return m, true
}
