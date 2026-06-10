package envelope

import "fmt"

// Expert-review wire vocabulary (#27): the subject, typed verdict enum, and
// payload for an expert's review of a worker diff. Additive to the frozen
// contract — it mirrors KindWorker (the worker-outcome tap): the expert-side
// review CAPABILITY produces a typed verdict that an observability tap can
// discriminate without parsing prose. The scheduler→expert review-GATING
// integration (auto-routing a worker diff to an expert and blocking the task
// on the verdict) is a separate follow-up; this package only freezes the
// verdict contract the capability emits.

// SubjectReview names the expert-review observability event (KindReview, #27),
// one per review an expert produces over a task's worker diff. Keyed by task
// id, mirroring SubjectWorker: the review is a fact about a task's diff, and a
// tap watching one task sees both its worker run and its review on adjacent
// subjects.
func SubjectReview(task string) string { return "mesh.review." + task }

// PatternReviews matches every expert-review event.
const PatternReviews = "mesh.review.>"

// ReviewVerdict is the typed outcome of one expert review over a worker diff
// (#27). It mirrors the claim/turn enum discipline (a closed, exact-token set,
// never substring-matched) so a gate can branch on it without scraping prose.
// A review that the runtime could not complete (child death, non-success turn,
// unparseable verdict) is NOT a verdict — it is a ReviewError, distinct from
// every real verdict below. Never fake-success: the absence of a clean verdict
// never silently becomes an approval.
type ReviewVerdict string

const (
	// ReviewApprove: the expert judged the diff acceptable as-is. A gating
	// integration may let the task proceed.
	ReviewApprove ReviewVerdict = "approve"
	// ReviewRequestChanges: the diff needs revision before it is acceptable.
	// The Notes carry the specifics; a gate may re-dispatch with feedback.
	ReviewRequestChanges ReviewVerdict = "request_changes"
	// ReviewReject: the diff is fundamentally wrong (wrong approach, breaks
	// the task's intent). A gate may fail the task rather than iterate.
	ReviewReject ReviewVerdict = "reject"
	// ReviewError: the review itself could not be produced — the expert's
	// runtime turn was lost (child death), reported a non-success result, or
	// returned no parseable verdict object. NOT a judgement on the diff; the
	// Code says why. A gate must treat this as "no review yet", never as an
	// approval.
	ReviewError ReviewVerdict = "error"
)

var reviewVerdicts = map[ReviewVerdict]bool{
	ReviewApprove:        true,
	ReviewRequestChanges: true,
	ReviewReject:         true,
	ReviewError:          true,
}

// ValidReviewVerdict reports whether v is a recognized review verdict.
func ValidReviewVerdict(v ReviewVerdict) bool { return reviewVerdicts[v] }

// Decided reports whether v is a real judgement on the diff (approve /
// request_changes / reject) as opposed to ReviewError (no review produced).
func (v ReviewVerdict) Decided() bool {
	return v == ReviewApprove || v == ReviewRequestChanges || v == ReviewReject
}

// ReviewErrorCode classifies why a review could not be produced. It travels in
// ReviewPayload only when Verdict == ReviewError, so a tap or a future gate can
// discriminate "the expert is gone" from "the diff under review was empty"
// without parsing prose — mirroring WorkerErrorCode's role for worker runs.
type ReviewErrorCode string

const (
	// ReviewRuntimeLost: the expert's resident runtime child died or the turn
	// was cancelled/timed out — no result arrived. Recoverable: the expert may
	// restart (--resume) and the review may be re-requested.
	ReviewRuntimeLost ReviewErrorCode = "runtime_lost"
	// ReviewRuntimeError: the runtime completed the turn but reported a
	// non-success result (is_error, non-success subtype, api_error_status).
	ReviewRuntimeError ReviewErrorCode = "runtime_error"
	// ReviewBadVerdict: the turn succeeded but its text did not carry a
	// parseable verdict object with a recognized verdict token. Never-fake-
	// success: an unparseable verdict is an error, never a silent approve.
	ReviewBadVerdict ReviewErrorCode = "bad_verdict"
	// ReviewEmptyDiff: the review request carried no diff and no changed files
	// — there was nothing to review. A caller that wants to gate an empty diff
	// decides its own policy; the capability refuses to invent a verdict.
	ReviewEmptyDiff ReviewErrorCode = "empty_diff"
	// ReviewInternal: producing or recording the review failed for a reason the
	// caller could not classify (a seam fault, not a runtime turn outcome).
	ReviewInternal ReviewErrorCode = "internal"
)

var reviewErrorCodes = map[ReviewErrorCode]bool{
	ReviewRuntimeLost:  true,
	ReviewRuntimeError: true,
	ReviewBadVerdict:   true,
	ReviewEmptyDiff:    true,
	ReviewInternal:     true,
}

// ValidReviewErrorCode reports whether c is a recognized review error code.
func ValidReviewErrorCode(c ReviewErrorCode) bool { return reviewErrorCodes[c] }

// ReviewPayload is the expert-review observability event (#27, KindReview):
// one per review an expert produces over a task's worker diff. An
// observability tap only — like KindWorker, it does not carry authority over
// any KV record; it records the typed verdict so dashboards and a future
// review-gating scheduler can see how a diff was judged.
//
// Task/Job/Branch/HeadSHA identify the diff that was reviewed (the metadata
// #26 already commits onto the worker branch). Verdict is the typed outcome;
// Code is set iff Verdict == ReviewError. Notes is the expert's free-text
// rationale — opaque, never a contract: a gate branches on Verdict, never on
// Notes. SessionID is the resident expert session the review ran under (the
// same-session-reuse proof surfaces here). NumTurns is the session's turn
// counter at review time — observability only, NOT a session-reuse proof on
// its own (the spike note on ResultEvent.NumTurns applies).
type ReviewPayload struct {
	Task      string          `json:"task"`
	Job       string          `json:"job,omitempty"`
	Branch    string          `json:"branch,omitempty"`
	HeadSHA   string          `json:"headSHA,omitempty"`
	Verdict   ReviewVerdict   `json:"verdict"`
	Code      ReviewErrorCode `json:"code,omitempty"`
	Notes     string          `json:"notes,omitempty"`
	SessionID string          `json:"sessionID,omitempty"`
	NumTurns  int             `json:"numTurns,omitempty"`
}

func (p ReviewPayload) validate() error {
	if err := requireField("task", p.Task); err != nil {
		return err
	}
	if !ValidReviewVerdict(p.Verdict) {
		return fmt.Errorf("unknown review verdict %q", p.Verdict)
	}
	if p.Verdict == ReviewError && !ValidReviewErrorCode(p.Code) {
		return fmt.Errorf("review error without a valid code (got %q)", p.Code)
	}
	if p.Verdict != ReviewError && p.Code != "" {
		return fmt.Errorf("decided review must not carry an error code (got %q)", p.Code)
	}
	return nil
}
