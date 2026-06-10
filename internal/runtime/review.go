package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Review is the resident-child review capability (#27): drive the SAME warm
// stream-json child the expert answers asks with to review a worker diff, and
// parse its reply into a typed verdict. It reuses Ask verbatim — one
// stdin user message on the held-open pipe, one result event back — so a
// review and the asks around it run in one resident session (the warm-process
// continuity the spike proved). No second process, no second authority.
//
// The verdict is STRUCTURED, never scraped from prose: the prompt asks the
// child to emit exactly one JSON object, and ParseVerdict decodes the child's
// result text into a typed ReviewVerdict. A turn that was lost (child death),
// reported a non-success result, or whose text held no parseable verdict
// object yields a typed ReviewError — never a silent approve (never-fake-
// success). The caller maps these onto the envelope.ReviewVerdict contract;
// this package stays below the envelope wire layer (see the package doc), so
// the verdict tokens are mirrored here as plain strings.
//
// Concurrency: Review is exactly an Ask, so it serializes on the proxy's
// askMu like every other turn — no review runs concurrently with an ask.

// ReviewVerdict is the runtime-layer verdict token. It mirrors
// envelope.ReviewVerdict (approve|request_changes|reject) without importing the
// envelope package (this package sits below the wire layer). VerdictNone is the
// zero value: no verdict was produced.
type ReviewVerdict string

const (
	// VerdictNone means no verdict was parsed (the zero value).
	VerdictNone ReviewVerdict = ""
	// VerdictApprove: the child judged the diff acceptable as-is.
	VerdictApprove ReviewVerdict = "approve"
	// VerdictRequestChanges: the diff needs revision.
	VerdictRequestChanges ReviewVerdict = "request_changes"
	// VerdictReject: the diff is fundamentally wrong.
	VerdictReject ReviewVerdict = "reject"
)

func validVerdict(v ReviewVerdict) bool {
	return v == VerdictApprove || v == VerdictRequestChanges || v == VerdictReject
}

// ReviewRequest is the diff to review. Diff is the unified-diff text (or any
// patch the worker produced); ChangedFiles/BaseSHA/HeadSHA/Branch are the
// commit metadata #26 records on the worker branch. Instruction is the task's
// own acceptance context (what the diff was supposed to do), so the child
// judges the diff against intent rather than in a vacuum. A request with an
// empty Diff AND no ChangedFiles has nothing to review — Review returns
// ErrEmptyReview rather than inventing a verdict.
type ReviewRequest struct {
	Instruction  string
	Diff         string
	ChangedFiles []string
	BaseSHA      string
	HeadSHA      string
	Branch       string
}

// ReviewOutcome is the typed result of one Review turn. Verdict is non-empty
// only on a clean, parsed judgement; otherwise Status carries the failure mode
// and Verdict stays VerdictNone. SessionID/NumTurns come from the underlying
// runtime result so a caller can prove which resident session the review ran
// under (the same-session-reuse proof). Notes is the child's free-text
// rationale — opaque, never a contract.
type ReviewOutcome struct {
	Verdict   ReviewVerdict
	Notes     string
	Status    TurnStatus // mirrors the Ask turn status (answered|error|lost)
	SessionID string
	NumTurns  int
	Turn      Turn // the full underlying turn (nil-safe Result)
}

// ErrEmptyReview means the review request carried nothing to review.
var ErrEmptyReview = errors.New("runtime: empty review request (no diff, no changed files)")

// ErrNoVerdict means the child completed the turn but its text held no
// parseable verdict object with a recognized verdict token. Distinct from a
// ResultError (non-success turn) and a ProcessExitedError (child death): the
// turn answered, but not in the verdict contract.
var ErrNoVerdict = errors.New("runtime: child reply held no parseable verdict")

// Review drives the resident child to review req and returns a typed verdict.
// The returned error mirrors Ask's discipline:
//   - nil with Verdict set: a clean parsed judgement.
//   - ErrEmptyReview: nothing to review (no child turn was spent).
//   - *ProcessExitedError (errors.Is ErrProcessExited): the child died — the
//     turn was lost. The caller may Restart and retry, exactly like an ask.
//   - *ResultError: the child reported a non-success result.
//   - ErrNoVerdict: the turn succeeded but carried no parseable verdict.
//   - ctx/timeout errors: the turn was cancelled or timed out (lost).
//
// In every error case Verdict is VerdictNone — never-fake-success: the absence
// of a clean verdict is never silently an approve.
func (p *Proxy) Review(ctx context.Context, req ReviewRequest) (ReviewOutcome, error) {
	if strings.TrimSpace(req.Diff) == "" && len(req.ChangedFiles) == 0 {
		return ReviewOutcome{Status: TurnLost}, ErrEmptyReview
	}

	prompt := BuildReviewPrompt(req)
	turn, err := p.Ask(ctx, prompt)
	out := ReviewOutcome{Status: turn.Status, SessionID: turn.SessionID, Turn: turn}
	if turn.Result != nil {
		out.NumTurns = turn.Result.NumTurns
	}
	if err != nil {
		// Child death, non-success result, cancel/timeout: no verdict. The
		// error type is preserved so the caller can branch (restart on
		// ProcessExited, etc.) exactly as it does for an ask.
		return out, err
	}

	verdict, notes, perr := ParseVerdict(turn.Text)
	if perr != nil {
		// The turn answered but not in the verdict contract. Surface the raw
		// text as notes so an operator can see what the child actually said.
		out.Notes = strings.TrimSpace(turn.Text)
		return out, perr
	}
	out.Verdict = verdict
	out.Notes = notes
	return out, nil
}

// verdictReply is the one structured shape the review contract asks the child
// to emit. Unknown fields are tolerated; only verdict + notes are read.
type verdictReply struct {
	Verdict string `json:"verdict"`
	Notes   string `json:"notes"`
}

// ParseVerdict extracts a typed verdict from the child's result text. It is
// strict in the never-scrape-prose sense: it finds the LAST balanced JSON
// object in the text (the child may narrate before emitting the verdict line)
// and decodes it; a verdict token outside the recognized set, or no JSON object
// at all, is ErrNoVerdict — never a guessed approval. Returning the LAST object
// tolerates a model that reasons in prose-with-braces first and emits its final
// answer as the closing object.
func ParseVerdict(text string) (ReviewVerdict, string, error) {
	objs := jsonObjects(text)
	// Scan from the last object backward: the child's final verdict line is its
	// answer, and earlier braces may be reasoning. The last object carrying a
	// recognized verdict token wins.
	for i := len(objs) - 1; i >= 0; i-- {
		var vr verdictReply
		if err := json.Unmarshal([]byte(objs[i]), &vr); err != nil {
			continue
		}
		v := ReviewVerdict(strings.ToLower(strings.TrimSpace(vr.Verdict)))
		if validVerdict(v) {
			return v, strings.TrimSpace(vr.Notes), nil
		}
	}
	return VerdictNone, "", fmt.Errorf("%w: %q", ErrNoVerdict, truncate(text, 256))
}

// jsonObjects returns every balanced top-level {...} substring in s, in order.
// It is brace-depth scanning that respects JSON string literals and escapes so
// a brace inside a quoted value does not throw off the depth count. It does not
// validate the candidates — ParseVerdict's json.Unmarshal does that — it only
// delimits them.
func jsonObjects(s string) []string {
	var objs []string
	depth := 0
	start := -1
	inStr := false
	escaped := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inStr {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{':
			if depth == 0 {
				start = i
			}
			depth++
		case '}':
			if depth > 0 {
				depth--
				if depth == 0 && start >= 0 {
					objs = append(objs, s[start:i+1])
					start = -1
				}
			}
		}
	}
	return objs
}

// BuildReviewPrompt renders the one user message that asks the resident child
// to review the diff and reply with a structured verdict. It names the exact
// JSON shape and verdict tokens so the reply is parseable (never scraped from
// prose), and it caps the diff so one oversized worker patch cannot blow the
// stdin frame.
func BuildReviewPrompt(req ReviewRequest) string {
	var b strings.Builder
	b.WriteString("You are a senior code reviewer. Review the following worker diff and decide whether it should be accepted.\n\n")
	if strings.TrimSpace(req.Instruction) != "" {
		b.WriteString("The diff was meant to accomplish:\n")
		b.WriteString(strings.TrimSpace(req.Instruction))
		b.WriteString("\n\n")
	}
	if req.Branch != "" || req.HeadSHA != "" {
		fmt.Fprintf(&b, "Branch: %s  base: %s  head: %s\n", req.Branch, shortSHA(req.BaseSHA), shortSHA(req.HeadSHA))
	}
	if len(req.ChangedFiles) > 0 {
		fmt.Fprintf(&b, "Changed files (%d):\n", len(req.ChangedFiles))
		for _, f := range req.ChangedFiles {
			b.WriteString("  " + f + "\n")
		}
	}
	if strings.TrimSpace(req.Diff) != "" {
		b.WriteString("\nDiff:\n```diff\n")
		b.WriteString(truncate(req.Diff, maxReviewDiffBytes))
		b.WriteString("\n```\n")
	}
	b.WriteString("\nReply with EXACTLY ONE JSON object on its own line and nothing after it, of the form:\n")
	b.WriteString(`{"verdict":"approve|request_changes|reject","notes":"<one or two sentences>"}` + "\n")
	b.WriteString("Use \"approve\" if the diff is acceptable as-is, \"request_changes\" if it needs revision, \"reject\" if it is fundamentally wrong. Do not omit the JSON object.")
	return b.String()
}

// maxReviewDiffBytes caps the diff embedded in one review prompt. The runtime
// stdin frame is bounded (maxLineBytes); a worker diff far larger than this is
// truncated with a marker rather than risking an over-long stdin write.
const maxReviewDiffBytes = 256 * 1024

func shortSHA(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// truncate bounds s to n bytes, appending a marker when it cuts.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "\n…[truncated]"
}
