package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestReviewReturnsTypedVerdict proves the resident child review path returns a
// parsed, typed verdict over the held-open stdin — the same Ask boundary, no
// second process. The fake child replies with the verdict the test chose.
func TestReviewReturnsTypedVerdict(t *testing.T) {
	for _, want := range []ReviewVerdict{VerdictApprove, VerdictRequestChanges, VerdictReject} {
		t.Run(string(want), func(t *testing.T) {
			p := newTestProxy(t, helperVerdictEnv+`={"verdict":"`+string(want)+`","notes":"because reasons"}`)
			mustStart(t, p)
			out, err := p.Review(testCtx(t), ReviewRequest{
				Instruction: "add RLS policy", Diff: "@@ -1 +1 @@\n-old\n+new\n",
				ChangedFiles: []string{"db/policy.sql"}, BaseSHA: "abc", HeadSHA: "def",
			})
			if err != nil {
				t.Fatalf("Review: %v", err)
			}
			if out.Verdict != want {
				t.Fatalf("verdict = %q, want %q", out.Verdict, want)
			}
			if out.Status != TurnAnswered {
				t.Fatalf("status = %q, want answered", out.Status)
			}
			if out.Notes != "because reasons" {
				t.Fatalf("notes = %q", out.Notes)
			}
			if out.SessionID == "" {
				t.Fatal("verdict carried no session id")
			}
		})
	}
}

// TestReviewBadVerdictIsNotApprove proves never-fake-success at the review
// layer: a turn that answered but carried no parseable verdict object yields
// ErrNoVerdict and VerdictNone — never a silent approve.
func TestReviewBadVerdictIsNotApprove(t *testing.T) {
	p := newTestProxy(t, helperVerdictEnv+`=I think it is probably fine but cannot say for sure`)
	mustStart(t, p)
	out, err := p.Review(testCtx(t), ReviewRequest{Diff: "some diff"})
	if !errors.Is(err, ErrNoVerdict) {
		t.Fatalf("err = %v, want ErrNoVerdict", err)
	}
	if out.Verdict != VerdictNone {
		t.Fatalf("verdict = %q, want none (never fake-approve)", out.Verdict)
	}
	// The turn itself completed at the protocol level.
	if out.Status != TurnAnswered {
		t.Fatalf("status = %q, want answered (the turn finished; only the verdict was unparseable)", out.Status)
	}
}

// TestReviewUnknownVerdictTokenIsBad proves a syntactically valid JSON object
// with an out-of-set verdict token (the model invented a verdict) is rejected,
// not coerced.
func TestReviewUnknownVerdictTokenIsBad(t *testing.T) {
	p := newTestProxy(t, helperVerdictEnv+`={"verdict":"looks-good-to-me","notes":"shipping"}`)
	mustStart(t, p)
	out, err := p.Review(testCtx(t), ReviewRequest{Diff: "x"})
	if !errors.Is(err, ErrNoVerdict) {
		t.Fatalf("err = %v, want ErrNoVerdict for an unknown token", err)
	}
	if out.Verdict != VerdictNone {
		t.Fatalf("verdict = %q, want none", out.Verdict)
	}
}

// TestReviewEmptyRequestSpendsNoTurn proves a review request with nothing to
// review fails fast with ErrEmptyReview and never touches the child.
func TestReviewEmptyRequestSpendsNoTurn(t *testing.T) {
	p := newTestProxy(t)
	mustStart(t, p)
	_, err := p.Review(testCtx(t), ReviewRequest{Diff: "   ", ChangedFiles: nil})
	if !errors.Is(err, ErrEmptyReview) {
		t.Fatalf("err = %v, want ErrEmptyReview", err)
	}
}

// TestAskThenReviewReuseSameSession is the #27 same-session proof at the
// runtime layer: an ask and a following review run on ONE resident process —
// same pid, same session id, monotonically increasing num_turns — with no
// respawn between them. The proxy never restarts; the held-open stdin carries
// both turns.
func TestAskThenReviewReuseSameSession(t *testing.T) {
	p := newTestProxy(t, helperVerdictEnv+`={"verdict":"approve","notes":"ok"}`)
	mustStart(t, p)
	pidBefore := p.Pid()

	ask := mustAsk(t, p, "how should auth handle RLS?")
	if ask.Status != TurnAnswered {
		t.Fatalf("ask status = %q", ask.Status)
	}

	out, err := p.Review(testCtx(t), ReviewRequest{Diff: "@@ diff @@", ChangedFiles: []string{"a.go"}})
	if err != nil {
		t.Fatalf("Review: %v", err)
	}

	if p.Pid() != pidBefore {
		t.Fatalf("pid changed %d -> %d: the child was respawned between ask and review", pidBefore, p.Pid())
	}
	if out.SessionID != ask.SessionID {
		t.Fatalf("session id changed %q -> %q between ask and review", ask.SessionID, out.SessionID)
	}
	// The fake child increments num_turns per stdin message; the review's turn
	// must be strictly after the ask's — proof it ran in the same session
	// ledger, not a fresh process whose counter reset.
	if out.NumTurns <= ask.Result.NumTurns {
		t.Fatalf("review num_turns %d not after ask num_turns %d (fresh session?)",
			out.NumTurns, ask.Result.NumTurns)
	}
}

// TestReviewAfterChildDeathIsLostNotHang proves a review against a dead
// resident child yields a typed ProcessExited error promptly — never an
// indefinite hang — and no verdict. The child is killed in-band via the DIE
// ask (the established proxy-test death trigger); the subsequent review fails
// fast off the closed stream. This is the kill-path acceptance at the runtime
// layer.
func TestReviewAfterChildDeathIsLostNotHang(t *testing.T) {
	p := newTestProxy(t)
	mustStart(t, p)

	// Kill the resident child: a DIE ask exits the process with no result.
	if _, err := p.Ask(testCtx(t), "DIE now"); !errors.Is(err, ErrProcessExited) {
		t.Fatalf("DIE ask err = %v, want ErrProcessExited", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	var out ReviewOutcome
	var err error
	go func() {
		out, err = p.Review(ctx, ReviewRequest{Diff: "x"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("Review hung after the child died")
	}
	if !errors.Is(err, ErrProcessExited) {
		t.Fatalf("err = %v, want ErrProcessExited", err)
	}
	if out.Verdict != VerdictNone {
		t.Fatalf("verdict = %q, want none after child death", out.Verdict)
	}
	if out.Status != TurnLost {
		t.Fatalf("status = %q, want lost", out.Status)
	}
}

// TestReviewRecoversViaRestart proves the documented recovery path: after the
// child dies a review fails typed, and Restart (--resume) brings a new child up
// so the review can be re-requested and succeeds.
func TestReviewRecoversViaRestart(t *testing.T) {
	p := newTestProxy(t, helperVerdictEnv+`={"verdict":"reject","notes":"wrong approach"}`)
	mustStart(t, p)
	sid := p.SessionID()

	// Kill the resident child in-band, then prove the review fails typed.
	if _, err := p.Ask(testCtx(t), "DIE"); !errors.Is(err, ErrProcessExited) {
		t.Fatalf("DIE ask err = %v, want ErrProcessExited", err)
	}
	if _, err := p.Review(testCtx(t), ReviewRequest{Diff: "x"}); !errors.Is(err, ErrProcessExited) {
		t.Fatalf("post-death review err = %v, want ErrProcessExited", err)
	}

	if err := p.Restart(testCtx(t)); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if p.SessionID() != sid {
		t.Fatalf("resumed session id = %q, want %q", p.SessionID(), sid)
	}

	out, err := p.Review(testCtx(t), ReviewRequest{Diff: "x"})
	if err != nil {
		t.Fatalf("review after restart: %v", err)
	}
	if out.Verdict != VerdictReject {
		t.Fatalf("verdict = %q, want reject", out.Verdict)
	}
}

// --- ParseVerdict / jsonObjects unit tests -----------------------------------

func TestParseVerdict(t *testing.T) {
	cases := []struct {
		name      string
		text      string
		want      ReviewVerdict
		wantNotes string
		wantErr   bool
	}{
		{"plain", `{"verdict":"approve","notes":"ok"}`, VerdictApprove, "ok", false},
		{"narrated", "Let me think... here is my call:\n{\"verdict\":\"reject\",\"notes\":\"no\"}", VerdictReject, "no", false},
		{"trailing prose ignored uses last obj",
			`{"verdict":"approve"} actually no: {"verdict":"request_changes","notes":"fix it"}`,
			VerdictRequestChanges, "fix it", false},
		{"uppercase token normalized", `{"verdict":"APPROVE"}`, VerdictApprove, "", false},
		{"brace inside string value", `{"verdict":"reject","notes":"this } breaks naive scanners"}`, VerdictReject, "this } breaks naive scanners", false},
		{"no json at all", "I approve this diff.", VerdictNone, "", true},
		{"unknown token", `{"verdict":"ship-it"}`, VerdictNone, "", true},
		{"empty", "", VerdictNone, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, notes, err := ParseVerdict(tc.text)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got verdict %q", v)
				}
				if !errors.Is(err, ErrNoVerdict) {
					t.Fatalf("err = %v, want ErrNoVerdict", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if v != tc.want {
				t.Fatalf("verdict = %q, want %q", v, tc.want)
			}
			if notes != tc.wantNotes {
				t.Fatalf("notes = %q, want %q", notes, tc.wantNotes)
			}
		})
	}
}

func TestJSONObjectsRespectsStringBraces(t *testing.T) {
	got := jsonObjects(`prefix {"a":"} { nested-looking"} mid {"b":1} end`)
	want := []string{`{"a":"} { nested-looking"}`, `{"b":1}`}
	if len(got) != len(want) {
		t.Fatalf("got %d objects %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if !strings.EqualFold(got[i], want[i]) {
			t.Fatalf("object[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBuildReviewPromptIsParseableAndBounded(t *testing.T) {
	p := BuildReviewPrompt(ReviewRequest{
		Instruction: "do X", Diff: strings.Repeat("x", maxReviewDiffBytes*2),
		ChangedFiles: []string{"a", "b"}, Branch: "mesh/worker/t1",
		BaseSHA: "0123456789abcdef", HeadSHA: "fedcba9876543210",
	})
	if !strings.Contains(p, reviewPromptMarker) {
		t.Fatal("prompt missing the stable reviewer marker")
	}
	if !strings.Contains(p, "request_changes") {
		t.Fatal("prompt did not name the verdict tokens")
	}
	if !strings.Contains(p, "[truncated]") {
		t.Fatal("oversized diff was not truncated")
	}
	// Short SHA in the prompt.
	if !strings.Contains(p, "0123456789ab") || strings.Contains(p, "0123456789abcdef") {
		t.Fatalf("base SHA not shortened in prompt:\n%s", p)
	}
}
