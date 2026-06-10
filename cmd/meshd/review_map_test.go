package main

import (
	"context"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	agentruntime "github.com/georgenijo/agent-mesh/internal/runtime"
)

// TestMapReviewOutcome pins the production runtime→verdict mapping: every
// runtime outcome and typed error lands on the right envelope.ReviewVerdict +
// code, and never-fake-success holds — no non-clean outcome ever becomes an
// approve. This is the load-bearing translation cmd/meshd's reviewFn relies on.
func TestMapReviewOutcome(t *testing.T) {
	resultErr := &agentruntime.ResultError{Result: &agentruntime.ResultEvent{Subtype: "error_during_execution", IsError: true}}

	cases := []struct {
		name        string
		out         agentruntime.ReviewOutcome
		err         error
		wantVerdict envelope.ReviewVerdict
		wantCode    envelope.ReviewErrorCode
	}{
		{"approve", agentruntime.ReviewOutcome{Verdict: agentruntime.VerdictApprove, SessionID: "s1", NumTurns: 2}, nil,
			envelope.ReviewApprove, ""},
		{"request_changes", agentruntime.ReviewOutcome{Verdict: agentruntime.VerdictRequestChanges}, nil,
			envelope.ReviewRequestChanges, ""},
		{"reject", agentruntime.ReviewOutcome{Verdict: agentruntime.VerdictReject}, nil,
			envelope.ReviewReject, ""},
		{"nil err but no verdict is bad_verdict (never approve)",
			agentruntime.ReviewOutcome{Verdict: agentruntime.VerdictNone}, nil,
			envelope.ReviewError, envelope.ReviewBadVerdict},
		{"empty review", agentruntime.ReviewOutcome{Status: agentruntime.TurnLost}, agentruntime.ErrEmptyReview,
			envelope.ReviewError, envelope.ReviewEmptyDiff},
		{"child death", agentruntime.ReviewOutcome{Status: agentruntime.TurnLost},
			&agentruntime.ProcessExitedError{Detail: "stdout closed"},
			envelope.ReviewError, envelope.ReviewRuntimeLost},
		{"no verdict parsed", agentruntime.ReviewOutcome{Status: agentruntime.TurnAnswered}, agentruntime.ErrNoVerdict,
			envelope.ReviewError, envelope.ReviewBadVerdict},
		{"non-success result", agentruntime.ReviewOutcome{Status: agentruntime.TurnError}, resultErr,
			envelope.ReviewError, envelope.ReviewRuntimeError},
		{"ctx cancel is lost", agentruntime.ReviewOutcome{Status: agentruntime.TurnLost}, context.Canceled,
			envelope.ReviewError, envelope.ReviewRuntimeLost},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapReviewOutcome(tc.out, tc.err)
			if got.Verdict != tc.wantVerdict {
				t.Fatalf("verdict = %q, want %q", got.Verdict, tc.wantVerdict)
			}
			if got.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", got.Code, tc.wantCode)
			}
			// The result must always be a valid wire payload (validate passes).
			p := envelope.ReviewPayload{Task: "t", Verdict: got.Verdict, Code: got.Code}
			if _, err := envelope.New(envelope.KindReview, "expert", envelope.SubjectReview("t"), p); err != nil {
				t.Fatalf("mapped result is not a valid review payload: %v", err)
			}
		})
	}
}

// TestMapReviewOutcomeCarriesSessionMetadata proves the session id and turn
// count survive the mapping — the same-session-reuse proof must reach the wire.
func TestMapReviewOutcomeCarriesSessionMetadata(t *testing.T) {
	got := mapReviewOutcome(agentruntime.ReviewOutcome{
		Verdict: agentruntime.VerdictApprove, SessionID: "sess-7", NumTurns: 5, Notes: "lgtm",
	}, nil)
	if got.SessionID != "sess-7" || got.NumTurns != 5 || got.Notes != "lgtm" {
		t.Fatalf("metadata lost: %+v", got)
	}
}
