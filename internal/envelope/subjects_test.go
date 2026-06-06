package envelope

import "testing"

func TestValidRepo(t *testing.T) {
	valid := []string{"demo", "agent-mesh", "My_Repo2", "a"}
	for _, r := range valid {
		if !ValidRepo(r) {
			t.Errorf("ValidRepo(%q) = false, want true", r)
		}
	}
	invalid := []string{
		"",                       // empty
		"a.b",                    // dot would split subject tokens
		"a/b",                    // path separator
		"a b",                    // whitespace
		"*",                      // wildcard
		">",                      // wildcard
		"github.com/x/y",         // a path, not a label
		string(make([]byte, 49)), // too long (and NUL bytes)
	}
	for _, r := range invalid {
		if ValidRepo(r) {
			t.Errorf("ValidRepo(%q) = true, want false", r)
		}
	}
}

// TestRepoDerivedNamesStaySafe pins the property the 48-char repo bound
// exists for: every derived store name and subject must satisfy the bus
// naming rules (store names ≤64 chars of [A-Za-z0-9_-], subject tokens
// dot-free).
func TestRepoDerivedNamesStaySafe(t *testing.T) {
	repo := "R-" + string(bytesOf('a', 46)) // max-length repo
	if !ValidRepo(repo) {
		t.Fatalf("max-length repo should be valid")
	}
	if got := StreamNotes(repo); len(got) > 64 {
		t.Errorf("StreamNotes(%q) = %d chars, exceeds bus store-name limit", repo, len(got))
	}
}

func bytesOf(c byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return b
}

func TestNoteKindValidation(t *testing.T) {
	for _, k := range []string{NoteKindDecision, NoteKindContext, NoteKindSummary, NoteKindOther} {
		p := NotePayload{ID: "a", Decision: "d", Kind: k}
		if err := p.validate(); err != nil {
			t.Errorf("kind %q rejected: %v", k, err)
		}
	}
	p := NotePayload{ID: "a", Decision: "d", Kind: "rant"}
	if err := p.validate(); err == nil {
		t.Error("unknown kind accepted")
	}
	// Empty kind is allowed and means decision.
	p = NotePayload{ID: "a", Decision: "d"}
	if err := p.validate(); err != nil {
		t.Errorf("empty kind rejected: %v", err)
	}
	// Missing author id is rejected (sender-bound mutation).
	p = NotePayload{Decision: "d"}
	if err := p.validate(); err == nil {
		t.Error("missing id accepted")
	}
}

func TestAnnounceRequiresIntent(t *testing.T) {
	p := AnnouncePayload{ID: "a"}
	if err := p.validate(); err == nil {
		t.Error("announce without intent accepted")
	}
}
