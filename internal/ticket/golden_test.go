package ticket

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// update rewrites the golden file from the current record shape:
//
//	go test ./internal/ticket -run TestGolden -update
var update = flag.Bool("update", false, "rewrite golden files from the current contract")

// goldenRecord is a fully-answered ticket — every field set, pinned
// timestamps and a fixed UUIDv7 ticket id — so every json tag is frozen and
// the regenerated file differs only when the contract differs.
func goldenRecord() Record {
	return Record{
		Ticket:     "01976f00-0000-7000-8000-00000000007e",
		State:      envelope.TicketAnswered,
		Asker:      "codex-7",
		Role:       "auth",
		To:         "claude-2",
		Q:          "RLS recursion fix?",
		Ctx:        "policy on users joins itself",
		CreatedAt:  time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
		ExpiresAt:  time.Date(2026, 6, 5, 0, 5, 0, 0, time.UTC),
		AcceptedBy: "claude-2",
		AnsweredBy: "claude-2",
		AnsweredAt: time.Date(2026, 6, 5, 0, 1, 0, 0, time.UTC),
		Answer:     "use is_admin() SECURITY DEFINER",
	}
}

// TestGoldenTicketRecord freezes the encoded shape of the tickets-KV record —
// the one authority for ticket state. A json-tag rename or dropped field
// fails here loudly instead of surfacing in downstream behavior tests.
func TestGoldenTicketRecord(t *testing.T) {
	path := filepath.Join("testdata", "ticket-record.v1.json")
	if *update {
		data, err := json.MarshalIndent(goldenRecord(), "", "  ")
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		data = append(data, '\n')
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v (run with -update to generate)", err)
	}

	var rec Record
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	if !reflect.DeepEqual(rec, goldenRecord()) {
		t.Errorf("decoded record = %+v, want %+v", rec, goldenRecord())
	}
	if !envelope.ValidTicketState(rec.State) {
		t.Errorf("golden state %q is not a valid TicketState", rec.State)
	}

	encoded, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got, want any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("re-encoded record drifted from golden\ngolden: %s\ngot:    %s", bytes.TrimSpace(data), encoded)
	}
}

// TestOptionalFieldsOmitted pins which fields an open, unanswered ticket
// omits on the wire: the omitempty/omitzero set is contract too.
func TestOptionalFieldsOmitted(t *testing.T) {
	rec := Record{
		Ticket:    "01976f00-0000-7000-8000-00000000007e",
		State:     envelope.TicketOpen,
		Asker:     "codex-7",
		Role:      "auth",
		Q:         "RLS recursion fix?",
		CreatedAt: time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
		ExpiresAt: time.Date(2026, 6, 5, 0, 5, 0, 0, time.UTC),
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"to", "ctx", "acceptedBy", "answeredBy", "answeredAt", "answer"} {
		if _, ok := m[key]; ok {
			t.Errorf("empty %q should be omitted: %s", key, data)
		}
	}
	for _, key := range []string{"ticket", "state", "asker", "role", "q", "createdAt", "expiresAt"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing required key %q: %s", key, data)
		}
	}
}
