package job

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

// update rewrites the golden files from the current record shape:
//
//	go test ./internal/job -run TestGolden -update
var update = flag.Bool("update", false, "rewrite golden files from the current contract")

// goldenRecord is a fully-populated job — every field set, pinned timestamp and
// a fixed UUIDv7 id — so every json tag is frozen and the regenerated file
// differs only when the contract differs.
func goldenRecord() Record {
	return Record{
		ID:        "01976f00-0000-7000-8000-00000000007e",
		Repo:      "owner/repo",
		Source:    SourceGitHub,
		SourceRef: "owner/repo#123",
		Title:     "Calendar export drops timezone",
		Body:      "ICS export writes naive local times; should be UTC with TZID.",
		State:     envelope.JobOpen,
		CreatedAt: time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC),
	}
}

// goldenEvent is a JobOpen transition with every field set.
func goldenEvent() Event {
	return Event{
		ID:     "01976f00-0000-7000-8000-00000000007e",
		From:   envelope.JobOpen,
		To:     envelope.JobOpen,
		By:     "codex-7",
		At:     time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC),
		Reason: "submitted",
	}
}

// TestGoldenJobRecord freezes the encoded shape of the jobs-KV record — the one
// authority for job state. A json-tag rename or dropped field fails here loudly
// instead of surfacing in downstream behavior tests.
func TestGoldenJobRecord(t *testing.T) {
	checkGolden(t, "job-record.v1.json", goldenRecord(), func(rec Record) {
		if !envelope.ValidJobState(rec.State) {
			t.Errorf("golden state %q is not a valid JobState", rec.State)
		}
	})
}

// TestGoldenJobEvent freezes the encoded shape of a job transition event.
func TestGoldenJobEvent(t *testing.T) {
	checkGolden(t, "job-event.v1.json", goldenEvent(), func(ev Event) {
		if !envelope.ValidJobState(ev.To) {
			t.Errorf("golden event To %q is not a valid JobState", ev.To)
		}
	})
}

func checkGolden[T any](t *testing.T, name string, want T, extra func(T)) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		data, err := json.MarshalIndent(want, "", "  ")
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

	var got T
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decoded = %+v, want %+v", got, want)
	}
	if extra != nil {
		extra(got)
	}

	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var gotAny, wantAny any
	if err := json.Unmarshal(encoded, &gotAny); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := json.Unmarshal(data, &wantAny); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(gotAny, wantAny) {
		t.Errorf("re-encoded drifted from golden\ngolden: %s\ngot:    %s", bytes.TrimSpace(data), encoded)
	}
}

// TestOptionalFieldsOmitted pins which fields a minimal manual job omits on the
// wire: the omitempty set is contract too.
func TestOptionalFieldsOmitted(t *testing.T) {
	rec := Record{
		ID:        "01976f00-0000-7000-8000-00000000007e",
		Repo:      "demo",
		Source:    SourceManual,
		Title:     "do the thing",
		State:     envelope.JobOpen,
		CreatedAt: time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"sourceRef", "body"} {
		if _, ok := m[key]; ok {
			t.Errorf("empty %q should be omitted: %s", key, data)
		}
	}
	for _, key := range []string{"id", "repo", "source", "title", "state", "createdAt"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing required key %q: %s", key, data)
		}
	}
}
