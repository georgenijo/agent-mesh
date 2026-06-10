package task

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
//	go test ./internal/task -run TestGolden -update
var update = flag.Bool("update", false, "rewrite golden files from the current contract")

// goldenRecord is a fully-populated task — every field set, pinned timestamp
// and fixed UUIDv7 ids — so every json tag is frozen and the regenerated file
// differs only when the contract differs.
func goldenRecord() Record {
	return Record{
		ID:          "01976f00-0000-7000-8000-0000000000a1",
		Job:         "01976f00-0000-7000-8000-00000000007e",
		Node:        "impl",
		Title:       "Implement RRULE builder",
		Description: "Add the recurrence-rule builder behind the events API.",
		Role:        "builder",
		DependsOn:   []string{"01976f00-0000-7000-8000-0000000000a0"},
		Files:       []string{"src/api.go", "src/rrule.go"},
		Acceptance:  []string{"unit tests pass", "API returns TZID-qualified times"},
		State:       envelope.TaskPending,
		CreatedAt:   time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC),
	}
}

// goldenEvent is a TaskPending transition with every field set.
func goldenEvent() Event {
	return Event{
		ID:     "01976f00-0000-7000-8000-0000000000a1",
		Job:    "01976f00-0000-7000-8000-00000000007e",
		From:   envelope.TaskPending,
		To:     envelope.TaskPending,
		By:     "coordinator",
		At:     time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC),
		Reason: "triaged",
	}
}

// TestGoldenTaskRecord freezes the encoded shape of the tasks-KV record — the
// one authority for task state and the DAG the scheduler (#25) reads.
func TestGoldenTaskRecord(t *testing.T) {
	checkGolden(t, "task-record.v1.json", goldenRecord(), func(rec Record) {
		if !envelope.ValidTaskState(rec.State) {
			t.Errorf("golden state %q is not a valid TaskState", rec.State)
		}
	})
}

// TestGoldenTaskEvent freezes the encoded shape of a task transition event.
func TestGoldenTaskEvent(t *testing.T) {
	checkGolden(t, "task-event.v1.json", goldenEvent(), func(ev Event) {
		if !envelope.ValidTaskState(ev.To) {
			t.Errorf("golden event To %q is not a valid TaskState", ev.To)
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

// TestOptionalFieldsOmitted pins which fields a minimal task omits on the
// wire: the omitempty set is contract too.
func TestOptionalFieldsOmitted(t *testing.T) {
	rec := Record{
		ID:        "01976f00-0000-7000-8000-0000000000a1",
		Job:       "01976f00-0000-7000-8000-00000000007e",
		Node:      "solo",
		Title:     "do the thing",
		Role:      "builder",
		State:     envelope.TaskPending,
		CreatedAt: time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"description", "dependsOn", "files", "acceptance"} {
		if _, ok := m[key]; ok {
			t.Errorf("empty %q should be omitted: %s", key, data)
		}
	}
	for _, key := range []string{"id", "job", "node", "title", "role", "state", "createdAt"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing required key %q: %s", key, data)
		}
	}
}
