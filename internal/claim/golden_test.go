package claim

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// update rewrites the golden file from the current record shape:
//
//	go test ./internal/claim -run TestGolden -update
var update = flag.Bool("update", false, "rewrite golden files from the current contract")

// goldenRecord is a representative claims-bucket record with every field set
// and a pinned timestamp, so the regenerated file differs only when the
// contract differs.
func goldenRecord() Record {
	return Record{
		Agent: "codex-7",
		Path:  "src/EventForm.tsx",
		Repo:  "demo",
		TS:    time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
	}
}

// TestGoldenClaimRecord freezes the encoded shape of the claims-KV record —
// the one authority for "who holds which path". A json-tag rename or dropped
// field fails here loudly instead of surfacing in downstream behavior tests.
func TestGoldenClaimRecord(t *testing.T) {
	path := filepath.Join("testdata", "claim-record.v1.json")
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

// TestKeyContract pins the claims-bucket key format. NUL separates the parts
// because it can appear in neither a valid repo id nor a cleaned path, so
// ("a","b/c") and ("a/b","c") cannot collide.
func TestKeyContract(t *testing.T) {
	if got, want := Key("a", "b/c"), "a\x00b/c"; got != want {
		t.Errorf("Key(a, b/c) = %q, want %q", got, want)
	}
	if Key("a", "b/c") == Key("a/b", "c") {
		t.Error("Key must not collide across repo/path splits")
	}
}
