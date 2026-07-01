package dashboard

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/config"
)

// TestServeRunLog covers the live-transcript endpoint: it serves a worker's
// per-task transcript, rejects path traversal in the task id, and treats an
// absent transcript as an empty 200 (not an error) so the UI can poll a run
// that has not started streaming yet.
func TestServeRunLog(t *testing.T) {
	dir := t.TempDir()
	d := &Dashboard{cfg: config.Config{MeshDir: dir}}
	if err := os.MkdirAll(d.cfg.RunsDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	const task = "019eb8d4-7e0c-7c55-b704-768254881110"
	if err := os.WriteFile(filepath.Join(d.cfg.RunsDir(), task+".jsonl"),
		[]byte(`{"type":"assistant","marker":"HELLO-9"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Happy path: the transcript is served.
	rr := httptest.NewRecorder()
	d.serveRunLog(rr, httptest.NewRequest("GET", "/api/runlog?task="+task, nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("happy path code = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "HELLO-9") {
		t.Fatalf("transcript not served: %q", rr.Body.String())
	}

	// Path traversal in the task id is rejected before any file access.
	rr = httptest.NewRecorder()
	d.serveRunLog(rr, httptest.NewRequest("GET", "/api/runlog?task=../../config", nil))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("traversal task id code = %d, want 400", rr.Code)
	}

	// An absent transcript is an empty 200, not an error.
	rr = httptest.NewRecorder()
	d.serveRunLog(rr, httptest.NewRequest("GET", "/api/runlog?task=019eb8d4-0000-7000-8000-000000000000", nil))
	if rr.Code != http.StatusOK || rr.Body.Len() != 0 {
		t.Fatalf("absent transcript code = %d body = %q, want 200 + empty", rr.Code, rr.Body.String())
	}
}
