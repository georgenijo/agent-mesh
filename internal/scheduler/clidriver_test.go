package scheduler

// CLIDriver contract tests against a REAL child process (a shell script
// speaking the one-shot result contract) — no LLM, no API key. The mapping
// under test is the locked fleet posture: success → ok with cost,
// api_error_status → rate_limited, anything else → worker_failed.

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// workerScript writes a fake worker emitting one literal JSON line on stdout.
func workerScript(t *testing.T, payload string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell script worker stub")
	}
	path := filepath.Join(t.TempDir(), "fakeworker.sh")
	script := "#!/bin/sh\ncat <<'WORKER_EOF'\n" + payload + "\nWORKER_EOF\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func runCLI(t *testing.T, payload string) (Result, error) {
	t.Helper()
	d := CLIDriver{CLI: workerScript(t, payload), Timeout: 5 * time.Second}
	rec := task.Record{ID: envelope.NewID(), Job: envelope.NewID(), Node: "impl",
		Title: "implement", Role: "builder"}
	w, err := d.Spawn(context.Background(), rec)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer func() {
		if terr := w.Teardown(); terr != nil {
			t.Fatalf("teardown: %v", terr)
		}
	}()
	return w.Run(context.Background())
}

func TestCLIDriverSuccessCarriesCost(t *testing.T) {
	res, err := runCLI(t, `{"type":"result","subtype":"success","is_error":false,`+
		`"result":"did the work","session_id":"s1","num_turns":1,"duration_ms":1,"total_cost_usd":0.0123}`)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Succeeded() {
		t.Fatalf("result code = %q, want success", res.Code)
	}
	if res.CostUSD != 0.0123 {
		t.Fatalf("CostUSD = %v, want 0.0123", res.CostUSD)
	}
	if res.Summary != "did the work" || res.SessionID != "s1" {
		t.Fatalf("summary/session = %q/%q", res.Summary, res.SessionID)
	}
}

func TestCLIDriverErrorResultIsNeverFakeSuccess(t *testing.T) {
	res, err := runCLI(t, `{"type":"result","subtype":"error_during_execution","is_error":true,`+
		`"result":"","session_id":"s1","total_cost_usd":0.002}`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != envelope.WorkerFailed {
		t.Fatalf("code = %q, want worker_failed", res.Code)
	}
	if res.CostUSD != 0.002 {
		t.Fatalf("CostUSD = %v, want 0.002 (failed runs still cost money)", res.CostUSD)
	}
}

func TestCLIDriverAPIErrorMapsToRateLimited(t *testing.T) {
	res, err := runCLI(t, `{"type":"result","subtype":"success","is_error":false,`+
		`"result":"","session_id":"s1","api_error_status":429}`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != envelope.WorkerRateLimited {
		t.Fatalf("code = %q, want rate_limited", res.Code)
	}
}

func TestCLIDriverNonResultStdoutIsTypedError(t *testing.T) {
	if _, err := runCLI(t, `not json at all`); err == nil {
		t.Fatal("prose stdout produced a result, want error")
	}
	if _, err := runCLI(t, `{"type":"assistant"}`); err == nil {
		t.Fatal("non-result envelope produced a result, want error")
	}
}

func TestCLIDriverMissingBinary(t *testing.T) {
	d := CLIDriver{}
	if _, err := d.Spawn(context.Background(), task.Record{}); err == nil {
		t.Fatal("empty CLI spawned, want error")
	}
	d = CLIDriver{CLI: filepath.Join(t.TempDir(), "nope"), Timeout: time.Second}
	w, err := d.Spawn(context.Background(), task.Record{Title: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Run(context.Background()); err == nil {
		t.Fatal("missing binary ran, want error")
	}
	if terr := w.Teardown(); terr != nil {
		t.Fatal(terr)
	}
}

func TestWorkerPromptRendersTaskFields(t *testing.T) {
	rec := task.Record{Title: "implement RRULE", Description: "weekly events",
		Files: []string{"src/x.go"}, Acceptance: []string{"tests pass"}}
	p := workerPrompt(rec)
	for _, want := range []string{"implement RRULE", "weekly events", "src/x.go", "tests pass"} {
		if !strings.Contains(p, want) {
			t.Errorf("prompt missing %q:\n%s", want, p)
		}
	}
}
