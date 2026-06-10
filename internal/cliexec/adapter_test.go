package cliexec_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/cliexec"
)

// fakeClaudeScript writes a shell script that ignores all args and emits a
// single result envelope. mode controls the envelope subtype:
//
//   - "success" — subtype=success, is_error=false
//   - "error"   — subtype=error, is_error=true, exit 0
//   - "exit1"   — no output, exits 1 (simulates a failed CLI invocation)
//   - "hang"    — sleeps forever (tests context cancellation)
//
// The script ignores all args, so it works regardless of what flags
// ClaudeAdapter prepends (-p --output-format json --model …).
func fakeClaudeScript(t *testing.T, mode string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell script fake CLI not supported on Windows")
	}

	var body string
	switch mode {
	case "success":
		body = `cat <<'EOF'
{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"test-session","total_cost_usd":0.001,"api_error_status":null}
EOF`
	case "error":
		body = `cat <<'EOF'
{"type":"result","subtype":"error","is_error":true,"result":"something went wrong","session_id":"test-session","total_cost_usd":0.0,"api_error_status":null}
EOF`
	case "exit1":
		body = `echo "fake: invocation failed" >&2
exit 1`
	case "hang":
		body = `sleep 999`
	default:
		t.Fatalf("unknown fake CLI mode %q", mode)
	}

	dir := t.TempDir()
	script := filepath.Join(dir, "fake-cli")
	content := "#!/bin/sh\n" + body + "\n"
	if err := os.WriteFile(script, []byte(content), 0o755); err != nil {
		t.Fatalf("write fake CLI script: %v", err)
	}
	return script
}

// TestClaudeAdapterCapabilities checks that the verified adapter reports
// full capability.
func TestClaudeAdapterCapabilities(t *testing.T) {
	a := cliexec.ClaudeAdapter{}
	caps := a.Capabilities()
	if !caps.StructuredOutput {
		t.Error("ClaudeAdapter: StructuredOutput should be true")
	}
	if !caps.ModelPin {
		t.Error("ClaudeAdapter: ModelPin should be true")
	}
	if !caps.HookPreToolUse {
		t.Error("ClaudeAdapter: HookPreToolUse should be true")
	}
	if !caps.HookStop {
		t.Error("ClaudeAdapter: HookStop should be true")
	}
	if !caps.HookSessionLifecycle {
		t.Error("ClaudeAdapter: HookSessionLifecycle should be true")
	}
}

// TestClaudeAdapterInvokeSuccess verifies the happy-path: fake CLI emits a
// success result envelope, adapter returns the raw bytes which parse cleanly.
func TestClaudeAdapterInvokeSuccess(t *testing.T) {
	bin := fakeClaudeScript(t, "success")
	a := cliexec.ClaudeAdapter{Binary: bin}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := a.Invoke(ctx, "do the work", cliexec.InvokeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}

	// The output must be parseable JSON with type=result.
	var envelope map[string]interface{}
	if err := json.Unmarshal(out, &envelope); err != nil {
		t.Fatalf("output not JSON: %v: %s", err, out)
	}
	if got := envelope["type"]; got != "result" {
		t.Errorf("expected type=result, got %v", got)
	}
	if got := envelope["subtype"]; got != "success" {
		t.Errorf("expected subtype=success, got %v", got)
	}
}

// TestClaudeAdapterInvokeExitError verifies that a non-zero exit maps to a
// non-nil error (never-fake-success contract).
func TestClaudeAdapterInvokeExitError(t *testing.T) {
	bin := fakeClaudeScript(t, "exit1")
	a := cliexec.ClaudeAdapter{Binary: bin}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := a.Invoke(ctx, "fail", cliexec.InvokeOptions{})
	if err == nil {
		t.Fatal("expected error for non-zero exit, got nil")
	}
}

// TestClaudeAdapterInvokeContextCancel verifies that a cancelled context kills
// the child and returns a context error.
func TestClaudeAdapterInvokeContextCancel(t *testing.T) {
	bin := fakeClaudeScript(t, "hang")
	a := cliexec.ClaudeAdapter{Binary: bin}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := a.Invoke(ctx, "hang", cliexec.InvokeOptions{WaitDelay: 100 * time.Millisecond})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

// TestClaudeAdapterInvokeModelPinned verifies that a Model in InvokeOptions
// is accepted and the adapter returns a result (the fake script ignores args).
func TestClaudeAdapterInvokeModelPinned(t *testing.T) {
	bin := fakeClaudeScript(t, "success")
	a := cliexec.ClaudeAdapter{Binary: bin}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := a.Invoke(ctx, "work", cliexec.InvokeOptions{Model: "sonnet"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}
}

// TestClaudeAdapterInvokeWorkDir verifies that WorkDir is accepted and does
// not prevent the fake CLI from running.
func TestClaudeAdapterInvokeWorkDir(t *testing.T) {
	bin := fakeClaudeScript(t, "success")
	a := cliexec.ClaudeAdapter{Binary: bin}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	out, err := a.Invoke(ctx, "work", cliexec.InvokeOptions{WorkDir: dir})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}
}

// TestClaudeAdapterInvokeEnvForwarded verifies that custom Env overrides in
// InvokeOptions are accepted (the fake script does not validate them, but the
// call must not fail).
func TestClaudeAdapterInvokeEnvForwarded(t *testing.T) {
	bin := fakeClaudeScript(t, "success")
	a := cliexec.ClaudeAdapter{Binary: bin}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := a.Invoke(ctx, "work", cliexec.InvokeOptions{
		Env: append(os.Environ(), "MESH_SOCKET=/tmp/fake.sock"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty output")
	}
}

// TestStubAdaptersReturnNotImplemented confirms that all three stub adapters
// return ErrNotImplemented when Invoke is called.
func TestStubAdaptersReturnNotImplemented(t *testing.T) {
	type adapterCase struct {
		name    string
		adapter cliexec.Adapter
	}
	cases := []adapterCase{
		{"CodexAdapter", cliexec.CodexAdapter{}},
		{"CursorAdapter", cliexec.CursorAdapter{}},
		{"AiderAdapter", cliexec.AiderAdapter{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.adapter.Invoke(context.Background(), "prompt", cliexec.InvokeOptions{})
			if err == nil {
				t.Fatalf("%s: expected ErrNotImplemented, got nil", tc.name)
			}
			if !errors.Is(err, cliexec.ErrNotImplemented) {
				t.Errorf("%s: expected errors.Is(err, ErrNotImplemented), got %v", tc.name, err)
			}
		})
	}
}

// TestStubAdaptersCapabilityFlags confirms that stub adapters report
// StructuredOutput=false (since they cannot be used without a verified contract).
func TestStubAdaptersCapabilityFlags(t *testing.T) {
	type adapterCase struct {
		name    string
		adapter cliexec.Adapter
	}
	cases := []adapterCase{
		{"CodexAdapter", cliexec.CodexAdapter{}},
		{"CursorAdapter", cliexec.CursorAdapter{}},
		{"AiderAdapter", cliexec.AiderAdapter{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			caps := tc.adapter.Capabilities()
			if caps.StructuredOutput {
				t.Errorf("%s: StructuredOutput should be false (unverified)", tc.name)
			}
			if caps.Notes == "" {
				t.Errorf("%s: Notes should explain the verification status", tc.name)
			}
		})
	}
}

// TestAdapterInterfaceSatisfied is a compile-time check that all four adapter
// types satisfy the Adapter interface. If any type doesn't implement the
// interface, this test file won't compile.
var (
	_ cliexec.Adapter = cliexec.ClaudeAdapter{}
	_ cliexec.Adapter = cliexec.CodexAdapter{}
	_ cliexec.Adapter = cliexec.CursorAdapter{}
	_ cliexec.Adapter = cliexec.AiderAdapter{}
)
