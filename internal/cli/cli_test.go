package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/socket"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// run invokes the CLI against an isolated MESH_DIR with autostart disabled
// paths (no daemons are running in these tests).
func run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	t.Setenv(config.EnvMeshDir, testsock.Dir(t))
	t.Setenv(config.EnvAgentSocket, "")
	var stdout, stderr bytes.Buffer
	code := Run(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestNoArgsIsUsage(t *testing.T) {
	code, _, _ := run(t)
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d", code, ExitUsage)
	}
}

func TestUnknownCommandIsUsage(t *testing.T) {
	code, _, stderr := run(t, "frobnicate")
	if code != ExitUsage || !strings.Contains(stderr, "unknown command") {
		t.Fatalf("exit = %d stderr = %q", code, stderr)
	}
}

func TestJoinRequiresNameAndRole(t *testing.T) {
	code, _, _ := run(t, "join", "--name", "x")
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d", code, ExitUsage)
	}
}

func TestJoinRejectsInvalidName(t *testing.T) {
	code, _, stderr := run(t, "join", "--name", "bad.name", "--role", "builder")
	if code != ExitUsage || !strings.Contains(stderr, "invalid name") {
		t.Fatalf("exit = %d stderr = %q", code, stderr)
	}
}

func TestWhoWithoutSidecarIsNotJoined(t *testing.T) {
	code, _, stderr := run(t, "who")
	if code != ExitNotJoined {
		t.Fatalf("exit = %d (stderr %q), want %d", code, stderr, ExitNotJoined)
	}
}

func TestStatusWithoutSidecarIsNotJoined(t *testing.T) {
	code, _, _ := run(t, "status", "working")
	if code != ExitNotJoined {
		t.Fatalf("exit = %d, want %d", code, ExitNotJoined)
	}
}

func TestStatusRequiresText(t *testing.T) {
	code, _, _ := run(t, "status")
	if code != ExitUsage {
		t.Fatalf("exit = %d, want %d", code, ExitUsage)
	}
}

func TestJoinNoAutostartFailsNotJoined(t *testing.T) {
	code, _, _ := run(t, "join", "--name", "x", "--role", "builder", "--no-autostart")
	if code != ExitNotJoined {
		t.Fatalf("exit = %d, want %d", code, ExitNotJoined)
	}
}

func TestOpsMissingMeshDirFails(t *testing.T) {
	t.Setenv(config.EnvMeshDir, "/tmp/mesh-ops-missing-dir-does-not-exist")
	var stdout, stderr bytes.Buffer
	code := Run([]string{"ops"}, &stdout, &stderr)
	if code != ExitError {
		t.Fatalf("exit = %d, want %d (stderr %q)", code, ExitError, stderr.String())
	}
	if !strings.Contains(stderr.String(), "mesh dir") && !strings.Contains(stderr.String(), "no such file") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestOpsJSONOnEmptyMeshDir(t *testing.T) {
	dir := testsock.Dir(t)
	t.Setenv(config.EnvMeshDir, dir)
	var stdout, stderr bytes.Buffer
	code := Run([]string{"ops", "--json"}, &stdout, &stderr)
	if code != ExitOK {
		t.Fatalf("exit = %d, want %d (stderr %q)", code, ExitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"meshDir"`) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

// TestExitCodeConstants pins the numeric values of all documented exit codes.
// These are the frozen taxonomy (ARCHITECTURE §4, agent-runbook.md):
// 0 ok · 1 error · 2 usage · 3 no-answer-yet · 4 no-such-ticket · 5 not-joined
// · 6 claim-lost · 7 dirty.
// Changing any constant here is a contract break that requires a decision log entry.
func TestExitCodeConstants(t *testing.T) {
	type codeRow struct {
		name string
		got  int
		want int
	}
	rows := []codeRow{
		{"ExitOK", ExitOK, 0},
		{"ExitError", ExitError, 1},
		{"ExitUsage", ExitUsage, 2},
		{"ExitNoAnswer", ExitNoAnswer, 3},
		{"ExitNoTicket", ExitNoTicket, 4},
		{"ExitNotJoined", ExitNotJoined, 5},
		{"ExitClaimLost", ExitClaimLost, 6},
		{"ExitDirty", ExitDirty, 7},
	}
	for _, row := range rows {
		if row.got != row.want {
			t.Errorf("%s = %d, want %d (frozen contract — requires decision log entry to change)",
				row.name, row.got, row.want)
		}
	}
}

// TestUnitExitCodeMatrix consolidates unit-level exit-code coverage for every
// verb path that does not require a live daemon. Rows that need a real
// coordinator are in test/e2e/contract_test.go TestExitCodeMatrix.
//
// Codes 3 and 4 are reserved for the P2 poll verb; they are rows here so the
// full taxonomy is enumerated in one place.
func TestUnitExitCodeMatrix(t *testing.T) {
	type row struct {
		name     string
		skip     string
		args     []string
		wantCode int
	}
	rows := []row{
		// --- exit 0 ---
		{"version_ok", "", []string{"version"}, ExitOK},
		{"help_ok", "", []string{"help"}, ExitOK},

		// --- exit 2: usage ---
		{"no_args", "", []string{}, ExitUsage},
		{"unknown_verb", "", []string{"frobnicate"}, ExitUsage},
		{"join_no_name", "", []string{"join", "--role", "builder"}, ExitUsage},
		{"join_no_role", "", []string{"join", "--name", "x"}, ExitUsage},
		{"join_invalid_name", "", []string{"join", "--name", "bad.name", "--role", "r"}, ExitUsage},
		{"status_no_text", "", []string{"status"}, ExitUsage},

		// --- exit 5: not joined (no sidecar in empty MESH_DIR) ---
		{"who_not_joined", "", []string{"who"}, ExitNotJoined},
		{"status_not_joined", "", []string{"status", "hi"}, ExitNotJoined},
		{"leave_not_joined", "", []string{"leave"}, ExitNotJoined},
		{"join_no_autostart", "", []string{"join", "--name", "x", "--role", "r", "--no-autostart"}, ExitNotJoined},

		// --- exit 3 reserved ---
		{
			name:     "reserved_exit_3_no_answer_yet",
			skip:     "exit 3 reserved for P2 poll (not yet covered at unit level)",
			args:     nil,
			wantCode: ExitNoAnswer,
		},

		// --- exit 4 reserved ---
		{
			name:     "reserved_exit_4_no_such_ticket",
			skip:     "exit 4 reserved for P2 poll (not yet covered at unit level)",
			args:     nil,
			wantCode: ExitNoTicket,
		},
	}
	for _, row := range rows {
		row := row
		t.Run(row.name, func(t *testing.T) {
			if row.skip != "" {
				t.Skip(row.skip)
			}
			code, _, _ := run(t, row.args...)
			if code != row.wantCode {
				t.Errorf("exit = %d, want %d", code, row.wantCode)
			}
		})
	}
}

// TestExitForCode covers the exitForCode mapping that turns socket error codes
// into process exit codes.
func TestExitForCode(t *testing.T) {
	rows := []struct {
		code     string
		wantExit int
	}{
		{socket.CodeNotJoined, ExitNotJoined},
		{socket.CodeBadRequest, ExitUsage},
		{socket.CodeUnavailable, ExitError},
		{socket.CodeInternal, ExitError},
		{"unknown_code", ExitError},
		{"", ExitError},
	}
	for _, row := range rows {
		got := exitForCode(row.code)
		if got != row.wantExit {
			t.Errorf("exitForCode(%q) = %d, want %d", row.code, got, row.wantExit)
		}
	}
}

// TestSetupErrJSON verifies that setup-time errors (config load failure,
// not-joined, ambiguous socket) write a JSON error object to stdout — not
// stderr — when --json is set, and exit with the correct code.
// emitSetupErr is the code path under test; emit() handles post-socket errors.
func TestSetupErrJSON(t *testing.T) {
	// assertSetupErrJSON is a shared helper that validates the JSON shape and
	// confirms it did not leak to stderr.
	assertSetupErrJSON := func(t *testing.T, stdout, stderr string) {
		t.Helper()
		outTrimmed := strings.TrimSpace(stdout)
		if outTrimmed == "" {
			t.Fatal("stdout empty: JSON error object must go to stdout with --json")
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(outTrimmed), &obj); err != nil {
			t.Fatalf("stdout is not valid JSON: %v\nstdout: %s", err, stdout)
		}
		if ok, exists := obj["ok"]; !exists || ok != false {
			t.Errorf("JSON missing or wrong 'ok': %v", obj)
		}
		if _, exists := obj["message"]; !exists {
			t.Errorf("JSON missing 'message': %v", obj)
		}
		// The JSON object must not appear on stderr.
		if strings.Contains(stderr, `"ok"`) {
			t.Errorf("JSON error object leaked to stderr: %q", stderr)
		}
	}

	t.Run("config_load_failure", func(t *testing.T) {
		// An invalid duration makes config.Load return an error before any
		// socket lookup; runWho calls emitSetupErr with the parsed --json flag.
		t.Setenv(config.EnvHeartbeatInterval, "not-a-duration")
		code, stdout, stderr := run(t, "who", "--json")
		if code != ExitError {
			t.Fatalf("exit = %d, want %d (ExitError)\nstdout: %s\nstderr: %s",
				code, ExitError, stdout, stderr)
		}
		assertSetupErrJSON(t, stdout, stderr)
	})

	t.Run("not_joined", func(t *testing.T) {
		// run() uses an empty MESH_DIR so resolveSocket finds no sockets and
		// returns ExitNotJoined; emitSetupErr must route the error to stdout.
		code, stdout, stderr := run(t, "who", "--json")
		if code != ExitNotJoined {
			t.Fatalf("exit = %d, want %d (ExitNotJoined)\nstdout: %s\nstderr: %s",
				code, ExitNotJoined, stdout, stderr)
		}
		assertSetupErrJSON(t, stdout, stderr)
	})

	t.Run("ambiguous_socket", func(t *testing.T) {
		// Create two stub .sock files in agents/ so resolveSocket returns
		// ExitUsage (ambiguous — need --socket or $MESH_SOCKET to pick one).
		dir := testsock.Dir(t)
		agentsDir := filepath.Join(dir, "agents")
		if err := os.MkdirAll(agentsDir, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"alpha.sock", "beta.sock"} {
			f, err := os.Create(filepath.Join(agentsDir, name))
			if err != nil {
				t.Fatal(err)
			}
			f.Close()
		}
		t.Setenv(config.EnvMeshDir, dir)
		t.Setenv(config.EnvAgentSocket, "")
		var stdout, stderr bytes.Buffer
		code := Run([]string{"who", "--json"}, &stdout, &stderr)
		if code != ExitUsage {
			t.Fatalf("exit = %d, want %d (ExitUsage)\nstdout: %s\nstderr: %s",
				code, ExitUsage, stdout.String(), stderr.String())
		}
		assertSetupErrJSON(t, stdout.String(), stderr.String())
	})
}

// TestErrorJSONShape pins the {"ok":false,"code":"...","message":"..."} shape
// that emit() writes to stdout when --json is set and the verb fails with a
// socket-level error response. A fake socket server is used so this test needs
// no live daemon.
//
// Note: pre-socket failures (resolveSocket returning not-joined when no socket
// file exists) bypass emit() and print to stderr even with --json. That
// asymmetry is frozen as-is; changing it is a separate decision.
func TestErrorJSONShape(t *testing.T) {
	// Start a fake socket server that returns a not_joined failure response.
	sockPath := testsock.Path(t, "fake.sock")
	srv := socket.NewServer(sockPath, func(req socket.Request) socket.Response {
		return socket.Fail(socket.CodeNotJoined, "not joined: no agent registered")
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("start fake server: %v", err)
	}
	defer srv.Stop()

	dir := testsock.Dir(t)
	t.Setenv(config.EnvMeshDir, dir)
	t.Setenv(config.EnvAgentSocket, sockPath)

	var stdout, stderr bytes.Buffer
	code := Run([]string{"who", "--json"}, &stdout, &stderr)
	if code != ExitNotJoined {
		t.Fatalf("exit = %d, want %d\nstdout: %s\nstderr: %s",
			code, ExitNotJoined, stdout.String(), stderr.String())
	}
	// The JSON error object must be on stdout (not stderr) when --json is set.
	if stdout.Len() == 0 {
		t.Fatal("stdout empty: JSON error object must go to stdout with --json")
	}
	var obj map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &obj); err != nil {
		t.Fatalf("error object not valid JSON: %v\nstdout: %s", err, stdout.String())
	}
	// Required fields: ok (bool false), code (string), message (string).
	if ok, exists := obj["ok"]; !exists || ok != false {
		t.Errorf("error object missing or wrong 'ok': %v", obj)
	}
	if _, exists := obj["code"]; !exists {
		t.Errorf("error object missing 'code': %v", obj)
	}
	if _, exists := obj["message"]; !exists {
		t.Errorf("error object missing 'message': %v", obj)
	}
	// No unexpected extra fields (keep the contract minimal).
	for k := range obj {
		switch k {
		case "ok", "code", "message":
		default:
			t.Errorf("error object has unexpected field %q: %v", k, obj)
		}
	}
}
