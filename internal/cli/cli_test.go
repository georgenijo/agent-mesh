package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/config"
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
