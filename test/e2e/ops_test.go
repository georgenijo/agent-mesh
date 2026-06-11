// Ops actuator verbs across real process boundaries (issue #35): doctor
// classifies, down tears down, and one raw-ps test pins the ground truth so
// an ops bug cannot hide an ops leak.
package e2e

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/ops"
)

// TestOpsDownTearsDownFleet is the dogfood loop: a fleet built purely by
// autostart (so every daemon carries the --mesh-dir argv marker) is torn
// down gracefully, and both `ops --json` and `ops doctor` agree it is gone.
func TestOpsDownTearsDownFleet(t *testing.T) {
	m := newMesh(t)
	for _, name := range []string{"alpha", "beta"} {
		if code, _, stderr := m.run("join", "--name", name, "--role", "builder"); code != 0 {
			t.Fatalf("join %s: %s", name, stderr)
		}
	}

	code, stdout, stderr := m.run("ops", "down", "--json", "--timeout", "3s")
	if code != 0 {
		t.Fatalf("ops down: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	var rep ops.DownReport
	if err := json.Unmarshal([]byte(stdout), &rep); err != nil {
		t.Fatalf("ops down --json unparseable: %v\n%s", err, stdout)
	}
	if !rep.Clean {
		t.Fatalf("down report not clean: %+v", rep)
	}
	// Two sidecars + the autostarted coordinator.
	if len(rep.Targets) < 3 {
		t.Fatalf("targets = %+v, want at least 3", rep.Targets)
	}
	for _, tg := range rep.Targets {
		if tg.Outcome != ops.KillTerminated && tg.Outcome != ops.KillAlreadyDead {
			t.Fatalf("target %d (%s) outcome = %s (%s)", tg.PID, tg.Name, tg.Outcome, tg.Detail)
		}
	}

	// Graceful teardown removed every socket and pidfile: doctor is clean.
	if code, stdout, stderr := m.run("ops", "doctor", "--json"); code != 0 {
		t.Fatalf("ops doctor after down: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
}

// TestOpsDoctorClassifies pins the doctor classifications and the exit-7
// contract end to end.
func TestOpsDoctorClassifies(t *testing.T) {
	m := newMesh(t)
	if code, _, stderr := m.run("join", "--name", "patient", "--role", "builder"); code != 0 {
		t.Fatalf("join: %s", stderr)
	}

	// Healthy fleet → exit 0.
	m.eventually(3*time.Second, "doctor reports clean", func() bool {
		code, _, _ := m.run("ops", "doctor", "--json")
		return code == 0
	})

	// A pidfile pointing at a dead pid → exit 7 with a dead_pidfile finding.
	ghostPidfile := m.agentPIDFile("ghost")
	if err := os.WriteFile(ghostPidfile, []byte("999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, stdout, _ := m.run("ops", "doctor", "--json")
	if code != 7 {
		t.Fatalf("doctor with dead pidfile: exit %d, want 7\n%s", code, stdout)
	}
	rep := parseDoctor(t, stdout)
	if f, ok := findFinding(rep, "ghost"); !ok || f.State != ops.StateDeadPidfile {
		t.Fatalf("ghost finding = %+v, want dead_pidfile", rep.Findings)
	}
	if err := os.Remove(ghostPidfile); err != nil {
		t.Fatal(err)
	}

	// SIGKILL the sidecar: its socket file lingers undialable → stale_socket.
	pid := readPidfile(t, m.agentPIDFile("patient"))
	if err := killProcess(pid); err != nil {
		t.Fatal(err)
	}
	m.eventually(3*time.Second, "doctor flags the killed sidecar", func() bool {
		code, stdout, _ := m.run("ops", "doctor", "--json")
		if code != 7 {
			return false
		}
		f, ok := findFinding(parseDoctor(t, stdout), "patient")
		return ok && f.State == ops.StateStaleSocket
	})
}

// TestOpsCleanRemovesResidue: SIGKILL leaves a stale socket + pidfile;
// `ops clean` confirms them dead, unlinks them, and doctor goes clean once
// the registry lease expires.
func TestOpsCleanRemovesResidue(t *testing.T) {
	m := newMesh(t)
	if code, _, stderr := m.run("join", "--name", "victim", "--role", "builder"); code != 0 {
		t.Fatalf("join: %s", stderr)
	}

	pid := readPidfile(t, m.agentPIDFile("victim"))
	if err := killProcess(pid); err != nil {
		t.Fatal(err)
	}
	m.eventually(2*time.Second, "killed sidecar pid gone", func() bool {
		return pidDead(pid)
	})

	if code, stdout, stderr := m.run("ops", "clean", "--json"); code != 0 {
		t.Fatalf("ops clean: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	for _, path := range []string{m.agentSocket("victim"), m.agentPIDFile("victim")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s still on disk after clean: %v", path, err)
		}
	}

	// Once the eviction lease fires, nothing remembers the victim.
	m.eventually(5*time.Second, "doctor clean after eviction", func() bool {
		code, _, _ := m.run("ops", "doctor", "--json")
		return code == 0
	})
}

// TestOpsDownGroundTruthPS is the one raw-ps circularity guard (DECISIONS
// 2026-06-05): it validates the mesh's own zero-alive claim against the OS,
// bypassing all mesh reporting. Keep exactly one of these.
func TestOpsDownGroundTruthPS(t *testing.T) {
	m := newMesh(t)
	for _, name := range []string{"one", "two"} {
		if code, _, stderr := m.run("join", "--name", name, "--role", "builder"); code != 0 {
			t.Fatalf("join %s: %s", name, stderr)
		}
	}

	before := rawMeshPIDs(t, m.dir)
	if len(before) < 3 { // two sidecars + coordinator
		t.Fatalf("ps ground truth sees %d mesh processes, want >= 3", len(before))
	}

	if code, stdout, stderr := m.run("ops", "down", "--timeout", "3s"); code != 0 {
		t.Fatalf("ops down: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	if after := rawMeshPIDs(t, m.dir); len(after) != 0 {
		t.Fatalf("ps ground truth still sees mesh processes after down: %v", after)
	}
}

func (m *mesh) agentPIDFile(name string) string {
	return m.dir + "/agents/" + name + ".pid"
}

func readPidfile(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func parseDoctor(t *testing.T, stdout string) ops.DoctorReport {
	t.Helper()
	var rep ops.DoctorReport
	if err := json.Unmarshal([]byte(stdout), &rep); err != nil {
		t.Fatalf("doctor --json unparseable: %v\n%s", err, stdout)
	}
	return rep
}

func findFinding(rep ops.DoctorReport, entity string) (ops.Finding, bool) {
	for _, f := range rep.Findings {
		if f.Entity == entity {
			return f, true
		}
	}
	return ops.Finding{}, false
}
