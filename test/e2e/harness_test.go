// Package e2e proves the P0 walking skeleton across real process boundaries:
// real binaries, real unix sockets, real bus — no mocks (issue #11).
//
// The audit lesson this enforces: green unit tests over mock stores can hide
// a system that does not actually run. This test fails if the socket, the
// sidecar, the bus, or the coordinator is bypassed, because every assertion
// flows through `mesh` → sidecar socket → bus → coordinator → registry.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/observe"
)

var (
	meshBin       string
	meshdBin      string
	fakeClaudeBin string
)

// TestMain delegates to testMain because os.Exit skips deferred cleanup —
// the direct form leaked one meshbin temp dir per run (issue #34).
func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	binDir, err := os.MkdirTemp("", "meshbin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(binDir)

	meshBin = filepath.Join(binDir, exeName("mesh"))
	meshdBin = filepath.Join(binDir, exeName("meshd"))
	fakeClaudeBin = filepath.Join(binDir, exeName("fakeclaude"))
	for target, pkg := range map[string]string{
		meshBin:       "github.com/georgenijo/agent-mesh/cmd/mesh",
		meshdBin:      "github.com/georgenijo/agent-mesh/cmd/meshd",
		fakeClaudeBin: "github.com/georgenijo/agent-mesh/test/e2e/fakeclaude",
	} {
		cmd := exec.Command("go", "build", "-o", target, pkg)
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n", pkg, err)
			return 1
		}
	}
	return m.Run()
}

func exeName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

// mesh is one test mesh: an isolated MESH_DIR with fast presence timings.
type mesh struct {
	t   *testing.T
	dir string
	env []string
}

func newMesh(t *testing.T) *mesh {
	t.Helper()
	dir, err := os.MkdirTemp("", "mesh") // short path: unix socket limit
	if err != nil {
		t.Fatal(err)
	}
	m := &mesh{
		t:   t,
		dir: dir,
		env: append(os.Environ(),
			"MESH_DIR="+dir,
			"MESH_MESHD="+meshdBin,
			"MESH_EXPERT_CLI="+fakeClaudeBin,
			"MESH_HEARTBEAT_INTERVAL=100ms",
			"MESH_AWAY_AFTER=400ms",
			"MESH_EVICT_AFTER=1200ms",
			"MESH_REGISTRATION_GRACE=300ms",
		),
	}
	t.Cleanup(func() {
		m.teardown()
		m.dumpLogsOnFailure()
		os.RemoveAll(dir)
	})
	return m
}

// run executes the mesh CLI and returns exit code, stdout, stderr.
func (m *mesh) run(args ...string) (int, string, string) {
	m.t.Helper()
	cmd := exec.Command(meshBin, args...)
	cmd.Env = m.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			m.t.Fatalf("run mesh %v: %v", args, err)
		}
	}
	return code, stdout.String(), stderr.String()
}

// startCoordinator boots the coordinator as an explicit separate process.
func (m *mesh) startCoordinator() {
	m.t.Helper()
	logf, err := os.Create(filepath.Join(m.dir, "coordinator-e2e.log"))
	if err != nil {
		m.t.Fatal(err)
	}
	// --mesh-dir makes this explicitly-launched coordinator visible to the
	// ops-plane ownership check (and to the raw-ps ground-truth test).
	cmd := exec.Command(meshdBin, "--mode", "coordinator", "--mesh-dir", m.dir)
	cmd.Env = m.env
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		m.t.Fatal(err)
	}
	m.t.Cleanup(func() {
		cmd.Process.Kill() //nolint:errcheck
		cmd.Wait()         //nolint:errcheck
		logf.Close()
	})
	m.waitDialable(filepath.Join(m.dir, "bus.sock"), 5*time.Second)
}

// startExpert launches `mesh expert serve` as a background foreground-style
// process: it execs meshd --mode expert, which joins the role and drives the
// fake claude runtime. Teardown is the shared ops-down path; a Kill cleanup is
// the backstop. It waits until the expert's sidecar socket accepts connections.
func (m *mesh) startExpert(name, role string) {
	m.t.Helper()
	logf, err := os.Create(filepath.Join(m.dir, "expert-"+name+".log"))
	if err != nil {
		m.t.Fatal(err)
	}
	cmd := exec.Command(meshBin, "expert", "serve", "--name", name, "--role", role, "--repo", "demo")
	cmd.Env = m.env
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		m.t.Fatal(err)
	}
	m.t.Cleanup(func() {
		cmd.Process.Kill() //nolint:errcheck
		cmd.Wait()         //nolint:errcheck
		logf.Close()
	})
	m.waitDialable(m.agentSocket(name), 10*time.Second)

	// The sidecar socket appears before the runtime child is up: runExpert
	// registers the agent live, THEN blocks in proxy.Start until the child
	// reports init. Under full-suite parallel load (every package spawning
	// freshly built binaries; on Windows, process creation + AV scanning of
	// new .exes) that gap can exceed the tests' eventually windows, which
	// must measure loop behavior, not spawn latency. Wait for the "expert
	// serving" line meshd logs only after proxy.Start + TrackChild succeed.
	// Generous deadline: the proxy's own StartTimeout (30s) fires first on a
	// genuinely broken spawn, and the logged error surfaces via the dump.
	logPath := filepath.Join(m.dir, "expert-"+name+".log")
	m.eventually(45*time.Second, "expert runtime child is serving", func() bool {
		data, err := os.ReadFile(logPath)
		return err == nil && strings.Contains(string(data), "expert serving")
	})
}

// startDashboard boots the dashboard process and returns its base URL.
func (m *mesh) startDashboard() string {
	m.t.Helper()
	// --mesh-dir is the ops-plane ownership marker: the dashboard now writes
	// a pidfile, so `ops down` must be able to argv-verify the pid as ours.
	cmd := exec.Command(meshdBin, "--mode", "dashboard", "--addr", "127.0.0.1:0", "--mesh-dir", m.dir)
	cmd.Env = m.env
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		m.t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		m.t.Fatal(err)
	}
	m.t.Cleanup(func() {
		cmd.Process.Kill() //nolint:errcheck
		cmd.Wait()         //nolint:errcheck
	})

	// First stdout line: "dashboard: http://127.0.0.1:PORT"
	buf := make([]byte, 256)
	deadline := time.Now().Add(5 * time.Second)
	var line string
	for time.Now().Before(deadline) {
		n, err := stdout.Read(buf)
		if n > 0 {
			line += string(buf[:n])
			if strings.Contains(line, "http://") && strings.Contains(line, "\n") {
				break
			}
		}
		if err != nil {
			break
		}
	}
	idx := strings.Index(line, "http://")
	if idx < 0 {
		m.t.Fatalf("dashboard never printed its address: %q", line)
	}
	return strings.TrimSpace(strings.Split(line[idx:], "\n")[0])
}

func (m *mesh) waitDialable(path string, timeout time.Duration) {
	m.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if conn, err := os.Stat(path); err == nil && conn.Mode()&os.ModeSocket != 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	m.t.Fatalf("socket %s never appeared", path)
}

// who runs `mesh who --json` through the given agent socket and parses it.
func (m *mesh) who(viaAgent string) (agents []agentcard.RegistryRecord, exit int) {
	m.t.Helper()
	code, stdout, _ := m.run("who", "--json", "--socket", m.agentSocket(viaAgent))
	if code != 0 {
		return nil, code
	}
	var res struct {
		Agents []agentcard.RegistryRecord `json:"agents"`
	}
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		m.t.Fatalf("who --json unparseable: %v\n%s", err, stdout)
	}
	return res.Agents, 0
}

func (m *mesh) agentSocket(name string) string {
	return filepath.Join(m.dir, "agents", name+".sock")
}

// teardown dogfoods the ops plane (issue #35): `mesh ops down` replaces the
// old registry-dependent killSidecars, which silently leaked sidecars when
// the bus was already dead (issue #33). The zero-alive assertion goes through
// `mesh ops --json`; TestOpsDownGroundTruthPS guards the circularity.
func (m *mesh) teardown() {
	if code, stdout, stderr := m.run("ops", "down", "--json", "--timeout", "2s"); code != 0 {
		m.t.Errorf("ops down: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	code, stdout, stderr := m.run("ops", "--json")
	if code != 0 {
		m.t.Errorf("ops snapshot after down: exit %d\nstderr: %s", code, stderr)
		return
	}
	var snap observe.Snapshot
	if err := json.Unmarshal([]byte(stdout), &snap); err != nil {
		m.t.Errorf("ops --json unparseable: %v\n%s", err, stdout)
		return
	}
	if snap.Coordinator.PIDAlive {
		m.t.Errorf("coordinator pid %d still alive after ops down", snap.Coordinator.PID)
	}
	for _, sc := range snap.Sidecars {
		if sc.PIDAlive {
			m.t.Errorf("sidecar %s pid %d still alive after ops down", sc.Name, sc.PID)
		}
	}
	for _, ch := range snap.Children {
		if ch.Alive {
			m.t.Errorf("child %d (%s) still alive after ops down", ch.PID, ch.Cmd)
		}
	}
	for _, svc := range snap.Services {
		if svc.PIDAlive {
			m.t.Errorf("service %s pid %d still alive after ops down", svc.Name, svc.PID)
		}
	}
}

func (m *mesh) dumpLogsOnFailure() {
	if !m.t.Failed() {
		return
	}
	matches, _ := filepath.Glob(filepath.Join(m.dir, "logs", "*.log")) //nolint:errcheck
	experts, _ := filepath.Glob(filepath.Join(m.dir, "expert-*.log"))  //nolint:errcheck
	matches = append(matches, experts...)
	matches = append(matches, filepath.Join(m.dir, "coordinator-e2e.log"))
	for _, path := range matches {
		if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
			m.t.Logf("--- %s ---\n%s", filepath.Base(path), data)
		}
	}
}

func (m *mesh) eventually(timeout time.Duration, what string, cond func() bool) {
	m.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	m.t.Fatalf("never happened: %s", what)
}

func findAgent(agents []agentcard.RegistryRecord, name string) (agentcard.RegistryRecord, bool) {
	for _, a := range agents {
		if a.Card.Name == name {
			return a, true
		}
	}
	return agentcard.RegistryRecord{}, false
}
