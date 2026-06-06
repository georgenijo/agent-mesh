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
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

var (
	meshBin  string
	meshdBin string
)

func TestMain(m *testing.M) {
	binDir, err := os.MkdirTemp("", "meshbin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer os.RemoveAll(binDir)

	meshBin = filepath.Join(binDir, "mesh")
	meshdBin = filepath.Join(binDir, "meshd")
	for target, pkg := range map[string]string{
		meshBin:  "github.com/georgenijo/agent-mesh/cmd/mesh",
		meshdBin: "github.com/georgenijo/agent-mesh/cmd/meshd",
	} {
		cmd := exec.Command("go", "build", "-o", target, pkg)
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n", pkg, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
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
			"MESH_HEARTBEAT_INTERVAL=100ms",
			"MESH_AWAY_AFTER=400ms",
			"MESH_EVICT_AFTER=1200ms",
			"MESH_REGISTRATION_GRACE=300ms",
		),
	}
	t.Cleanup(func() {
		m.killSidecars()
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
	cmd := exec.Command(meshdBin, "--mode", "coordinator")
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

// startDashboard boots the dashboard process and returns its base URL.
func (m *mesh) startDashboard() string {
	m.t.Helper()
	cmd := exec.Command(meshdBin, "--mode", "dashboard", "--addr", "127.0.0.1:0")
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

func (m *mesh) killSidecars() {
	// Best-effort: find sidecar pids via the registry if the bus is alive.
	cli, err := bus.Dial(filepath.Join(m.dir, "bus.sock"), bus.ClientOptions{DialTimeout: 300 * time.Millisecond})
	if err != nil {
		return
	}
	defer cli.Close()
	keys, err := cli.KVList(envelope.BucketRegistry)
	if err != nil {
		return
	}
	for _, kv := range keys {
		var rec agentcard.RegistryRecord
		if json.Unmarshal(kv.Value, &rec) == nil && rec.Card.PID > 0 {
			syscall.Kill(rec.Card.PID, syscall.SIGKILL) //nolint:errcheck
		}
	}
}

func (m *mesh) dumpLogsOnFailure() {
	if !m.t.Failed() {
		return
	}
	matches, _ := filepath.Glob(filepath.Join(m.dir, "logs", "*.log")) //nolint:errcheck
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

// TestP0AcceptanceFlow is the issue #11 proof: join → status → who --json →
// bus tap → dashboard → crash → away → evict, across real processes.
func TestP0AcceptanceFlow(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	// --- join (CLI autostarts the sidecar as a separate detached process) ---
	code, stdout, stderr := m.run("join", "--name", "test", "--role", "builder", "--caps", "go,backend")
	if code != 0 {
		t.Fatalf("join exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	// A second agent observes the mesh (and survives the crash below).
	if code, _, stderr := m.run("join", "--name", "observer", "--role", "reviewer"); code != 0 {
		t.Fatalf("observer join exit %d: %s", code, stderr)
	}

	// --- status through the real CLI/socket path ---
	if code, _, stderr := m.run("status", "working", "--socket", m.agentSocket("test")); code != 0 {
		t.Fatalf("status exit %d: %s", code, stderr)
	}

	// --- who --json sees the agent, live, with the latest status ---
	m.eventually(3*time.Second, "who shows test live with status", func() bool {
		agents, exit := m.who("observer")
		if exit != 0 {
			return false
		}
		rec, ok := findAgent(agents, "test")
		return ok && rec.State == agentcard.PresenceLive && rec.LastStatus == "working" &&
			rec.Card.Role == "builder" && rec.Card.HasCap("go")
	})

	// --- a raw bus tap observes status events (nothing is bypassed) ---
	tap, err := bus.Dial(filepath.Join(m.dir, "bus.sock"), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tap.Close()
	sawStatus := make(chan string, 8)
	if _, err := tap.Subscribe(envelope.PatternAll, func(env envelope.Envelope) {
		if env.Kind == envelope.KindStatus {
			var p envelope.StatusPayload
			if envelope.DecodeInto(env, &p) == nil {
				sawStatus <- p.Text
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := m.run("status", "tapped", "--socket", m.agentSocket("test")); code != 0 {
		t.Fatalf("status exit %d: %s", code, stderr)
	}
	select {
	case text := <-sawStatus:
		if text != "tapped" {
			t.Fatalf("tap saw %q, want %q", text, "tapped")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bus tap never saw the status event")
	}

	// --- dashboard renders the roster from the live tap ---
	base := m.startDashboard()
	m.eventually(5*time.Second, "dashboard roster shows the agent", func() bool {
		resp, err := http.Get(base + "/api/roster")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var body struct {
			Agents []agentcard.RegistryRecord `json:"agents"`
		}
		if json.NewDecoder(resp.Body).Decode(&body) != nil {
			return false
		}
		rec, ok := findAgent(body.Agents, "test")
		return ok && rec.LastStatus == "tapped"
	})

	// --- crash the sidecar: lease expiry must mark away, then evict ---
	agents, _ := m.who("observer")
	rec, ok := findAgent(agents, "test")
	if !ok || rec.Card.PID <= 0 {
		t.Fatalf("no pid recorded for test agent: %+v", rec)
	}
	if err := syscall.Kill(rec.Card.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill sidecar %d: %v", rec.Card.PID, err)
	}

	m.eventually(5*time.Second, "killed agent goes away", func() bool {
		agents, exit := m.who("observer")
		if exit != 0 {
			return false
		}
		rec, ok := findAgent(agents, "test")
		return ok && rec.State == agentcard.PresenceAway
	})
	m.eventually(5*time.Second, "killed agent is evicted", func() bool {
		agents, exit := m.who("observer")
		if exit != 0 {
			return false
		}
		_, ok := findAgent(agents, "test")
		return !ok
	})

	// The observer is untouched by its peer's crash.
	agents, _ = m.who("observer")
	if rec, ok := findAgent(agents, "observer"); !ok || rec.State != agentcard.PresenceLive {
		t.Fatalf("observer state wrong: %+v", agents)
	}

	// --- graceful leave exits cleanly and deregisters ---
	if code, _, stderr := m.run("leave", "--socket", m.agentSocket("observer")); code != 0 {
		t.Fatalf("leave exit %d: %s", code, stderr)
	}
	m.eventually(3*time.Second, "observer socket removed after leave", func() bool {
		_, err := os.Stat(m.agentSocket("observer"))
		return os.IsNotExist(err)
	})
}

// TestExitCodesAcrossProcesses pins the CLI exit-code contract end to end.
func TestExitCodesAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	// who with no sidecar at all → 5 (not joined).
	if code, _, _ := m.run("who"); code != 5 {
		t.Fatalf("who with no sidecar: exit %d, want 5", code)
	}
	// usage error → 2.
	if code, _, _ := m.run("join", "--role", "builder"); code != 2 {
		t.Fatalf("join without name: exit %d, want 2", code)
	}
	// happy path → 0.
	if code, _, _ := m.run("join", "--name", "solo", "--role", "builder"); code != 0 {
		t.Fatal("join failed")
	}
	if code, _, _ := m.run("status", "fine"); code != 0 {
		t.Fatal("status failed")
	}
	// leave → 0; second leave finds no socket → 5.
	if code, _, _ := m.run("leave"); code != 0 {
		t.Fatal("leave failed")
	}
	m.eventually(3*time.Second, "socket removed", func() bool {
		_, err := os.Stat(m.agentSocket("solo"))
		return os.IsNotExist(err)
	})
	if code, _, _ := m.run("leave"); code != 5 {
		t.Fatalf("second leave: exit %d, want 5", code)
	}
}
