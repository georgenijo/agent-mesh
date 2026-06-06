package e2e

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

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
