package e2e

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/observe"
)

// upServiceJSON mirrors autostart.ServiceUp over the `mesh up --json` wire.
type upServiceJSON struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	PID    int    `json:"pid"`
	Addr   string `json:"addr"`
	URL    string `json:"url"`
}

// upReportJSON mirrors cli.upReport.
type upReportJSON struct {
	MeshDir     string `json:"meshDir"`
	Coordinator struct {
		Status    string `json:"status"`
		BusSocket string `json:"busSocket"`
	} `json:"coordinator"`
	Dashboard upServiceJSON `json:"dashboard"`
	Observe   upServiceJSON `json:"observe"`
}

func (m *mesh) up(extra ...string) (int, upReportJSON, string) {
	m.t.Helper()
	args := append([]string{"up", "--json"}, extra...)
	code, stdout, stderr := m.run(args...)
	var rep upReportJSON
	if code == 0 {
		if err := json.Unmarshal([]byte(stdout), &rep); err != nil {
			m.t.Fatalf("up --json unparseable: %v\n%s", err, stdout)
		}
	}
	return code, rep, stderr
}

func getOK(t *testing.T, url string) []byte {
	t.Helper()
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	buf := make([]byte, 1<<20)
	n, _ := resp.Body.Read(buf)
	return buf[:n]
}

// TestMeshUpIdempotentAndServing is the acceptance run for `mesh up`: one
// command from a cold mesh brings up coordinator + dashboard + observe, both
// URLs serve, doctor is clean, and a second `up` is a pid-stable no-op.
// :0 addrs keep parallel CI runs from fighting over the default ports.
func TestMeshUpIdempotentAndServing(t *testing.T) {
	m := newMesh(t)

	code, cold, stderr := m.up("--dashboard-addr", "127.0.0.1:0", "--observe-addr", "127.0.0.1:0")
	if code != 0 {
		t.Fatalf("cold up: exit %d\n%s", code, stderr)
	}
	if cold.Coordinator.Status != "started" {
		t.Fatalf("cold coordinator status = %q, want started", cold.Coordinator.Status)
	}
	for _, svc := range []upServiceJSON{cold.Dashboard, cold.Observe} {
		if svc.Status != "started" {
			t.Fatalf("cold %s status = %q, want started", svc.Name, svc.Status)
		}
		if svc.PID <= 0 || svc.URL == "" || strings.HasSuffix(svc.Addr, ":0") {
			t.Fatalf("cold %s report incomplete: %+v", svc.Name, svc)
		}
	}

	// Both URLs actually serve.
	if body := getOK(t, cold.Dashboard.URL+"/"); !strings.Contains(string(body), "<") {
		t.Fatalf("dashboard / does not look like HTML: %.80s", body)
	}
	getOK(t, cold.Dashboard.URL+"/ui/")
	var snap observe.Snapshot
	if err := json.Unmarshal(getOK(t, cold.Observe.URL+"/api/snapshot"), &snap); err != nil {
		t.Fatalf("observe snapshot unparseable: %v", err)
	}
	if snap.Meta.MeshDir != m.dir {
		t.Fatalf("observe snapshot meshDir = %q, want %q", snap.Meta.MeshDir, m.dir)
	}

	// Doctor is clean right after up.
	if code, stdout, stderr := m.run("ops", "doctor", "--json"); code != 0 {
		t.Fatalf("ops doctor after up: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	// Warm up: same addrs, same pids, nothing respawned.
	code, warm, stderr := m.up("--dashboard-addr", "127.0.0.1:0", "--observe-addr", "127.0.0.1:0")
	if code != 0 {
		t.Fatalf("warm up: exit %d\n%s", code, stderr)
	}
	if warm.Coordinator.Status != "already_running" {
		t.Fatalf("warm coordinator status = %q, want already_running", warm.Coordinator.Status)
	}
	for i, pair := range [][2]upServiceJSON{{cold.Dashboard, warm.Dashboard}, {cold.Observe, warm.Observe}} {
		was, now := pair[0], pair[1]
		if now.Status != "already_running" {
			t.Fatalf("warm service %d status = %q, want already_running", i, now.Status)
		}
		if now.PID != was.PID || now.Addr != was.Addr {
			t.Fatalf("warm %s changed: was pid=%d addr=%s, now pid=%d addr=%s",
				now.Name, was.PID, was.Addr, now.PID, now.Addr)
		}
	}
	// The run files agree — re-read pids straight from disk.
	for _, f := range []struct {
		path string
		want int
	}{
		{filepath.Join(m.dir, "dashboard.pid"), warm.Dashboard.PID},
		{filepath.Join(m.dir, "observe.pid"), warm.Observe.PID},
	} {
		pid, err := observe.ReadPIDFile(f.path)
		if err != nil {
			t.Fatalf("%s: %v", f.path, err)
		}
		if pid != f.want {
			t.Fatalf("%s pid = %d, want %d", f.path, pid, f.want)
		}
	}
	// newMesh's cleanup tears everything down via `ops down` and asserts the
	// zero-leak loops (now including services).
}

// TestMeshUpPortConflictFallsBack pins the cross-mesh port story: a foreign
// holder on the configured dashboard port must not break `mesh up` — the
// daemon lands on an OS-picked port and the report/addr file say where.
func TestMeshUpPortConflictFallsBack(t *testing.T) {
	m := newMesh(t)

	holder, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	busy := holder.Addr().String()

	code, rep, stderr := m.up("--dashboard-addr", busy, "--observe-addr", "127.0.0.1:0")
	if code != 0 {
		t.Fatalf("up with busy dashboard port: exit %d\n%s", code, stderr)
	}
	if rep.Dashboard.Addr == busy {
		t.Fatalf("dashboard claims the busy address %q", busy)
	}
	getOK(t, rep.Dashboard.URL+"/")

	addrBytes, err := os.ReadFile(filepath.Join(m.dir, "dashboard.addr"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(addrBytes)); got != rep.Dashboard.Addr {
		t.Fatalf("addr file = %q, report = %q — one authority, must match", got, rep.Dashboard.Addr)
	}
}
