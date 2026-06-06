package observe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func TestSnapshotEndpoint(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, "127.0.0.1:0", nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/snapshot", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var snap Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if snap.Meta.MeshDir != cfg.MeshDir {
		t.Fatalf("meshDir = %q", snap.Meta.MeshDir)
	}
	if snap.Meta.CollectedAt.IsZero() {
		t.Fatal("collectedAt missing")
	}
}

func TestIndexServesHTML(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	srv := New(cfg, "127.0.0.1:0", nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

// TestServerRunFiles pins the run-file protocol for the observe daemon:
// Start writes observe.pid (this process) and observe.addr (real bound
// address); Stop removes both; the snapshot endpoint reports the service.
func TestServerRunFiles(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	srv := New(cfg, "127.0.0.1:0", nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}

	pid, err := ReadPIDFile(cfg.ObservePID())
	if err != nil {
		t.Fatalf("pidfile after Start: %v", err)
	}
	if pid != os.Getpid() {
		t.Fatalf("pidfile pid = %d, want %d", pid, os.Getpid())
	}
	addr, err := ReadAddrFile(cfg.ObserveAddrFile())
	if err != nil {
		t.Fatalf("addr file after Start: %v", err)
	}
	if addr != srv.Addr() {
		t.Fatalf("addr file = %q, want bound %q", addr, srv.Addr())
	}

	snap, err := Collect(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var svc *ServiceInfo
	for i := range snap.Services {
		if snap.Services[i].Name == "observe" {
			svc = &snap.Services[i]
		}
	}
	if svc == nil {
		t.Fatal("snapshot has no observe service entry")
	}
	if !svc.PIDAlive || !svc.Dialable || svc.Addr != addr || len(svc.Drift) != 0 {
		t.Fatalf("service entry = %+v, want alive+dialable at %s with no drift", svc, addr)
	}

	srv.Stop()
	if _, err := os.Stat(cfg.ObservePID()); !os.IsNotExist(err) {
		t.Fatalf("pidfile still present after Stop (err=%v)", err)
	}
	if _, err := os.Stat(cfg.ObserveAddrFile()); !os.IsNotExist(err) {
		t.Fatalf("addr file still present after Stop (err=%v)", err)
	}
}

// TestCollectServicesDrift pins the residue classifications: a dead pidfile
// and an addr file with no pidfile beside it.
func TestCollectServicesDrift(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	// Dashboard: pidfile pointing at a (virtually certainly) dead pid.
	if err := os.WriteFile(cfg.DashboardPID(), []byte("99999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Observe: addr file with no pidfile.
	if err := os.WriteFile(cfg.ObserveAddrFile(), []byte("127.0.0.1:1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	snap, err := Collect(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]Drift{}
	for _, svc := range snap.Services {
		got[svc.Name] = svc.Drift
	}
	if d := got["dashboard"]; len(d) != 1 || d[0] != DriftDeadPidfile {
		t.Fatalf("dashboard drift = %v, want [dead_pidfile]", d)
	}
	if d := got["observe"]; len(d) != 1 || d[0] != DriftStaleAddrFile {
		t.Fatalf("observe drift = %v, want [stale_addrfile]", d)
	}
	if len(snap.Anomalies) != 2 {
		t.Fatalf("anomalies = %v, want one per drifted service", snap.Anomalies)
	}
}
