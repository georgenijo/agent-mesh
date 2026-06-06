package dashboard

import (
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/observe"
)

// TestRunFilesWrittenAndRemoved pins the run-file protocol: Start writes
// dashboard.pid (this process) and dashboard.addr (the REAL bound address),
// Stop removes both.
func TestRunFilesWrittenAndRemoved(t *testing.T) {
	cfg, _, d := startStack(t)

	pid, err := observe.ReadPIDFile(cfg.DashboardPID())
	if err != nil {
		t.Fatalf("pidfile after Start: %v", err)
	}
	if pid != os.Getpid() {
		t.Fatalf("pidfile pid = %d, want %d", pid, os.Getpid())
	}
	addr, err := observe.ReadAddrFile(cfg.DashboardAddrFile())
	if err != nil {
		t.Fatalf("addr file after Start: %v", err)
	}
	if addr != d.Addr() {
		t.Fatalf("addr file = %q, want bound %q", addr, d.Addr())
	}
	if strings.HasSuffix(addr, ":0") {
		t.Fatalf("addr file holds the unresolved :0 address %q", addr)
	}

	d.Stop()
	if _, err := os.Stat(cfg.DashboardPID()); !os.IsNotExist(err) {
		t.Fatalf("pidfile still present after Stop (err=%v)", err)
	}
	if _, err := os.Stat(cfg.DashboardAddrFile()); !os.IsNotExist(err) {
		t.Fatalf("addr file still present after Stop (err=%v)", err)
	}
}

// TestListenFallbackOnBusyPort pins the port-conflict behavior: when the
// configured address is held by a foreign listener, the dashboard binds an
// OS-picked port instead of dying, and the addr file records where it landed.
func TestListenFallbackOnBusyPort(t *testing.T) {
	// The "foreign" holder.
	holder, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	busy := holder.Addr().String()

	cfg, _, _ := startStack(t) // brings up coordinator + a default dashboard
	d2 := New(cfg, busy, nil)
	if err := d2.Start(); err != nil {
		t.Fatalf("Start on busy addr must fall back, got %v", err)
	}
	defer d2.Stop()

	if d2.Addr() == busy {
		t.Fatalf("dashboard claims the busy address %q", busy)
	}
	if _, portStr, _ := net.SplitHostPort(d2.Addr()); portStr != "" {
		if port, _ := strconv.Atoi(portStr); port == 0 {
			t.Fatalf("bound port is 0: %q", d2.Addr())
		}
	}
	addr, err := observe.ReadAddrFile(cfg.DashboardAddrFile())
	if err != nil {
		t.Fatal(err)
	}
	if addr != d2.Addr() {
		t.Fatalf("addr file = %q, want %q", addr, d2.Addr())
	}
}
