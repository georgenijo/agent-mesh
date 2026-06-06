package ops

import (
	"net"
	"os"
	"strconv"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// deadSocket leaves a real socket file with nothing serving it (stdlib-only:
// listen, suppress unlink-on-close, close).
func deadSocket(t *testing.T, path string) {
	t.Helper()
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	ln.SetUnlinkOnClose(false)
	ln.Close()
}

func findEntry(rep CleanReport, path string) (CleanEntry, bool) {
	for _, e := range rep.Entries {
		if e.Path == path {
			return e, true
		}
	}
	return CleanEntry{}, false
}

func TestCleanRemovesDeadResidue(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	deadSocket(t, cfg.AgentSocket("crashed"))
	if err := os.WriteFile(cfg.AgentPIDFile("crashed"), []byte("999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadSocket(t, cfg.BusSocket())
	if err := os.WriteFile(cfg.CoordinatorPID(), []byte("999998\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The flock file must survive clean (autostart election races on it).
	if err := os.WriteFile(cfg.CoordinatorLock(), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	rep, err := Clean(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		cfg.AgentSocket("crashed"), cfg.AgentPIDFile("crashed"),
		cfg.BusSocket(), cfg.CoordinatorPID(),
	} {
		e, ok := findEntry(rep, path)
		if !ok || e.Action != CleanRemoved {
			t.Fatalf("%s: entry = %+v, want removed", path, e)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("%s still on disk after clean", path)
		}
	}
	if _, err := os.Lstat(cfg.CoordinatorLock()); err != nil {
		t.Fatalf("coordinator.lock must be left alone: %v", err)
	}
}

func TestCleanKeepsLiveArtifacts(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	// A live listener on the agent socket.
	ln, err := net.Listen("unix", cfg.AgentSocket("busy"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// A pidfile naming a live pid (this test process).
	if err := os.WriteFile(cfg.AgentPIDFile("hung"), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A dead socket whose sibling pidfile is alive: kept (down's job).
	deadSocket(t, cfg.AgentSocket("hung"))

	rep, err := Clean(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := findEntry(rep, cfg.AgentSocket("busy")); e.Action != CleanKeptDialable {
		t.Fatalf("busy socket: %+v, want kept_dialable", e)
	}
	if e, _ := findEntry(rep, cfg.AgentPIDFile("hung")); e.Action != CleanKeptAlive {
		t.Fatalf("hung pidfile: %+v, want kept_alive", e)
	}
	if e, _ := findEntry(rep, cfg.AgentSocket("hung")); e.Action != CleanKeptAlive {
		t.Fatalf("hung socket: %+v, want kept_alive", e)
	}
	for _, path := range []string{cfg.AgentSocket("busy"), cfg.AgentSocket("hung"), cfg.AgentPIDFile("hung")} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("%s removed by clean: %v", path, err)
		}
	}
}

func TestCleanSkipsSymlinks(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	victim := cfg.MeshDir + "/victim"
	if err := os.WriteFile(victim, []byte("precious"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A planted symlink masquerading as a pidfile must not be followed.
	if err := os.Symlink(victim, cfg.AgentPIDFile("trap")); err != nil {
		t.Fatal(err)
	}

	rep, err := Clean(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := findEntry(rep, cfg.AgentPIDFile("trap")); e.Action != CleanSkipped {
		t.Fatalf("symlinked pidfile: %+v, want skipped", e)
	}
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("symlink target touched: %v", err)
	}
}

// TestCleanServiceRunFiles pins the service-residue rules: dead run files go;
// a live service keeps BOTH its pidfile and addr file (the pidfile — never a
// TCP dial — is the authority for the addr file too).
func TestCleanServiceRunFiles(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	// Dashboard: dead residue.
	if err := os.WriteFile(cfg.DashboardPID(), []byte("999997\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.DashboardAddrFile(), []byte("127.0.0.1:8737\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Observe: "live" service (this test process's pid).
	if err := os.WriteFile(cfg.ObservePID(), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.ObserveAddrFile(), []byte("127.0.0.1:1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	rep, err := Clean(cfg)
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{cfg.DashboardPID(), cfg.DashboardAddrFile()} {
		e, ok := findEntry(rep, path)
		if !ok || e.Action != CleanRemoved {
			t.Fatalf("%s: entry=%+v ok=%v, want removed", path, e, ok)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s still on disk", path)
		}
	}
	for _, path := range []string{cfg.ObservePID(), cfg.ObserveAddrFile()} {
		e, ok := findEntry(rep, path)
		if !ok || e.Action != CleanKeptAlive {
			t.Fatalf("%s: entry=%+v ok=%v, want kept_alive", path, e, ok)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s was removed despite a live pid", path)
		}
	}
}
