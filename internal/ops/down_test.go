package ops

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/observe"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// TestHelperProcess is the stdlib re-exec trick: the test binary doubles as
// a fake daemon whose argv carries (or lacks) the --mesh-dir marker.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	time.Sleep(30 * time.Second) // killed long before this elapses
	os.Exit(0)
}

// spawnHelper starts the helper daemon with the given trailing argv and a
// pidfile under cfg, and returns its pid.
func spawnHelper(t *testing.T, cfg config.Config, name string, extraArgv ...string) int {
	t.Helper()
	args := append([]string{"-test.run=TestHelperProcess", "--"}, extraArgv...)
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill() //nolint:errcheck
		cmd.Wait()         //nolint:errcheck
	})
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	pidfile := cfg.AgentPIDFile(name)
	if err := os.WriteFile(pidfile, []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return cmd.Process.Pid
}

func findTarget(rep DownReport, pid int) (DownTarget, bool) {
	for _, tg := range rep.Targets {
		if tg.PID == pid {
			return tg, true
		}
	}
	return DownTarget{}, false
}

func TestDownTerminatesArgvOwnedProcess(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	pid := spawnHelper(t, cfg, "owned", "--mesh-dir", cfg.MeshDir)

	rep, err := Down(cfg, DownOptions{TermTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	tg, ok := findTarget(rep, pid)
	if !ok {
		t.Fatalf("pid %d missing from targets: %+v", pid, rep.Targets)
	}
	// The helper has no SIGTERM handler, so default disposition kills it.
	if tg.Outcome != KillTerminated {
		t.Fatalf("outcome = %s (%s), want terminated", tg.Outcome, tg.Detail)
	}
	if !rep.Clean {
		t.Fatalf("report not clean: %+v", rep)
	}
	if alive := aliveByPS([]int{pid}); alive[pid] {
		t.Fatalf("pid %d still alive after down", pid)
	}
}

func TestDownSkipsUnverifiedProcess(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	// No --mesh-dir marker in argv: ownership unprovable, must NOT be signaled.
	pid := spawnHelper(t, cfg, "stranger")

	rep, err := Down(cfg, DownOptions{TermTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	tg, ok := findTarget(rep, pid)
	if !ok {
		t.Fatalf("pid %d missing from targets: %+v", pid, rep.Targets)
	}
	if tg.Outcome != KillSkippedUnverified {
		t.Fatalf("outcome = %s (%s), want skipped_unverified", tg.Outcome, tg.Detail)
	}
	if rep.Clean {
		t.Fatal("report claims clean with a live unverified process")
	}
	if alive := aliveByPS([]int{pid}); !alive[pid] {
		t.Fatalf("pid %d was signaled despite failing ownership verification", pid)
	}
}

func TestDownReportsAlreadyDead(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.AgentPIDFile("corpse"), []byte("999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rep, err := Down(cfg, DownOptions{TermTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	tg, ok := findTarget(rep, 999999)
	if !ok {
		t.Fatalf("pid 999999 missing from targets: %+v", rep.Targets)
	}
	if tg.Outcome != KillAlreadyDead {
		t.Fatalf("outcome = %s, want already_dead", tg.Outcome)
	}
	if !rep.Clean {
		t.Fatalf("report not clean: %+v", rep)
	}
}

func TestDownEmptyMeshIsClean(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	rep, err := Down(cfg, DownOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Targets) != 0 || !rep.Clean {
		t.Fatalf("want clean empty report, got %+v", rep)
	}
}

func TestDownSelfPIDGuard(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	// A pidfile pointing at the test process itself must never be targeted.
	if err := os.WriteFile(cfg.AgentPIDFile("self"), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rep, err := Down(cfg, DownOptions{TermTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := findTarget(rep, os.Getpid()); ok {
		t.Fatalf("self pid in target set: %+v", rep.Targets)
	}
}

func TestGatherTargetsDedupesAcrossSources(t *testing.T) {
	snap := observe.Snapshot{
		Sidecars: []observe.SidecarInfo{{
			Name:           "alpha",
			SocketDialable: true,
			PID:            4242,
			PIDFilePID:     4242,
			Registry:       liveRecord("alpha", 4242),
		}},
	}
	targets := gatherTargets(snap)
	if len(targets) != 1 {
		t.Fatalf("targets = %+v, want one deduped entry", targets)
	}
	if len(targets[0].Sources) != 3 {
		t.Fatalf("sources = %v, want pidfile+registry+runtime", targets[0].Sources)
	}
}

func TestArgvOwnedMatchesTokenExactly(t *testing.T) {
	dir := testsock.Dir(t)
	cfg := config.Config{MeshDir: dir}
	pid := spawnHelper(t, cfg, "near-miss", "--mesh-dir", dir+"0") // prefix collision
	if ok, _ := argvOwned(pid, dir); ok {
		t.Fatalf("argvOwned matched %s0 against %s", dir, dir)
	}
	if ok, detail := argvOwned(pid, dir+"0"); !ok {
		t.Fatalf("argvOwned rejected exact match: %s", detail)
	}
	// Inline form.
	pid2 := spawnHelper(t, cfg, "inline", fmt.Sprintf("--mesh-dir=%s", dir))
	if ok, detail := argvOwned(pid2, dir); !ok {
		t.Fatalf("argvOwned rejected inline form: %s", detail)
	}
}

func TestGatherTargetsIncludesServices(t *testing.T) {
	snap := observe.Snapshot{
		Services: []observe.ServiceInfo{
			{Name: "dashboard", PID: 4321, PIDAlive: true},
			{Name: "observe", PID: 0}, // no pid recorded → no target
		},
	}
	targets := gatherTargets(snap)
	if len(targets) != 1 {
		t.Fatalf("targets = %+v, want exactly the dashboard", targets)
	}
	if targets[0].Kind != KindService || targets[0].Name != "dashboard" || targets[0].PID != 4321 {
		t.Fatalf("target = %+v, want service/dashboard/4321", targets[0])
	}
}
