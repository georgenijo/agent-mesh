// Package autostart boots missing mesh daemons so `mesh join` just works:
// the CLI spawns a sidecar when its socket is absent, and a sidecar spawns
// the coordinator when the bus socket is absent. Coordinator election across
// racing sidecars is serialized with an exclusive flock.
package autostart

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/config"
)

// waitFor is how long we wait for a spawned daemon's socket to come up.
const waitFor = 5 * time.Second

// FindMeshd locates the meshd binary: $MESH_MESHD, then next to the current
// executable. Deliberately NO bare $PATH fallback: autostart spawns the
// result as a detached session-leading daemon, so resolving it from a
// possibly writable PATH entry would be a search-path trust hole. Operators
// with an unusual layout opt in explicitly via $MESH_MESHD.
func FindMeshd() (string, error) {
	if p := os.Getenv(config.EnvMeshdBin); p != "" {
		return p, nil
	}
	if self, err := os.Executable(); err == nil {
		cand := filepath.Join(filepath.Dir(self), "meshd")
		if info, err := os.Stat(cand); err == nil && !info.IsDir() {
			return cand, nil
		}
	}
	return "", fmt.Errorf("autostart: meshd binary not found (install it beside mesh, or set %s)", config.EnvMeshdBin)
}

// dialable reports whether something accepts connections at the socket path.
func dialable(path string) bool {
	conn, err := net.DialTimeout("unix", path, 250*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// EnsureCoordinator makes sure a coordinator (and thus the bus) is running,
// spawning one if needed. Concurrent callers are serialized by an exclusive
// flock so exactly one of them spawns. The bool reports whether THIS call
// spawned it (false = already running).
func EnsureCoordinator(cfg config.Config) (bool, error) {
	if dialable(cfg.BusSocket()) {
		return false, nil
	}
	if err := cfg.EnsureDirs(); err != nil {
		return false, err
	}

	lock, err := os.OpenFile(cfg.CoordinatorLock(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false, fmt.Errorf("autostart: open coordinator lock: %w", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return false, fmt.Errorf("autostart: flock: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck

	// Someone else may have spawned it while we waited for the lock.
	if dialable(cfg.BusSocket()) {
		return false, nil
	}

	meshd, err := FindMeshd()
	if err != nil {
		return false, err
	}
	// --mesh-dir in the argv is the ops-plane ownership marker: it lets
	// `mesh ops down` verify via ps that a pid belongs to this mesh.
	if err := spawnDetached(cfg, "coordinator", meshd, "--mode", "coordinator", "--mesh-dir", cfg.MeshDir); err != nil {
		return false, err
	}
	if err := waitDialable(cfg.BusSocket(), "coordinator"); err != nil {
		return false, err
	}
	return true, nil
}

// SpawnSidecar starts a detached sidecar for the agent and waits for its
// socket to accept connections.
func SpawnSidecar(cfg config.Config, card agentcard.Card) error {
	meshd, err := FindMeshd()
	if err != nil {
		return err
	}
	args := []string{
		"--mode", "sidecar",
		"--name", card.Name,
		"--role", card.Role,
		"--mesh-dir", cfg.MeshDir, // ops-plane ownership marker (see EnsureCoordinator)
	}
	if len(card.Caps) > 0 {
		args = append(args, "--caps", joinComma(card.Caps))
	}
	if card.Repo != "" {
		args = append(args, "--repo", card.Repo)
	}
	if card.Model != "" {
		args = append(args, "--model", card.Model)
	}
	if err := spawnDetached(cfg, "sidecar-"+card.Name, meshd, args...); err != nil {
		return err
	}
	return waitDialable(cfg.AgentSocket(card.Name), "sidecar")
}

// spawnDetached launches meshd in its own session with stdout/stderr captured
// to a per-daemon log file under $MESH_DIR/logs.
func spawnDetached(cfg config.Config, logName, bin string, args ...string) error {
	logDir := filepath.Join(cfg.MeshDir, "logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return fmt.Errorf("autostart: create log dir: %w", err)
	}
	logFile, err := os.OpenFile(filepath.Join(logDir, logName+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("autostart: open log file: %w", err)
	}
	defer logFile.Close()

	cmd := exec.Command(bin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = os.Environ()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // survive the parent
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("autostart: start %s: %w", logName, err)
	}
	return cmd.Process.Release()
}

func waitDialable(path, what string) error {
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		if dialable(path) {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("autostart: %s did not come up at %s within %s (check $MESH_DIR/logs)", what, path, waitFor)
}

func joinComma(items []string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}
