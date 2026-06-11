package autostart

import (
	"fmt"
	"os"
	"time"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/observe"
)

// EnsureStatus is the typed outcome of an idempotent ensure: either this call
// started the daemon, or a live one was found and left alone.
type EnsureStatus string

const (
	StatusStarted        EnsureStatus = "started"
	StatusAlreadyRunning EnsureStatus = "already_running"
)

// ServiceUp reports one ensured HTTP daemon. Addr is the REAL bound address
// from the daemon's addr file (a port-conflict fallback or :0 may have moved
// it off the configured default).
type ServiceUp struct {
	Name   string       `json:"name"`
	Status EnsureStatus `json:"status"`
	PID    int          `json:"pid,omitempty"`
	Addr   string       `json:"addr"`
	URL    string       `json:"url"`
}

// EnsureDashboard makes sure the dashboard is running for this mesh,
// spawning one if needed.
func EnsureDashboard(cfg config.Config, addr string) (ServiceUp, error) {
	if addr == "" {
		addr = cfg.DashboardAddr
	}
	return ensureService(cfg, "dashboard", addr,
		cfg.DashboardLock(), cfg.DashboardPID(), cfg.DashboardAddrFile())
}

// EnsureObserve makes sure the observe server is running for this mesh,
// spawning one if needed.
func EnsureObserve(cfg config.Config, addr string) (ServiceUp, error) {
	if addr == "" {
		addr = cfg.ObserveAddr
	}
	return ensureService(cfg, "observe", addr,
		cfg.ObserveLock(), cfg.ObservePID(), cfg.ObserveAddrFile())
}

// ensureService is the shared idempotent bring-up: already-running pre-check,
// flock election (clone of EnsureCoordinator's), stale run-file removal,
// detached spawn with the --mesh-dir ownership marker, then readiness via the
// run-file protocol (see observe/runfiles.go — addr file last, atomically).
//
// "Already running" means pid alive AND its addr-file address dialable. A
// live pid that is NOT serving is a typed error, never a respawn and never a
// fake success — that drift is `mesh ops doctor`'s to explain.
func ensureService(cfg config.Config, name, addr, lockPath, pidFile, addrFile string) (ServiceUp, error) {
	up := ServiceUp{Name: name}

	if svc, running := serviceRunning(pidFile, addrFile); running {
		svc.Name, svc.Status = name, StatusAlreadyRunning
		return svc, nil
	}
	if err := cfg.EnsureDirs(); err != nil {
		return up, err
	}

	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return up, fmt.Errorf("autostart: open %s lock: %w", name, err)
	}
	defer lock.Close()
	if err := lockExclusive(lock); err != nil {
		return up, fmt.Errorf("autostart: flock: %w", err)
	}
	defer unlockFile(lock) //nolint:errcheck

	// Someone else may have brought it up while we waited for the lock.
	if svc, running := serviceRunning(pidFile, addrFile); running {
		svc.Name, svc.Status = name, StatusAlreadyRunning
		return svc, nil
	}

	// Alive pid without a dialable addr: refuse rather than double-spawn.
	if pid, err := observe.ReadPIDFile(pidFile); err == nil && observe.PIDAlive(pid) {
		return up, fmt.Errorf("autostart: %s pid %d is alive but not serving; run `mesh ops doctor`", name, pid)
	}

	// Dead residue; we hold the lock, safe to clear before respawning.
	os.Remove(pidFile)  //nolint:errcheck
	os.Remove(addrFile) //nolint:errcheck

	meshd, err := FindMeshd()
	if err != nil {
		return up, err
	}
	// --addr always explicit (flag beats env in the child, deterministic);
	// --mesh-dir is the ops-plane ownership marker (see EnsureCoordinator).
	if err := spawnDetached(cfg, name, meshd,
		"--mode", name, "--mesh-dir", cfg.MeshDir, "--addr", addr); err != nil {
		return up, err
	}

	svc, err := waitServiceReady(pidFile, addrFile, name)
	if err != nil {
		return up, err
	}
	svc.Name, svc.Status = name, StatusStarted
	return svc, nil
}

// serviceRunning applies the idempotence test: pidfile names a live pid AND
// the addr-file address answers TCP. Dialability alone is never "ours" — a
// foreign process may hold a recycled port.
func serviceRunning(pidFile, addrFile string) (ServiceUp, bool) {
	pid, err := observe.ReadPIDFile(pidFile)
	if err != nil || !observe.PIDAlive(pid) {
		return ServiceUp{}, false
	}
	addr, err := observe.ReadAddrFile(addrFile)
	if err != nil || !observe.DialableTCP(addr) {
		return ServiceUp{}, false
	}
	return ServiceUp{PID: pid, Addr: addr, URL: "http://" + addr}, true
}

// waitServiceReady polls for the run-file protocol's readiness signal: the
// addr file is written last and atomically, so its appearance guarantees a
// complete pidfile and an accepting listener. One confirming TCP dial guards
// against reading files a dying daemon left behind.
func waitServiceReady(pidFile, addrFile, what string) (ServiceUp, error) {
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		if addr, err := observe.ReadAddrFile(addrFile); err == nil {
			pid, pidErr := observe.ReadPIDFile(pidFile)
			if pidErr == nil && observe.DialableTCP(addr) {
				return ServiceUp{PID: pid, Addr: addr, URL: "http://" + addr}, nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return ServiceUp{}, fmt.Errorf("autostart: %s did not come up within %s (check $MESH_DIR/logs/%s.log)", what, waitFor, what)
}
