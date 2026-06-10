// Package observe is the ops-plane runtime inspector: it compares what is
// actually running on disk (coordinator, sidecar sockets, PIDs, logs) against
// the authoritative registry and sidecar-reported child CLI processes.
//
// It is deliberately separate from internal/dashboard, which observes mesh
// events for the product UI.
package observe

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
	"github.com/georgenijo/agent-mesh/internal/socket"
)

const dialTimeout = 250 * time.Millisecond
const runtimeTimeout = 2 * time.Second

// Drift is a typed misalignment between the fact sources (filesystem,
// registry, runtime IPC, pidfiles). Typed per the envelope results rule:
// never free text in a machine contract.
type Drift string

const (
	DriftGhostAgent       Drift = "ghost_agent"       // registry record, socket not dialable
	DriftPresenceMismatch Drift = "presence_mismatch" // registry live, socket dialable, runtime silent
	DriftStalePID         Drift = "stale_pid"         // registry/runtime pid not alive
	DriftOrphanSocket     Drift = "orphan_socket"     // socket file, no registry record
	DriftDeadPidfile      Drift = "dead_pidfile"      // pidfile whose pid is not alive
	DriftOrphanPidfile    Drift = "orphan_pidfile"    // pidfile with live pid, no socket, no registry
	DriftStaleAddrFile    Drift = "stale_addrfile"    // service addr file with no pidfile beside it
)

// Snapshot is the full runtime observability contract shared by mesh ops and
// meshd --mode observe.
type Snapshot struct {
	Meta        Meta            `json:"meta"`
	Coordinator CoordinatorInfo `json:"coordinator"`
	Sidecars    []SidecarInfo   `json:"sidecars"`
	Children    []ChildInfo     `json:"children"`
	Services    []ServiceInfo   `json:"services,omitempty"`
	Anomalies   []string        `json:"anomalies,omitempty"`
}

// Meta identifies when and where the snapshot was collected.
type Meta struct {
	MeshDir     string    `json:"meshDir"`
	CollectedAt time.Time `json:"collectedAt"`
}

// CoordinatorInfo describes the mesh control-plane process and bus socket.
type CoordinatorInfo struct {
	PID              int    `json:"pid,omitempty"`
	PIDAlive         bool   `json:"pidAlive"`
	BusSocketPresent bool   `json:"busSocketPresent"`
	BusDialable      bool   `json:"busDialable"`
	LockPresent      bool   `json:"lockPresent"`
	LogPath          string `json:"logPath,omitempty"`
}

// SidecarInfo describes one agent daemon socket and how it lines up with the
// registry and its pidfile.
type SidecarInfo struct {
	Name           string                    `json:"name"`
	Socket         string                    `json:"socket"`
	SocketPresent  bool                      `json:"socketPresent"`
	SocketDialable bool                      `json:"socketDialable"`
	PID            int                       `json:"pid,omitempty"`
	PIDAlive       bool                      `json:"pidAlive"`
	PIDFile        string                    `json:"pidFile,omitempty"`
	PIDFilePID     int                       `json:"pidFilePid,omitempty"`
	LogPath        string                    `json:"logPath,omitempty"`
	Registry       *agentcard.RegistryRecord `json:"registry,omitempty"`
	ClaimLosses    []meshapi.ClaimLoss       `json:"claimLosses,omitempty"`
	Drift          []Drift                   `json:"drift,omitempty"`
}

// ServiceInfo describes an optional HTTP daemon (dashboard, observe) via its
// run files (<name>.pid, <name>.addr — see runfiles.go). Addr is the REAL
// bound address from the addr file; never dial the configured default, a
// port-conflict fallback may have moved it.
type ServiceInfo struct {
	Name     string  `json:"name"` // "dashboard" | "observe"
	PID      int     `json:"pid,omitempty"`
	PIDAlive bool    `json:"pidAlive"`
	PIDFile  string  `json:"pidFile,omitempty"`
	Addr     string  `json:"addr,omitempty"`
	AddrFile string  `json:"addrFile,omitempty"`
	Dialable bool    `json:"dialable"`
	LogPath  string  `json:"logPath,omitempty"`
	Drift    []Drift `json:"drift,omitempty"`
}

// ChildInfo is a child agent CLI process reported by a sidecar.
type ChildInfo struct {
	Sidecar   string    `json:"sidecar"`
	PID       int       `json:"pid"`
	Cmd       string    `json:"cmd"`
	Alive     bool      `json:"alive"`
	StartedAt time.Time `json:"startedAt"`
	State     string    `json:"state"`
}

// Collect builds a runtime snapshot for cfg.MeshDir.
func Collect(cfg config.Config) (Snapshot, error) {
	if _, err := os.Stat(cfg.MeshDir); err != nil {
		return Snapshot{}, fmt.Errorf("observe: mesh dir %s: %w", cfg.MeshDir, err)
	}

	now := time.Now().UTC()
	snap := Snapshot{
		Meta: Meta{MeshDir: cfg.MeshDir, CollectedAt: now},
		Coordinator: CoordinatorInfo{
			LogPath: filepath.Join(cfg.MeshDir, "logs", "coordinator.log"),
		},
	}

	if pid, err := ReadPIDFile(cfg.CoordinatorPID()); err == nil {
		snap.Coordinator.PID = pid
		snap.Coordinator.PIDAlive = PIDAlive(pid)
	}
	if _, err := os.Stat(cfg.CoordinatorLock()); err == nil {
		snap.Coordinator.LockPresent = true
	}
	if _, err := os.Stat(cfg.BusSocket()); err == nil {
		snap.Coordinator.BusSocketPresent = true
	}
	snap.Coordinator.BusDialable = Dialable(cfg.BusSocket())

	registry := map[string]agentcard.RegistryRecord{}
	if snap.Coordinator.BusDialable {
		if recs, err := readRegistry(cfg); err == nil {
			for _, rec := range recs {
				registry[rec.Card.Name] = rec
			}
		}
	}

	// Sidecar pidfiles are the third fact source (issue #35): they make an
	// agent visible even after registry eviction with a hung socket — or
	// with no socket at all.
	pidfilePaths, _ := filepath.Glob(filepath.Join(cfg.AgentsDir(), "*.pid"))
	pidfiles := make(map[string]int, len(pidfilePaths)) // name → pid (0 if unparseable)
	for _, path := range pidfilePaths {
		name := strings.TrimSuffix(filepath.Base(path), ".pid")
		pid, err := ReadPIDFile(path)
		if err != nil {
			pid = 0
		}
		pidfiles[name] = pid
	}

	socketPaths, _ := filepath.Glob(filepath.Join(cfg.AgentsDir(), "*.sock"))
	seen := make(map[string]struct{}, len(socketPaths))

	attachPidfile := func(info *SidecarInfo) {
		pid, ok := pidfiles[info.Name]
		if !ok {
			return
		}
		info.PIDFile = cfg.AgentPIDFile(info.Name)
		info.PIDFilePID = pid
		if info.PID == 0 && pid > 0 {
			info.PID = pid
			info.PIDAlive = PIDAlive(pid)
		}
		// An unparseable pidfile (pid 0) is dead residue too.
		if !PIDAlive(pid) {
			info.Drift = append(info.Drift, DriftDeadPidfile)
		}
	}

	for _, sockPath := range socketPaths {
		name := strings.TrimSuffix(filepath.Base(sockPath), ".sock")
		seen[name] = struct{}{}

		info := SidecarInfo{
			Name:          name,
			Socket:        sockPath,
			SocketPresent: true,
			LogPath:       filepath.Join(cfg.MeshDir, "logs", "sidecar-"+name+".log"),
		}
		info.SocketDialable = Dialable(sockPath)

		var runtime *meshapi.RuntimeResult
		if info.SocketDialable {
			if rt, err := queryRuntime(sockPath); err == nil {
				runtime = &rt
				info.PID = rt.SidecarPID
				info.PIDAlive = PIDAlive(rt.SidecarPID)
				info.ClaimLosses = append([]meshapi.ClaimLoss(nil), rt.ClaimLosses...)
				for _, child := range rt.Children {
					alive := child.State == "running" && PIDAlive(child.PID)
					snap.Children = append(snap.Children, ChildInfo{
						Sidecar:   name,
						PID:       child.PID,
						Cmd:       child.Cmd,
						Alive:     alive,
						StartedAt: child.StartedAt,
						State:     child.State,
					})
				}
			}
		}

		if rec, ok := registry[name]; ok {
			recCopy := rec
			info.Registry = &recCopy
			if info.PID == 0 && rec.Card.PID > 0 {
				info.PID = rec.Card.PID
				info.PIDAlive = PIDAlive(rec.Card.PID)
			}
			if rec.State == agentcard.PresenceLive {
				if !info.SocketDialable {
					info.Drift = append(info.Drift, DriftGhostAgent)
				} else if runtime == nil {
					info.Drift = append(info.Drift, DriftPresenceMismatch)
				}
			}
			if info.PID > 0 && !info.PIDAlive {
				info.Drift = append(info.Drift, DriftStalePID)
			}
		} else {
			info.Drift = append(info.Drift, DriftOrphanSocket)
			if info.PID > 0 && !info.PIDAlive {
				info.Drift = append(info.Drift, DriftStalePID)
			}
		}

		attachPidfile(&info)
		snap.Sidecars = append(snap.Sidecars, info)
	}

	for name, rec := range registry {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		info := SidecarInfo{
			Name:     name,
			Socket:   cfg.AgentSocket(name),
			Registry: &rec,
			LogPath:  filepath.Join(cfg.MeshDir, "logs", "sidecar-"+name+".log"),
		}
		if rec.Card.PID > 0 {
			info.PID = rec.Card.PID
			info.PIDAlive = PIDAlive(rec.Card.PID)
			if !info.PIDAlive {
				info.Drift = append(info.Drift, DriftStalePID)
			}
		}
		info.Drift = append(info.Drift, DriftGhostAgent)
		attachPidfile(&info)
		snap.Sidecars = append(snap.Sidecars, info)
	}

	// Pidfile-only entries: no socket, no registry record — the previously
	// invisible class (evicted from the registry, socket gone or never up,
	// process possibly still alive).
	pidfileNames := make([]string, 0, len(pidfiles))
	for name := range pidfiles {
		if _, ok := seen[name]; !ok {
			pidfileNames = append(pidfileNames, name)
		}
	}
	sort.Strings(pidfileNames)
	for _, name := range pidfileNames {
		info := SidecarInfo{
			Name:    name,
			Socket:  cfg.AgentSocket(name),
			LogPath: filepath.Join(cfg.MeshDir, "logs", "sidecar-"+name+".log"),
		}
		attachPidfile(&info)
		if info.PIDFilePID > 0 && info.PIDAlive {
			info.Drift = append(info.Drift, DriftOrphanPidfile)
		}
		snap.Sidecars = append(snap.Sidecars, info)
	}

	collectServices(cfg, &snap)

	if len(socketPaths) > 0 && !snap.Coordinator.BusDialable {
		snap.Anomalies = append(snap.Anomalies, "coordinator_down: sidecar sockets exist but bus is not dialable")
	}
	for _, sc := range snap.Sidecars {
		for _, d := range sc.Drift {
			snap.Anomalies = append(snap.Anomalies, fmt.Sprintf("%s: %s", sc.Name, d))
		}
	}
	for _, svc := range snap.Services {
		for _, d := range svc.Drift {
			snap.Anomalies = append(snap.Anomalies, fmt.Sprintf("%s: %s", svc.Name, d))
		}
	}

	return snap, nil
}

// collectServices reads the optional HTTP daemons' run files. A service with
// neither file is simply absent — a fresh mesh stays clean.
func collectServices(cfg config.Config, snap *Snapshot) {
	for _, svc := range []struct {
		name, pidFile, addrFile string
	}{
		{"dashboard", cfg.DashboardPID(), cfg.DashboardAddrFile()},
		{"observe", cfg.ObservePID(), cfg.ObserveAddrFile()},
	} {
		info := ServiceInfo{
			Name:    svc.name,
			LogPath: filepath.Join(cfg.MeshDir, "logs", svc.name+".log"),
		}
		havePid, haveAddr := false, false
		if pid, err := ReadPIDFile(svc.pidFile); err == nil {
			havePid = true
			info.PIDFile = svc.pidFile
			info.PID = pid
			info.PIDAlive = PIDAlive(pid)
		} else if _, statErr := os.Stat(svc.pidFile); statErr == nil {
			havePid = true // present but garbage: dead residue
			info.PIDFile = svc.pidFile
		}
		if addr, err := ReadAddrFile(svc.addrFile); err == nil {
			haveAddr = true
			info.AddrFile = svc.addrFile
			info.Addr = addr
			info.Dialable = DialableTCP(addr)
		}
		if !havePid && !haveAddr {
			continue
		}
		if havePid && !info.PIDAlive {
			info.Drift = append(info.Drift, DriftDeadPidfile)
		}
		if haveAddr && !havePid {
			info.Drift = append(info.Drift, DriftStaleAddrFile)
		}
		snap.Services = append(snap.Services, info)
	}
}

// ReadPIDFile parses a decimal pid from a pidfile.
func ReadPIDFile(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("invalid pid file %s", path)
	}
	return pid, nil
}

// PIDAlive reports whether the pid exists. Note: on Unix an unreaped zombie
// may still count as alive here; internal/ops uses process-state details for
// teardown where available.
func PIDAlive(pid int) bool {
	return pidAlive(pid)
}

// Dialable reports whether something accepts connections at the socket path.
func Dialable(path string) bool {
	conn, err := net.DialTimeout("unix", path, dialTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func readRegistry(cfg config.Config) ([]agentcard.RegistryRecord, error) {
	cli, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{DialTimeout: dialTimeout})
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	keys, err := cli.KVList(envelope.BucketRegistry)
	if err != nil {
		return nil, err
	}
	out := make([]agentcard.RegistryRecord, 0, len(keys))
	for _, kv := range keys {
		var rec agentcard.RegistryRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

func queryRuntime(sockPath string) (meshapi.RuntimeResult, error) {
	resp, err := socket.Do(sockPath, socket.Request{Verb: meshapi.VerbRuntime}, runtimeTimeout)
	if err != nil {
		return meshapi.RuntimeResult{}, err
	}
	if !resp.OK {
		return meshapi.RuntimeResult{}, fmt.Errorf("runtime: %s", resp.Message)
	}
	var rt meshapi.RuntimeResult
	if err := json.Unmarshal(resp.Data, &rt); err != nil {
		return meshapi.RuntimeResult{}, err
	}
	return rt, nil
}
