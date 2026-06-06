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
	"strconv"
	"strings"
	"syscall"
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

// Snapshot is the full runtime observability contract shared by mesh ops and
// meshd --mode observe.
type Snapshot struct {
	Meta        Meta            `json:"meta"`
	Coordinator CoordinatorInfo `json:"coordinator"`
	Sidecars    []SidecarInfo   `json:"sidecars"`
	Children    []ChildInfo     `json:"children"`
	Anomalies   []string        `json:"anomalies,omitempty"`
}

// Meta identifies when and where the snapshot was collected.
type Meta struct {
	MeshDir     string    `json:"meshDir"`
	CollectedAt time.Time `json:"collectedAt"`
}

// CoordinatorInfo describes the mesh control-plane process and bus socket.
type CoordinatorInfo struct {
	PID         int    `json:"pid,omitempty"`
	PIDAlive    bool   `json:"pidAlive"`
	BusDialable bool   `json:"busDialable"`
	LockPresent bool   `json:"lockPresent"`
	LogPath     string `json:"logPath,omitempty"`
}

// SidecarInfo describes one agent daemon socket and how it lines up with the
// registry.
type SidecarInfo struct {
	Name           string                    `json:"name"`
	Socket         string                    `json:"socket"`
	SocketDialable bool                      `json:"socketDialable"`
	PID            int                       `json:"pid,omitempty"`
	PIDAlive       bool                      `json:"pidAlive"`
	LogPath        string                    `json:"logPath,omitempty"`
	Registry       *agentcard.RegistryRecord `json:"registry,omitempty"`
	Drift          []string                  `json:"drift,omitempty"`
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

	if pid, err := readPID(cfg.CoordinatorPID()); err == nil {
		snap.Coordinator.PID = pid
		snap.Coordinator.PIDAlive = pidAlive(pid)
	}
	if _, err := os.Stat(cfg.CoordinatorLock()); err == nil {
		snap.Coordinator.LockPresent = true
	}
	snap.Coordinator.BusDialable = dialable(cfg.BusSocket())

	registry := map[string]agentcard.RegistryRecord{}
	if snap.Coordinator.BusDialable {
		if recs, err := readRegistry(cfg); err == nil {
			for _, rec := range recs {
				registry[rec.Card.Name] = rec
			}
		}
	}

	socketPaths, _ := filepath.Glob(filepath.Join(cfg.AgentsDir(), "*.sock"))
	seenSockets := make(map[string]struct{}, len(socketPaths))

	for _, sockPath := range socketPaths {
		name := strings.TrimSuffix(filepath.Base(sockPath), ".sock")
		seenSockets[name] = struct{}{}

		info := SidecarInfo{
			Name:    name,
			Socket:  sockPath,
			LogPath: filepath.Join(cfg.MeshDir, "logs", "sidecar-"+name+".log"),
		}
		info.SocketDialable = dialable(sockPath)

		var runtime *meshapi.RuntimeResult
		if info.SocketDialable {
			if rt, err := queryRuntime(sockPath); err == nil {
				runtime = &rt
				info.PID = rt.SidecarPID
				info.PIDAlive = pidAlive(rt.SidecarPID)
				for _, child := range rt.Children {
					alive := child.State == "running" && pidAlive(child.PID)
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
				info.PIDAlive = pidAlive(rec.Card.PID)
			}
			if rec.State == agentcard.PresenceLive {
				if !info.SocketDialable {
					info.Drift = append(info.Drift, "ghost_agent")
				} else if runtime == nil {
					info.Drift = append(info.Drift, "presence_mismatch")
				}
			}
			if info.PID > 0 && !info.PIDAlive {
				info.Drift = append(info.Drift, "stale_pid")
			}
		} else {
			info.Drift = append(info.Drift, "orphan_socket")
			if info.PID > 0 && !info.PIDAlive {
				info.Drift = append(info.Drift, "stale_pid")
			}
		}

		snap.Sidecars = append(snap.Sidecars, info)
	}

	for name, rec := range registry {
		if _, ok := seenSockets[name]; ok {
			continue
		}
		info := SidecarInfo{
			Name:     name,
			Socket:   cfg.AgentSocket(name),
			Registry: &rec,
			LogPath:  filepath.Join(cfg.MeshDir, "logs", "sidecar-"+name+".log"),
		}
		if rec.Card.PID > 0 {
			info.PID = rec.Card.PID
			info.PIDAlive = pidAlive(rec.Card.PID)
			if !info.PIDAlive {
				info.Drift = append(info.Drift, "stale_pid")
			}
		}
		info.Drift = append(info.Drift, "ghost_agent")
		snap.Sidecars = append(snap.Sidecars, info)
	}

	if len(socketPaths) > 0 && !snap.Coordinator.BusDialable {
		snap.Anomalies = append(snap.Anomalies, "coordinator_down: sidecar sockets exist but bus is not dialable")
	}
	for _, sc := range snap.Sidecars {
		for _, d := range sc.Drift {
			snap.Anomalies = append(snap.Anomalies, fmt.Sprintf("%s: %s", sc.Name, d))
		}
	}

	return snap, nil
}

func readPID(path string) (int, error) {
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

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func dialable(path string) bool {
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
