// Package config resolves Agent Mesh paths and timing knobs.
//
// Everything is overridable via environment variables so tests and e2e runs
// can use temp dirs and fast clocks without touching production defaults.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Env variable names.
const (
	EnvMeshDir           = "MESH_DIR"
	EnvHeartbeatInterval = "MESH_HEARTBEAT_INTERVAL"
	EnvAwayAfter         = "MESH_AWAY_AFTER"
	EnvEvictAfter        = "MESH_EVICT_AFTER"
	EnvRegistrationGrace = "MESH_REGISTRATION_GRACE"
	EnvClaimTTL          = "MESH_CLAIM_TTL"
	EnvDashboardAddr     = "MESH_DASHBOARD_ADDR"
	EnvObserveAddr       = "MESH_OBSERVE_ADDR"
	EnvAgentSocket       = "MESH_SOCKET"     // CLI → sidecar socket override
	EnvMeshdBin          = "MESH_MESHD"      // path to meshd for autostart
	EnvExpertCLI         = "MESH_EXPERT_CLI" // agent CLI an expert responder drives (default "claude")
)

// Defaults.
const (
	// DefaultExpertCLI is the agent CLI an expert responder drives when
	// MESH_EXPERT_CLI is unset. A literal (not internal/runtime.DefaultBinary)
	// so config carries no dependency on the runtime package.
	DefaultExpertCLI = "claude"

	DefaultHeartbeatInterval = 5 * time.Second
	DefaultAwayAfter         = 15 * time.Second // 3 missed beats
	DefaultEvictAfter        = 60 * time.Second
	DefaultRegistrationGrace = 10 * time.Second
	DefaultDashboardAddr     = "127.0.0.1:8737"
	DefaultObserveAddr       = "127.0.0.1:8739"
)

// Config carries resolved paths and timings for all meshd modes and the CLI.
type Config struct {
	MeshDir           string
	HeartbeatInterval time.Duration
	AwayAfter         time.Duration // last beat older than this → away
	EvictAfter        time.Duration // last beat older than this → evicted
	RegistrationGrace time.Duration // no away/evict this soon after register
	ClaimTTL          time.Duration // claim lease backstop; renewed each heartbeat
	DashboardAddr     string
	ObserveAddr       string
	ExpertCLI         string // agent CLI an expert responder drives (meshd --mode expert)
}

// Load resolves config from the environment with defaults.
func Load() (Config, error) {
	cfg := Config{
		HeartbeatInterval: DefaultHeartbeatInterval,
		AwayAfter:         DefaultAwayAfter,
		EvictAfter:        DefaultEvictAfter,
		RegistrationGrace: DefaultRegistrationGrace,
		DashboardAddr:     DefaultDashboardAddr,
		ObserveAddr:       DefaultObserveAddr,
		ExpertCLI:         DefaultExpertCLI,
	}

	if dir := os.Getenv(EnvMeshDir); dir != "" {
		cfg.MeshDir = dir
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return Config{}, fmt.Errorf("config: resolve home dir: %w", err)
		}
		cfg.MeshDir = filepath.Join(home, ".mesh")
	}

	for _, d := range []struct {
		env string
		dst *time.Duration
	}{
		{EnvHeartbeatInterval, &cfg.HeartbeatInterval},
		{EnvAwayAfter, &cfg.AwayAfter},
		{EnvEvictAfter, &cfg.EvictAfter},
		{EnvRegistrationGrace, &cfg.RegistrationGrace},
		{EnvClaimTTL, &cfg.ClaimTTL},
	} {
		raw := os.Getenv(d.env)
		if raw == "" {
			continue
		}
		dur, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("config: %s=%q: %w", d.env, raw, err)
		}
		if dur <= 0 {
			return Config{}, fmt.Errorf("config: %s must be positive, got %q", d.env, raw)
		}
		*d.dst = dur
	}

	if addr := os.Getenv(EnvDashboardAddr); addr != "" {
		cfg.DashboardAddr = addr
	}
	if addr := os.Getenv(EnvObserveAddr); addr != "" {
		cfg.ObserveAddr = addr
	}
	if cli := os.Getenv(EnvExpertCLI); cli != "" {
		cfg.ExpertCLI = cli
	}

	if cfg.AwayAfter < cfg.HeartbeatInterval {
		return Config{}, fmt.Errorf("config: away-after (%s) must be >= heartbeat interval (%s)",
			cfg.AwayAfter, cfg.HeartbeatInterval)
	}
	if cfg.EvictAfter <= cfg.AwayAfter {
		return Config{}, fmt.Errorf("config: evict-after (%s) must be > away-after (%s)",
			cfg.EvictAfter, cfg.AwayAfter)
	}
	// Claim lease backstop: like the registry record TTL, it must outlast
	// every legitimate silent window (the eviction sweep is the primary
	// release path; the TTL self-heals if the coordinator is down). Derived
	// from EvictAfter unless explicitly set.
	if cfg.ClaimTTL == 0 {
		cfg.ClaimTTL = 2 * (cfg.EvictAfter + cfg.RegistrationGrace)
	}
	if cfg.ClaimTTL <= cfg.HeartbeatInterval {
		return Config{}, fmt.Errorf("config: claim-ttl (%s) must be > heartbeat interval (%s)",
			cfg.ClaimTTL, cfg.HeartbeatInterval)
	}
	return cfg, nil
}

// BusSocket is the coordinator-owned bus socket path.
func (c Config) BusSocket() string { return filepath.Join(c.MeshDir, "bus.sock") }

// AgentsDir holds per-agent sidecar sockets.
func (c Config) AgentsDir() string { return filepath.Join(c.MeshDir, "agents") }

// AgentSocket is the sidecar socket path for the named agent.
func (c Config) AgentSocket(name string) string {
	return filepath.Join(c.AgentsDir(), name+".sock")
}

// AgentPIDFile is the pidfile the sidecar writes beside its socket. It is the
// fact source of last resort for the ops plane: an agent evicted from the
// registry whose socket is hung is otherwise invisible.
func (c Config) AgentPIDFile(name string) string {
	return filepath.Join(c.AgentsDir(), name+".pid")
}

// CoordinatorLock is the flock file used to elect a single coordinator
// autostarter when several sidecars race to boot one.
func (c Config) CoordinatorLock() string { return filepath.Join(c.MeshDir, "coordinator.lock") }

// StreamsDir holds the bus server's durable stream files (one JSONL per
// stream). Owned by the coordinator-embedded bus server only.
func (c Config) StreamsDir() string { return filepath.Join(c.MeshDir, "streams") }

// CoordinatorPID is written by the running coordinator for ops inspection.
func (c Config) CoordinatorPID() string { return filepath.Join(c.MeshDir, "coordinator.pid") }

// DashboardPID is written by the running dashboard for ops inspection.
func (c Config) DashboardPID() string { return filepath.Join(c.MeshDir, "dashboard.pid") }

// DashboardAddrFile holds the dashboard's real bound address (it may differ
// from DashboardAddr after a port-conflict fallback or :0). The one authority
// for "where is the UI" — the daemon's stdout goes to a logfile when spawned
// detached.
func (c Config) DashboardAddrFile() string { return filepath.Join(c.MeshDir, "dashboard.addr") }

// DashboardLock is the flock file electing a single dashboard autostarter.
func (c Config) DashboardLock() string { return filepath.Join(c.MeshDir, "dashboard.lock") }

// DashboardTokenFile holds the write-API bearer token the dashboard generated
// on start. The UI reads it from the page's own JSON embedding (never directly
// from disk); CLI users can read it too. Observer endpoints stay unauthenticated.
func (c Config) DashboardTokenFile() string {
	return filepath.Join(c.MeshDir, "dashboard.token")
}

// ObservePID is written by the running observe server for ops inspection.
func (c Config) ObservePID() string { return filepath.Join(c.MeshDir, "observe.pid") }

// ObserveAddrFile holds the observe server's real bound address (see
// DashboardAddrFile).
func (c Config) ObserveAddrFile() string { return filepath.Join(c.MeshDir, "observe.addr") }

// ObserveLock is the flock file electing a single observe autostarter.
func (c Config) ObserveLock() string { return filepath.Join(c.MeshDir, "observe.lock") }

// EnsureDirs creates the mesh directories with owner-only permissions.
func (c Config) EnsureDirs() error {
	for _, dir := range []string{c.MeshDir, c.AgentsDir()} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("config: create %s: %w", dir, err)
		}
	}
	return nil
}
