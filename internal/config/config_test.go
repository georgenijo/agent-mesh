package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv(EnvMeshDir, t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HeartbeatInterval != DefaultHeartbeatInterval {
		t.Fatalf("heartbeat = %s", cfg.HeartbeatInterval)
	}
	if cfg.AwayAfter != DefaultAwayAfter || cfg.EvictAfter != DefaultEvictAfter {
		t.Fatalf("away=%s evict=%s", cfg.AwayAfter, cfg.EvictAfter)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(EnvMeshDir, dir)
	t.Setenv(EnvHeartbeatInterval, "100ms")
	t.Setenv(EnvAwayAfter, "300ms")
	t.Setenv(EnvEvictAfter, "1s")
	t.Setenv(EnvRegistrationGrace, "200ms")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MeshDir != dir {
		t.Fatalf("mesh dir = %s", cfg.MeshDir)
	}
	if cfg.HeartbeatInterval != 100*time.Millisecond {
		t.Fatalf("heartbeat = %s", cfg.HeartbeatInterval)
	}
	if cfg.BusSocket() != filepath.Join(dir, "bus.sock") {
		t.Fatalf("bus socket = %s", cfg.BusSocket())
	}
	if cfg.AgentSocket("test") != filepath.Join(dir, "agents", "test.sock") {
		t.Fatalf("agent socket = %s", cfg.AgentSocket("test"))
	}
}

func TestLoadRejectsBadDurations(t *testing.T) {
	t.Setenv(EnvMeshDir, t.TempDir())

	t.Setenv(EnvHeartbeatInterval, "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("want error for unparseable duration")
	}

	t.Setenv(EnvHeartbeatInterval, "5s")
	t.Setenv(EnvAwayAfter, "1s") // less than heartbeat
	if _, err := Load(); err == nil {
		t.Fatal("want error for away < heartbeat")
	}

	t.Setenv(EnvAwayAfter, "15s")
	t.Setenv(EnvEvictAfter, "10s") // evict <= away
	if _, err := Load(); err == nil {
		t.Fatal("want error for evict <= away")
	}
}
