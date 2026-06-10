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

// TestLoadPlannerKnobs pins the triage planner contract: no planner by
// default (an autostarted coordinator must never spawn LLM processes
// unasked), model defaults to the cheap pin but an explicit empty restores
// the CLI default, and the timeout parses like every other duration knob.
func TestLoadPlannerKnobs(t *testing.T) {
	t.Setenv(EnvMeshDir, t.TempDir())

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PlannerCLI != "" {
		t.Fatalf("PlannerCLI default = %q, want empty (triage disabled)", cfg.PlannerCLI)
	}
	if cfg.PlannerModel != DefaultPlannerModel {
		t.Fatalf("PlannerModel default = %q, want %q", cfg.PlannerModel, DefaultPlannerModel)
	}
	if cfg.TriageTimeout != DefaultTriageTimeout {
		t.Fatalf("TriageTimeout default = %s, want %s", cfg.TriageTimeout, DefaultTriageTimeout)
	}

	t.Setenv(EnvPlannerCLI, "/usr/local/bin/claude")
	t.Setenv(EnvPlannerModel, "") // explicit empty = CLI default model
	t.Setenv(EnvTriageTimeout, "30s")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PlannerCLI != "/usr/local/bin/claude" {
		t.Fatalf("PlannerCLI = %q", cfg.PlannerCLI)
	}
	if cfg.PlannerModel != "" {
		t.Fatalf("PlannerModel = %q, want empty after explicit unset", cfg.PlannerModel)
	}
	if cfg.TriageTimeout != 30*time.Second {
		t.Fatalf("TriageTimeout = %s", cfg.TriageTimeout)
	}

	t.Setenv(EnvTriageTimeout, "-5s")
	if _, err := Load(); err == nil {
		t.Fatal("want error for non-positive triage timeout")
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
