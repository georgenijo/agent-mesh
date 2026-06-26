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
	// #64 retry/backoff defaults.
	if cfg.TriageMaxAttempts != DefaultTriageMaxAttempts {
		t.Fatalf("TriageMaxAttempts default = %d, want %d", cfg.TriageMaxAttempts, DefaultTriageMaxAttempts)
	}
	if cfg.TriageBackoff != DefaultTriageBackoff {
		t.Fatalf("TriageBackoff default = %s, want %s", cfg.TriageBackoff, DefaultTriageBackoff)
	}

	t.Setenv(EnvPlannerCLI, "/usr/local/bin/claude")
	t.Setenv(EnvPlannerModel, "") // explicit empty = CLI default model
	t.Setenv(EnvTriageTimeout, "30s")
	t.Setenv(EnvTriageMaxAttempts, "6")
	t.Setenv(EnvTriageBackoff, "5s")
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
	if cfg.TriageMaxAttempts != 6 {
		t.Fatalf("TriageMaxAttempts = %d, want 6", cfg.TriageMaxAttempts)
	}
	if cfg.TriageBackoff != 5*time.Second {
		t.Fatalf("TriageBackoff = %s, want 5s", cfg.TriageBackoff)
	}

	t.Setenv(EnvTriageTimeout, "30s") // restore valid value
	t.Setenv(EnvTriageMaxAttempts, "0")
	if _, err := Load(); err == nil {
		t.Fatal("want error for non-positive triage max attempts")
	}
	t.Setenv(EnvTriageMaxAttempts, "4") // restore valid value
	t.Setenv(EnvTriageBackoff, "-5s")
	if _, err := Load(); err == nil {
		t.Fatal("want error for non-positive triage backoff")
	}

	t.Setenv(EnvTriageBackoff, "30s") // restore valid value
	t.Setenv(EnvTriageTimeout, "-5s")
	if _, err := Load(); err == nil {
		t.Fatal("want error for non-positive triage timeout")
	}
}

func TestLoadWorkerKnobs(t *testing.T) {
	t.Setenv(EnvMeshDir, t.TempDir())

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkerCLI != "" {
		t.Fatalf("WorkerCLI default = %q, want empty (scheduler disabled)", cfg.WorkerCLI)
	}
	if cfg.WorkerModel != DefaultWorkerModel {
		t.Fatalf("WorkerModel default = %q, want %q", cfg.WorkerModel, DefaultWorkerModel)
	}
	if cfg.WorkerTimeout != DefaultWorkerTimeout {
		t.Fatalf("WorkerTimeout default = %s, want %s", cfg.WorkerTimeout, DefaultWorkerTimeout)
	}
	if cfg.BudgetUSD != 0 {
		t.Fatalf("BudgetUSD default = %v, want 0 (unlimited)", cfg.BudgetUSD)
	}
	if cfg.MaxWorkers != DefaultMaxWorkers {
		t.Fatalf("MaxWorkers default = %d, want %d", cfg.MaxWorkers, DefaultMaxWorkers)
	}

	t.Setenv(EnvWorkerCLI, "/usr/local/bin/claude")
	t.Setenv(EnvWorkerModel, "") // explicit empty = CLI default model
	t.Setenv(EnvWorkerTimeout, "90s")
	t.Setenv(EnvBudgetUSD, "12.50")
	t.Setenv(EnvMaxWorkers, "8")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WorkerCLI != "/usr/local/bin/claude" {
		t.Fatalf("WorkerCLI = %q", cfg.WorkerCLI)
	}
	if cfg.WorkerModel != "" {
		t.Fatalf("WorkerModel = %q, want empty after explicit unset", cfg.WorkerModel)
	}
	if cfg.WorkerTimeout != 90*time.Second {
		t.Fatalf("WorkerTimeout = %s", cfg.WorkerTimeout)
	}
	if cfg.BudgetUSD != 12.50 {
		t.Fatalf("BudgetUSD = %v, want 12.50", cfg.BudgetUSD)
	}
	if cfg.MaxWorkers != 8 {
		t.Fatalf("MaxWorkers = %d, want 8", cfg.MaxWorkers)
	}

	t.Setenv(EnvBudgetUSD, "-1")
	if _, err := Load(); err == nil {
		t.Fatal("want error for negative budget")
	}
	t.Setenv(EnvBudgetUSD, "10")
	t.Setenv(EnvMaxWorkers, "0")
	if _, err := Load(); err == nil {
		t.Fatal("want error for non-positive max workers")
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

func TestLoadWorkerWorktreeKnobs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(EnvMeshDir, dir)
	t.Setenv(EnvReposDir, "/srv/repos")
	t.Setenv(EnvKeepWorktrees, KeepWorktreesAlways)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReposDir != "/srv/repos" {
		t.Fatalf("ReposDir = %q", cfg.ReposDir)
	}
	if cfg.KeepWorktrees != KeepWorktreesAlways {
		t.Fatalf("KeepWorktrees = %q", cfg.KeepWorktrees)
	}
	if got, want := cfg.WorkersDir(), filepath.Join(dir, "workers"); got != want {
		t.Fatalf("WorkersDir = %q, want %q", got, want)
	}
}

func TestLoadWorkerWorktreeDefaults(t *testing.T) {
	t.Setenv(EnvMeshDir, t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReposDir != "" {
		t.Fatalf("ReposDir default = %q, want empty (driver refuses without explicit mapping)", cfg.ReposDir)
	}
	if cfg.KeepWorktrees != KeepWorktreesOnFailure {
		t.Fatalf("KeepWorktrees default = %q, want %q", cfg.KeepWorktrees, KeepWorktreesOnFailure)
	}
}

func TestLoadRejectsBadBudget(t *testing.T) {
	t.Setenv(EnvMeshDir, t.TempDir())

	for _, raw := range []string{"nan", "inf", "+inf"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(EnvBudgetUSD, raw)
			if _, err := Load(); err == nil {
				t.Fatalf("MESH_BUDGET_USD=%q: want error, got nil", raw)
			}
		})
	}
}

func TestLoadRejectsBadKeepWorktrees(t *testing.T) {
	t.Setenv(EnvMeshDir, t.TempDir())
	t.Setenv(EnvKeepWorktrees, "sometimes")
	if _, err := Load(); err == nil {
		t.Fatal("Load accepted MESH_KEEP_WORKTREES=sometimes")
	}
}

func TestLoadAuditFanoutKnob(t *testing.T) {
	t.Setenv(EnvMeshDir, t.TempDir())

	// Default: on.
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AuditFanout {
		t.Fatal("AuditFanout default = false, want true (on by default)")
	}

	for _, on := range []string{"on", "true", "1"} {
		t.Setenv(EnvAuditFanout, on)
		cfg, err := Load()
		if err != nil {
			t.Fatalf("MESH_AUDIT_FANOUT=%q: %v", on, err)
		}
		if !cfg.AuditFanout {
			t.Fatalf("MESH_AUDIT_FANOUT=%q gave AuditFanout=false", on)
		}
	}
	for _, off := range []string{"off", "false", "0"} {
		t.Setenv(EnvAuditFanout, off)
		cfg, err := Load()
		if err != nil {
			t.Fatalf("MESH_AUDIT_FANOUT=%q: %v", off, err)
		}
		if cfg.AuditFanout {
			t.Fatalf("MESH_AUDIT_FANOUT=%q gave AuditFanout=true", off)
		}
	}

	t.Setenv(EnvAuditFanout, "maybe")
	if _, err := Load(); err == nil {
		t.Fatal("Load accepted MESH_AUDIT_FANOUT=maybe")
	}
}
