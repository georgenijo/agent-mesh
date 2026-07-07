package config

import (
	"strings"
	"testing"
	"time"
)

func TestBackoffDefaultAndEnv(t *testing.T) {
	t.Setenv(EnvMeshDir, t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backoff != DefaultReDispatchBackoff {
		t.Fatalf("default Backoff = %s, want %s", cfg.Backoff, DefaultReDispatchBackoff)
	}

	t.Setenv(EnvReDispatchBackoff, "90s")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backoff != 90*time.Second {
		t.Fatalf("env Backoff = %s, want 90s", cfg.Backoff)
	}

	// A non-positive value is rejected at Load, same as the other duration knobs.
	t.Setenv(EnvReDispatchBackoff, "0s")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for zero backoff")
	}
}

// TestValidateParityWithLoad pins that Validate rejects exactly what a bad Load
// would: a resolved config that violates a cross-field invariant fails both.
func TestValidateParityWithLoad(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(EnvMeshDir, dir)
	// evict-after <= away-after is a boot-rejected Load case.
	t.Setenv(EnvHeartbeatInterval, "1s")
	t.Setenv(EnvAwayAfter, "5s")
	t.Setenv(EnvEvictAfter, "3s")
	if _, err := Load(); err == nil {
		t.Fatal("Load accepted evict<=away")
	}

	// The same invariant, checked directly through Validate on a hand-built config.
	base := Config{
		HeartbeatInterval: time.Second, AwayAfter: 5 * time.Second, EvictAfter: 3 * time.Second,
		RegistrationGrace: time.Second, ClaimTTL: 10 * time.Second,
		TriageTimeout: time.Minute, TriageBackoff: time.Second, WorkerTimeout: time.Minute,
		ReviewTimeout: time.Minute, Backoff: time.Second, MaxWorkers: 1, TriageMaxAttempts: 1,
		ReviewPoolSize: 1, KeepWorktrees: KeepWorktreesOnFailure,
	}
	if err := Validate(base); err == nil || !strings.Contains(err.Error(), "evict-after") {
		t.Fatalf("Validate err = %v, want evict-after violation", err)
	}
}

func TestValidateRangeChecks(t *testing.T) {
	ok := Config{
		HeartbeatInterval: 5 * time.Second, AwayAfter: 15 * time.Second, EvictAfter: 60 * time.Second,
		RegistrationGrace: 10 * time.Second, ClaimTTL: 140 * time.Second,
		TriageTimeout: 2 * time.Minute, TriageBackoff: 30 * time.Second, WorkerTimeout: 10 * time.Minute,
		ReviewTimeout: 5 * time.Minute, Backoff: 30 * time.Second, MaxWorkers: 4, TriageMaxAttempts: 4,
		ReviewPoolSize: 1, KeepWorktrees: KeepWorktreesOnFailure,
	}
	if err := Validate(ok); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	bad := func(mut func(*Config), want string) {
		t.Helper()
		c := ok
		mut(&c)
		if err := Validate(c); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate err = %v, want mention of %q", err, want)
		}
	}
	bad(func(c *Config) { c.MaxWorkers = 0 }, "MAX_WORKERS")
	bad(func(c *Config) { c.BudgetUSD = -1 }, "BUDGET")
	bad(func(c *Config) { c.ReviewPoolSize = 0 }, "REVIEW_POOL_SIZE")
	bad(func(c *Config) { c.ReviewRetries = -1 }, "REVIEW_RETRIES")
	bad(func(c *Config) { c.TriageMaxAttempts = 0 }, "TRIAGE_MAX_ATTEMPTS")
	bad(func(c *Config) { c.KeepWorktrees = "nope" }, "KEEP_WORKTREES")
	bad(func(c *Config) { c.Backoff = 0 }, "REDISPATCH_BACKOFF")
}
