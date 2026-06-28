package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/config"
)

// saveEnv saves key's current value and schedules restoration after the test.
func saveEnv(t *testing.T, key string) {
	t.Helper()
	old, ok := os.LookupEnv(key)
	t.Cleanup(func() {
		if ok {
			os.Setenv(key, old) //nolint:errcheck
		} else {
			os.Unsetenv(key) //nolint:errcheck
		}
	})
}

// writeJSONFile marshals v as JSON and writes it to path.
func writeJSONFile(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestApplyUpConfigFileNotExist(t *testing.T) {
	if err := applyUpConfigFile(filepath.Join(t.TempDir(), "does-not-exist.json")); err != nil {
		t.Fatalf("missing config file must not error, got %v", err)
	}
}

func TestApplyUpConfigFileParsesFields(t *testing.T) {
	dir := t.TempDir()
	budget := 42.5
	ae := true
	writeJSONFile(t, filepath.Join(dir, "config.json"), upFileConfig{
		PlannerCLI:    "my-planner",
		PlannerModel:  "opus",
		WorkerCLI:     "my-worker",
		WorkerModel:   "haiku",
		ReposDir:      "/repos",
		ReviewRole:    "reviewer",
		BudgetUSD:     &budget,
		AutoExperts:   &ae,
		JobsAddr:      "127.0.0.1:9000",
		GitHubRepo:    "owner/repo",
		DashboardAddr: "127.0.0.1:9001",
		ObserveAddr:   "127.0.0.1:9002",
	})

	for _, key := range []string{
		config.EnvPlannerCLI, config.EnvPlannerModel,
		config.EnvWorkerCLI, config.EnvWorkerModel,
		config.EnvReposDir, config.EnvReviewRole,
		config.EnvBudgetUSD, config.EnvAutoExperts,
		config.EnvJobsAddr, config.EnvGitHubRepo,
		config.EnvDashboardAddr, config.EnvObserveAddr,
	} {
		saveEnv(t, key)
		os.Unsetenv(key) //nolint:errcheck
	}

	if err := applyUpConfigFile(filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	check := func(key, want string) {
		t.Helper()
		if got := os.Getenv(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	check(config.EnvPlannerCLI, "my-planner")
	check(config.EnvPlannerModel, "opus")
	check(config.EnvWorkerCLI, "my-worker")
	check(config.EnvWorkerModel, "haiku")
	check(config.EnvReposDir, "/repos")
	check(config.EnvReviewRole, "reviewer")
	check(config.EnvBudgetUSD, "42.5")
	check(config.EnvAutoExperts, "on")
	check(config.EnvJobsAddr, "127.0.0.1:9000")
	check(config.EnvGitHubRepo, "owner/repo")
	check(config.EnvDashboardAddr, "127.0.0.1:9001")
	check(config.EnvObserveAddr, "127.0.0.1:9002")
}

func TestApplyUpConfigFileEnvPrecedence(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, filepath.Join(dir, "config.json"), upFileConfig{
		PlannerCLI: "file-planner",
	})

	saveEnv(t, config.EnvPlannerCLI)
	os.Setenv(config.EnvPlannerCLI, "env-planner") //nolint:errcheck

	if err := applyUpConfigFile(filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := os.Getenv(config.EnvPlannerCLI); got != "env-planner" {
		t.Errorf("env must win over config file: got %q, want %q", got, "env-planner")
	}
}

func TestApplyUpConfigFileBadJSON(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "bad.json")
	os.WriteFile(p, []byte("{not valid json}"), 0o600) //nolint:errcheck
	if err := applyUpConfigFile(p); err == nil {
		t.Fatal("expected error for bad JSON, got nil")
	}
}

func TestApplyUpConfigFileAutoExpertsFalse(t *testing.T) {
	dir := t.TempDir()
	ae := false
	writeJSONFile(t, filepath.Join(dir, "config.json"), upFileConfig{AutoExperts: &ae})

	saveEnv(t, config.EnvAutoExperts)
	os.Unsetenv(config.EnvAutoExperts) //nolint:errcheck

	if err := applyUpConfigFile(filepath.Join(dir, "config.json")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := os.Getenv(config.EnvAutoExperts); got != "off" {
		t.Errorf("autoExperts=false: got %q, want %q", got, "off")
	}
}
