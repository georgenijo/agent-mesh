package dashboard

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestSettingsAPI(t *testing.T) {
	cfg, _, d := startStack(t)
	base := "http://" + d.Addr()

	resp, err := http.Get(base + "/api/settings")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/settings: status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type %q, want application/json", ct)
	}

	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	// Verify all required fields are present.
	requiredFields := []string{
		"workerCLI", "workerModel",
		"plannerCLI", "plannerModel",
		"maxWorkers",
		"workerTimeout", "triageTimeout", "reviewTimeout",
		"budgetUSD",
		"reviewRole", "reviewPoolSize", "reviewRetries",
		"reposDir",
		"dashboardAddr", "observeAddr",
		"autoExperts", "expertIdleTTL",
	}
	for _, field := range requiredFields {
		if _, ok := got[field]; !ok {
			t.Errorf("missing field %q in /api/settings response", field)
		}
	}

	// Spot-check a few values against the config used to start the stack.
	if v, ok := got["workerCLI"].(string); !ok || v != cfg.WorkerCLI {
		t.Errorf("workerCLI = %v, want %q", got["workerCLI"], cfg.WorkerCLI)
	}
	if v, ok := got["workerModel"].(string); !ok || v != cfg.WorkerModel {
		t.Errorf("workerModel = %v, want %q", got["workerModel"], cfg.WorkerModel)
	}
	if v, ok := got["maxWorkers"].(float64); !ok || int(v) != cfg.MaxWorkers {
		t.Errorf("maxWorkers = %v, want %d", got["maxWorkers"], cfg.MaxWorkers)
	}
	if v, ok := got["reviewPoolSize"].(float64); !ok || int(v) != cfg.ReviewPoolSize {
		t.Errorf("reviewPoolSize = %v, want %d", got["reviewPoolSize"], cfg.ReviewPoolSize)
	}
	if v, ok := got["reviewRetries"].(float64); !ok || int(v) != cfg.ReviewRetries {
		t.Errorf("reviewRetries = %v, want %d", got["reviewRetries"], cfg.ReviewRetries)
	}
	if v, ok := got["autoExperts"].(bool); !ok || v != cfg.AutoExperts {
		t.Errorf("autoExperts = %v, want %v", got["autoExperts"], cfg.AutoExperts)
	}
	if v, ok := got["budgetUSD"].(float64); !ok || v != cfg.BudgetUSD {
		t.Errorf("budgetUSD = %v, want %v", got["budgetUSD"], cfg.BudgetUSD)
	}
}
