package e2e

// Cross-process settings-screen acceptance (v1 settings screen): the dashboard
// write path (POST /api/settings) delegates to the settings authority over the
// real coordinator-embedded bus, and the coordinator republishes its effective
// config. Assertions are over typed JSON only — never prose.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func dashToken(t *testing.T, m *mesh) string {
	t.Helper()
	path := filepath.Join(m.dir, "dashboard.token")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && len(bytes.TrimSpace(b)) > 0 {
			return strings.TrimSpace(string(b))
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dashboard token file never appeared at %s", path)
	return ""
}

func getSettings(t *testing.T, base string) map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/api/settings")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/settings: status %d", resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func postSettings(t *testing.T, base, token, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest("POST", base+"/api/settings", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var out map[string]any
	json.Unmarshal(raw, &out) //nolint:errcheck
	return resp.StatusCode, out
}

// TestSettingsWritePathAcrossProcesses drives the full settings spine over real
// meshd processes: a fresh mesh reports nothing armed (d), an invalid write is a
// typed 400 (c), and a valid hot write persists and is reflected in the
// coordinator's effective snapshot (a, at the write-path level).
func TestSettingsWritePathAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()
	base := m.startDashboard()
	token := dashToken(t, m)

	// (d) Fresh mesh: nothing staged, nothing armed.
	got := getSettings(t, base)
	if got["staged"] != nil {
		t.Fatalf("fresh mesh has a staged record: %v", got["staged"])
	}
	eff, _ := got["effective"].(map[string]any)
	// The coordinator publishes its effective snapshot on Start; the dashboard
	// records it. Allow a moment for the SSE tap to populate it.
	if eff == nil {
		m.eventually(3*time.Second, "effective settings snapshot observed", func() bool {
			g := getSettings(t, base)
			e, _ := g["effective"].(map[string]any)
			return e != nil
		})
		eff, _ = getSettings(t, base)["effective"].(map[string]any)
	}
	if eff != nil {
		if eff["autoExperts"] == true {
			t.Fatal("fresh mesh reports auto-experts armed")
		}
	}

	// (c) Invalid write → typed 400 naming the field, never a false success.
	code, body := postSettings(t, base, token, `{"maxWorkers":0}`)
	if code != http.StatusBadRequest {
		t.Fatalf("invalid write: status %d, want 400 (%v)", code, body)
	}
	if msg, _ := body["message"].(string); !strings.Contains(msg, "maxWorkers") {
		t.Fatalf("400 message does not name the field: %q", msg)
	}

	// (a) Valid hot write (budget raise) → 201, persisted, reflected in the
	// coordinator's republished effective snapshot.
	code, body = postSettings(t, base, token, `{"stagedRev":0,"budgetUSD":25,"maxWorkers":3}`)
	if code != http.StatusCreated {
		t.Fatalf("valid write: status %d, want 201 (%v)", code, body)
	}
	if applied, _ := body["applied"].(map[string]any); applied["hot"] != true {
		t.Fatalf("hot change not reported applied: %v", body["applied"])
	}

	// The staged record is now durable and visible, and the coordinator's
	// effective snapshot reflects the raised budget within a sweep.
	m.eventually(5*time.Second, "effective budget reflects the staged raise", func() bool {
		g := getSettings(t, base)
		staged, _ := g["staged"].(map[string]any)
		if staged == nil || staged["budgetUSD"] != float64(25) {
			return false
		}
		e, _ := g["effective"].(map[string]any)
		return e != nil && e["budgetUSD"] == float64(25)
	})

	// A stale stagedRev is a typed 409 (no silent clobber).
	code, _ = postSettings(t, base, token, `{"stagedRev":0,"budgetUSD":30}`)
	if code != http.StatusConflict {
		t.Fatalf("stale rev: status %d, want 409", code)
	}
}
