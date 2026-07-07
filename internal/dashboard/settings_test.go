package dashboard

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func postSettings(t *testing.T, base, token, body string) (*http.Response, map[string]any) {
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
	return resp, out
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

func TestGetSettingsShape(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()
	got := getSettings(t, base)
	for _, k := range []string{"staged", "stagedRev", "effective", "defaults", "meta", "envOverridden"} {
		if _, ok := got[k]; !ok {
			t.Errorf("GET /api/settings missing key %q", k)
		}
	}
	// meta is the apply-class table.
	meta, ok := got["meta"].([]any)
	if !ok || len(meta) == 0 {
		t.Fatalf("meta is empty or wrong type: %T", got["meta"])
	}
	// defaults carries the compiled default projection.
	defaults, ok := got["defaults"].(map[string]any)
	if !ok || defaults["maxWorkers"] == nil {
		t.Fatalf("defaults missing maxWorkers: %v", got["defaults"])
	}
}

func TestUpdateSettingsRequiresAuth(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()

	resp, _ := postSettings(t, base, "", `{"budgetUSD":5}`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: status %d, want 401", resp.StatusCode)
	}
	resp2, _ := postSettings(t, base, "wrong", `{"budgetUSD":5}`)
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong token: status %d, want 403", resp2.StatusCode)
	}
}

func TestUpdateSettingsRejectsInvalid(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()
	resp, body := postSettings(t, base, d.WriteToken(), `{"maxWorkers":0}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid: status %d, want 400", resp.StatusCode)
	}
	msg, _ := body["message"].(string)
	if !strings.Contains(msg, "maxWorkers") {
		t.Fatalf("400 message does not name the field: %q", msg)
	}
}

func TestUpdateSettingsHappyPathAndStaleRev(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()
	token := d.WriteToken()

	// First write (create-only): stagedRev 0.
	resp, out := postSettings(t, base, token, `{"stagedRev":0,"budgetUSD":5,"maxWorkers":3}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: status %d, want 201 (%v)", resp.StatusCode, out)
	}
	applied, _ := out["applied"].(map[string]any)
	if applied["hot"] != true {
		t.Errorf("expected hot applied=true, got %v", out["applied"])
	}

	// The staged record is now visible with a bumped stagedRev.
	got := getSettings(t, base)
	rev, _ := got["stagedRev"].(float64)
	if rev == 0 {
		t.Fatalf("stagedRev did not advance: %v", got["stagedRev"])
	}
	staged, _ := got["staged"].(map[string]any)
	if staged == nil || staged["budgetUSD"] != float64(5) {
		t.Fatalf("staged budget not persisted: %v", got["staged"])
	}

	// A stale stagedRev loses the CAS race → 409.
	resp2, _ := postSettings(t, base, token, `{"stagedRev":0,"budgetUSD":6}`)
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("stale rev: status %d, want 409", resp2.StatusCode)
	}
}

func TestUpdateSettingsArmingRequiresConfirm(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()
	token := d.WriteToken()

	// reviewRole is an arming knob: without confirm → 409 confirmation_required.
	resp, body := postSettings(t, base, token, `{"stagedRev":0,"reviewRole":"reviewer"}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("arming without confirm: status %d, want 409 (%v)", resp.StatusCode, body)
	}
	if body["error"] != "confirmation_required" {
		t.Fatalf("error = %v, want confirmation_required", body["error"])
	}
	arming, _ := body["arming"].([]any)
	if len(arming) == 0 || arming[0] != "reviewRole" {
		t.Fatalf("arming list = %v, want [reviewRole]", body["arming"])
	}

	// With confirm:true → 201.
	resp2, out := postSettings(t, base, token, `{"stagedRev":0,"reviewRole":"reviewer","confirm":true}`)
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("arming with confirm: status %d, want 201 (%v)", resp2.StatusCode, out)
	}
}
