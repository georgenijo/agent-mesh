package dashboard

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/job"
)

// TestCreateJobRequiresAuth pins the security posture: POST /api/jobs without a
// valid Bearer token is rejected with 401/403; read endpoints remain open.
func TestCreateJobRequiresAuth(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()

	// No Authorization header → 401.
	resp, err := http.Post(base+"/api/jobs", "application/json",
		strings.NewReader(`{"repo":"r","title":"t"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: status %d, want 401", resp.StatusCode)
	}

	// Wrong token → 403.
	req, _ := http.NewRequest("POST", base+"/api/jobs",
		strings.NewReader(`{"repo":"r","title":"t"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong token: status %d, want 403", resp2.StatusCode)
	}
}

// TestCreateJobHappyPath pins the full create flow: valid token + valid body
// → 201 with the new job id, state=open, and the job visible in GET /api/jobs.
func TestCreateJobHappyPath(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()
	token := d.WriteToken()

	body := `{"repo":"myrepo","title":"first job","body":"some details"}`
	req, _ := http.NewRequest("POST", base+"/api/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /api/jobs: status %d, want 201", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type %q, want application/json", ct)
	}

	var result struct {
		Job   string `json:"job"`
		Repo  string `json:"repo"`
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Job == "" {
		t.Fatal("response missing job id")
	}
	if result.Repo != "myrepo" {
		t.Fatalf("repo = %q, want myrepo", result.Repo)
	}
	if result.State != "open" {
		t.Fatalf("state = %q, want open", result.State)
	}

	// The job must be immediately visible in GET /api/jobs.
	resp2, err := http.Get(base + "/api/jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/jobs: status %d, want 200", resp2.StatusCode)
	}
	var listResp struct {
		Jobs []job.Record `json:"jobs"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode jobs list: %v", err)
	}
	found := false
	for _, j := range listResp.Jobs {
		if j.ID == result.Job {
			found = true
			if j.Title != "first job" {
				t.Errorf("title = %q, want first job", j.Title)
			}
			if j.Body != "some details" {
				t.Errorf("body = %q, want some details", j.Body)
			}
		}
	}
	if !found {
		t.Fatalf("created job %q not found in GET /api/jobs", result.Job)
	}
}

// TestCreateJobValidation pins input validation: missing repo and missing title
// each return 400 with a typed JSON error body.
func TestCreateJobValidation(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()
	token := d.WriteToken()

	cases := []struct {
		name string
		body string
		want string // substring expected in response body
	}{
		{"missing repo", `{"title":"t"}`, "repo"},
		{"missing title", `{"repo":"r"}`, "title"},
		{"empty repo", `{"repo":"","title":"t"}`, "repo"},
		{"empty title", `{"repo":"r","title":""}`, "title"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest("POST", base+"/api/jobs", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", resp.StatusCode)
			}
			var buf bytes.Buffer
			buf.ReadFrom(resp.Body) //nolint:errcheck
			if !strings.Contains(buf.String(), tc.want) {
				t.Errorf("response %q does not mention %q", buf.String(), tc.want)
			}
		})
	}
}

// TestListJobsEmptySlice pins the initial GET /api/jobs shape: an empty mesh
// returns {"jobs":[]} (never null) so the UI never has to null-guard the array.
func TestListJobsEmptySlice(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()

	resp, err := http.Get(base + "/api/jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/jobs: status %d, want 200", resp.StatusCode)
	}
	var body struct {
		Jobs []job.Record `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Jobs == nil {
		t.Fatal("jobs field is null; want empty array")
	}
}

// TestWriteTokenEndpoint pins the token endpoint shape: it returns JSON with a
// non-empty "token" key matching the token the dashboard actually uses.
func TestWriteTokenEndpoint(t *testing.T) {
	_, _, d := startStack(t)
	resp, err := http.Get(fmt.Sprintf("http://%s/api/write-token", d.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/write-token: status %d, want 200", resp.StatusCode)
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Token == "" {
		t.Fatal("token is empty")
	}
	if body.Token != d.WriteToken() {
		t.Fatalf("token from endpoint %q does not match WriteToken()", body.Token)
	}
}

// TestTokenFileWrittenAndRemovedOnStop pins the token file lifecycle: written
// on Start with owner-only permissions, removed on Stop.
func TestTokenFileWrittenAndRemovedOnStop(t *testing.T) {
	cfg, _, d := startStack(t)

	tokenPath := cfg.DashboardTokenFile()
	raw, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("token file after Start: %v", err)
	}
	if strings.TrimSpace(string(raw)) != d.WriteToken() {
		t.Fatalf("token file content %q != WriteToken()", strings.TrimSpace(string(raw)))
	}
	info, err := os.Stat(tokenPath)
	if err != nil {
		t.Fatalf("stat token file: %v", err)
	}
	if runtime.GOOS != "windows" {
		if mode := info.Mode().Perm(); mode != 0o600 {
			t.Fatalf("token file mode %04o, want 0600", mode)
		}
	}

	d.Stop()
	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Fatalf("token file still present after Stop (err=%v)", err)
	}
}

// TestObserverEndpointsStayUnauthenticated pins the security boundary: all
// read-only endpoints must NOT require a bearer token.
func TestObserverEndpointsStayUnauthenticated(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()

	for _, path := range []string{"/", "/api/roster", "/api/claims", "/api/jobs"} {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d, want 200 (observer endpoints must be unauthenticated)", path, resp.StatusCode)
		}
	}
}

// TestCreateJobPublishesJobEvent pins the SSE contract: a successful POST must
// cause a KindJob envelope on the SSE stream so the UI sees the intake without
// polling.
func TestCreateJobPublishesJobEvent(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()
	token := d.WriteToken()

	// Connect SSE before creating the job so we don't miss the event.
	sseResp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	type frame struct {
		Type     string `json:"type"`
		Envelope struct {
			Kind string `json:"kind"`
		} `json:"envelope"`
	}

	jobSeen := make(chan struct{}, 1)
	go func() {
		sc := bufio.NewScanner(sseResp.Body)
		for sc.Scan() {
			line := sc.Text()
			data, ok := strings.CutPrefix(line, "data: ")
			if !ok {
				continue
			}
			var f frame
			if err := json.Unmarshal([]byte(data), &f); err != nil {
				continue
			}
			if f.Type == "event" && f.Envelope.Kind == "job" {
				select {
				case jobSeen <- struct{}{}:
				default:
				}
			}
		}
	}()

	req, _ := http.NewRequest("POST", base+"/api/jobs",
		strings.NewReader(`{"repo":"evtrepo","title":"evt job"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /api/jobs: status %d, want 201", resp.StatusCode)
	}

	select {
	case <-jobSeen:
		// KindJob envelope arrived on the SSE stream.
	case <-time.After(3 * time.Second):
		t.Fatal("KindJob envelope never appeared on SSE stream after job create")
	}
}

// TestListReposEmpty pins the /api/repos response when ReposDir is not
// configured: the endpoint returns 200 with an empty repos array (never null).
func TestListReposEmpty(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()

	resp, err := http.Get(base + "/api/repos")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/repos: status %d, want 200", resp.StatusCode)
	}
	var body struct {
		Repos []repoEntry `json:"repos"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Repos == nil {
		t.Fatal("repos field is null; want empty array")
	}
	if len(body.Repos) != 0 {
		t.Fatalf("expected 0 repos, got %d", len(body.Repos))
	}
}

// TestListReposDetectsGitDirs pins the /api/repos detection logic: only
// immediate subdirectories with a .git entry are returned; non-git dirs and
// regular files are ignored.
func TestListReposDetectsGitDirs(t *testing.T) {
	cfg, _, d := startStack(t)
	base := "http://" + d.Addr()

	// Build a temporary repos directory with two git repos and one plain dir.
	reposDir := filepath.Join(cfg.MeshDir, "test-repos")
	if err := os.MkdirAll(reposDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alpha", "beta"} {
		gitDir := filepath.Join(reposDir, name, ".git")
		if err := os.MkdirAll(gitDir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// Plain directory without .git — must not appear in the list.
	if err := os.MkdirAll(filepath.Join(reposDir, "notgit"), 0o700); err != nil {
		t.Fatal(err)
	}

	// Inject the repos dir into the dashboard config and call the handler
	// directly without restarting (the handler reads d.cfg.ReposDir).
	d.cfg.ReposDir = reposDir

	resp, err := http.Get(base + "/api/repos")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/repos: status %d, want 200", resp.StatusCode)
	}
	var body struct {
		Repos []repoEntry `json:"repos"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Repos) != 2 {
		t.Fatalf("expected 2 repos, got %d: %v", len(body.Repos), body.Repos)
	}
	names := make(map[string]bool)
	for _, r := range body.Repos {
		names[r.Name] = true
		if r.Path == "" {
			t.Errorf("repo %q has empty path", r.Name)
		}
	}
	for _, want := range []string{"alpha", "beta"} {
		if !names[want] {
			t.Errorf("expected repo %q in list, got %v", want, body.Repos)
		}
	}
	if names["notgit"] {
		t.Error("non-git directory 'notgit' should not appear in repos list")
	}
}
