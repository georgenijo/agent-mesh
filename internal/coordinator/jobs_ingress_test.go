package coordinator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/bus"
)

// startBusForIngress spins up a coordinator (bus + KV store) and returns a
// bus.Client wired to it, for use in handler-level tests that need real KV.
func startBusForIngress(t *testing.T) *bus.Client {
	t.Helper()
	cfg := fastConfig(t)
	c := startCoordinator(t, cfg)
	cli, err := bus.Dial(c.cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cli.Close)
	return cli
}

func TestJobsIngressHandler_Success(t *testing.T) {
	cli := startBusForIngress(t)
	ji := &jobsIngress{cli: cli}

	body := `{"repo":"myrepo","title":"Do the thing","body":"some details"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	ji.serveJobs(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d; body: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not JSON: %v", err)
	}
	jobID, ok := resp["job"].(string)
	if !ok || jobID == "" {
		t.Fatalf("want non-empty job id, got %v", resp)
	}
}

func TestJobsIngressHandler_MalformedJSON(t *testing.T) {
	cli := startBusForIngress(t)
	ji := &jobsIngress{cli: cli}

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader("{not valid json"))
	rr := httptest.NewRecorder()

	ji.serveJobs(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestJobsIngressHandler_MissingFields(t *testing.T) {
	cli := startBusForIngress(t)
	ji := &jobsIngress{cli: cli}

	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing repo", `{"title":"hello"}`},
		{"missing title", `{"repo":"myrepo"}`},
		{"empty body", `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(tc.body))
			rr := httptest.NewRecorder()
			ji.serveJobs(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("want 400, got %d; body: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestJobsIngress_DisabledByDefault(t *testing.T) {
	// When MESH_JOBS_ADDR is not set, cfg.JobsAddr is empty and no ingress starts.
	cfg := fastConfig(t)
	// Confirm default is empty — the coordinator must not start a listener.
	if cfg.JobsAddr != "" {
		t.Fatalf("expected JobsAddr empty by default, got %q", cfg.JobsAddr)
	}

	c := startCoordinator(t, cfg)
	if c.jobsIngress != nil {
		t.Fatal("jobsIngress should be nil when JobsAddr is empty")
	}
}

func TestJobsIngress_StartsWhenConfigured(t *testing.T) {
	cfg := fastConfig(t)
	cfg.JobsAddr = "127.0.0.1:0" // :0 = OS-assigned free port

	c := New(cfg, nil)
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)

	if c.jobsIngress == nil {
		t.Fatal("jobsIngress should be non-nil when JobsAddr is set")
	}
	// Sanity-check: the bound address is non-empty.
	if c.jobsIngress.addr() == "" {
		t.Fatal("bound address should not be empty")
	}
}
