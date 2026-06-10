package e2e

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestSubmitCreatesObservableOpenJob proves the #23 intake path end-to-end:
// `mesh submit` records a top-level Job (JobOpen) through the real
// CLI → sidecar socket → bus → jobs KV path, and the derived KindJob envelope
// reaches the mesh.> tap (here the dashboard SSE stream). Assertions are over
// typed JSON only — never prose.
func TestSubmitCreatesObservableOpenJob(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()
	base := m.startDashboard()

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}

	// Open the mesh.> tap before submitting so the live event is observed.
	jobEnvelopes := tapJobEnvelopes(t, base+"/events")

	code, stdout, stderr := m.run("submit", "do X", "--repo", "demo", "--json", "--socket", m.agentSocket("intake"))
	if code != 0 {
		t.Fatalf("submit exit %d: stderr %s stdout %s", code, stderr, stdout)
	}
	var res struct {
		Job    string `json:"job"`
		Repo   string `json:"repo"`
		State  string `json:"state"`
		Source string `json:"source"`
	}
	if err := json.Unmarshal([]byte(stdout), &res); err != nil {
		t.Fatalf("submit json: %v\n%s", err, stdout)
	}
	if res.Job == "" {
		t.Fatalf("submit returned empty job id: %s", stdout)
	}
	if res.State != "open" {
		t.Fatalf("submit state = %q, want open: %s", res.State, stdout)
	}
	if res.Source != "manual" {
		t.Fatalf("submit source = %q, want manual", res.Source)
	}

	// The KindJob envelope for this job id must appear on the mesh.> tap as open.
	m.eventually(5*time.Second, "KindJob envelope for the submitted job appears on the mesh.> tap", func() bool {
		for _, p := range jobEnvelopes() {
			if p.ID == res.Job && p.State == "open" {
				return true
			}
		}
		return false
	})
}

// jobPayload is the slice of envelope.JobPayload the tap assertion needs.
type jobPayload struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// tapJobEnvelopes opens the dashboard SSE stream and collects the JobPayloads of
// every KindJob envelope it sees. It returns a snapshot accessor.
func tapJobEnvelopes(t *testing.T, url string) func() []jobPayload {
	t.Helper()
	resp, err := http.Get(url) //nolint:bodyclose // closed in cleanup below
	if err != nil {
		t.Fatalf("open SSE tap: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	var (
		mu   = make(chan struct{}, 1)
		seen []jobPayload
	)
	mu <- struct{}{}
	go func() {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			data, ok := strings.CutPrefix(line, "data: ")
			if !ok {
				continue
			}
			var ev struct {
				Type     string `json:"type"`
				Envelope struct {
					Kind    string     `json:"kind"`
					Payload jobPayload `json:"payload"`
				} `json:"envelope"`
			}
			if json.Unmarshal([]byte(data), &ev) != nil {
				continue
			}
			if ev.Type != "event" || ev.Envelope.Kind != "job" {
				continue
			}
			<-mu
			seen = append(seen, ev.Envelope.Payload)
			mu <- struct{}{}
		}
	}()

	return func() []jobPayload {
		<-mu
		out := append([]jobPayload(nil), seen...)
		mu <- struct{}{}
		return out
	}
}
