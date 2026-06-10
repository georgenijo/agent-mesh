package e2e

// Cross-process triage acceptance (#24): a submitted job is decomposed into a
// task DAG by a coordinator-spawned planner CHILD PROCESS (test/e2e/
// fakeplanner speaking the verified one-shot contract), and the lifecycle is
// observable on the mesh.> tap. Assertions are over typed JSON only — never
// prose. Additive to the shared harness: the fakeplanner binary is built
// here, not in TestMain.

import (
	"bufio"
	"encoding/json"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildFakePlanner compiles the fakeplanner binary into this mesh's dir.
func buildFakePlanner(t *testing.T, m *mesh) string {
	t.Helper()
	bin := filepath.Join(m.dir, exeName("fakeplanner"))
	cmd := exec.Command("go", "build", "-o", bin, "github.com/georgenijo/agent-mesh/test/e2e/fakeplanner")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build fakeplanner: %v\n%s", err, out)
	}
	return bin
}

func TestTriageProducesTaskDAGAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	m.env = append(m.env, "MESH_PLANNER_CLI="+buildFakePlanner(t, m))
	m.startCoordinator()
	base := m.startDashboard()

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}

	jobEnvelopes := tapJobEnvelopes(t, base+"/events")
	taps := tapTriageEnvelopes(t, base+"/events")

	code, stdout, stderr := m.run("submit", "do X", "--repo", "demo", "--json", "--socket", m.agentSocket("intake"))
	if code != 0 {
		t.Fatalf("submit exit %d: stderr %s stdout %s", code, stderr, stdout)
	}
	var res struct {
		Job string `json:"job"`
	}
	if err := json.Unmarshal([]byte(stdout), &res); err != nil || res.Job == "" {
		t.Fatalf("submit json: %v\n%s", err, stdout)
	}

	// The job reaches triaged on the mesh.> tap.
	m.eventually(10*time.Second, "KindJob triaged envelope on the mesh.> tap", func() bool {
		for _, p := range jobEnvelopes() {
			if p.ID == res.Job && p.State == "triaged" {
				return true
			}
		}
		return false
	})

	// One pending KindTask envelope per planned node, and a typed ok
	// KindTriage outcome carrying the node count.
	m.eventually(10*time.Second, "task and triage envelopes on the mesh.> tap", func() bool {
		tasks, triages := taps()
		pending := 0
		for _, p := range tasks {
			if p.Job == res.Job && p.State == "pending" {
				pending++
			}
		}
		for _, p := range triages {
			if p.Job == res.Job && p.Result == "ok" && p.Tasks == 2 && pending == 2 {
				return true
			}
		}
		return false
	})
}

func TestTriageMalformedPlannerFailsJobTyped(t *testing.T) {
	m := newMesh(t)
	m.env = append(m.env,
		"MESH_PLANNER_CLI="+buildFakePlanner(t, m),
		"FAKEPLANNER_MODE=garbage",
	)
	m.startCoordinator()
	base := m.startDashboard()

	if code, _, stderr := m.run("join", "--name", "intake", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join exit %d: %s", code, stderr)
	}
	jobEnvelopes := tapJobEnvelopes(t, base+"/events")
	taps := tapTriageEnvelopes(t, base+"/events")

	code, stdout, _ := m.run("submit", "do X", "--repo", "demo", "--json", "--socket", m.agentSocket("intake"))
	if code != 0 {
		t.Fatalf("submit exit %d: %s", code, stdout)
	}
	var res struct {
		Job string `json:"job"`
	}
	if err := json.Unmarshal([]byte(stdout), &res); err != nil || res.Job == "" {
		t.Fatalf("submit json: %v\n%s", err, stdout)
	}

	// Typed failure: the job fails and the triage event carries the code.
	m.eventually(10*time.Second, "typed planner_failed triage outcome on the tap", func() bool {
		_, triages := taps()
		for _, p := range triages {
			if p.Job == res.Job && p.Result == "error" && p.Code == "planner_failed" {
				return true
			}
		}
		return false
	})
	m.eventually(10*time.Second, "KindJob failed envelope on the tap", func() bool {
		for _, p := range jobEnvelopes() {
			if p.ID == res.Job && p.State == "failed" {
				return true
			}
		}
		return false
	})

	// The coordinator survived: presence still works end-to-end.
	if code, _, stderr := m.run("join", "--name", "after", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join after triage failure exit %d: %s", code, stderr)
	}
	m.eventually(5*time.Second, "agent joined after triage failure is visible", func() bool {
		agents, exit := m.who("after")
		if exit != 0 {
			return false
		}
		_, found := findAgent(agents, "after")
		return found
	})
}

// taskTapPayload is the slice of envelope.TaskPayload the tap needs.
type taskTapPayload struct {
	ID    string `json:"id"`
	Job   string `json:"job"`
	State string `json:"state"`
}

// triageTapPayload is the slice of envelope.TriagePayload the tap needs.
type triageTapPayload struct {
	Job    string `json:"job"`
	Result string `json:"result"`
	Tasks  int    `json:"tasks"`
	Code   string `json:"code"`
}

// tapTriageEnvelopes opens the dashboard SSE stream and collects every
// KindTask and KindTriage envelope payload. Returns a snapshot accessor.
func tapTriageEnvelopes(t *testing.T, url string) func() ([]taskTapPayload, []triageTapPayload) {
	t.Helper()
	resp, err := http.Get(url) //nolint:bodyclose // closed in cleanup below
	if err != nil {
		t.Fatalf("open SSE tap: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	var (
		mu      = make(chan struct{}, 1)
		tasks   []taskTapPayload
		triages []triageTapPayload
	)
	mu <- struct{}{}
	go func() {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			data, ok := strings.CutPrefix(sc.Text(), "data: ")
			if !ok {
				continue
			}
			var ev struct {
				Type     string `json:"type"`
				Envelope struct {
					Kind    string          `json:"kind"`
					Payload json.RawMessage `json:"payload"`
				} `json:"envelope"`
			}
			if json.Unmarshal([]byte(data), &ev) != nil || ev.Type != "event" {
				continue
			}
			switch ev.Envelope.Kind {
			case "task":
				var p taskTapPayload
				if json.Unmarshal(ev.Envelope.Payload, &p) == nil {
					<-mu
					tasks = append(tasks, p)
					mu <- struct{}{}
				}
			case "triage":
				var p triageTapPayload
				if json.Unmarshal(ev.Envelope.Payload, &p) == nil {
					<-mu
					triages = append(triages, p)
					mu <- struct{}{}
				}
			}
		}
	}()

	return func() ([]taskTapPayload, []triageTapPayload) {
		<-mu
		ts := append([]taskTapPayload(nil), tasks...)
		tr := append([]triageTapPayload(nil), triages...)
		mu <- struct{}{}
		return ts, tr
	}
}
