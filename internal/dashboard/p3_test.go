package dashboard

// TestSSEP3LifecycleContract locks the P3 observer contract: pushing synthetic
// KindJob, KindTask, KindWorker, KindTriage, and KindFleet envelopes through
// the bus tap must result in SSE frames that populate the corresponding
// snapshot state (jobs/tasks/workers/triage/fleet frame types) and be visible
// on a new SSE connection's initial snapshot. Counts are derived from the
// authoritative envelope fields only, never from transient UI counters.
//
// This is the "done-gate" for issue #58: real envelopes in → real frames out.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// collectP3Frames reads the SSE stream until all provided predicates return
// true or the deadline expires. Each predicate is called on every incoming
// frame; once it returns true it is removed. Returns nil on success, the
// first unsatisfied predicate name on timeout.
//
// Race-safety: the `want` predicates are accessed only by the scanner goroutine
// (which calls them and sends matched names). The `remaining` map is accessed
// only by the caller goroutine (which deletes matched names). The two goroutines
// communicate exclusively through the `done` and `fail` channels.
func collectP3Frames(t *testing.T, es *http.Response, deadline time.Duration, want map[string]func(msg map[string]json.RawMessage) bool) error {
	t.Helper()
	// preds is the scanner goroutine's exclusive copy of outstanding predicates.
	// It is only ever accessed from inside the goroutine below.
	preds := make(map[string]func(msg map[string]json.RawMessage) bool, len(want))
	for k, v := range want {
		preds[k] = v
	}
	// remaining tracks which names still need satisfying; owned by the caller.
	remaining := make(map[string]struct{}, len(want))
	for k := range want {
		remaining[k] = struct{}{}
	}

	done := make(chan string, len(want)) // satisfied predicate names; buffered to avoid scanner blocking
	fail := make(chan error, 1)

	go func() {
		sc := bufio.NewScanner(es.Body)
		for sc.Scan() {
			line := sc.Text()
			data, ok := strings.CutPrefix(line, "data: ")
			if !ok {
				continue
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal([]byte(data), &raw); err != nil {
				continue
			}
			// Iterate preds — exclusively owned by this goroutine.
			for name, pred := range preds {
				if pred(raw) {
					delete(preds, name) // stop firing once satisfied
					done <- name
				}
			}
		}
		if err := sc.Err(); err != nil {
			fail <- err
		}
	}()

	timer := time.After(deadline)
	for len(remaining) > 0 {
		select {
		case <-timer:
			var names []string
			for n := range remaining {
				names = append(names, n)
			}
			return fmt.Errorf("timed out waiting for: %v", names)
		case err := <-fail:
			return fmt.Errorf("scanner error: %w", err)
		case name := <-done:
			delete(remaining, name) // caller-goroutine exclusive write
		}
	}
	return nil
}

// frameType returns the "type" string from a raw SSE frame map.
func frameType(raw map[string]json.RawMessage) string {
	var t string
	_ = json.Unmarshal(raw["type"], &t)
	return t
}

// TestSSEP3JobFrameOnEnvelope pins the job-panel contract: a KindJob envelope
// published on the bus must arrive as a "jobs" frame on the SSE stream, with
// the correct id and state.
func TestSSEP3JobFrameOnEnvelope(t *testing.T) {
	_, cli, d := startStack(t)
	base := "http://" + d.Addr()

	sseResp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	const jobID = "job-sse-test-01"
	env, err := envelope.New(envelope.KindJob, "coordinator", envelope.SubjectJob(jobID),
		&envelope.JobPayload{
			ID:     jobID,
			Repo:   "testrepo",
			Source: "manual",
			Title:  "SSE lifecycle test",
			State:  envelope.JobOpen,
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}

	// Check that a "jobs" frame with our job arrives.
	if err := collectP3Frames(t, sseResp, 3*time.Second, map[string]func(map[string]json.RawMessage) bool{
		"jobs frame contains job": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "jobs" {
				return false
			}
			var payload struct {
				Jobs []jobSnap `json:"jobs"`
			}
			if err := json.Unmarshal(raw["jobs"], &payload.Jobs); err != nil {
				// "jobs" field is the array directly
				return false
			}
			for _, j := range payload.Jobs {
				if j.ID == jobID && j.State == envelope.JobOpen {
					return true
				}
			}
			return false
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSSEP3TaskFrameOnEnvelope pins the task-panel contract: a KindTask
// envelope must arrive as a "tasks" frame with the correct id and state.
func TestSSEP3TaskFrameOnEnvelope(t *testing.T) {
	_, cli, d := startStack(t)
	base := "http://" + d.Addr()

	sseResp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	const taskID = "task-sse-test-01"
	env, err := envelope.New(envelope.KindTask, "coordinator", envelope.SubjectTask(taskID),
		&envelope.TaskPayload{
			ID:    taskID,
			Job:   "job-sse-parent",
			Role:  "builder",
			Title: "Write the tests",
			State: envelope.TaskPending,
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}

	if err := collectP3Frames(t, sseResp, 3*time.Second, map[string]func(map[string]json.RawMessage) bool{
		"tasks frame contains task with worker label": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "tasks" {
				return false
			}
			var tasks []taskSnap
			if err := json.Unmarshal(raw["tasks"], &tasks); err != nil {
				return false
			}
			for _, tk := range tasks {
				if tk.ID == taskID && tk.State == envelope.TaskPending &&
					tk.Worker == taskWorkerName(taskID) {
					return true
				}
			}
			return false
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSSEP3WorkerFrameOnEnvelope pins the worker-panel contract: a KindWorker
// envelope must arrive as a "workers" frame.
func TestSSEP3WorkerFrameOnEnvelope(t *testing.T) {
	_, cli, d := startStack(t)
	base := "http://" + d.Addr()

	sseResp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	const workerTask = "task-worker-test-01"
	env, err := envelope.New(envelope.KindWorker, "worker-proc", envelope.SubjectWorker(workerTask),
		&envelope.WorkerPayload{
			Task:    workerTask,
			Job:     "job-worker-parent",
			Result:  envelope.WorkerOK,
			CostUSD: 0.042,
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}

	if err := collectP3Frames(t, sseResp, 3*time.Second, map[string]func(map[string]json.RawMessage) bool{
		"workers frame contains run": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "workers" {
				return false
			}
			var workers []workerSnap
			if err := json.Unmarshal(raw["workers"], &workers); err != nil {
				return false
			}
			for _, w := range workers {
				if w.Task == workerTask && w.Result == envelope.WorkerOK {
					return true
				}
			}
			return false
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSSEP3TriageFrameOnEnvelope pins the triage contract: a KindTriage
// envelope must arrive as a "triage" frame.
func TestSSEP3TriageFrameOnEnvelope(t *testing.T) {
	_, cli, d := startStack(t)
	base := "http://" + d.Addr()

	sseResp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	const triageJob = "job-triage-test-01"
	env, err := envelope.New(envelope.KindTriage, "coordinator", envelope.SubjectTriage(triageJob),
		&envelope.TriagePayload{
			Job:    triageJob,
			Result: envelope.TriageOK,
			Tasks:  3,
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}

	if err := collectP3Frames(t, sseResp, 3*time.Second, map[string]func(map[string]json.RawMessage) bool{
		"triage frame contains attempt": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "triage" {
				return false
			}
			var triages []triageSnap
			if err := json.Unmarshal(raw["triages"], &triages); err != nil {
				return false
			}
			for _, tr := range triages {
				if tr.Job == triageJob && tr.Result == envelope.TriageOK && tr.Tasks == 3 {
					return true
				}
			}
			return false
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSSEP3FleetFrameOnEnvelope pins the fleet contract: a KindFleet envelope
// must arrive as a "fleet" frame with the correct state.
func TestSSEP3FleetFrameOnEnvelope(t *testing.T) {
	_, cli, d := startStack(t)
	base := "http://" + d.Addr()

	sseResp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	env, err := envelope.New(envelope.KindFleet, "coordinator", envelope.SubjectFleet,
		&envelope.FleetPayload{
			State:    envelope.FleetRunning,
			SpentUSD: 1.23,
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}

	if err := collectP3Frames(t, sseResp, 3*time.Second, map[string]func(map[string]json.RawMessage) bool{
		"fleet frame shows running": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "fleet" {
				return false
			}
			var payload struct {
				Fleet fleetSnap `json:"fleet"`
			}
			if err := json.Unmarshal([]byte(fmt.Sprintf(`{"fleet":%s}`, raw["fleet"])), &payload); err != nil {
				return false
			}
			return payload.Fleet.State == envelope.FleetRunning
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestSSEP3SnapshotReplayOnConnect locks the reconnect contract: a browser
// that connects after some P3 traffic must receive the current jobs/tasks/
// workers/triage snapshots as initial frames (not only live traffic).
func TestSSEP3SnapshotReplayOnConnect(t *testing.T) {
	_, cli, d := startStack(t)
	base := "http://" + d.Addr()

	// Publish a full set of P3 events before any browser connects.
	const jobID = "job-reconnect-01"
	const taskID = "task-reconnect-01"
	const taskWorker = "task-reconnect-01"

	for _, env := range []func() (envelope.Envelope, error){
		func() (envelope.Envelope, error) {
			return envelope.New(envelope.KindJob, "coordinator", envelope.SubjectJob(jobID),
				&envelope.JobPayload{ID: jobID, Repo: "r", Source: "manual", Title: "reconnect test", State: envelope.JobRunning})
		},
		func() (envelope.Envelope, error) {
			return envelope.New(envelope.KindTask, "coordinator", envelope.SubjectTask(taskID),
				&envelope.TaskPayload{ID: taskID, Job: jobID, Role: "builder", Title: "t", State: envelope.TaskRunning})
		},
		func() (envelope.Envelope, error) {
			return envelope.New(envelope.KindWorker, "w", envelope.SubjectWorker(taskWorker),
				&envelope.WorkerPayload{Task: taskWorker, Job: jobID, Result: envelope.WorkerOK})
		},
		func() (envelope.Envelope, error) {
			return envelope.New(envelope.KindTriage, "coordinator", envelope.SubjectTriage(jobID),
				&envelope.TriagePayload{Job: jobID, Result: envelope.TriageOK, Tasks: 1})
		},
		func() (envelope.Envelope, error) {
			return envelope.New(envelope.KindFleet, "coordinator", envelope.SubjectFleet,
				&envelope.FleetPayload{State: envelope.FleetRunning})
		},
	} {
		e, err := env()
		if err != nil {
			t.Fatal(err)
		}
		if err := cli.Publish(e); err != nil {
			t.Fatal(err)
		}
	}

	// Give the dashboard a moment to ingest the events before the browser connects.
	time.Sleep(100 * time.Millisecond)

	// Now connect a fresh SSE stream.
	sseResp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	// The initial snapshot frames must contain the pre-existing state.
	if err := collectP3Frames(t, sseResp, 3*time.Second, map[string]func(map[string]json.RawMessage) bool{
		"jobs snapshot contains job": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "jobs" {
				return false
			}
			var jobs []jobSnap
			if err := json.Unmarshal(raw["jobs"], &jobs); err != nil {
				return false
			}
			for _, j := range jobs {
				if j.ID == jobID {
					return true
				}
			}
			return false
		},
		"tasks snapshot contains task": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "tasks" {
				return false
			}
			var tasks []taskSnap
			if err := json.Unmarshal(raw["tasks"], &tasks); err != nil {
				return false
			}
			for _, tk := range tasks {
				if tk.ID == taskID {
					return true
				}
			}
			return false
		},
		"workers snapshot contains run": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "workers" {
				return false
			}
			var workers []workerSnap
			if err := json.Unmarshal(raw["workers"], &workers); err != nil {
				return false
			}
			for _, w := range workers {
				if w.Task == taskWorker {
					return true
				}
			}
			return false
		},
		"triage snapshot contains attempt": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "triage" {
				return false
			}
			var triages []triageSnap
			if err := json.Unmarshal(raw["triages"], &triages); err != nil {
				return false
			}
			for _, tr := range triages {
				if tr.Job == jobID {
					return true
				}
			}
			return false
		},
		"fleet snapshot present": func(raw map[string]json.RawMessage) bool {
			return frameType(raw) == "fleet"
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestAgentCostsAccumulateOnWorkerRun locks the per-agent cost contract:
// a KindWorker envelope with CostUSD must produce an "agentcosts" SSE frame
// and be reflected in GET /api/agent-costs. The worker's model is snapshotted
// from its registry card when it is present in the live roster.
func TestAgentCostsAccumulateOnWorkerRun(t *testing.T) {
	// Use a fast roster tick so the worker's card is visible before recordWorker runs.
	_, cli, d := startStackEvery(t, 25*time.Millisecond)
	base := "http://" + d.Addr()

	// Register a worker agent with a known model so the dashboard can snapshot it.
	const taskID = "task-agentcost-01"
	workerName := taskWorkerName(taskID)
	const model = "sonnet"
	card := agentcard.Card{ID: workerName, Name: workerName, Role: "worker", Model: model}
	regEnv, err := envelope.New(envelope.KindRegister, workerName, envelope.SubjectRegister,
		&envelope.RegisterPayload{Card: card})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(regEnv); err != nil {
		t.Fatal(err)
	}

	// Give the dashboard's rosterLoop (25ms tick) time to ingest the registration.
	time.Sleep(100 * time.Millisecond)

	sseResp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	const cost = 0.0123
	workerEnv, err := envelope.New(envelope.KindWorker, workerName, envelope.SubjectWorker(taskID),
		&envelope.WorkerPayload{
			Task:    taskID,
			Job:     "job-agentcost-01",
			Result:  envelope.WorkerOK,
			CostUSD: cost,
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(workerEnv); err != nil {
		t.Fatal(err)
	}

	if err := collectP3Frames(t, sseResp, 3*time.Second, map[string]func(map[string]json.RawMessage) bool{
		"agentcosts frame carries worker cost and model": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "agentcosts" {
				return false
			}
			var agents []agentCostEntry
			if err := json.Unmarshal(raw["agents"], &agents); err != nil {
				return false
			}
			for _, a := range agents {
				if a.Name == workerName && a.CostUSD == cost && a.Model == model {
					return true
				}
			}
			return false
		},
	}); err != nil {
		t.Fatal(err)
	}

	// REST endpoint must also reflect the accumulated cost.
	resp, err := http.Get(base + "/api/agent-costs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/agent-costs: status %d", resp.StatusCode)
	}
	var body struct {
		Agents []agentCostEntry `json:"agents"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, a := range body.Agents {
		if a.Name == workerName && a.CostUSD == cost {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("GET /api/agent-costs: worker %q with cost %v not found; got %+v",
			workerName, cost, body.Agents)
	}
}

// TestSSEP3JobStateTransitionUpdatesPanel locks the state-transition contract:
// a second KindJob envelope for the same job id (with an updated state) must
// replace the first in the "jobs" snapshot, not append a duplicate.
func TestSSEP3JobStateTransitionUpdatesPanel(t *testing.T) {
	_, cli, d := startStack(t)
	base := "http://" + d.Addr()

	sseResp, err := http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer sseResp.Body.Close()

	const jobID = "job-transition-01"
	mkJob := func(state envelope.JobState) envelope.Envelope {
		e, err := envelope.New(envelope.KindJob, "coordinator", envelope.SubjectJob(jobID),
			&envelope.JobPayload{ID: jobID, Repo: "r", Source: "manual", Title: "transition test", State: state})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}

	if err := cli.Publish(mkJob(envelope.JobOpen)); err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(mkJob(envelope.JobRunning)); err != nil {
		t.Fatal(err)
	}

	// Wait for a "jobs" frame where the job is running (not open) — no duplicates.
	if err := collectP3Frames(t, sseResp, 3*time.Second, map[string]func(map[string]json.RawMessage) bool{
		"jobs panel shows running, not duplicate": func(raw map[string]json.RawMessage) bool {
			if frameType(raw) != "jobs" {
				return false
			}
			var jobs []jobSnap
			if err := json.Unmarshal(raw["jobs"], &jobs); err != nil {
				return false
			}
			var found int
			for _, j := range jobs {
				if j.ID == jobID {
					found++
					if j.State != envelope.JobRunning {
						return false
					}
				}
			}
			return found == 1
		},
	}); err != nil {
		t.Fatal(err)
	}
}
