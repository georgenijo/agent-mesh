package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestExpertRehydratesFromBlackboardAcrossProcesses is the acceptance gate for
// issue #28 (restart-without-amnesia): a decision recorded on the durable
// blackboard is rehydrated into an expert's warm runtime child on start, with no
// manual reload, and shapes its answers. Everything is real and cross-process
// (sidecars, bus, durable note stream, the runtime stream-json child); the
// "brain" is the fake claude, which honestly echoes only memory it was actually
// given.
//
// FAKECLAUDE_MSGLOG must be set before newMesh so it flows through os.Environ()
// into the expert child's env (the shared harness is not modified). It records
// every user message the child received, so the test proves the memory primer
// was delivered across the process boundary — not merely that the answer text
// happened to match.
func TestExpertRehydratesFromBlackboardAcrossProcesses(t *testing.T) {
	msgLog := filepath.Join(t.TempDir(), "fakeclaude-msgs.jsonl")
	t.Setenv("FAKECLAUDE_MSGLOG", msgLog)

	m := newMesh(t)
	m.startCoordinator()

	if code, _, stderr := m.run("join", "--name", "asker", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join asker exit %d: %s", code, stderr)
	}

	// Record a durable project decision on the blackboard BEFORE the expert
	// exists: this is the long-term memory the expert must recover on start.
	const decision = "auth shards by tenant_id"
	if code, _, stderr := m.run("note", decision, "--repo", "demo", "--kind", "decision",
		"--socket", m.agentSocket("asker")); code != 0 {
		t.Fatalf("note exit %d: %s", code, stderr)
	}

	// Bring up the expert; it must prime from the blackboard on start.
	m.startExpert("auth-expert", "auth")
	m.eventually(5*time.Second, "auth-expert is a live auth agent", func() bool {
		agents, exit := m.who("asker")
		if exit != 0 {
			return false
		}
		rec, ok := findAgent(agents, "auth-expert")
		return ok && rec.Card.Role == "auth" && string(rec.State) == "live"
	})

	// The expert's first turn is the memory primer (a context-setting inject),
	// not a ticket; the msglog captures it. Wait until the primer — carrying the
	// recorded decision — has been delivered to the child process.
	m.eventually(5*time.Second, "memory primer delivered to the expert child", func() bool {
		return msgLogContains(t, msgLog, decision)
	})

	// Ask a question; the warm, rehydrated expert answers FROM its blackboard
	// memory, so the durable decision shows up in the answer.
	code, stdout, stderr := m.run("ask", "--role", "auth", "--json", "--socket", m.agentSocket("asker"),
		"How should auth handle sharding?")
	if code != 0 {
		t.Fatalf("ask exit %d: %s", code, stderr)
	}
	var ask struct {
		Ticket string `json:"ticket"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal([]byte(stdout), &ask); err != nil {
		t.Fatalf("ask json: %v\n%s", err, stdout)
	}
	if ask.Ticket == "" || ask.Result != "pending" {
		t.Fatalf("ask result = %+v", ask)
	}

	var answer string
	m.eventually(10*time.Second, "expert answers from rehydrated memory", func() bool {
		code, stdout, _ := m.run("poll", ask.Ticket, "--json", "--socket", m.agentSocket("asker"))
		if code != 0 {
			return false
		}
		var poll struct {
			Result string `json:"result"`
			Answer string `json:"answer"`
		}
		if json.Unmarshal([]byte(stdout), &poll) != nil || poll.Result != "answered" {
			return false
		}
		answer = poll.Answer
		return true
	})

	// The answer carries the blackboard decision: the expert recovered prior
	// project knowledge from durable memory with no manual reload.
	if !strings.Contains(answer, decision) {
		t.Fatalf("answer did not reflect rehydrated blackboard memory: %q", answer)
	}
}

// msgLogContains reports whether any user message recorded by the fake child
// contains substr. A missing file (no turn yet) is simply "not yet".
func msgLogContains(t *testing.T, path, substr string) bool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec struct {
			Content string `json:"content"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil {
			continue
		}
		if strings.Contains(rec.Content, substr) {
			return true
		}
	}
	return false
}
