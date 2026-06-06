package e2e

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestExpertServeAnswersRoleAskAcrossProcesses is the acceptance gate for the
// expert responder loop: a role-routed ask is answered automatically by a
// resident expert — no human runs `mesh inbox` / `mesh answer`. The expert
// drives a fake claude (MESH_EXPERT_CLI) over the real runtime stream-json
// contract; everything else (sidecars, bus, tickets KV) is real and
// cross-process.
func TestExpertServeAnswersRoleAskAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	if code, _, stderr := m.run("join", "--name", "asker", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join asker exit %d: %s", code, stderr)
	}

	// Bring up the expert and wait until the asker sees it as a live auth agent
	// (ensureResponder rejects the ask otherwise).
	m.startExpert("auth-expert", "auth")
	m.eventually(5*time.Second, "auth-expert is a live auth agent", func() bool {
		agents, exit := m.who("asker")
		if exit != 0 {
			return false
		}
		rec, ok := findAgent(agents, "auth-expert")
		return ok && rec.Card.Role == "auth" && string(rec.State) == "live"
	})

	// The ask returns a pending ticket immediately.
	start := time.Now()
	code, stdout, stderr := m.run("ask", "--role", "auth", "--json", "--socket", m.agentSocket("asker"), "How should auth handle RLS?")
	if code != 0 {
		t.Fatalf("ask exit %d: %s", code, stderr)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("ask took %s; expected immediate ticket return", time.Since(start))
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

	// poll exits 3 (no answer yet) at least once before the loop answers — the
	// asker never blocked on the responder's turn. The loop runs ~every
	// heartbeat, so this race is reliable but tolerated if it has already
	// answered.
	if code, out, _ := m.run("poll", ask.Ticket, "--json", "--socket", m.agentSocket("asker")); code != 0 && code != 3 {
		t.Fatalf("first poll exit %d stdout %s, want 0 or 3", code, out)
	}

	// The expert loop answers automatically; poll then exits 0 with the answer.
	var answer string
	m.eventually(10*time.Second, "expert loop answers the ticket", func() bool {
		code, stdout, _ := m.run("poll", ask.Ticket, "--json", "--socket", m.agentSocket("asker"))
		if code != 0 {
			return false
		}
		var poll struct {
			Result     string `json:"result"`
			Answer     string `json:"answer"`
			AnsweredBy string `json:"answeredBy"`
		}
		if json.Unmarshal([]byte(stdout), &poll) != nil || poll.Result != "answered" {
			return false
		}
		if poll.AnsweredBy != "auth-expert" {
			t.Fatalf("answeredBy = %q, want auth-expert", poll.AnsweredBy)
		}
		answer = poll.Answer
		return true
	})

	if !strings.Contains(answer, "How should auth handle RLS?") {
		t.Fatalf("answer did not echo the question through the runtime: %q", answer)
	}
}
