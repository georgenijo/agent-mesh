package e2e

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestAutoExpertSpawnsOnUnownedRoleAsk is the acceptance gate for #117:
// autonomous on-demand experts. With MESH_AUTO_EXPERTS armed, a role-routed ask
// to a role NO live agent fills must NOT fail — the coordinator launches a
// resident expert for that role ITSELF, and the originally-published ask is
// re-delivered to it so the cold first ask is answered. No human starts the
// expert (the whole value of the mesh): the only manual actor is the asker.
//
// Everything is real and cross-process: sidecars, bus, tickets KV, and a real
// meshd --mode expert the coordinator execs, driving a fake claude
// (MESH_EXPERT_CLI) over the runtime stream-json contract.
func TestAutoExpertSpawnsOnUnownedRoleAsk(t *testing.T) {
	m := newMesh(t)
	// Arms BOTH halves: the asker's sidecar stops short-circuiting a role-ask
	// with no owner, and the coordinator watches for it and spawns.
	m.env = append(m.env, "MESH_AUTO_EXPERTS=on")
	m.startCoordinator()

	if code, _, stderr := m.run("join", "--name", "asker", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join asker exit %d: %s", code, stderr)
	}

	// Ground truth: no auth agent exists, so anything that answers the ask must
	// have been spawned by the coordinator.
	agents, _ := m.who("asker")
	if rec, ok := findAgentByRole(agents, "auth"); ok {
		t.Fatalf("an auth agent (%s) already exists; cannot prove autonomous spawn", rec.Card.Name)
	}

	// Pre-#117 this exits non-zero ("no live agent with role auth"). Armed, it
	// returns a pending ticket immediately and never blocks on the spawn.
	start := time.Now()
	code, stdout, stderr := m.run("ask", "--role", "auth", "--json", "--socket", m.agentSocket("asker"), "How should auth handle RLS?")
	if code != 0 {
		t.Fatalf("ask to un-owned role exit %d: %s", code, stderr)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("ask took %s; expected an immediate pending ticket, not a blocking spawn", time.Since(start))
	}
	var ask struct {
		Ticket string `json:"ticket"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal([]byte(stdout), &ask); err != nil {
		t.Fatalf("ask json: %v\n%s", err, stdout)
	}
	if ask.Ticket == "" || ask.Result != "pending" {
		t.Fatalf("ask result = %+v (want a pending ticket)", ask)
	}

	// The coordinator autonomously brings up a live auth expert. Registration
	// happens early in the expert's startup (before its runtime child), so this
	// is quick even under parallel-build load.
	m.eventually(30*time.Second, "coordinator spawns a live auth expert", func() bool {
		agents, exit := m.who("asker")
		if exit != 0 {
			return false
		}
		rec, ok := findAgentByRole(agents, "auth")
		return ok && string(rec.State) == "live"
	})

	// And that spawned expert answers the original ask (re-delivered by the
	// coordinator once it was listening) — the cold first ask, not just later ones.
	var answer, answeredBy string
	m.eventually(45*time.Second, "auto-spawned expert answers the ticket", func() bool {
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
		answer, answeredBy = poll.Answer, poll.AnsweredBy
		return true
	})

	if !strings.HasPrefix(answeredBy, "expert-auth") {
		t.Fatalf("answeredBy = %q, want an expert-auth-* the coordinator spawned", answeredBy)
	}
	if !strings.Contains(answer, "How should auth handle RLS?") {
		t.Fatalf("answer did not echo the question through the runtime: %q", answer)
	}
}

// TestAutoExpertOffKeepsFailFast guards the opt-in: with MESH_AUTO_EXPERTS
// unset (the default), a role-ask to an un-owned role fails fast exactly as
// before #117 — no ticket, no spawn.
func TestAutoExpertOffKeepsFailFast(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	if code, _, stderr := m.run("join", "--name", "asker", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join asker exit %d: %s", code, stderr)
	}

	code, stdout, stderr := m.run("ask", "--role", "auth", "--json", "--socket", m.agentSocket("asker"), "anyone home?")
	if code == 0 {
		t.Fatalf("ask to un-owned role succeeded with auto-experts off; want fail-fast")
	}
	if !strings.Contains(stdout+stderr, "no live agent with role") {
		t.Fatalf("output = %q / %q, want the pre-#117 no-owner error", stdout, stderr)
	}
}
