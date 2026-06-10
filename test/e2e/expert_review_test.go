package e2e

import (
	"encoding/json"
	"regexp"
	"strconv"
	"testing"
	"time"
)

// answerTag extracts the pid/session/turn the fakeclaude stamps on every answer
// ("expert answer [pid=N session=S turn=T]: ..."). It is the cross-process
// proof surface for resident-process reuse: two answers carrying the same pid +
// session id (and an increasing turn counter) came from ONE long-lived child,
// not two spawns.
var answerTag = regexp.MustCompile(`\[pid=(\d+) session=([^ ]+) turn=(\d+)\]`)

func parseAnswerTag(t *testing.T, answer string) (pid int, session string, turn int) {
	t.Helper()
	m := answerTag.FindStringSubmatch(answer)
	if m == nil {
		t.Fatalf("answer carried no resident-process tag: %q", answer)
	}
	pid, _ = strconv.Atoi(m[1])
	turn, _ = strconv.Atoi(m[3])
	return pid, m[2], turn
}

// askAndAwaitAnswer asks a role question through the asker and returns the
// answer text once the expert loop has answered it. It is local to this test
// file (the shared harness is not modified).
func askAndAwaitAnswer(t *testing.T, m *mesh, asker, question string) string {
	t.Helper()
	code, stdout, stderr := m.run("ask", "--role", "auth", "--json", "--socket", m.agentSocket(asker), question)
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
	if ask.Ticket == "" {
		t.Fatalf("ask returned no ticket: %s", stdout)
	}
	var answer string
	m.eventually(10*time.Second, "expert answers the ticket", func() bool {
		code, out, _ := m.run("poll", ask.Ticket, "--json", "--socket", m.agentSocket(asker))
		if code != 0 {
			return false
		}
		var poll struct {
			Result string `json:"result"`
			Answer string `json:"answer"`
		}
		if json.Unmarshal([]byte(out), &poll) != nil || poll.Result != "answered" {
			return false
		}
		answer = poll.Answer
		return true
	})
	return answer
}

// TestExpertReusesResidentSessionAcrossAsks is the #27 same-session acceptance
// gate across real processes: one resident expert answers two consecutive
// role-routed asks WITHOUT respawning between them. The proof is runtime
// metadata surfaced by the fake child — both answers carry the same pid and the
// same session id, and the turn counter advances — exactly the "second ask uses
// the same resident process/session path proven by runtime metadata" criterion.
func TestExpertReusesResidentSessionAcrossAsks(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	if code, _, stderr := m.run("join", "--name", "asker", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join asker exit %d: %s", code, stderr)
	}

	m.startExpert("auth-expert", "auth")
	m.eventually(5*time.Second, "auth-expert is a live auth agent", func() bool {
		agents, exit := m.who("asker")
		if exit != 0 {
			return false
		}
		rec, ok := findAgent(agents, "auth-expert")
		return ok && rec.Card.Role == "auth" && string(rec.State) == "live"
	})

	ans1 := askAndAwaitAnswer(t, m, "asker", "first question about RLS")
	pid1, sess1, turn1 := parseAnswerTag(t, ans1)

	ans2 := askAndAwaitAnswer(t, m, "asker", "second question about indexes")
	pid2, sess2, turn2 := parseAnswerTag(t, ans2)

	if pid1 != pid2 {
		t.Fatalf("expert respawned between asks: pid %d -> %d", pid1, pid2)
	}
	if sess1 != sess2 {
		t.Fatalf("session id changed between asks: %q -> %q", sess1, sess2)
	}
	if turn2 <= turn1 {
		t.Fatalf("turn counter did not advance in the resident session: %d -> %d (fresh process?)", turn1, turn2)
	}
}
