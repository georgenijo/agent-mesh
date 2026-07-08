package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestAnswerCacheExactMatchAcrossProcesses is the acceptance gate for Feature 6
// (#29 exact-match answer cache): a second identical role-ask is answered from
// the cache without another LLM turn. FAKECLAUDE_MSGLOG proves the child was
// not invoked on the cache hit — the msglog line count stays flat.
func TestAnswerCacheExactMatchAcrossProcesses(t *testing.T) {
	msgLog := filepath.Join(t.TempDir(), "fakeclaude-msgs.jsonl")
	t.Setenv("FAKECLAUDE_MSGLOG", msgLog)
	t.Setenv("MESH_ANSWER_CACHE", "on")
	t.Setenv("MESH_ANSWER_CACHE_TTL", "15m")

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

	const q1 = "How should auth handle RLS?"
	answer1 := askAndPoll(t, m, "asker", "auth", q1)
	if !strings.Contains(answer1, q1) {
		t.Fatalf("first answer did not echo question: %q", answer1)
	}

	m.eventually(5*time.Second, "first ask reached the LLM child", func() bool {
		return msgLogUserCount(t, msgLog) >= 1
	})
	afterFirst := msgLogUserCount(t, msgLog)
	if afterFirst != 1 {
		t.Fatalf("msglog user messages after first ask = %d, want 1", afterFirst)
	}

	// Identical ask: cache hit — answered, but the child must not see another turn.
	answer2 := askAndPoll(t, m, "asker", "auth", q1)
	if answer2 == "" {
		t.Fatal("second ask produced empty answer")
	}
	if msgLogUserCount(t, msgLog) != afterFirst {
		t.Fatalf("msglog grew on cache hit: got %d, want %d", msgLogUserCount(t, msgLog), afterFirst)
	}

	// A different question must miss the cache and invoke the LLM again.
	const q2 = "How should auth handle sessions?"
	answer3 := askAndPoll(t, m, "asker", "auth", q2)
	if !strings.Contains(answer3, q2) {
		t.Fatalf("third answer did not echo new question: %q", answer3)
	}
	m.eventually(5*time.Second, "different question reached the LLM child", func() bool {
		return msgLogUserCount(t, msgLog) > afterFirst
	})
	if got := msgLogUserCount(t, msgLog); got != afterFirst+1 {
		t.Fatalf("msglog after different ask = %d, want %d", got, afterFirst+1)
	}
}

// TestAnswerCacheOffStillInvokesTwice proves the default-off path: without the
// cache armed, two identical asks each drive an LLM turn.
func TestAnswerCacheOffStillInvokesTwice(t *testing.T) {
	msgLog := filepath.Join(t.TempDir(), "fakeclaude-msgs.jsonl")
	t.Setenv("FAKECLAUDE_MSGLOG", msgLog)
	t.Setenv("MESH_ANSWER_CACHE", "off")

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

	const q = "How should auth handle RLS?"
	_ = askAndPoll(t, m, "asker", "auth", q)
	m.eventually(5*time.Second, "first ask reached the LLM child", func() bool {
		return msgLogUserCount(t, msgLog) >= 1
	})
	_ = askAndPoll(t, m, "asker", "auth", q)
	m.eventually(5*time.Second, "second ask reached the LLM child (cache off)", func() bool {
		return msgLogUserCount(t, msgLog) >= 2
	})
	if got := msgLogUserCount(t, msgLog); got != 2 {
		t.Fatalf("msglog with cache off = %d, want 2", got)
	}
}

// askAndPoll issues a role-ask and waits until poll returns an answered ticket.
func askAndPoll(t *testing.T, m *mesh, asker, role, question string) string {
	t.Helper()
	code, stdout, stderr := m.run("ask", "--role", role, "--json", "--socket", m.agentSocket(asker), question)
	if code != 0 {
		t.Fatalf("ask %q exit %d: %s", question, code, stderr)
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
	m.eventually(10*time.Second, "ticket answered: "+ask.Ticket, func() bool {
		code, stdout, _ := m.run("poll", ask.Ticket, "--json", "--socket", m.agentSocket(asker))
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
	return answer
}

// msgLogUserCount returns how many user-message lines fakeclaude appended.
func msgLogUserCount(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		n++
	}
	return n
}
