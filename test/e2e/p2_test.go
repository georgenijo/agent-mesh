package e2e

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestP2AsyncAskAnswerAcrossProcesses(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	if code, _, stderr := m.run("join", "--name", "asker", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("join asker exit %d: %s", code, stderr)
	}
	for _, name := range []string{"auth1", "auth2"} {
		if code, _, stderr := m.run("join", "--name", name, "--role", "auth", "--repo", "demo"); code != 0 {
			t.Fatalf("join %s exit %d: %s", name, code, stderr)
		}
	}

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
		State  string `json:"state"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal([]byte(stdout), &ask); err != nil {
		t.Fatalf("ask json: %v\n%s", err, stdout)
	}
	if ask.Ticket == "" || ask.Result != "pending" {
		t.Fatalf("ask result = %+v", ask)
	}

	code, stdout, _ = m.run("poll", ask.Ticket, "--json", "--socket", m.agentSocket("asker"))
	if code != 3 {
		t.Fatalf("pre-answer poll exit %d stdout %s, want 3", code, stdout)
	}

	type inboxResult struct {
		Items []struct {
			Ticket   string `json:"ticket"`
			Question string `json:"question"`
		} `json:"items"`
	}
	var acceptedBy string
	m.eventually(5*time.Second, "exactly one auth responder accepts role ask", func() bool {
		holders := 0
		for _, name := range []string{"auth1", "auth2"} {
			code, out, _ := m.run("inbox", "--json", "--socket", m.agentSocket(name))
			if code != 0 {
				return false
			}
			var inbox inboxResult
			if json.Unmarshal([]byte(out), &inbox) != nil {
				return false
			}
			for _, item := range inbox.Items {
				if item.Ticket == ask.Ticket {
					holders++
					acceptedBy = name
				}
			}
		}
		return holders == 1
	})

	if code, _, stderr := m.run("answer", ask.Ticket, "Use SECURITY DEFINER helper.", "--socket", m.agentSocket(acceptedBy)); code != 0 {
		t.Fatalf("answer by %s exit %d: %s", acceptedBy, code, stderr)
	}
	code, stdout, stderr = m.run("poll", ask.Ticket, "--json", "--socket", m.agentSocket("asker"))
	if code != 0 {
		t.Fatalf("post-answer poll exit %d stderr %s stdout %s", code, stderr, stdout)
	}
	if !strings.Contains(stdout, "SECURITY DEFINER") {
		t.Fatalf("post-answer poll missing answer: %s", stdout)
	}

	code, _, _ = m.run("poll", "missing-ticket", "--json", "--socket", m.agentSocket("asker"))
	if code != 4 {
		t.Fatalf("missing-ticket poll exit %d, want 4", code)
	}
}
