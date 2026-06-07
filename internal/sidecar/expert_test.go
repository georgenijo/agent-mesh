package sidecar

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
)

// TestServeExpertAnswersAcceptedTicket proves the responder loop closes a real
// role-routed ask end to end — through the live coordinator, bus, and tickets
// KV — with no manual `mesh inbox` / `mesh answer`. The "brain" is a fake
// ExpertFunc; the runtime child is exercised separately by the e2e test.
func TestServeExpertAnswersAcceptedTicket(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")
	asker := startSidecar(t, cfg, "asker")
	_ = asker

	// Promote the expert to role "auth" so it subscribes mesh.ask.role.auth and
	// auto-accepts role-routed tickets into its own inbox.
	resp := do(t, cfg, "expert", meshapi.VerbJoin, meshapi.JoinArgs{
		Card: agentcard.Card{Name: "expert", Role: "auth"},
	})
	if !resp.OK {
		t.Fatalf("re-join as auth failed: %+v", resp)
	}

	// Wait until the asker sees the expert as a live auth agent (ensureResponder
	// rejects an ask otherwise).
	deadline := time.Now().Add(2 * time.Second)
	for {
		live := false
		for _, rec := range whoAgents(t, cfg, "asker") {
			if rec.Card.Name == "expert" && rec.Card.Role == "auth" && rec.State == agentcard.PresenceLive {
				live = true
			}
		}
		if live {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expert never became a live auth agent")
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = expert.ServeExpert(ctx, func(_ context.Context, question, _ string) (ExpertResult, error) {
			return ExpertResult{Answer: "expert says: " + question, OK: true}, nil
		}, 15*time.Millisecond)
	}()

	// Ask as a role-routed question; it must return a pending ticket immediately.
	resp = do(t, cfg, "asker", meshapi.VerbAsk, meshapi.AskArgs{Role: "auth", Question: "how to shard?"})
	if !resp.OK {
		t.Fatalf("ask failed: %+v", resp)
	}
	var ask meshapi.AskVerbResult
	if err := json.Unmarshal(resp.Data, &ask); err != nil {
		t.Fatal(err)
	}
	if ask.Ticket == "" || ask.Result != envelope.AskPending {
		t.Fatalf("ask result = %+v, want a pending ticket", ask)
	}

	// The loop answers without anyone running inbox/answer.
	deadline = time.Now().Add(3 * time.Second)
	for {
		resp = do(t, cfg, "asker", meshapi.VerbPoll, meshapi.PollArgs{Ticket: ask.Ticket})
		if !resp.OK {
			t.Fatalf("poll failed: %+v", resp)
		}
		var poll meshapi.PollResult
		if err := json.Unmarshal(resp.Data, &poll); err != nil {
			t.Fatal(err)
		}
		if poll.Result == envelope.AskAnswered {
			if !strings.Contains(poll.Answer, "how to shard?") {
				t.Fatalf("answer %q did not echo the question", poll.Answer)
			}
			if poll.AnsweredBy != "expert" {
				t.Fatalf("answeredBy = %q, want expert", poll.AnsweredBy)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ticket never answered by the expert loop: %+v", poll)
		}
		time.Sleep(15 * time.Millisecond)
	}
}
