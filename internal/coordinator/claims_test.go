package coordinator

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/claim"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

func takeClaim(t *testing.T, cli *bus.Client, id, repo, path string) {
	t.Helper()
	// TTL well past the eviction window: if the claim disappears, it was the
	// coordinator's reclaim, not the lease backstop.
	out := claim.Take(cli, id, repo, path, time.Minute)
	if out.Result != envelope.ClaimClaimed {
		t.Fatalf("Take(%s, %s, %s) = %v (err %v)", id, repo, path, out.Result, out.Err)
	}
}

func waitClaimGone(t *testing.T, cli *bus.Client, repo, path string, timeout time.Duration) {
	t.Helper()
	key := claim.Key(repo, path)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, found, err := cli.KVGet(envelope.BucketClaims, key)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("claim %s/%s never released", repo, path)
}

func claimAudit(t *testing.T, cli *bus.Client, id string) []AuditEntry {
	t.Helper()
	entries, err := cli.StreamRead(envelope.StreamAudit, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []AuditEntry
	for _, e := range entries {
		var a AuditEntry
		if err := json.Unmarshal(e.Data, &a); err != nil {
			t.Fatal(err)
		}
		if a.Kind == "claim" && a.ID == id {
			out = append(out, a)
		}
	}
	return out
}

// TestEvictionReclaimsClaims is reclaim-on-death end to end: an agent takes a
// claim, goes silent, is evicted — and the coordinator promptly frees its
// claim (long before the TTL backstop) while a live agent's claim survives.
func TestEvictionReclaimsClaims(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "doomed")
	register(t, cli, "survivor")
	waitState(t, cli, "doomed", agentcard.PresenceLive, time.Second)
	waitState(t, cli, "survivor", agentcard.PresenceLive, time.Second)
	takeClaim(t, cli, "doomed", "demo", "src/main.go")
	takeClaim(t, cli, "survivor", "demo", "src/other.go")

	// survivor keeps beating; doomed goes silent.
	stopBeats := make(chan struct{})
	go func() {
		ticker := time.NewTicker(cfg.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopBeats:
				return
			case <-ticker.C:
				heartbeat(t, cli, "survivor")
			}
		}
	}()
	defer close(stopBeats)

	waitGone(t, cli, "doomed", 2*time.Second)
	waitClaimGone(t, cli, "demo", "src/main.go", time.Second)

	// The live agent's claim is untouched by the reclaim.
	if _, found, err := cli.KVGet(envelope.BucketClaims, claim.Key("demo", "src/other.go")); err != nil || !found {
		t.Fatalf("survivor's claim gone (found=%v err=%v)", found, err)
	}

	audits := claimAudit(t, cli, "doomed")
	if len(audits) != 1 {
		t.Fatalf("claim audits for doomed = %+v, want exactly one", audits)
	}
	a := audits[0]
	if a.Event != "reclaimed" || a.Path != "src/main.go" || a.Repo != "demo" {
		t.Fatalf("audit = %+v, want event=reclaimed path=src/main.go repo=demo", a)
	}
}

// TestGracefulLeaveReleasesClaims: a polite leave frees the agent's claims
// immediately, audited as released (not reclaimed).
func TestGracefulLeaveReleasesClaims(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "polite")
	waitState(t, cli, "polite", agentcard.PresenceLive, time.Second)
	takeClaim(t, cli, "polite", "demo", "a/b.go")

	env, err := envelope.New(envelope.KindLeave, "polite", envelope.SubjectLeave,
		&envelope.LeavePayload{ID: "polite", Reason: "done"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}
	waitGone(t, cli, "polite", time.Second)
	waitClaimGone(t, cli, "demo", "a/b.go", time.Second)

	audits := claimAudit(t, cli, "polite")
	if len(audits) != 1 {
		t.Fatalf("claim audits for polite = %+v, want exactly one", audits)
	}
	a := audits[0]
	if a.Event != "released" || a.Path != "a/b.go" || a.Repo != "demo" {
		t.Fatalf("audit = %+v, want event=released path=a/b.go repo=demo", a)
	}
}
