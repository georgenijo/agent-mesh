package coordinator

import (
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// TestForgedLeaveIsRejected: a peer may not evict another agent by sending a
// leave whose payload ID does not match the envelope sender.
func TestForgedLeaveIsRejected(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "victim")
	waitState(t, cli, "victim", agentcard.PresenceLive, time.Second)

	forged, err := envelope.New(envelope.KindLeave, "attacker", envelope.SubjectLeave,
		&envelope.LeavePayload{ID: "victim", Reason: "forged"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(forged); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if _, found := getRecord(t, cli, "victim"); !found {
		t.Fatal("forged leave deleted the victim's registry record")
	}
}

// TestForgedHeartbeatAndStatusRejected: same authority rule for the other
// mutating reducers.
func TestForgedHeartbeatAndStatusRejected(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "victim")
	waitState(t, cli, "victim", agentcard.PresenceLive, time.Second)

	forgedStatus, err := envelope.New(envelope.KindStatus, "attacker", envelope.SubjectStatus("victim"),
		&envelope.StatusPayload{ID: "victim", Text: "pwned"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(forgedStatus); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	rec, _ := getRecord(t, cli, "victim")
	if rec.LastStatus == "pwned" {
		t.Fatal("forged status landed in the registry")
	}

	// Let the victim go away, then try to resurrect it with a forged beat.
	waitState(t, cli, "victim", agentcard.PresenceAway, 2*time.Second)
	forgedBeat, err := envelope.New(envelope.KindHeartbeat, "attacker", envelope.SubjectHeartbeat("victim"),
		&envelope.HeartbeatPayload{ID: "victim"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(forgedBeat); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	rec, found := getRecord(t, cli, "victim")
	if found && rec.State == agentcard.PresenceLive {
		t.Fatal("forged heartbeat resurrected an away agent")
	}
}

// TestRegisterThenLeaveOrdering: rapid register→leave from one agent must
// never strand a live record — the reducer consumes both in publish order.
func TestRegisterThenLeaveOrdering(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	for i := 0; i < 50; i++ {
		register(t, cli, "flash")
		leave, err := envelope.New(envelope.KindLeave, "flash", envelope.SubjectLeave,
			&envelope.LeavePayload{ID: "flash", Reason: "instant"})
		if err != nil {
			t.Fatal(err)
		}
		if err := cli.Publish(leave); err != nil {
			t.Fatal(err)
		}
	}
	waitGone(t, cli, "flash", 2*time.Second)
}

// TestReregisterPreservesRegisteredAt: a bus reconnect re-register must not
// re-arm the grace window.
func TestReregisterPreservesRegisteredAt(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "stable")
	waitState(t, cli, "stable", agentcard.PresenceLive, time.Second)
	first, _ := getRecord(t, cli, "stable")

	time.Sleep(50 * time.Millisecond)
	register(t, cli, "stable") // re-register (reconnect path)

	deadline := time.Now().Add(time.Second)
	for {
		rec, found := getRecord(t, cli, "stable")
		if found && rec.LastSeen.After(first.LastSeen) {
			if !rec.RegisteredAt.Equal(first.RegisteredAt) {
				t.Fatalf("RegisteredAt reset on re-register: %s -> %s", first.RegisteredAt, rec.RegisteredAt)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("re-register never observed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestHeartbeatCarriesStatus: the status text on a heartbeat repopulates
// LastStatus (e.g. after a coordinator restart wiped the registry).
func TestHeartbeatCarriesStatus(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "chatty")
	waitState(t, cli, "chatty", agentcard.PresenceLive, time.Second)

	env, err := envelope.New(envelope.KindHeartbeat, "chatty", envelope.SubjectHeartbeat("chatty"),
		&envelope.HeartbeatPayload{ID: "chatty", Status: "still working"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		rec, _ := getRecord(t, cli, "chatty")
		if rec.LastStatus == "still working" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("heartbeat status never landed: %+v", rec)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
