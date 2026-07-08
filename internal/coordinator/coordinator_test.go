package coordinator

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func fastConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		MeshDir:           testsock.Dir(t),
		HeartbeatInterval: 50 * time.Millisecond,
		AwayAfter:         150 * time.Millisecond,
		EvictAfter:        400 * time.Millisecond,
		RegistrationGrace: 100 * time.Millisecond,
		ClaimTTL:          time.Second,
		TriageTimeout:     time.Minute,
		TriageBackoff:     time.Second,
		TriageMaxAttempts: 4,
		WorkerTimeout:     time.Minute,
		ReviewTimeout:     time.Minute,
		ReviewPoolSize:    1,
		MaxWorkers:        4,
		Backoff:           time.Second,
		KeepWorktrees:     config.KeepWorktreesOnFailure,
		// AuditFanout left false (zero) so TestAuditFanoutDisabled can assert
		// the off path; tests that need fan-out set AuditFanout: true explicitly.
		StruggleTestRepeat:    config.DefaultStruggleTestRepeat,
		StruggleEditRepeat:    config.DefaultStruggleEditRepeat,
		StruggleCooldown:      config.DefaultStruggleCooldown,
		StruggleMaxAsks:       config.DefaultStruggleMaxAsks,
		AnswerCacheTTL:        config.DefaultAnswerCacheTTL,
		AnswerCacheIncludeCtx: true,
	}
}

func startCoordinator(t *testing.T, cfg config.Config) *Coordinator {
	t.Helper()
	c := New(cfg, nil)
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return c
}

func dialBus(t *testing.T, cfg config.Config) *bus.Client {
	t.Helper()
	cli, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cli.Close)
	return cli
}

func register(t *testing.T, cli *bus.Client, id string) {
	t.Helper()
	card := agentcard.Card{ID: id, Name: id, Role: "builder", Caps: []string{"go"}}
	env, err := envelope.New(envelope.KindRegister, id, envelope.SubjectRegister, &envelope.RegisterPayload{Card: card})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}
}

func heartbeat(t *testing.T, cli *bus.Client, id string) {
	t.Helper()
	env, err := envelope.New(envelope.KindHeartbeat, id, envelope.SubjectHeartbeat(id), &envelope.HeartbeatPayload{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}
}

func getRecord(t *testing.T, cli *bus.Client, id string) (agentcard.RegistryRecord, bool) {
	t.Helper()
	kv, found, err := cli.KVGet(envelope.BucketRegistry, id)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		return agentcard.RegistryRecord{}, false
	}
	var rec agentcard.RegistryRecord
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		t.Fatal(err)
	}
	return rec, true
}

func waitState(t *testing.T, cli *bus.Client, id string, want agentcard.PresenceState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		rec, found := getRecord(t, cli, id)
		if found && rec.State == want {
			return
		}
		if found {
			last = string(rec.State)
		} else {
			last = "<absent>"
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("agent %s never reached state %q (last %q)", id, want, last)
}

func waitGone(t *testing.T, cli *bus.Client, id string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, found := getRecord(t, cli, id); !found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("agent %s never evicted", id)
}

func TestThreeAgentsRegisterAndAppearLive(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	for _, id := range []string{"a1", "a2", "a3"} {
		register(t, cli, id)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		keys, err := cli.KVList(envelope.BucketRegistry)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry has %d entries, want 3", len(keys))
		}
		time.Sleep(10 * time.Millisecond)
	}
	rec, found := getRecord(t, cli, "a2")
	if !found || rec.State != agentcard.PresenceLive || rec.Card.Role != "builder" {
		t.Fatalf("a2 record = %+v found=%v", rec, found)
	}
}

func TestStatusUpdatesRecord(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "a1")
	waitState(t, cli, "a1", agentcard.PresenceLive, time.Second)

	env, err := envelope.New(envelope.KindStatus, "a1", envelope.SubjectStatus("a1"),
		&envelope.StatusPayload{ID: "a1", Text: "working"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		rec, _ := getRecord(t, cli, "a1")
		if rec.LastStatus == "working" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status never landed: %+v", rec)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestTwoTierEviction is the core P0 lease behavior: a silent agent goes
// away, then is evicted, and the eviction is announced as a leave event.
func TestTwoTierEviction(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	evicted := make(chan string, 1)
	if _, err := cli.Subscribe(envelope.SubjectLeave, func(env envelope.Envelope) {
		var p envelope.LeavePayload
		if err := envelope.DecodeInto(env, &p); err == nil && p.Reason == "evicted" {
			evicted <- p.ID
		}
	}); err != nil {
		t.Fatal(err)
	}

	register(t, cli, "doomed")
	register(t, cli, "survivor")
	waitState(t, cli, "doomed", agentcard.PresenceLive, time.Second)

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

	waitState(t, cli, "doomed", agentcard.PresenceAway, 2*time.Second)
	waitGone(t, cli, "doomed", 2*time.Second)

	select {
	case id := <-evicted:
		if id != "doomed" {
			t.Fatalf("evict leave for %q, want doomed", id)
		}
	case <-time.After(time.Second):
		t.Fatal("no mesh.leave published for eviction")
	}

	// The beating agent must still be live.
	rec, found := getRecord(t, cli, "survivor")
	if !found || rec.State != agentcard.PresenceLive {
		t.Fatalf("survivor = %+v found=%v", rec, found)
	}
}

func TestRegistrationGraceHoldsOffReaping(t *testing.T) {
	cfg := fastConfig(t)
	cfg.AwayAfter = 50 * time.Millisecond
	cfg.EvictAfter = 80 * time.Millisecond
	cfg.RegistrationGrace = 500 * time.Millisecond
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "fresh")
	waitState(t, cli, "fresh", agentcard.PresenceLive, time.Second)

	// Well past away/evict thresholds but inside grace: still live.
	time.Sleep(200 * time.Millisecond)
	rec, found := getRecord(t, cli, "fresh")
	if !found || rec.State != agentcard.PresenceLive {
		t.Fatalf("fresh agent reaped inside grace: %+v found=%v", rec, found)
	}

	// After grace with no beats: reaped.
	waitGone(t, cli, "fresh", 2*time.Second)
}

func TestHeartbeatRecoversAwayAgent(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "flaky")
	waitState(t, cli, "flaky", agentcard.PresenceAway, 2*time.Second)

	heartbeat(t, cli, "flaky")
	waitState(t, cli, "flaky", agentcard.PresenceLive, time.Second)
}

func TestGracefulLeaveRemovesRecord(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "polite")
	waitState(t, cli, "polite", agentcard.PresenceLive, time.Second)

	env, err := envelope.New(envelope.KindLeave, "polite", envelope.SubjectLeave,
		&envelope.LeavePayload{ID: "polite", Reason: "done"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}
	waitGone(t, cli, "polite", time.Second)
}

func TestAuditStreamRecordsTransitions(t *testing.T) {
	cfg := fastConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	register(t, cli, "tracked")
	waitState(t, cli, "tracked", agentcard.PresenceAway, 2*time.Second)
	waitGone(t, cli, "tracked", 2*time.Second)

	entries, err := cli.StreamRead(envelope.StreamAudit, 0)
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	for _, e := range entries {
		var a AuditEntry
		if err := json.Unmarshal(e.Data, &a); err != nil {
			t.Fatal(err)
		}
		if a.ID == "tracked" {
			events = append(events, a.Event)
		}
	}
	want := []string{"registered", "away", "evicted"}
	if len(events) != len(want) {
		t.Fatalf("audit events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("audit events = %v, want %v", events, want)
		}
	}
}
