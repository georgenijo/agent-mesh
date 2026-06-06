package observe

import (
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/coordinator"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/sidecar"
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
	}
}

func startStack(t *testing.T) (config.Config, *sidecar.Sidecar) {
	t.Helper()
	cfg := fastConfig(t)
	coord := coordinator.New(cfg, nil)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord.Stop)

	card := agentcard.Card{Name: "alpha", Role: "builder", PID: os.Getpid()}
	sc, err := sidecar.New(cfg, card, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sc.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.Stop)
	return cfg, sc
}

func registerGhost(t *testing.T, cfg config.Config, id string) {
	t.Helper()
	cli, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	card := agentcard.Card{ID: id, Name: id, Role: "builder", PID: 999999}
	env, err := envelope.New(envelope.KindRegister, id, envelope.SubjectRegister, &envelope.RegisterPayload{Card: card})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		keys, err := cli.KVList(envelope.BucketRegistry)
		if err == nil && len(keys) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("ghost never registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCollectShowsHealthyStack(t *testing.T) {
	cfg, _ := startStack(t)

	deadline := time.Now().Add(3 * time.Second)
	var snap Snapshot
	var err error
	for {
		snap, err = Collect(cfg)
		if err == nil && snap.Coordinator.BusDialable && len(snap.Sidecars) == 1 &&
			snap.Sidecars[0].SocketDialable && snap.Sidecars[0].Registry != nil {
			break
		}
		if time.Now().After(deadline) {
			b, _ := json.Marshal(snap)
			t.Fatalf("snapshot never became healthy: err=%v snap=%s", err, b)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !snap.Coordinator.PIDAlive || snap.Coordinator.PID <= 0 {
		t.Fatalf("coordinator pid: %+v", snap.Coordinator)
	}
	if snap.Sidecars[0].Name != "alpha" {
		t.Fatalf("sidecar name = %q", snap.Sidecars[0].Name)
	}
	if len(snap.Anomalies) > 0 {
		t.Fatalf("unexpected anomalies: %v", snap.Anomalies)
	}
}

func TestCollectDetectsGhostAgent(t *testing.T) {
	cfg := fastConfig(t)
	coord := coordinator.New(cfg, nil)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord.Stop)
	registerGhost(t, cfg, "ghost")

	snap, err := Collect(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var ghost *SidecarInfo
	for i := range snap.Sidecars {
		if snap.Sidecars[i].Name == "ghost" {
			ghost = &snap.Sidecars[i]
			break
		}
	}
	if ghost == nil {
		t.Fatal("ghost agent missing from snapshot")
	}
	if !contains(ghost.Drift, "ghost_agent") {
		t.Fatalf("drift = %v, want ghost_agent", ghost.Drift)
	}
}

func TestCollectReadsCoordinatorPIDFile(t *testing.T) {
	cfg := fastConfig(t)
	if err := os.MkdirAll(cfg.MeshDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.CoordinatorPID(), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	snap, err := Collect(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Coordinator.PID != os.Getpid() || !snap.Coordinator.PIDAlive {
		t.Fatalf("coordinator pid snapshot: %+v", snap.Coordinator)
	}
}

func contains(items []string, want string) bool {
	for _, s := range items {
		if s == want {
			return true
		}
	}
	return false
}
