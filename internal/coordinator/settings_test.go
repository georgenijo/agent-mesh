package coordinator

import (
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/settings"
)

func ptr[T any](v T) *T { return &v }

// effectiveAfterPoke subscribes to mesh.settings, publishes a KindSettings poke
// (as the dashboard write path does), and returns the coordinator's authoritative
// effective republish. Using a poke avoids racing the one-shot Start publish.
func effectiveAfterPoke(t *testing.T, cli *bus.Client) envelope.SettingsPayload {
	t.Helper()
	got := make(chan envelope.SettingsPayload, 8)
	if _, err := cli.Subscribe(envelope.SubjectSettings, func(env envelope.Envelope) {
		if env.From != coordinatorID {
			return // ignore our own poke; only the coordinator's republish counts
		}
		var p envelope.SettingsPayload
		if envelope.DecodeInto(env, &p) == nil {
			got <- p
		}
	}); err != nil {
		t.Fatal(err)
	}
	poke, err := envelope.New(envelope.KindSettings, "tester", envelope.SubjectSettings, &envelope.SettingsPayload{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(poke); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		return p
	case <-time.After(3 * time.Second):
		t.Fatal("coordinator did not republish an effective settings snapshot")
		return envelope.SettingsPayload{}
	}
}

// TestStagedSettingsOverlaidOnRestart pins the core settings authority: a
// desired-config record persisted in the settings bucket is overlaid onto the
// coordinator's Config on the NEXT Start (the common autostart path), and the
// coordinator's effective snapshot on mesh.settings reflects it.
func TestStagedSettingsOverlaidOnRestart(t *testing.T) {
	cfg := fastConfig(t)

	// --- first lifetime: stage a restart-coordinator change (WorkerModel).
	c1 := New(cfg, nil)
	if err := c1.Start(); err != nil {
		t.Fatal(err)
	}
	cli1, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := settings.NewStore(cli1).Put(settings.Record{
		UpdatedBy:   "op",
		WorkerModel: ptr("opus"),
		MaxWorkers:  ptr(9),
	}, 0); err != nil {
		t.Fatalf("stage settings: %v", err)
	}
	cli1.Close()
	c1.Stop() // hard bounce; the settings bucket is persisted

	// --- second lifetime: same MeshDir. The staged record is replayed and
	// overlaid at Start.
	c2 := New(cfg, nil)
	if err := c2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c2.Stop)
	cli2 := freshBus(t, cfg)

	p := effectiveAfterPoke(t, cli2)
	if p.WorkerModel != "opus" {
		t.Fatalf("effective workerModel = %q, want opus (overlay not applied on boot)", p.WorkerModel)
	}
	if p.MaxWorkers != 9 {
		t.Fatalf("effective maxWorkers = %d, want 9", p.MaxWorkers)
	}
	if p.Rev != 1 {
		t.Fatalf("effective rev = %d, want 1 (staged provenance lost)", p.Rev)
	}
}

// TestFreshMeshNothingArmed pins the safe default: a mesh with no staged record
// boots with every arming knob off — the coordinator's effective snapshot shows
// auto-experts off and no worker/planner CLI, exactly as pre-settings behavior.
func TestFreshMeshNothingArmed(t *testing.T) {
	cfg := fastConfig(t)
	c := startCoordinator(t, cfg)
	_ = c
	cli := freshBus(t, cfg)

	p := effectiveAfterPoke(t, cli)
	if p.AutoExperts {
		t.Error("fresh mesh has auto-experts armed")
	}
	if p.WorkerCLI != "" || p.PlannerCLI != "" {
		t.Errorf("fresh mesh armed a CLI: worker=%q planner=%q", p.WorkerCLI, p.PlannerCLI)
	}
	if p.Rev != 0 {
		t.Errorf("fresh mesh reports staged rev %d, want 0", p.Rev)
	}
}
