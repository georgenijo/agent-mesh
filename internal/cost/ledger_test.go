package cost_test

import (
	"testing"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/cost"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// newTestBus starts an in-process bus with BucketCostLedger persisted in a
// temp directory, mimicking the production coordinator setup.
func newTestBus(t *testing.T) *bus.Client {
	t.Helper()
	sockPath := testsock.Path(t, "bus.sock")
	srv := bus.NewServer(sockPath, bus.Options{
		PersistDir:     t.TempDir(),
		PersistBuckets: []string{envelope.BucketCostLedger},
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("start bus: %v", err)
	}
	t.Cleanup(srv.Stop)
	cli, err := bus.Dial(sockPath, bus.ClientOptions{})
	if err != nil {
		t.Fatalf("dial bus: %v", err)
	}
	t.Cleanup(cli.Close)
	return cli
}

func TestLedgerLoadEmpty(t *testing.T) {
	cli := newTestBus(t)
	l := cost.New(cli)
	snap, err := l.Load()
	if err != nil {
		t.Fatalf("Load on empty ledger: %v", err)
	}
	if snap.SpentUSD != 0 {
		t.Errorf("SpentUSD = %v, want 0", snap.SpentUSD)
	}
	if len(snap.ByModel) != 0 {
		t.Errorf("ByModel = %v, want empty", snap.ByModel)
	}
	if len(snap.ByAgent) != 0 {
		t.Errorf("ByAgent = %v, want empty", snap.ByAgent)
	}
}

func TestLedgerAccrual(t *testing.T) {
	cli := newTestBus(t)
	l := cost.New(cli)

	if err := l.Add(0.10, "sonnet", "w-aaa"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := l.Add(0.25, "sonnet", "w-aaa"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := l.Add(0.05, "opus", "w-bbb"); err != nil {
		t.Fatalf("Add: %v", err)
	}

	snap, err := l.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	const wantTotal = 0.40
	if diff := snap.SpentUSD - wantTotal; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("SpentUSD = %v, want %v", snap.SpentUSD, wantTotal)
	}
	if snap.ByModel["sonnet"]-0.35 > 1e-9 || snap.ByModel["sonnet"]-0.35 < -1e-9 {
		t.Errorf("ByModel[sonnet] = %v, want 0.35", snap.ByModel["sonnet"])
	}
	if snap.ByModel["opus"]-0.05 > 1e-9 || snap.ByModel["opus"]-0.05 < -1e-9 {
		t.Errorf("ByModel[opus] = %v, want 0.05", snap.ByModel["opus"])
	}
	if snap.ByAgent["w-aaa"]-0.35 > 1e-9 || snap.ByAgent["w-aaa"]-0.35 < -1e-9 {
		t.Errorf("ByAgent[w-aaa] = %v, want 0.35", snap.ByAgent["w-aaa"])
	}
	if snap.ByAgent["w-bbb"]-0.05 > 1e-9 || snap.ByAgent["w-bbb"]-0.05 < -1e-9 {
		t.Errorf("ByAgent[w-bbb] = %v, want 0.05", snap.ByAgent["w-bbb"])
	}
}

// TestLedgerPersistence verifies that totals survive a ledger/client restart
// by creating a second client to the same bus (same persistent bucket) and
// confirming Load on the new ledger returns the prior accumulated total.
func TestLedgerPersistence(t *testing.T) {
	sockPath := testsock.Path(t, "bus.sock")
	persistDir := t.TempDir()

	// First coordinator lifetime: write some cost.
	srv1 := bus.NewServer(sockPath, bus.Options{
		PersistDir:     persistDir,
		PersistBuckets: []string{envelope.BucketCostLedger},
	})
	if err := srv1.Start(); err != nil {
		t.Fatalf("start srv1: %v", err)
	}
	cli1, err := bus.Dial(sockPath, bus.ClientOptions{})
	if err != nil {
		t.Fatalf("dial srv1: %v", err)
	}
	l1 := cost.New(cli1)
	if err := l1.Add(1.50, "sonnet", "w-ccc"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	cli1.Close()
	srv1.Stop()

	// Second coordinator lifetime: restart and confirm total persisted.
	srv2 := bus.NewServer(sockPath, bus.Options{
		PersistDir:     persistDir,
		PersistBuckets: []string{envelope.BucketCostLedger},
	})
	if err := srv2.Start(); err != nil {
		t.Fatalf("start srv2: %v", err)
	}
	defer srv2.Stop()
	cli2, err := bus.Dial(sockPath, bus.ClientOptions{})
	if err != nil {
		t.Fatalf("dial srv2: %v", err)
	}
	defer cli2.Close()

	l2 := cost.New(cli2)
	snap, err := l2.Load()
	if err != nil {
		t.Fatalf("Load after restart: %v", err)
	}
	if snap.SpentUSD-1.50 > 1e-9 || snap.SpentUSD-1.50 < -1e-9 {
		t.Errorf("SpentUSD after restart = %v, want 1.50", snap.SpentUSD)
	}
	if snap.ByModel["sonnet"]-1.50 > 1e-9 || snap.ByModel["sonnet"]-1.50 < -1e-9 {
		t.Errorf("ByModel[sonnet] after restart = %v, want 1.50", snap.ByModel["sonnet"])
	}
	if snap.ByAgent["w-ccc"]-1.50 > 1e-9 || snap.ByAgent["w-ccc"]-1.50 < -1e-9 {
		t.Errorf("ByAgent[w-ccc] after restart = %v, want 1.50", snap.ByAgent["w-ccc"])
	}
}

func TestLedgerNoModelDoesNotPanic(t *testing.T) {
	cli := newTestBus(t)
	l := cost.New(cli)
	if err := l.Add(0.01, "", ""); err != nil {
		t.Fatalf("Add with empty model: %v", err)
	}
	snap, err := l.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if snap.SpentUSD-0.01 > 1e-9 || snap.SpentUSD-0.01 < -1e-9 {
		t.Errorf("SpentUSD = %v, want 0.01", snap.SpentUSD)
	}
	if len(snap.ByModel) != 0 {
		t.Errorf("ByModel should be empty for no-model add, got %v", snap.ByModel)
	}
}
