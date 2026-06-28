// Package cost persists cumulative spend and per-model cost breakdowns across
// coordinator restarts. The scheduler is the sole writer; the dashboard reads
// via GET /api/cost.
package cost

import (
	"encoding/json"
	"errors"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

const ledgerKey = "total"

// Snapshot is the durable cost record stored in BucketCostLedger.
type Snapshot struct {
	SpentUSD float64            `json:"spentUSD"`
	ByModel  map[string]float64 `json:"byModel,omitempty"`
}

// Ledger reads and writes cost totals to the coordinator-owned KV bucket.
// The scheduler is its sole writer (from the loop goroutine), so unconditional
// puts are safe within a single coordinator lifetime; CAS is used on startup
// reads and write-with-verify to be safe across restarts.
type Ledger struct {
	cli *bus.Client
}

// New wraps cli in a Ledger. The bucket envelope.BucketCostLedger must be
// persisted by the coordinator so totals survive restarts.
func New(cli *bus.Client) *Ledger { return &Ledger{cli: cli} }

// Load returns the current persisted snapshot. Returns a zero Snapshot (no
// error) when no record exists yet — a fresh coordinator with no prior runs.
func (l *Ledger) Load() (Snapshot, error) {
	snap, _, err := l.load()
	return snap, err
}

// Add atomically adds delta to the cumulative total and credits model (when
// non-empty) in the per-model breakdown. It retries up to 5 times on CAS
// conflicts; in practice, the scheduler's single loop goroutine is the only
// writer so contention is near-zero.
func (l *Ledger) Add(delta float64, model string) error {
	const maxRetries = 5
	for i := 0; i < maxRetries; i++ {
		snap, rev, err := l.load()
		if err != nil {
			return err
		}
		snap.SpentUSD += delta
		if model != "" {
			if snap.ByModel == nil {
				snap.ByModel = make(map[string]float64)
			}
			snap.ByModel[model] += delta
		}
		_, err = l.cli.KVPut(envelope.BucketCostLedger, ledgerKey, snap,
			bus.PutOptions{CAS: bus.Rev(rev)})
		if err == nil {
			return nil
		}
		if !errors.Is(err, bus.ErrCASLost) {
			return err
		}
	}
	return errors.New("cost: ledger CAS retries exhausted")
}

// load returns the current snapshot and its KV revision. rev == 0 means the
// key does not exist yet (a fresh coordinator). Rev 0 is used in
// bus.PutOptions to mean "create-only / unconditional initial write";
// callers that need to distinguish "absent" from "revision 0" should inspect
// the returned Snapshot, not the revision.
func (l *Ledger) load() (Snapshot, uint64, error) {
	kv, found, err := l.cli.KVGet(envelope.BucketCostLedger, ledgerKey)
	if err != nil {
		return Snapshot{}, 0, err
	}
	if !found {
		return Snapshot{}, 0, nil
	}
	var snap Snapshot
	if err := json.Unmarshal(kv.Value, &snap); err != nil {
		return Snapshot{}, 0, err
	}
	return snap, kv.Rev, nil
}
