package settings

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

var update = flag.Bool("update", false, "rewrite golden files from the current record shape")

func ptr[T any](v T) *T { return &v }

// fullRecord is a Record with every knob set — the golden's subject and the
// round-trip fidelity case.
func fullRecord() Record {
	return Record{
		Rev: 3, UpdatedAt: time.Date(2026, 7, 7, 0, 0, 0, 0, time.UTC), UpdatedBy: "george",
		BudgetUSD: ptr(5.0), MaxWorkers: ptr(6), ReDispatchBackoff: ptr("45s"),
		WorkerCLI: ptr("claude"), WorkerModel: ptr("opus"), PlannerCLI: ptr("claude"),
		PlannerModel: ptr(""), ExpertCLI: ptr("claude"), WorkerTimeout: ptr("12m0s"),
		TriageTimeout: ptr("3m0s"), TriageBackoff: ptr("20s"), TriageMaxAttempts: ptr(5),
		ReviewRole: ptr("reviewer"), ReviewPoolSize: ptr(2), ReviewRetries: ptr(3),
		ReviewTimeout: ptr("6m0s"), KeepWorktrees: ptr("always"), AutoExperts: ptr(true),
		AuditFanout: ptr(false), ExpertIdleTTL: ptr("10m0s"), JobsAddr: ptr("127.0.0.1:8740"),
		HeartbeatInterval: ptr("5s"), AwayAfter: ptr("15s"), EvictAfter: ptr("1m0s"),
		ClaimTTL: ptr("2m20s"), DashboardAddr: ptr("127.0.0.1:8737"), ObserveAddr: ptr("127.0.0.1:8739"),
	}
}

func TestGoldenRecord(t *testing.T) {
	path := filepath.Join("testdata", "record.golden.json")
	data, err := json.MarshalIndent(fullRecord(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v (run with -update)", err)
	}
	if !bytes.Equal(data, want) {
		t.Errorf("record wire shape drifted from golden\ngolden: %s\ngot:    %s", want, data)
	}
	// Round-trips back to an identical Record (pointer fidelity, incl. empty vs unset).
	var back Record
	if err := json.Unmarshal(want, &back); err != nil {
		t.Fatal(err)
	}
	reData, _ := json.MarshalIndent(back, "", "  ")
	reData = append(reData, '\n')
	if !bytes.Equal(reData, want) {
		t.Errorf("round-trip drifted:\n%s", reData)
	}
}

func newStore(t *testing.T) (Store, func()) {
	t.Helper()
	path := testsock.Path(t, "bus.sock")
	srv := bus.NewServer(path, bus.Options{})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	cli, err := bus.Dial(path, bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	return NewStore(cli).withNow(func() time.Time { return base }), func() {
		cli.Close()
		srv.Stop()
	}
}

func TestPutCreateThenRevCAS(t *testing.T) {
	store, cleanup := newStore(t)
	defer cleanup()

	// First write: create-only. casRev must be 0.
	rec, err := store.Put(Record{UpdatedBy: "a", BudgetUSD: ptr(1.0)}, 0)
	if err != nil {
		t.Fatalf("create put: %v", err)
	}
	if rec.Rev != 1 {
		t.Fatalf("rev = %d, want 1", rec.Rev)
	}
	if rec.UpdatedAt.IsZero() {
		t.Fatal("UpdatedAt not stamped")
	}

	got, kvRev, found, err := store.Get()
	if err != nil || !found {
		t.Fatalf("get found=%v err=%v", found, err)
	}
	if got.BudgetUSD == nil || *got.BudgetUSD != 1.0 {
		t.Fatalf("budget round-trip failed: %+v", got.BudgetUSD)
	}

	// Second write with the current kv rev succeeds, bumps Rev.
	rec2, err := store.Put(Record{UpdatedBy: "b", BudgetUSD: ptr(2.0)}, kvRev)
	if err != nil {
		t.Fatalf("rev put: %v", err)
	}
	if rec2.Rev != 2 {
		t.Fatalf("rev = %d, want 2", rec2.Rev)
	}

	// A stale casRev loses the CAS race.
	if _, err := store.Put(Record{UpdatedBy: "c"}, kvRev); !errors.Is(err, ErrCASLost) {
		t.Fatalf("stale put err = %v, want ErrCASLost", err)
	}
}

func TestPutRejectsBadRecord(t *testing.T) {
	store, cleanup := newStore(t)
	defer cleanup()
	cases := map[string]Record{
		"bad duration":     {ReDispatchBackoff: ptr("not-a-dur")},
		"negative budget":  {BudgetUSD: ptr(-1.0)},
		"zero workers":     {MaxWorkers: ptr(0)},
		"bad keepwt":       {KeepWorktrees: ptr("sometimes")},
		"pool < 1":         {ReviewPoolSize: ptr(0)},
		"neg retries":      {ReviewRetries: ptr(-1)},
		"bad role token":   {ReviewRole: ptr("bad role!")},
		"away < heartbeat": {AwayAfter: ptr("1s"), HeartbeatInterval: ptr("5s")},
	}
	for name, rec := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := store.Put(rec, 0); !errors.Is(err, ErrBadRecord) {
				t.Fatalf("err = %v, want ErrBadRecord", err)
			}
		})
	}
}

func TestValidateRecordEmptyOK(t *testing.T) {
	// An empty record (nothing staged) is valid — defaults hold everywhere.
	if err := ValidateRecord(Record{}); err != nil {
		t.Fatalf("empty record rejected: %v", err)
	}
	// The full record is valid too.
	if err := ValidateRecord(fullRecord()); err != nil {
		t.Fatalf("full record rejected: %v", err)
	}
}

func TestOverlayPrecedence(t *testing.T) {
	base := defaultConfig()

	// staged applies when env is absent.
	os.Unsetenv(config.EnvBudgetUSD)
	rec := Record{BudgetUSD: ptr(9.0), MaxWorkers: ptr(7), ReDispatchBackoff: ptr("90s")}
	got := Overlay(base, rec)
	if got.BudgetUSD != 9.0 {
		t.Errorf("staged budget not applied: %v", got.BudgetUSD)
	}
	if got.MaxWorkers != 7 {
		t.Errorf("staged maxWorkers not applied: %v", got.MaxWorkers)
	}
	if got.Backoff != 90*time.Second {
		t.Errorf("staged backoff not applied: %v", got.Backoff)
	}

	// env masks staged: config.Load already resolved env into base, so Overlay
	// must keep base's value when the env var is present.
	t.Setenv(config.EnvBudgetUSD, "3")
	base.BudgetUSD = 3.0 // as config.Load would have set it
	got = Overlay(base, Record{BudgetUSD: ptr(9.0)})
	if got.BudgetUSD != 3.0 {
		t.Errorf("env did not mask staged: got %v, want 3", got.BudgetUSD)
	}
}

func TestOverlayThreeWayModel(t *testing.T) {
	os.Unsetenv(config.EnvWorkerModel)
	base := defaultConfig() // WorkerModel = "sonnet"

	// nil pointer: falls through to the default.
	if got := Overlay(base, Record{}); got.WorkerModel != config.DefaultWorkerModel {
		t.Errorf("nil model changed default: %q", got.WorkerModel)
	}
	// empty-string pointer: explicit "use the CLI default" — distinct from unset.
	if got := Overlay(base, Record{WorkerModel: ptr("")}); got.WorkerModel != "" {
		t.Errorf("empty model not applied: %q", got.WorkerModel)
	}
	// value pointer: pins the model.
	if got := Overlay(base, Record{WorkerModel: ptr("opus")}); got.WorkerModel != "opus" {
		t.Errorf("value model not applied: %q", got.WorkerModel)
	}
}

func TestArmsDelta(t *testing.T) {
	// Creating with an arming field set counts as arming.
	if got := armsDelta(Record{}, false, Record{AutoExperts: ptr(true)}); len(got) != 1 || got[0] != "autoExperts" {
		t.Fatalf("armsDelta create = %v, want [autoExperts]", got)
	}
	// Changing a non-arming field arms nothing.
	if got := armsDelta(Record{}, true, Record{MaxWorkers: ptr(9)}); len(got) != 0 {
		t.Fatalf("armsDelta non-arming = %v, want none", got)
	}
	// Re-writing the same arming value arms nothing (no delta).
	prev := Record{WorkerCLI: ptr("claude")}
	if got := armsDelta(prev, true, Record{WorkerCLI: ptr("claude")}); len(got) != 0 {
		t.Fatalf("armsDelta same value = %v, want none", got)
	}
}

func TestProjectAndDefaults(t *testing.T) {
	p := Defaults()
	if p.MaxWorkers != config.DefaultMaxWorkers {
		t.Errorf("defaults maxWorkers = %d", p.MaxWorkers)
	}
	if p.ReDispatchBackoff != config.DefaultReDispatchBackoff.String() {
		t.Errorf("defaults backoff = %q", p.ReDispatchBackoff)
	}
	if p.KeepWorktrees != config.KeepWorktreesOnFailure {
		t.Errorf("defaults keepWorktrees = %q", p.KeepWorktrees)
	}
	// A projection carries provenance from the record.
	rec := Record{Rev: 4, UpdatedBy: "x"}
	if pp := Project(defaultConfig(), rec); pp.Rev != 4 || pp.UpdatedBy != "x" {
		t.Errorf("projection provenance lost: %+v", pp)
	}
}

func TestMetaCoversEveryKnob(t *testing.T) {
	// Every arming field is in the meta table, and the meta table has no
	// unknown apply classes.
	for _, m := range Meta() {
		switch m.ApplyClass {
		case ApplyHot, ApplyRestartCoordinator, ApplyRestartFleet:
		default:
			t.Errorf("field %s has unknown apply class %q", m.Field, m.ApplyClass)
		}
		if m.EnvName == "" {
			t.Errorf("field %s has no env name", m.Field)
		}
	}
	arming := ArmingFields()
	want := map[string]bool{"autoExperts": true, "workerCLI": true, "plannerCLI": true, "reviewRole": true, "jobsAddr": true}
	if len(arming) != len(want) {
		t.Fatalf("arming fields = %v, want keys %v", arming, want)
	}
	for _, f := range arming {
		if !want[f] {
			t.Errorf("unexpected arming field %q", f)
		}
	}
}

// TestEventAppendedOnPut checks the change Event lands on StreamSettings.
func TestEventAppendedOnPut(t *testing.T) {
	store, cleanup := newStore(t)
	defer cleanup()
	if _, err := store.Put(Record{UpdatedBy: "op", BudgetUSD: ptr(5.0)}, 0); err != nil {
		t.Fatal(err)
	}
	entries, err := store.cli.StreamRead(envelope.StreamSettings, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("events = %d, want 1", len(entries))
	}
	var ev Event
	if err := json.Unmarshal(entries[0].Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.By != "op" || ev.Rev != 1 {
		t.Fatalf("event = %+v", ev)
	}
	found := false
	for _, c := range ev.Changes {
		if c.Field == "budgetUSD" && c.Old == "«unset»" && c.New == "5" {
			found = true
		}
	}
	if !found {
		t.Fatalf("budgetUSD change not recorded: %+v", ev.Changes)
	}
}
