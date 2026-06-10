package task

import (
	"errors"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func taskStore(t *testing.T) (Store, *bus.Client, func()) {
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
	base := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	return NewStore(cli).withNow(func() time.Time { return base }), cli, func() {
		cli.Close()
		srv.Stop()
	}
}

func TestCreateAllPersistsDAGReadableByJob(t *testing.T) {
	store, cli, cleanup := taskStore(t)
	defer cleanup()

	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	recs := FromPlan("job-1", validPlan(), now)
	if err := store.CreateAll(recs); err != nil {
		t.Fatal(err)
	}
	// Another job's task must not bleed into the listing.
	other := FromPlan("job-2", Plan{Version: 1, Nodes: []Node{{ID: "solo", Title: "x", Role: "builder"}}}, now)
	if err := store.CreateAll(other); err != nil {
		t.Fatal(err)
	}

	got, err := store.ListByJob("job-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(recs) {
		t.Fatalf("ListByJob = %d records, want %d", len(got), len(recs))
	}
	want := map[string]Record{}
	for _, r := range recs {
		want[r.ID] = r
	}
	for _, g := range got {
		w, ok := want[g.ID]
		if !ok {
			t.Fatalf("unexpected task %+v", g)
		}
		if g.Node != w.Node || g.Role != w.Role || g.State != envelope.TaskPending {
			t.Fatalf("task drifted: got %+v want %+v", g, w)
		}
	}

	// Single-record read path.
	one, found, err := store.Get(recs[0].ID)
	if err != nil || !found {
		t.Fatalf("Get found=%v err=%v", found, err)
	}
	if one.Title != recs[0].Title {
		t.Fatalf("Get = %+v, want %+v", one, recs[0])
	}

	// One TaskPending event per created task on the task-events stream.
	entries, err := cli.StreamRead(envelope.StreamTasks, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(recs)+len(other) {
		t.Fatalf("stream has %d events, want %d", len(entries), len(recs)+len(other))
	}
}

func TestCreateAllRejectsBadRecords(t *testing.T) {
	store, _, cleanup := taskStore(t)
	defer cleanup()

	now := time.Now().UTC()
	cases := map[string]Record{
		"missing job":   {ID: envelope.NewID(), Node: "a", Title: "t", Role: "builder", State: envelope.TaskPending, CreatedAt: now},
		"missing node":  {ID: envelope.NewID(), Job: "j", Title: "t", Role: "builder", State: envelope.TaskPending, CreatedAt: now},
		"missing title": {ID: envelope.NewID(), Job: "j", Node: "a", Role: "builder", State: envelope.TaskPending, CreatedAt: now},
		"missing role":  {ID: envelope.NewID(), Job: "j", Node: "a", Title: "t", State: envelope.TaskPending, CreatedAt: now},
		"wrong state":   {ID: envelope.NewID(), Job: "j", Node: "a", Title: "t", Role: "builder", State: envelope.TaskRunning, CreatedAt: now},
	}
	for name, rec := range cases {
		if err := store.CreateAll([]Record{rec}); !errors.Is(err, ErrBadRecord) {
			t.Errorf("%s: err = %v, want ErrBadRecord", name, err)
		}
	}
}

func TestCreateAllDuplicateIDIsCASLost(t *testing.T) {
	store, _, cleanup := taskStore(t)
	defer cleanup()

	rec := FromPlan("job-1", Plan{Version: 1, Nodes: []Node{{ID: "a", Title: "t", Role: "builder"}}}, time.Now().UTC())
	if err := store.CreateAll(rec); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateAll(rec); !errors.Is(err, ErrCASLost) {
		t.Fatalf("duplicate create err = %v, want ErrCASLost", err)
	}
}
