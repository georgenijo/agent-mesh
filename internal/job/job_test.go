package job

import (
	"errors"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func jobStore(t *testing.T) (Store, func()) {
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
	base := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	return NewStore(cli).withNow(func() time.Time { return base }), func() {
		cli.Close()
		srv.Stop()
	}
}

func TestCreateAssignsIDAndForcesOpen(t *testing.T) {
	store, cleanup := jobStore(t)
	defer cleanup()

	// State is forced to open even if a caller passes a later state.
	rec, err := store.Create(Record{Repo: "demo", Source: SourceManual, Title: "do X", State: envelope.JobDone})
	if err != nil {
		t.Fatal(err)
	}
	if rec.ID == "" {
		t.Fatal("expected generated id")
	}
	if rec.State != envelope.JobOpen {
		t.Fatalf("state = %s, want open", rec.State)
	}
	if rec.CreatedAt.IsZero() {
		t.Fatal("expected createdAt set")
	}

	got, found, err := store.Get(rec.ID)
	if err != nil || !found {
		t.Fatalf("get found=%v err=%v", found, err)
	}
	if got != rec {
		t.Fatalf("get = %+v, want %+v", got, rec)
	}
}

func TestCreateNoDedupNewIDEachSubmit(t *testing.T) {
	store, cleanup := jobStore(t)
	defer cleanup()

	a, err := store.Create(Record{Repo: "demo", Source: SourceManual, Title: "same task"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Create(Record{Repo: "demo", Source: SourceManual, Title: "same task"})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == b.ID {
		t.Fatalf("duplicate submit reused id %s; want new id per submit", a.ID)
	}
	list, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("list len = %d, want 2", len(list))
	}
}

func TestCreateValidation(t *testing.T) {
	store, cleanup := jobStore(t)
	defer cleanup()

	cases := map[string]Record{
		"missing repo":   {Source: SourceManual, Title: "t"},
		"missing title":  {Repo: "demo", Source: SourceManual},
		"missing source": {Repo: "demo", Title: "t"},
		"bad source":     {Repo: "demo", Source: "slack", Title: "t"},
	}
	for name, rec := range cases {
		if _, err := store.Create(rec); !errors.Is(err, ErrBadRecord) {
			t.Errorf("%s: err = %v, want ErrBadRecord", name, err)
		}
	}
}

func TestListSortedByCreatedAt(t *testing.T) {
	store, cleanup := jobStore(t)
	defer cleanup()

	t1 := time.Date(2026, 6, 8, 9, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 6, 8, 11, 0, 0, 0, time.UTC)
	// Insert out of order.
	if _, err := store.Create(Record{Repo: "demo", Source: SourceManual, Title: "second", CreatedAt: t2}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(Record{Repo: "demo", Source: SourceManual, Title: "third", CreatedAt: t3}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(Record{Repo: "demo", Source: SourceManual, Title: "first", CreatedAt: t1}); err != nil {
		t.Fatal(err)
	}
	list, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"first", "second", "third"}
	if len(list) != len(want) {
		t.Fatalf("list len = %d, want %d", len(list), len(want))
	}
	for i, w := range want {
		if list[i].Title != w {
			t.Errorf("list[%d].Title = %q, want %q", i, list[i].Title, w)
		}
	}
}

func TestGetMissing(t *testing.T) {
	store, cleanup := jobStore(t)
	defer cleanup()
	_, found, err := store.Get("nope")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected not found")
	}
}
