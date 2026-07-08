package answercache

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func newTestBus(t *testing.T) *bus.Client {
	t.Helper()
	path := testsock.Path(t, "answercache-bus.sock")
	srv := bus.NewServer(path, bus.Options{})
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)
	cli, err := bus.Dial(path, bus.ClientOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(cli.Close)
	return cli
}

func TestKeyCanonicalization(t *testing.T) {
	// Trim + stable JSON: whitespace variants collide.
	a := Key(" auth ", "  how? ", " ctx ", true)
	b := Key("auth", "how?", "ctx", true)
	if a == "" || a != b {
		t.Fatalf("trimmed keys diverged: %q vs %q", a, b)
	}

	// Different questions must not collide.
	if Key("auth", "how?", "", true) == Key("auth", "other?", "", true) {
		t.Fatal("different q collided")
	}

	// Different roles must not collide.
	if Key("auth", "how?", "", true) == Key("db", "how?", "", true) {
		t.Fatal("different role collided")
	}

	// includeCtx=true: ctx participates.
	withCtx := Key("auth", "how?", "tenant", true)
	withoutCtxVal := Key("auth", "how?", "", true)
	if withCtx == withoutCtxVal {
		t.Fatal("includeCtx=true ignored ctx")
	}

	// includeCtx=false: ctx is forced empty — different ctx strings share a key.
	k1 := Key("auth", "how?", "tenant-a", false)
	k2 := Key("auth", "how?", "tenant-b", false)
	k3 := Key("auth", "how?", "", false)
	if k1 != k2 || k1 != k3 {
		t.Fatalf("includeCtx=false keys diverged: %q %q %q", k1, k2, k3)
	}

	// Key is hex SHA-256 (64 chars).
	if len(a) != 64 {
		t.Fatalf("key len = %d, want 64 hex chars", len(a))
	}
}

func TestKeyStableJSONShape(t *testing.T) {
	// Pin the hashed payload shape so a future field reorder would fail loudly.
	raw, _ := json.Marshal(keyMaterial{Role: "auth", Q: "how?", Ctx: "x"})
	want := `{"role":"auth","q":"how?","ctx":"x"}`
	if string(raw) != want {
		t.Fatalf("key material JSON = %s, want %s", raw, want)
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	cli := newTestBus(t)
	store := New(cli, time.Minute, true)

	if _, ok := store.Get("auth", "how?", ""); ok {
		t.Fatal("empty store returned a hit")
	}

	now := time.Now().UTC().Truncate(time.Second)
	err := store.Put(Entry{
		Role: "auth", Q: "how?", Ctx: "tenant",
		Answer: "use RLS", AnsweredBy: "expert-1", AnsweredAt: now,
		SourceTicket: "T1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, ok := store.Get("auth", "how?", "tenant")
	if !ok {
		t.Fatal("Get miss after Put")
	}
	if got.Answer != "use RLS" || got.AnsweredBy != "expert-1" || got.SourceTicket != "T1" {
		t.Fatalf("entry = %+v", got)
	}
	if !got.AnsweredAt.Equal(now) {
		t.Fatalf("AnsweredAt = %v, want %v", got.AnsweredAt, now)
	}

	// Different ctx is a miss when includeCtx is on.
	if _, ok := store.Get("auth", "how?", "other"); ok {
		t.Fatal("different ctx should miss when includeCtx=true")
	}
}

func TestPutSkipsEmptyRoleOrAnswer(t *testing.T) {
	cli := newTestBus(t)
	store := New(cli, time.Minute, true)

	if err := store.Put(Entry{Role: "", Q: "q", Answer: "a"}); err != nil {
		t.Fatalf("empty role Put: %v", err)
	}
	if err := store.Put(Entry{Role: "auth", Q: "q", Answer: ""}); err != nil {
		t.Fatalf("empty answer Put: %v", err)
	}
	if _, ok := store.Get("auth", "q", ""); ok {
		t.Fatal("skipped Put still produced a hit")
	}
	if _, ok := store.Get("", "q", ""); ok {
		t.Fatal("empty-role Get should miss")
	}
}

func TestGetCorruptJSONIsMiss(t *testing.T) {
	cli := newTestBus(t)
	store := New(cli, time.Minute, true)
	key := Key("auth", "how?", "", true)
	if _, err := cli.KVPut("answer-cache", key, "not-json", bus.PutOptions{TTL: time.Minute}); err != nil {
		t.Fatalf("seed corrupt: %v", err)
	}
	if _, ok := store.Get("auth", "how?", ""); ok {
		t.Fatal("corrupt JSON should degrade to miss")
	}
}

func TestPutGetTTLExpiry(t *testing.T) {
	cli := newTestBus(t)
	store := New(cli, 80*time.Millisecond, true)

	if err := store.Put(Entry{Role: "auth", Q: "how?", Answer: "use RLS"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok := store.Get("auth", "how?", ""); !ok {
		t.Fatal("miss before TTL")
	}
	time.Sleep(120 * time.Millisecond)
	if _, ok := store.Get("auth", "how?", ""); ok {
		t.Fatal("hit after TTL expiry")
	}
}

func TestHitsRePut(t *testing.T) {
	cli := newTestBus(t)
	store := New(cli, time.Minute, false)

	if err := store.Put(Entry{Role: "auth", Q: "how?", Ctx: "ignored", Answer: "a"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	e, ok := store.Get("auth", "how?", "other-ctx")
	if !ok {
		t.Fatal("includeCtx=false should hit across ctx")
	}
	e.Hits++
	if err := store.Put(*e); err != nil {
		t.Fatalf("re-Put hits: %v", err)
	}
	again, ok := store.Get("auth", "how?", "")
	if !ok || again.Hits != 1 {
		t.Fatalf("hits after re-Put = %+v ok=%v", again, ok)
	}
}
