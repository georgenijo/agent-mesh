package bus

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// newTestServer starts a server on a temp socket and returns it with a dialer.
func newTestServer(t *testing.T, opts Options) (*Server, string) {
	t.Helper()
	path := testsock.Path(t, "bus.sock")
	s := NewServer(path, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(s.Stop)
	return s, path
}

func dialTest(t *testing.T, path string, opts ClientOptions) *Client {
	t.Helper()
	c, err := Dial(path, opts)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func statusEnv(t *testing.T, from, subject, text string) envelope.Envelope {
	t.Helper()
	env, err := envelope.New(envelope.KindStatus, from, subject, &envelope.StatusPayload{ID: from, Text: text})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestPubSubWildcards(t *testing.T) {
	_, path := newTestServer(t, Options{})
	pub := dialTest(t, path, ClientOptions{})
	subC := dialTest(t, path, ClientOptions{})

	var all, statusOnly atomic.Int64
	gotAll := make(chan envelope.Envelope, 16)
	if _, err := subC.Subscribe("mesh.>", func(e envelope.Envelope) {
		all.Add(1)
		gotAll <- e
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := subC.Subscribe("mesh.status.*", func(e envelope.Envelope) {
		statusOnly.Add(1)
	}); err != nil {
		t.Fatal(err)
	}

	if err := pub.Publish(statusEnv(t, "codex", "mesh.status.codex", "hi")); err != nil {
		t.Fatal(err)
	}
	hb, err := envelope.New(envelope.KindHeartbeat, "codex", "mesh.heartbeat.codex", &envelope.HeartbeatPayload{ID: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(hb); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(2 * time.Second)
	for all.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("mesh.> saw %d envelopes, want 2", all.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Give the status-only sub a beat to (incorrectly) receive the heartbeat.
	time.Sleep(50 * time.Millisecond)
	if statusOnly.Load() != 1 {
		t.Fatalf("mesh.status.* saw %d envelopes, want 1", statusOnly.Load())
	}
	env := <-gotAll
	if env.Kind != envelope.KindStatus && env.Kind != envelope.KindHeartbeat {
		t.Fatalf("unexpected kind %q", env.Kind)
	}
}

func TestPublishRejectsInvalidEnvelope(t *testing.T) {
	_, path := newTestServer(t, Options{})
	c := dialTest(t, path, ClientOptions{})
	bad := envelope.Envelope{SchemaVersion: 99, Kind: envelope.KindStatus, ID: "x", From: "a", Subject: "mesh.x", TS: time.Now()}
	err := c.Publish(bad)
	if !envelope.IsDecodeError(err, envelope.CodeUnsupportedVersion) {
		t.Fatalf("want typed envelope error at publish edge, got %v", err)
	}
}

func TestKVCASRaceExactlyOneWinner(t *testing.T) {
	_, path := newTestServer(t, Options{})

	const racers = 20
	clients := make([]*Client, 4)
	for i := range clients {
		clients[i] = dialTest(t, path, ClientOptions{})
	}

	var wins, losses, errs atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := clients[i%len(clients)].KVPut("claims", "EventForm.tsx",
				map[string]string{"agent": fmt.Sprintf("racer-%d", i)}, PutOptions{CAS: CreateOnly()})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, ErrCASLost):
				losses.Add(1)
			default:
				errs.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if wins.Load() != 1 || losses.Load() != racers-1 || errs.Load() != 0 {
		t.Fatalf("wins=%d losses=%d errs=%d, want 1/%d/0", wins.Load(), losses.Load(), errs.Load(), racers-1)
	}
}

func TestKVCASRevisionGuard(t *testing.T) {
	_, path := newTestServer(t, Options{})
	c := dialTest(t, path, ClientOptions{})

	rev1, err := c.KVPut("b", "k", "v1", PutOptions{CAS: CreateOnly()})
	if err != nil {
		t.Fatal(err)
	}
	// Stale revision loses.
	if _, err := c.KVPut("b", "k", "v2", PutOptions{CAS: Rev(rev1 + 99)}); !errors.Is(err, ErrCASLost) {
		t.Fatalf("stale rev: want ErrCASLost, got %v", err)
	}
	// Correct revision wins.
	rev2, err := c.KVPut("b", "k", "v2", PutOptions{CAS: Rev(rev1)})
	if err != nil {
		t.Fatal(err)
	}
	if rev2 <= rev1 {
		t.Fatalf("revision did not advance: %d -> %d", rev1, rev2)
	}
}

func TestKVTTLExpiresAndRenews(t *testing.T) {
	_, path := newTestServer(t, Options{JanitorInterval: 20 * time.Millisecond})
	c := dialTest(t, path, ClientOptions{})

	if _, err := c.KVPut("registry", "codex", "alive", PutOptions{TTL: 80 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := c.KVGet("registry", "codex"); !found {
		t.Fatal("key should exist before TTL")
	}

	// Renew once — lease semantics.
	time.Sleep(50 * time.Millisecond)
	if _, err := c.KVPut("registry", "codex", "alive", PutOptions{TTL: 80 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, found, _ := c.KVGet("registry", "codex"); !found {
		t.Fatal("renewed key should still exist")
	}

	// Stop renewing — it must expire.
	time.Sleep(120 * time.Millisecond)
	if _, found, _ := c.KVGet("registry", "codex"); found {
		t.Fatal("key should have expired")
	}
	keys, err := c.KVList("registry")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("expired key still listed: %v", keys)
	}
}

func TestStreamBoundedRetention(t *testing.T) {
	_, path := newTestServer(t, Options{MaxStreamLen: 10})
	c := dialTest(t, path, ClientOptions{})

	for i := 1; i <= 25; i++ {
		seq, err := c.StreamAppend("audit", map[string]int{"n": i})
		if err != nil {
			t.Fatal(err)
		}
		if seq != uint64(i) {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
	}
	entries, err := c.StreamRead("audit", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 10 {
		t.Fatalf("retained %d entries, want 10", len(entries))
	}
	if entries[0].Seq != 16 || entries[9].Seq != 25 {
		t.Fatalf("retained range %d..%d, want 16..25", entries[0].Seq, entries[9].Seq)
	}
	// Partial read from mid-window.
	tail, err := c.StreamRead("audit", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail) != 6 || tail[0].Seq != 20 {
		t.Fatalf("tail read wrong: len=%d first=%d", len(tail), tail[0].Seq)
	}
}

func TestServerStopIsDeterministic(t *testing.T) {
	path := testsock.Path(t, "bus.sock")
	s := NewServer(path, Options{})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	c, err := Dial(path, ClientOptions{ReconnectMin: 10 * time.Millisecond, ReconnectMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() { s.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return")
	}
	// Stop twice is safe.
	s.Stop()

	// Client operations now fail fast (disconnected), not hang.
	errCh := make(chan error, 1)
	go func() { errCh <- c.Ping() }()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("ping should fail after server stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ping hung after server stop")
	}
}

func TestClientReconnectsAndResubscribes(t *testing.T) {
	path := testsock.Path(t, "bus.sock")
	s1 := NewServer(path, Options{})
	if err := s1.Start(); err != nil {
		t.Fatal(err)
	}

	reconnected := make(chan struct{}, 1)
	c, err := Dial(path, ClientOptions{
		ReconnectMin: 10 * time.Millisecond,
		ReconnectMax: 50 * time.Millisecond,
		OnReconnect:  func() { reconnected <- struct{}{} },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	got := make(chan envelope.Envelope, 4)
	if _, err := c.Subscribe("mesh.>", func(e envelope.Envelope) { got <- e }); err != nil {
		t.Fatal(err)
	}

	s1.Stop()
	s2 := NewServer(path, Options{})
	if err := s2.Start(); err != nil {
		t.Fatal(err)
	}
	defer s2.Stop()

	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("client did not reconnect")
	}

	// A fresh publisher's message must reach the pre-restart subscription.
	pub := dialTest(t, path, ClientOptions{})
	if err := pub.Publish(statusEnv(t, "codex", "mesh.status.codex", "back")); err != nil {
		t.Fatal(err)
	}
	select {
	case env := <-got:
		var p envelope.StatusPayload
		if err := envelope.DecodeInto(env, &p); err != nil || p.Text != "back" {
			t.Fatalf("payload = %+v err=%v", p, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resubscribed handler never received the message")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	_, path := newTestServer(t, Options{})
	c := dialTest(t, path, ClientOptions{})

	var n atomic.Int64
	sub, err := c.Subscribe("mesh.>", func(envelope.Envelope) { n.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(statusEnv(t, "a", "mesh.status.a", "one")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { return n.Load() == 1 })

	sub.Unsubscribe()
	if err := c.Publish(statusEnv(t, "a", "mesh.status.a", "two")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if n.Load() != 1 {
		t.Fatalf("received %d messages after unsubscribe, want 1", n.Load())
	}
}

func TestStoreNamesValidatedAndCapped(t *testing.T) {
	_, path := newTestServer(t, Options{})
	c := dialTest(t, path, ClientOptions{})

	// Bad names are typed rejections, not silent bucket creation.
	for _, bad := range []string{"", "a.b", "x/../y", "name with space", "mesh.>"} {
		if _, err := c.KVPut(bad, "k", "v", PutOptions{}); err == nil {
			t.Fatalf("bucket name %q accepted", bad)
		}
		if _, err := c.StreamAppend(bad, "v"); err == nil {
			t.Fatalf("stream name %q accepted", bad)
		}
	}

	// Distinct-name count is capped.
	var capped bool
	for i := 0; i < 100; i++ {
		if _, err := c.KVPut(fmt.Sprintf("bucket-%d", i), "k", "v", PutOptions{}); err != nil {
			capped = true
			break
		}
	}
	if !capped {
		t.Fatal("creating 100 distinct buckets was not capped")
	}
}

// TestStreamReadDoesNotAllocateSlot is the F4 regression: reading a stream
// that was never written must not consume a stream slot. mesh context on an
// empty repo and the dashboard tailing every repo it sees both issue reads;
// if each allocated a slot, the bounded budget would leak until exhausted and
// real appends would start failing.
func TestStreamReadDoesNotAllocateSlot(t *testing.T) {
	_, path := newTestServer(t, Options{})
	c := dialTest(t, path, ClientOptions{})

	// Read far more distinct never-written streams than the slot cap. Each
	// returns empty; none may allocate.
	for i := 0; i < maxStoreNames*3; i++ {
		entries, err := c.StreamRead(fmt.Sprintf("notes-r%d", i), 0)
		if err != nil {
			t.Fatalf("read of empty stream %d errored: %v", i, err)
		}
		if len(entries) != 0 {
			t.Fatalf("empty stream %d returned %d entries", i, len(entries))
		}
	}

	// The slot budget is untouched: a real append still succeeds, and reading
	// it back works.
	if _, err := c.StreamAppend("audit", "x"); err != nil {
		t.Fatalf("append after many empty reads failed (slot leak): %v", err)
	}
	entries, err := c.StreamRead("audit", 0)
	if err != nil || len(entries) != 1 {
		t.Fatalf("append/read after empty reads: entries=%d err=%v", len(entries), err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition never became true")
}
