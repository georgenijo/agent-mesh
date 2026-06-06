// Middle test tier (#40): many bus.Clients hammering one bus.Server in a
// single process, where -race can observe cross-goroutine interactions —
// fan-out backpressure, revision-CAS contention, janitor-vs-lazy expiry,
// bounded-stream windows, wildcard discrimination under interleaving.
//
// This tier never execs a binary: real processes are the e2e tier's job
// (test/e2e).
// Sizing invariant: every subscription's expected volume stays under the
// cap-256 queues (Subscription.msgs drops on overflow in Client.dispatch;
// Options.OutboundDepth disconnects on overflow in serverConn.respond), and
// sub.dropped is asserted to be zero so an overflow fails loudly, not flakily.
package bus

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// droppedCount reads a subscription's overflow counter under the same lock
// Client.dispatch increments it with.
func droppedCount(sub *Subscription) uint64 {
	sub.c.mu.Lock()
	defer sub.c.mu.Unlock()
	return sub.dropped
}

func TestFanoutConcurrentPublishersExactDelivery(t *testing.T) {
	_, path := newTestServer(t, Options{})

	patterns := []string{"mesh.>", "mesh.status.*", "mesh.status.codex", "mesh.heartbeat.*"}
	subjects := []string{"mesh.status.codex", "mesh.status.claude", "mesh.heartbeat.codex", "mesh.note.repoA"}
	const (
		publishers   = 4
		perPublisher = 50
	)

	// Pre-build every envelope (publisher goroutines only publish). The
	// payload text carries the per-publisher sequence number so ordering is
	// checkable on the receive side.
	plan := make([][]envelope.Envelope, publishers)
	for p := range plan {
		from := fmt.Sprintf("pub-%d", p)
		plan[p] = make([]envelope.Envelope, 0, perPublisher)
		for n := 0; n < perPublisher; n++ {
			subj := subjects[(p+n)%len(subjects)]
			plan[p] = append(plan[p], statusEnv(t, from, subj, strconv.Itoa(n)))
		}
	}

	// Predict per-pattern delivery with the same Match the server routes by.
	expected := make([]int, len(patterns))
	for i, pat := range patterns {
		for _, envs := range plan {
			for _, env := range envs {
				if Match(pat, env.Subject) {
					expected[i]++
				}
			}
		}
		// Sizing guard: stay under the cap-256 queue bounds.
		if expected[i] >= 256 {
			t.Fatalf("pattern %q expects %d deliveries; resize the test under the 256-deep queues", pat, expected[i])
		}
	}

	var mu sync.Mutex
	received := make([][]envelope.Envelope, len(patterns))
	subs := make([]*Subscription, len(patterns))
	for i, pat := range patterns {
		c := dialTest(t, path, ClientOptions{})
		sub, err := c.Subscribe(pat, func(e envelope.Envelope) {
			mu.Lock()
			received[i] = append(received[i], e)
			mu.Unlock()
		})
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = sub
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for p := 0; p < publishers; p++ {
		c := dialTest(t, path, ClientOptions{})
		wg.Add(1)
		go func(envs []envelope.Envelope, c *Client) {
			defer wg.Done()
			<-start
			for _, env := range envs {
				if err := c.Publish(env); err != nil {
					t.Errorf("publish %s: %v", env.Subject, err)
					return
				}
			}
		}(plan[p], c)
	}
	close(start)
	wg.Wait()

	for i := range patterns {
		waitFor(t, 5*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(received[i]) >= expected[i]
		})
	}
	// A beat for any over-delivery to surface before the exact-count check.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for i, pat := range patterns {
		got := received[i]
		if len(got) != expected[i] {
			t.Errorf("pattern %q received %d envelopes, want exactly %d", pat, len(got), expected[i])
		}
		seen := make(map[string]bool, len(got))
		lastSeq := make(map[string]int)
		for _, env := range got {
			if seen[env.ID] {
				t.Errorf("pattern %q received duplicate envelope id %s", pat, env.ID)
			}
			seen[env.ID] = true
			var p envelope.StatusPayload
			if err := envelope.DecodeInto(env, &p); err != nil {
				t.Errorf("pattern %q: decode payload: %v", pat, err)
				continue
			}
			seq, err := strconv.Atoi(p.Text)
			if err != nil {
				t.Errorf("pattern %q: payload text %q is not a sequence number", pat, p.Text)
				continue
			}
			if last, ok := lastSeq[env.From]; ok && seq <= last {
				t.Errorf("pattern %q: %s seq %d arrived after %d — per-publisher order broken", pat, env.From, seq, last)
			}
			lastSeq[env.From] = seq
		}
	}
	for i, sub := range subs {
		if n := droppedCount(sub); n != 0 {
			t.Errorf("pattern %q dropped %d messages — subscription queue overflowed", patterns[i], n)
		}
	}
}

// TestKVRevCASCounterNoLostUpdates is the revision-guarded complement to
// TestKVCASRaceExactlyOneWinner (which covers create-only CAS): contended
// read-modify-write increments must never lose an update.
func TestKVRevCASCounterNoLostUpdates(t *testing.T) {
	_, path := newTestServer(t, Options{})

	clients := make([]*Client, 4)
	for i := range clients {
		clients[i] = dialTest(t, path, ClientOptions{})
	}
	if _, err := clients[0].KVPut("counters", "shared", 0, PutOptions{CAS: CreateOnly()}); err != nil {
		t.Fatal(err)
	}

	const (
		goroutinesPerClient = 4
		winsPerGoroutine    = 10
	)
	total := len(clients) * goroutinesPerClient * winsPerGoroutine

	var casLost, wins atomic.Int64
	var errMu sync.Mutex
	var unexpected []error
	fail := func(err error) {
		errMu.Lock()
		unexpected = append(unexpected, err)
		errMu.Unlock()
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, c := range clients {
		for g := 0; g < goroutinesPerClient; g++ {
			wg.Add(1)
			go func(c *Client) {
				defer wg.Done()
				<-start
				for landed := 0; landed < winsPerGoroutine; {
					kv, found, err := c.KVGet("counters", "shared")
					if err != nil {
						fail(fmt.Errorf("get: %w", err))
						return
					}
					if !found {
						fail(errors.New("counter key vanished"))
						return
					}
					var cur int
					if err := json.Unmarshal(kv.Value, &cur); err != nil {
						fail(fmt.Errorf("counter value: %w", err))
						return
					}
					_, err = c.KVPut("counters", "shared", cur+1, PutOptions{CAS: Rev(kv.Rev)})
					switch {
					case err == nil:
						landed++
						wins.Add(1)
					case errors.Is(err, ErrCASLost):
						casLost.Add(1) // legitimate loss; reread and retry
					default:
						fail(fmt.Errorf("put: %w", err))
						return
					}
				}
			}(c)
		}
	}
	close(start)
	wg.Wait()

	if len(unexpected) != 0 {
		t.Fatalf("%d errors other than ErrCASLost, first: %v", len(unexpected), unexpected[0])
	}
	if wins.Load() != int64(total) {
		t.Fatalf("recorded %d successful puts, want %d", wins.Load(), total)
	}
	kv, found, err := clients[0].KVGet("counters", "shared")
	if err != nil || !found {
		t.Fatalf("final read: found=%v err=%v", found, err)
	}
	var final int
	if err := json.Unmarshal(kv.Value, &final); err != nil {
		t.Fatal(err)
	}
	if final != total {
		t.Fatalf("counter = %d after %d successful CAS puts — %d updates lost", final, total, total-final)
	}
	if casLost.Load() == 0 {
		t.Fatal("no ErrCASLost observed — contention never exercised the revision guard")
	}
}

// TestKVLazyExpiryWithoutJanitor pins the server.go Options doc: "Reads also
// lazily treat expired keys as absent, so TTL correctness does not depend on
// janitor timing." The hour-long JanitorInterval guarantees the janitor never
// fires here, so every expiry observed is the lazy read-side path.
func TestKVLazyExpiryWithoutJanitor(t *testing.T) {
	_, path := newTestServer(t, Options{JanitorInterval: time.Hour})
	c := dialTest(t, path, ClientOptions{})

	// 250ms TTL: generous enough that the put+get round trips land well
	// inside it even under a CI scheduler stall, so the pre-expiry existence
	// check below cannot fail spuriously. Lazy expiry is what's under test,
	// not tight timing — the parked janitor already guarantees every observed
	// expiry is the read-side path.
	if _, err := c.KVPut("leases", "worker", "alive", PutOptions{TTL: 250 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := c.KVGet("leases", "worker"); err != nil || !found {
		t.Fatalf("key should exist before TTL: found=%v err=%v", found, err)
	}

	time.Sleep(300 * time.Millisecond) // past the TTL; janitor still an hour away

	if _, found, err := c.KVGet("leases", "worker"); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("expired key still returned by KVGet — lazy expiry broken")
	}
	keys, err := c.KVList("leases")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := keys["worker"]; ok {
		t.Fatalf("expired key still listed: %v", keys)
	}
	// The CAS path must also treat the expired entry as absent: create-only
	// on the same key wins instead of returning ErrCASLost.
	if _, err := c.KVPut("leases", "worker", "reborn", PutOptions{CAS: CreateOnly()}); err != nil {
		t.Fatalf("create-only put on expired key must succeed, got %v", err)
	}
	kv, found, err := c.KVGet("leases", "worker")
	if err != nil || !found {
		t.Fatalf("recreated key missing: found=%v err=%v", found, err)
	}
	if string(kv.Value) != `"reborn"` {
		t.Fatalf("recreated value = %s, want %q", kv.Value, "reborn")
	}
}

func TestStreamConcurrentAppendersBoundedWindow(t *testing.T) {
	_, path := newTestServer(t, Options{MaxStreamLen: 50})

	clients := make([]*Client, 4)
	for i := range clients {
		clients[i] = dialTest(t, path, ClientOptions{})
	}

	const (
		appenders    = 8
		perGoroutine = 25
	)
	const total = appenders * perGoroutine // 200

	seqs := make(chan uint64, total)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < appenders; g++ {
		wg.Add(1)
		go func(g int, c *Client) {
			defer wg.Done()
			<-start
			for n := 0; n < perGoroutine; n++ {
				seq, err := c.StreamAppend("board", map[string]int{"appender": g, "n": n})
				if err != nil {
					t.Errorf("append: %v", err)
					return
				}
				seqs <- seq
			}
		}(g, clients[g%len(clients)])
	}
	close(start)
	wg.Wait()
	close(seqs)

	// The 200 returned seqs must be exactly the set 1..200, no duplicates.
	got := make(map[uint64]bool, total)
	for seq := range seqs {
		if got[seq] {
			t.Errorf("duplicate seq %d returned to appenders", seq)
		}
		got[seq] = true
	}
	if len(got) != total {
		t.Fatalf("got %d distinct seqs, want %d", len(got), total)
	}
	for s := uint64(1); s <= total; s++ {
		if !got[s] {
			t.Fatalf("seq %d missing — returned seqs are not exactly 1..%d", s, total)
		}
	}

	// The retained window is the last MaxStreamLen entries: 151..200,
	// contiguous ascending.
	entries, err := clients[0].StreamRead("board", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 50 {
		t.Fatalf("retained %d entries, want 50", len(entries))
	}
	for i, e := range entries {
		if want := uint64(total - 50 + 1 + i); e.Seq != want {
			t.Fatalf("entry %d has seq %d, want %d (contiguous 151..200)", i, e.Seq, want)
		}
	}
	// Reading from below the retained window clamps to the window start
	// rather than erroring.
	below, err := clients[1].StreamRead("board", 10)
	if err != nil {
		t.Fatalf("read below retained window must clamp, not error: %v", err)
	}
	if len(below) != 50 || below[0].Seq != total-50+1 {
		t.Fatalf("read from 10: len=%d first=%d, want 50 entries clamped to %d", len(below), below[0].Seq, total-50+1)
	}
}

func TestWildcardDiscriminationUnderConcurrentLoad(t *testing.T) {
	_, path := newTestServer(t, Options{})

	patterns := []string{"mesh.>", "mesh.*", "mesh.note.*", "mesh.note.>"}
	// One-, two-, three-, and four-token subjects.
	subjects := []string{"mesh", "mesh.note", "mesh.note.repoA", "mesh.note.repoA.fileB"}
	const (
		publishers = 4
		rounds     = 10
	)

	var mu sync.Mutex
	counts := make([]map[string]int, len(patterns))
	subs := make([]*Subscription, len(patterns))
	for i, pat := range patterns {
		counts[i] = make(map[string]int)
		c := dialTest(t, path, ClientOptions{})
		sub, err := c.Subscribe(pat, func(e envelope.Envelope) {
			mu.Lock()
			counts[i][e.Subject]++
			mu.Unlock()
		})
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = sub
	}

	plan := make([][]envelope.Envelope, publishers)
	for p := range plan {
		from := fmt.Sprintf("pub-%d", p)
		for r := 0; r < rounds; r++ {
			for _, subj := range subjects {
				plan[p] = append(plan[p], statusEnv(t, from, subj, fmt.Sprintf("round-%d", r)))
			}
		}
	}
	expected := make([]map[string]int, len(patterns))
	expectedTotal := make([]int, len(patterns))
	for i, pat := range patterns {
		expected[i] = make(map[string]int)
		for _, envs := range plan {
			for _, env := range envs {
				if Match(pat, env.Subject) {
					expected[i][env.Subject]++
					expectedTotal[i]++
				}
			}
		}
		if expectedTotal[i] >= 256 {
			t.Fatalf("pattern %q expects %d deliveries; resize the test under the 256-deep queues", pat, expectedTotal[i])
		}
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for p := 0; p < publishers; p++ {
		c := dialTest(t, path, ClientOptions{})
		wg.Add(1)
		go func(envs []envelope.Envelope, c *Client) {
			defer wg.Done()
			<-start
			for _, env := range envs {
				if err := c.Publish(env); err != nil {
					t.Errorf("publish %s: %v", env.Subject, err)
					return
				}
			}
		}(plan[p], c)
	}
	close(start)
	wg.Wait()

	for i := range patterns {
		waitFor(t, 5*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			n := 0
			for _, c := range counts[i] {
				n += c
			}
			return n >= expectedTotal[i]
		})
	}
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for i, pat := range patterns {
		for _, subj := range subjects {
			if got, want := counts[i][subj], expected[i][subj]; got != want {
				t.Errorf("pattern %q received %d × %q, want exactly %d", pat, got, subj, want)
			}
		}
	}
	// The discriminations the subject taxonomy depends on, spelled out.
	if got := counts[0]["mesh"]; got != 0 {
		t.Errorf(`"mesh.>" received the bare subject "mesh" %d times, want 0 (> needs at least one trailing token)`, got)
	}
	if got := counts[1]["mesh.note.repoA"]; got != 0 {
		t.Errorf(`"mesh.*" received three-token "mesh.note.repoA" %d times, want 0 (* is exactly one token)`, got)
	}
	if got := counts[2]["mesh.note.repoA.fileB"]; got != 0 {
		t.Errorf(`"mesh.note.*" received four-token "mesh.note.repoA.fileB" %d times, want 0`, got)
	}
	if got := counts[3]["mesh.note.repoA.fileB"]; got != publishers*rounds {
		t.Errorf(`"mesh.note.>" received "mesh.note.repoA.fileB" %d times, want %d`, got, publishers*rounds)
	}
	for i, sub := range subs {
		if n := droppedCount(sub); n != 0 {
			t.Errorf("pattern %q dropped %d messages — subscription queue overflowed", patterns[i], n)
		}
	}
}
