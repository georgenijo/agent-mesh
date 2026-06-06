package claim

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// newTestBus starts a real bus server on a temp socket (the engine is only
// meaningful against real CAS semantics — no mocks) and returns a dialed
// client plus the socket path for extra clients.
func newTestBus(t *testing.T) (*bus.Client, string) {
	t.Helper()
	path := testsock.Path(t, "bus.sock")
	srv := bus.NewServer(path, bus.Options{})
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)
	return dialTestBus(t, path), path
}

func dialTestBus(t *testing.T, path string) *bus.Client {
	t.Helper()
	cli, err := bus.Dial(path, bus.ClientOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(cli.Close)
	return cli
}

func mustTake(t *testing.T, cli *bus.Client, agent, repo, path string, ttl time.Duration) Outcome {
	t.Helper()
	out := Take(cli, agent, repo, path, ttl)
	if out.Result != envelope.ClaimClaimed {
		t.Fatalf("Take(%s, %s, %s) = %v (err %v), want claimed", agent, repo, path, out.Result, out.Err)
	}
	return out
}

func TestNormalizePath(t *testing.T) {
	ok := []struct{ in, want string }{
		{"a", "a"},
		{"./a", "a"},
		{"a//b", "a/b"},
		{"a/./b", "a/b"},
		{"a/b/../c", "a/c"},
		{"a/b/", "a/b"},
		{"/abs/x.go", "/abs/x.go"},
	}
	for _, tc := range ok {
		got, err := NormalizePath(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("NormalizePath(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}

	// Equivalent spellings must collide on the same key.
	a, _ := NormalizePath("./a")
	b, _ := NormalizePath("a")
	if a != b || Key("r", a) != Key("r", b) {
		t.Errorf("equivalent paths diverged: %q vs %q", a, b)
	}

	bad := []string{"", "   ", ".", "./", "..", "../x", "a/../..", "a/../../b"}
	for _, in := range bad {
		got, err := NormalizePath(in)
		if !errors.Is(err, ErrBadPath) {
			t.Errorf("NormalizePath(%q) = %q, %v; want ErrBadPath", in, got, err)
		}
	}
}

// TestConcurrentTakeSingleWinner is the core CAS guarantee: two agents racing
// for one path produce exactly one claimed and one lost, and the loser sees
// the winner's record. Run under -race.
func TestConcurrentTakeSingleWinner(t *testing.T) {
	_, sock := newTestBus(t)
	c1 := dialTestBus(t, sock)
	c2 := dialTestBus(t, sock)

	for i := 0; i < 10; i++ {
		path := fmt.Sprintf("dir/file%d.go", i)
		type res struct {
			agent string
			out   Outcome
		}
		results := make(chan res, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, p := range []struct {
			cli   *bus.Client
			agent string
		}{{c1, "a1"}, {c2, "a2"}} {
			wg.Add(1)
			go func(cli *bus.Client, agent string) {
				defer wg.Done()
				<-start
				results <- res{agent, Take(cli, agent, "r", path, time.Minute)}
			}(p.cli, p.agent)
		}
		close(start)
		wg.Wait()
		close(results)

		var winner, loser res
		var nClaimed, nLost int
		for r := range results {
			switch r.out.Result {
			case envelope.ClaimClaimed:
				nClaimed++
				winner = r
			case envelope.ClaimLost:
				nLost++
				loser = r
			default:
				t.Fatalf("%s got %v (err %v)", r.agent, r.out.Result, r.out.Err)
			}
		}
		if nClaimed != 1 || nLost != 1 {
			t.Fatalf("path %s: claimed=%d lost=%d, want exactly one of each", path, nClaimed, nLost)
		}
		if loser.out.Owner.Agent != winner.agent {
			t.Fatalf("loser sees owner %q, want %q", loser.out.Owner.Agent, winner.agent)
		}
		if loser.out.Rev != winner.out.Rev {
			t.Fatalf("loser rev %d != winner rev %d", loser.out.Rev, winner.out.Rev)
		}
	}
}

func TestReleaseIfOwner(t *testing.T) {
	cli, _ := newTestBus(t)
	mustTake(t, cli, "owner", "r", "src/x.go", time.Minute)

	// A stranger cannot release someone else's claim.
	out := Release(cli, "stranger", "r", "src/x.go")
	if out.Result != NotOwner || out.Owner.Agent != "owner" {
		t.Fatalf("stranger release = %v owner=%q, want not_owner/owner", out.Result, out.Owner.Agent)
	}
	if _, found, _ := cli.KVGet(envelope.BucketClaims, Key("r", "src/x.go")); !found {
		t.Fatal("claim vanished after a not_owner release")
	}

	// The owner can.
	out = Release(cli, "owner", "r", "src/x.go")
	if out.Result != Released {
		t.Fatalf("owner release = %v (err %v), want released", out.Result, out.Err)
	}
	if _, found, _ := cli.KVGet(envelope.BucketClaims, Key("r", "src/x.go")); found {
		t.Fatal("claim still present after release")
	}

	// Releasing an absent claim is idempotent.
	out = Release(cli, "owner", "r", "src/x.go")
	if out.Result != Released {
		t.Fatalf("double release = %v (err %v), want released", out.Result, out.Err)
	}
}

// TestSelfRetakeRefreshesLease: a re-fired hook re-takes its own claim; the
// lease must move forward while TS keeps the original claim time.
func TestSelfRetakeRefreshesLease(t *testing.T) {
	cli, _ := newTestBus(t)
	first := mustTake(t, cli, "a1", "r", "pkg/x.go", 500*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	second := Take(cli, "a1", "r", "pkg/x.go", 500*time.Millisecond)
	if second.Result != envelope.ClaimClaimed {
		t.Fatalf("self re-take = %v (err %v), want claimed", second.Result, second.Err)
	}
	if second.Rev <= first.Rev {
		t.Fatalf("re-take rev %d not above original %d", second.Rev, first.Rev)
	}
	if !second.Owner.TS.Equal(first.Owner.TS) {
		t.Fatalf("re-take moved TS: %s -> %s", first.Owner.TS, second.Owner.TS)
	}

	// Past the original lease deadline: the refresh must have kept it alive.
	time.Sleep(300 * time.Millisecond)
	if _, found, _ := cli.KVGet(envelope.BucketClaims, Key("r", "pkg/x.go")); !found {
		t.Fatal("claim expired despite re-take refreshing the lease")
	}
}

func TestRenewOwnedSkipsLostClaims(t *testing.T) {
	cli, _ := newTestBus(t)
	mustTake(t, cli, "a1", "r", "p1", 400*time.Millisecond)
	mustTake(t, cli, "a1", "r", "p2", 400*time.Millisecond)
	mustTake(t, cli, "b1", "r", "p3", 400*time.Millisecond)

	// a1 legitimately loses p2: released, then retaken by b1.
	if out := Release(cli, "a1", "r", "p2"); out.Result != Released {
		t.Fatalf("release p2 = %v", out.Result)
	}
	b1p2 := mustTake(t, cli, "b1", "r", "p2", 400*time.Millisecond)

	n, err := RenewOwned(cli, "a1", time.Second)
	if err != nil || n != 1 {
		t.Fatalf("RenewOwned = %d, %v; want 1, nil", n, err)
	}

	// b1's claims were not touched by a1's renewal pass.
	kv, found, _ := cli.KVGet(envelope.BucketClaims, Key("r", "p2"))
	if !found || kv.Rev != b1p2.Rev {
		t.Fatalf("b1's p2 changed: found=%v rev=%d want rev=%d", found, kv.Rev, b1p2.Rev)
	}

	// Past the original lease: the renewed claim survives, the others lapse.
	time.Sleep(500 * time.Millisecond)
	if _, found, _ := cli.KVGet(envelope.BucketClaims, Key("r", "p1")); !found {
		t.Fatal("renewed claim p1 expired")
	}
	if _, found, _ := cli.KVGet(envelope.BucketClaims, Key("r", "p3")); found {
		t.Fatal("unrenewed claim p3 survived its lease")
	}
}

func TestReleaseAllOwnedBy(t *testing.T) {
	cli, _ := newTestBus(t)
	mustTake(t, cli, "a1", "r1", "a", time.Minute)
	mustTake(t, cli, "a1", "r1", "b", time.Minute)
	mustTake(t, cli, "a1", "r2", "c", time.Minute)
	mustTake(t, cli, "b1", "r1", "z", time.Minute)

	released := ReleaseAllOwnedBy(cli, "a1")
	if len(released) != 3 {
		t.Fatalf("released %d claims, want 3: %+v", len(released), released)
	}
	want := []struct{ repo, path string }{{"r1", "a"}, {"r1", "b"}, {"r2", "c"}}
	for i, w := range want {
		if released[i].Repo != w.repo || released[i].Path != w.path {
			t.Fatalf("released[%d] = %s/%s, want %s/%s", i, released[i].Repo, released[i].Path, w.repo, w.path)
		}
	}

	held, err := ListAll(cli)
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 1 || held[0].Agent != "b1" || held[0].Path != "z" {
		t.Fatalf("remaining claims = %+v, want only b1's z", held)
	}

	// Idempotent: a second pass finds nothing.
	if again := ReleaseAllOwnedBy(cli, "a1"); len(again) != 0 {
		t.Fatalf("second pass released %+v, want none", again)
	}
}

func TestListAllSkipsUnparseable(t *testing.T) {
	cli, _ := newTestBus(t)
	mustTake(t, cli, "a1", "r", "good", time.Minute)
	if _, err := cli.KVPut(envelope.BucketClaims, "garbage", json.RawMessage(`[1,2]`), bus.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	held, err := ListAll(cli)
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 1 || held[0].Path != "good" {
		t.Fatalf("ListAll = %+v, want only the good record", held)
	}
}

func TestTakeRejectsBadArgs(t *testing.T) {
	cli, _ := newTestBus(t)
	cases := []struct {
		agent, repo, path string
		wantErr           error
	}{
		{"", "r", "a", ErrBadAgent},
		{"a1", "no/slashes", "a", ErrBadRepo},
		{"a1", "r", "..", ErrBadPath},
	}
	for _, tc := range cases {
		out := Take(cli, tc.agent, tc.repo, tc.path, time.Minute)
		if out.Result != envelope.ClaimError || !errors.Is(out.Err, tc.wantErr) {
			t.Errorf("Take(%q,%q,%q) = %v err=%v, want error/%v", tc.agent, tc.repo, tc.path, out.Result, out.Err, tc.wantErr)
		}
		rel := Release(cli, tc.agent, tc.repo, tc.path)
		if rel.Result != ReleaseError || !errors.Is(rel.Err, tc.wantErr) {
			t.Errorf("Release(%q,%q,%q) = %v err=%v, want error/%v", tc.agent, tc.repo, tc.path, rel.Result, rel.Err, tc.wantErr)
		}
	}
}
