package bus

import (
	"errors"
	"testing"
	"time"
)

// TestKVDeleteRevGuard pins the guarded-delete contract release-if-owner
// depends on: matching rev deletes, stale rev is a typed CAS loss, absent key
// is an idempotent success, rev=0 is rejected.
func TestKVDeleteRevGuard(t *testing.T) {
	_, path := newTestServer(t, Options{})
	c := dialTest(t, path, ClientOptions{})

	rev, err := c.KVPut("claims", "k", "v1", PutOptions{})
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Stale rev: someone else rewrote the key since we read it.
	rev2, err := c.KVPut("claims", "k", "v2", PutOptions{})
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if err := c.KVDeleteRev("claims", "k", rev); !errors.Is(err, ErrCASLost) {
		t.Fatalf("stale-rev delete: err=%v, want ErrCASLost", err)
	}
	if _, found, _ := c.KVGet("claims", "k"); !found {
		t.Fatal("stale-rev delete removed the key")
	}

	// Matching rev deletes.
	if err := c.KVDeleteRev("claims", "k", rev2); err != nil {
		t.Fatalf("matching-rev delete: %v", err)
	}
	if _, found, _ := c.KVGet("claims", "k"); found {
		t.Fatal("key survived matching-rev delete")
	}

	// Absent key: idempotent success.
	if err := c.KVDeleteRev("claims", "k", rev2); err != nil {
		t.Fatalf("absent-key delete: %v", err)
	}

	// rev=0 is meaningless for delete and rejected.
	if err := c.KVDeleteRev("claims", "k", 0); err == nil {
		t.Fatal("rev=0 delete accepted")
	}
}

// TestKVDeleteRevExpiredKeyIsGone: an expired (but not yet janitored) key
// behaves as absent — guarded delete succeeds idempotently.
func TestKVDeleteRevExpiredKeyIsGone(t *testing.T) {
	_, path := newTestServer(t, Options{JanitorInterval: time.Hour}) // janitor never fires
	c := dialTest(t, path, ClientOptions{})

	rev, err := c.KVPut("claims", "k", "v", PutOptions{TTL: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := c.KVDeleteRev("claims", "k", rev+999); err != nil {
		t.Fatalf("expired-key guarded delete: %v", err)
	}
}
