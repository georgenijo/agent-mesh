package bus

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// newBucketServer starts a server that persists the named buckets under dir.
// Tests restart servers by Stop-ing one and starting a fresh one on the same
// dir; Cleanup is a safety net (Stop is idempotent).
func newBucketServer(t *testing.T, dir string, buckets []string, opts Options) (*Server, string) {
	t.Helper()
	opts.PersistDir = dir
	opts.PersistBuckets = buckets
	path := testsock.Path(t, "bus.sock")
	s := NewServer(path, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(s.Stop)
	return s, path
}

func mustGet(t *testing.T, c *Client, bucket, key string) (KVValue, bool) {
	t.Helper()
	v, found, err := c.KVGet(bucket, key)
	if err != nil {
		t.Fatalf("get %s/%s: %v", bucket, key, err)
	}
	return v, found
}

// TestBucketPersistReplayAcrossRestart is the core #65 guarantee: records put
// into a persisted bucket survive a coordinator Stop/Start.
func TestBucketPersistReplayAcrossRestart(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "buckets")

	s1, path1 := newBucketServer(t, dir, []string{"jobs", "tasks"}, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	if _, err := c1.KVPut("jobs", "job-1", map[string]string{"title": "fix bug", "state": "open"}, PutOptions{CAS: CreateOnly()}); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.KVPut("tasks", "task-1", map[string]string{"job": "job-1", "role": "builder"}, PutOptions{CAS: CreateOnly()}); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.KVPut("tasks", "task-2", map[string]string{"job": "job-1", "role": "reviewer"}, PutOptions{CAS: CreateOnly()}); err != nil {
		t.Fatal(err)
	}
	c1.Close()
	s1.Stop()

	s2, path2 := newBucketServer(t, dir, []string{"jobs", "tasks"}, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})

	job, found := mustGet(t, c2, "jobs", "job-1")
	if !found {
		t.Fatal("job-1 did not survive restart")
	}
	var jm map[string]string
	if err := json.Unmarshal(job.Value, &jm); err != nil {
		t.Fatal(err)
	}
	if jm["title"] != "fix bug" || jm["state"] != "open" {
		t.Fatalf("job-1 replayed as %+v", jm)
	}
	tasks, err := c2.KVList("tasks")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("replayed %d tasks, want 2", len(tasks))
	}
}

// TestBucketPersistDeleteSurvivesRestart: a delete before restart must not
// resurrect the key on replay (the op log replays the del).
func TestBucketPersistDeleteSurvivesRestart(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "buckets")

	s1, path1 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	if _, err := c1.KVPut("jobs", "job-1", "v1", PutOptions{CAS: CreateOnly()}); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.KVPut("jobs", "job-2", "v2", PutOptions{CAS: CreateOnly()}); err != nil {
		t.Fatal(err)
	}
	if err := c1.KVDelete("jobs", "job-1"); err != nil {
		t.Fatal(err)
	}
	c1.Close()
	s1.Stop()

	s2, path2 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	if _, found := mustGet(t, c2, "jobs", "job-1"); found {
		t.Fatal("deleted job-1 resurrected on restart")
	}
	if _, found := mustGet(t, c2, "jobs", "job-2"); !found {
		t.Fatal("job-2 lost on restart")
	}
}

// TestBucketPersistRevisionResumes: the bucket revision counter must continue
// past the highest pre-restart rev, so a post-restart CAS cannot collide.
func TestBucketPersistRevisionResumes(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "buckets")

	s1, path1 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	var lastRev uint64
	for i := 0; i < 3; i++ {
		rev, err := c1.KVPut("jobs", "job-1", i, PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		lastRev = rev
	}
	c1.Close()
	s1.Stop()

	s2, path2 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})

	// The replayed key carries its last rev; a CAS guarded by it must succeed,
	// and the new rev must be strictly greater (counter resumed, not reset).
	v, found := mustGet(t, c2, "jobs", "job-1")
	if !found {
		t.Fatal("job-1 lost")
	}
	if v.Rev != lastRev {
		t.Fatalf("replayed rev = %d, want %d", v.Rev, lastRev)
	}
	newRev, err := c2.KVPut("jobs", "job-1", "next", PutOptions{CAS: Rev(v.Rev)})
	if err != nil {
		t.Fatalf("CAS on replayed rev failed: %v", err)
	}
	if newRev <= lastRev {
		t.Fatalf("post-restart rev = %d, want > %d", newRev, lastRev)
	}
}

// TestBucketNotPersistedStaysInMemory: a bucket NOT in PersistBuckets (e.g.
// registry, claims) must NOT survive a restart — the explicit non-goal.
func TestBucketNotPersistedStaysInMemory(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "buckets")

	s1, path1 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	if _, err := c1.KVPut("registry", "agent-1", "alive", PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.KVPut("jobs", "job-1", "v", PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c1.Close()
	s1.Stop()

	// No bucket-registry.jsonl was ever written.
	if _, err := os.Stat(filepath.Join(dir, "bucket-registry.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("registry (a lease bucket) was persisted: %v", err)
	}

	s2, path2 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	if _, found := mustGet(t, c2, "registry", "agent-1"); found {
		t.Fatal("registry record survived restart (should be in-memory lease only)")
	}
	if _, found := mustGet(t, c2, "jobs", "job-1"); !found {
		t.Fatal("jobs record lost (should be durable)")
	}
}

// TestBucketNoPersistWithoutDir: with PersistBuckets empty, nothing touches the
// filesystem and a restart has no history (P0 in-memory behavior unchanged).
func TestBucketNoPersistWithoutDir(t *testing.T) {
	jsonlInCwd := func() int {
		matches, _ := filepath.Glob("bucket-*.jsonl")
		return len(matches)
	}
	baseline := jsonlInCwd()

	s1, path1 := newTestServer(t, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	if _, err := c1.KVPut("jobs", "job-1", "v", PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c1.Close()
	s1.Stop()

	if n := jsonlInCwd(); n != baseline {
		t.Fatalf("in-memory server created %d bucket files", n-baseline)
	}

	s2, path2 := newTestServer(t, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	if _, found := mustGet(t, c2, "jobs", "job-1"); found {
		t.Fatal("in-memory restart resurrected a record")
	}
}

// TestBucketPersistLeaseExpiryDropped: a key whose TTL lease expired while the
// server was down must not be resurrected on load. Jobs/tasks carry no TTL, but
// the mechanism must be honest for any future leased persisted bucket.
func TestBucketPersistLeaseExpiryDropped(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "buckets")

	s1, path1 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	if _, err := c1.KVPut("jobs", "ephemeral", "v", PutOptions{TTL: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.KVPut("jobs", "durable", "v", PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c1.Close()
	s1.Stop()

	time.Sleep(80 * time.Millisecond) // outlive the lease while "down"

	s2, path2 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	if _, found := mustGet(t, c2, "jobs", "ephemeral"); found {
		t.Fatal("expired lease resurrected on load")
	}
	if _, found := mustGet(t, c2, "jobs", "durable"); !found {
		t.Fatal("untimed record dropped on load")
	}
}

// TestBucketPersistCompactsAndBounds: many overwrites of the same keys must be
// bounded on disk by compaction, and the compacted snapshot must still replay
// the live state.
func TestBucketPersistCompactsAndBounds(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "buckets")

	s1, path1 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	// 3 live keys, each overwritten many times: op count >> live keys, so the
	// floor-bounded threshold must trigger at least one compaction.
	for round := 0; round < 200; round++ {
		for _, k := range []string{"a", "b", "c"} {
			if _, err := c1.KVPut("jobs", k, round, PutOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	c1.Close()
	s1.Stop()

	file := filepath.Join(dir, "bucket-jobs.jsonl")
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Count(data, []byte{'\n'})
	if lines > compactBucketThreshold(3) {
		t.Fatalf("op log holds %d lines, want <= %d (compaction bound)", lines, compactBucketThreshold(3))
	}

	s2, path2 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	keys, err := c2.KVList("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("replayed %d keys after compaction, want 3", len(keys))
	}
	for _, k := range []string{"a", "b", "c"} {
		v, found := mustGet(t, c2, "jobs", k)
		if !found {
			t.Fatalf("key %s lost after compaction", k)
		}
		var n int
		if err := json.Unmarshal(v.Value, &n); err != nil || n != 199 {
			t.Fatalf("key %s = %v (err %v), want last write 199", k, n, err)
		}
	}
}

// TestBucketPersistTornFinalLine: a torn tail (crash mid-append) costs only the
// torn op; the intact prefix replays and the file stays appendable.
func TestBucketPersistTornFinalLine(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "buckets")

	s1, path1 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	for _, k := range []string{"a", "b", "c"} {
		if _, err := c1.KVPut("jobs", k, k, PutOptions{CAS: CreateOnly()}); err != nil {
			t.Fatal(err)
		}
	}
	c1.Close()
	s1.Stop()

	file := filepath.Join(dir, "bucket-jobs.jsonl")
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(file, int64(len(data)-7)); err != nil { // tear last op mid-JSON
		t.Fatal(err)
	}

	s2, path2 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	c2 := dialTest(t, path2, ClientOptions{})
	keys, err := c2.KVList("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("replayed %d keys, want 2 (torn op dropped)", len(keys))
	}
	// File stays appendable after the load-time repair: a new put then a second
	// restart must show all three.
	if _, err := c2.KVPut("jobs", "d", "d", PutOptions{CAS: CreateOnly()}); err != nil {
		t.Fatal(err)
	}
	c2.Close()
	s2.Stop()

	s3, path3 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	defer s3.Stop()
	c3 := dialTest(t, path3, ClientOptions{})
	keys, err = c3.KVList("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("after repair+append, replayed %d keys, want 3", len(keys))
	}
}

// TestBucketPersistSkipsCorruptMidFileLine: a damaged but newline-framed op
// mid-file is skipped; the surrounding ops replay.
func TestBucketPersistSkipsCorruptMidFileLine(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "buckets")

	s1, path1 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if _, err := c1.KVPut("jobs", k, k, PutOptions{CAS: CreateOnly()}); err != nil {
			t.Fatal(err)
		}
	}
	c1.Close()
	s1.Stop()

	file := filepath.Join(dir, "bucket-jobs.jsonl")
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(data), "\n")
	lines[2] = "this is not json\n" // damage the 3rd op, keep framing
	if err := os.WriteFile(file, []byte(strings.Join(lines, "")), 0o600); err != nil {
		t.Fatal(err)
	}

	s2, path2 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	keys, err := c2.KVList("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 4 {
		t.Fatalf("replayed %d keys, want 4 (corrupt op skipped)", len(keys))
	}
}

// TestBucketPersistCleansStaleTemps: a leftover compaction *.tmp from a crash
// mid-compact is removed on load and never replayed.
func TestBucketPersistCleansStaleTemps(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "buckets")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "bucket-jobs-123.tmp")
	if err := os.WriteFile(stale, []byte("garbage\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	s1, path1 := newBucketServer(t, dir, []string{"jobs"}, Options{})
	defer s1.Stop()
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale compaction temp not cleaned: %v", err)
	}
	c1 := dialTest(t, path1, ClientOptions{})
	if _, err := c1.KVPut("jobs", "job-1", "v", PutOptions{CAS: CreateOnly()}); err != nil {
		t.Fatal(err)
	}
}
