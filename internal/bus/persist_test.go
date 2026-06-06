package bus

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// newPersistServer starts a server whose streams persist under dir. Tests
// restart servers by calling Stop explicitly and starting a fresh one on the
// same dir; the Cleanup is a safety net (Stop is idempotent).
func newPersistServer(t *testing.T, dir string, opts Options) (*Server, string) {
	t.Helper()
	opts.StreamDir = dir
	path := testsock.Path(t, "bus.sock")
	s := NewServer(path, opts)
	if err := s.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(s.Stop)
	return s, path
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return bytes.Count(data, []byte{'\n'})
}

func TestStreamPersistReplayAcrossRestart(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "streams")

	s1, path1 := newPersistServer(t, dir, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	for i := 1; i <= 5; i++ {
		seq, err := c1.StreamAppend("notes-repo1", map[string]int{"n": i})
		if err != nil {
			t.Fatal(err)
		}
		if seq != uint64(i) {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
	}
	before, err := c1.StreamRead("notes-repo1", 0)
	if err != nil {
		t.Fatal(err)
	}
	c1.Close()
	s1.Stop()

	s2, path2 := newPersistServer(t, dir, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	after, err := c2.StreamRead("notes-repo1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("replayed %d entries, want %d", len(after), len(before))
	}
	for i := range before {
		if after[i].Seq != before[i].Seq || !after[i].TS.Equal(before[i].TS) || !bytes.Equal(after[i].Data, before[i].Data) {
			t.Fatalf("entry %d: got %+v, want %+v", i, after[i], before[i])
		}
	}
	// Seq numbering resumes after the replayed history.
	seq, err := c2.StreamAppend("notes-repo1", map[string]int{"n": 6})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 6 {
		t.Fatalf("post-restart seq = %d, want 6", seq)
	}
}

func TestStreamPersistRetentionCompacts(t *testing.T) {
	const max = 10
	dir := filepath.Join(testsock.Dir(t), "streams")

	s1, path1 := newPersistServer(t, dir, Options{MaxStreamLen: max})
	c1 := dialTest(t, path1, ClientOptions{})
	for i := 1; i <= 3*max; i++ {
		if _, err := c1.StreamAppend("audit", map[string]int{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	c1.Close()
	s1.Stop()

	// Disk retention is bounded: the file was compacted along the way.
	file := filepath.Join(dir, "audit.jsonl")
	if n := countLines(t, file); n > 2*max {
		t.Fatalf("file holds %d lines, want <= %d (compaction)", n, 2*max)
	}

	s2, path2 := newPersistServer(t, dir, Options{MaxStreamLen: max})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	entries, err := c2.StreamRead("audit", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != max {
		t.Fatalf("replayed %d entries, want %d", len(entries), max)
	}
	if entries[0].Seq != 2*max+1 || entries[max-1].Seq != 3*max {
		t.Fatalf("replayed range %d..%d, want %d..%d", entries[0].Seq, entries[max-1].Seq, 2*max+1, 3*max)
	}
	// Seq numbering continues from the full history, not the retained window.
	seq, err := c2.StreamAppend("audit", map[string]int{"n": 3*max + 1})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 3*max+1 {
		t.Fatalf("post-restart seq = %d, want %d", seq, 3*max+1)
	}
}

func TestStreamPersistTornFinalLine(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "streams")

	s1, path1 := newPersistServer(t, dir, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	for i := 1; i <= 5; i++ {
		if _, err := c1.StreamAppend("notes-repo1", map[string]int{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	c1.Close()
	s1.Stop()

	// Tear the final line: truncate mid-JSON, as if the process died mid-write.
	file := filepath.Join(dir, "notes-repo1.jsonl")
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(file, int64(len(data)-7)); err != nil {
		t.Fatal(err)
	}

	s2, path2 := newPersistServer(t, dir, Options{})
	c2 := dialTest(t, path2, ClientOptions{})
	entries, err := c2.StreamRead("notes-repo1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("replayed %d entries, want 4 (intact prefix)", len(entries))
	}
	for i, e := range entries {
		if e.Seq != uint64(i+1) {
			t.Fatalf("entry %d has seq %d, want %d", i, e.Seq, i+1)
		}
	}
	// The torn entry's seq is reissued: history resumes after the last seq
	// that survived.
	seq, err := c2.StreamAppend("notes-repo1", map[string]int{"n": 99})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 5 {
		t.Fatalf("post-torn seq = %d, want 5", seq)
	}
	c2.Close()
	s2.Stop()

	// Restart once more: the load-time truncation repair must have kept the
	// file appendable — the new entry must not have glued onto the torn
	// fragment.
	s3, path3 := newPersistServer(t, dir, Options{})
	defer s3.Stop()
	c3 := dialTest(t, path3, ClientOptions{})
	entries, err = c3.StreamRead("notes-repo1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("after repair+append, replayed %d entries, want 5", len(entries))
	}
	if entries[4].Seq != 5 || !bytes.Contains(entries[4].Data, []byte("99")) {
		t.Fatalf("last entry = %+v, want seq 5 with data n=99", entries[4])
	}
}

func TestStreamPersistSkipsCorruptMidFileLine(t *testing.T) {
	dir := filepath.Join(testsock.Dir(t), "streams")

	s1, path1 := newPersistServer(t, dir, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	for i := 1; i <= 5; i++ {
		if _, err := c1.StreamAppend("audit", map[string]int{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	c1.Close()
	s1.Stop()

	// Damage line 3 in place, keeping its newline framing intact.
	file := filepath.Join(dir, "audit.jsonl")
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(data), "\n")
	lines[2] = "this is not json\n"
	if err := os.WriteFile(file, []byte(strings.Join(lines, "")), 0o600); err != nil {
		t.Fatal(err)
	}

	s2, path2 := newPersistServer(t, dir, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	entries, err := c2.StreamRead("audit", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("replayed %d entries, want 4 (corrupt line skipped)", len(entries))
	}
	want := []uint64{1, 2, 4, 5}
	for i, e := range entries {
		if e.Seq != want[i] {
			t.Fatalf("entry %d has seq %d, want %d", i, e.Seq, want[i])
		}
	}
	// Numbering still resumes after the highest surviving seq.
	seq, err := c2.StreamAppend("audit", map[string]int{"n": 6})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 6 {
		t.Fatalf("seq = %d, want 6", seq)
	}
}

func TestStreamNoPersistWithoutDir(t *testing.T) {
	// With StreamDir unset nothing may touch the filesystem: a gating bug
	// would create <stream>.jsonl relative to the working directory.
	jsonlInCwd := func() int {
		matches, err := filepath.Glob("*.jsonl")
		if err != nil {
			t.Fatal(err)
		}
		return len(matches)
	}
	baseline := jsonlInCwd()

	s1, path1 := newTestServer(t, Options{})
	c1 := dialTest(t, path1, ClientOptions{})
	for i := 1; i <= 3; i++ {
		if _, err := c1.StreamAppend("audit", map[string]int{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	c1.Close()
	s1.Stop()

	if n := jsonlInCwd(); n != baseline {
		t.Fatalf("in-memory server created %d stream files", n-baseline)
	}

	// A fresh in-memory server has no history: P0 behavior unchanged.
	s2, path2 := newTestServer(t, Options{})
	defer s2.Stop()
	c2 := dialTest(t, path2, ClientOptions{})
	entries, err := c2.StreamRead("audit", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("in-memory restart replayed %d entries, want 0", len(entries))
	}
	seq, err := c2.StreamAppend("audit", map[string]int{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("fresh in-memory stream seq = %d, want 1", seq)
	}
}
