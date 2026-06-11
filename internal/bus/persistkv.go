package bus

// KV bucket persistence: when Options.PersistBuckets names a bucket, every
// put/delete on it is mirrored to <PersistDir>/bucket-<name>.jsonl as an
// append-only op log — one JSON line per mutation — so that bucket's records
// survive a coordinator restart.
//
// This is the durability substrate for the autonomous work hierarchy (#65):
// jobs (#23) and tasks (#24) are authoritative KV records with no live owner
// to re-establish them, unlike the registry (sidecars re-register) and claims
// (holders re-establish). Persisting their buckets makes a coordinator restart
// non-destructive so the #25 scheduler can rely on the DAG.
//
// Deliberately NOT persisted: registry and claims. Those are TTL leases that
// self-heal by re-registration / re-establishment (DECISIONS.md: "Every claim
// and presence record is a TTL lease with reclaim-on-death"); mirroring them
// would create a second, staler source of truth for a fact a live owner
// already re-asserts. One authority per fact.
//
// Design mirrors persist.go (the blackboard JSONL precedent, DECISIONS.md
// 2026-06-05): the in-memory bucket stays the one authority; the file is its
// durable mirror, loaded on Start *before* the socket binds so no client can
// mutate a bucket while its history is still replaying.
//
// Durability class matches streams: appends ride the page cache with no fsync,
// so an acknowledged mutation survives a *process* crash (the kernel holds the
// dirty page) but an OS crash or power loss may lose the tail. That is
// deliberately out of scope for a local-first bus. Corruption is pushed to
// load time: a torn or corrupt op-log line degrades (skip/repair + warn),
// never fails startup.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const bucketFilePrefix = "bucket-"

// bucketOp is one persisted KV mutation. A "put" carries the full stored value
// plus its revision and (zero-unless-leased) expiry; a "del" carries only the
// key. Replaying the ops in file order rebuilds the live bucket.
type bucketOp struct {
	Op        string          `json:"op"` // "put" | "del"
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value,omitempty"`
	Rev       uint64          `json:"rev,omitempty"`
	ExpiresAt time.Time       `json:"expiresAt,omitempty"` // zero = no TTL
}

// bucketFile tracks one bucket's on-disk op-log mirror.
type bucketFile struct {
	// f is the append handle, kept open across mutations. nil until the first
	// persisted op (lazy create) and after a compaction swaps the file out.
	f *os.File
	// lines counts physical lines in the file — valid and corrupt alike —
	// because corrupt lines occupy disk too and must count toward the
	// compaction trigger that eventually purges them.
	lines int
}

func (s *Server) bucketFilePath(bucket string) string {
	return filepath.Join(s.opts.PersistDir, bucketFilePrefix+bucket+streamFileExt)
}

// persisted reports whether a bucket is configured for durability. The set is
// tiny (jobs, tasks), so a linear scan is cheaper than a map and keeps the
// Options shape a plain slice.
func (s *Server) persisted(bucket string) bool {
	if s.opts.PersistDir == "" {
		return false
	}
	for _, b := range s.opts.PersistBuckets {
		if b == bucket {
			return true
		}
	}
	return false
}

// loadBuckets replays every persisted bucket into memory. Called from Start
// before the socket is bound, so no mutation can race the load. Content
// corruption degrades per line; an unreadable directory is a real environment
// problem and fails startup honestly (never fake-success).
func (s *Server) loadBuckets() error {
	if len(s.opts.PersistBuckets) == 0 {
		return nil
	}
	if err := os.MkdirAll(s.opts.PersistDir, 0o700); err != nil {
		return fmt.Errorf("bus: create persist dir: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, bucket := range s.opts.PersistBuckets {
		if !validStoreName(bucket) {
			return fmt.Errorf("bus: invalid persist bucket name %q", bucket)
		}
		if err := s.loadBucket(bucket); err != nil {
			return err
		}
	}
	return nil
}

// loadBucket replays one bucket-<name>.jsonl op log into a fresh kvBucket. The
// file is walked line by line so a single corrupt line — torn tail from a
// crash mid-append, or a damaged line mid-file — costs only that op, never the
// bucket (degrade, don't throw). A key whose lease already expired at load
// time is dropped, so a restart never resurrects a dead lease (a no-op for the
// untimed job/task records, correct for any future leased persisted bucket).
func (s *Server) loadBucket(bucket string) error {
	path := s.bucketFilePath(bucket)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // never written; nothing to replay
		}
		return fmt.Errorf("bus: load bucket %s: %w", bucket, err)
	}

	now := time.Now()
	b := &kvBucket{entries: make(map[string]*kvEntry)}
	bf := &bucketFile{}
	var maxRev uint64
	off := 0
	for off < len(data) {
		lineStart := off
		nl := bytes.IndexByte(data[off:], '\n')
		terminated := nl >= 0
		var line []byte
		if terminated {
			line = data[off : off+nl]
			off += nl + 1
		} else {
			line = data[off:]
			off = len(data)
		}

		var op bucketOp
		if err := json.Unmarshal(line, &op); err != nil {
			if !terminated {
				// Torn final line: the process died mid-write. Truncate the
				// fragment so the next append starts on a fresh line instead of
				// gluing onto it. Repair failure degrades.
				s.opts.Logger.Warn("bus: bucket file ends in torn line, truncating", "bucket", bucket, "offset", lineStart)
				if terr := os.Truncate(path, int64(lineStart)); terr != nil {
					s.opts.Logger.Warn("bus: truncate torn bucket line failed", "bucket", bucket, "error", terr)
				}
				break
			}
			s.opts.Logger.Warn("bus: skipping corrupt bucket op", "bucket", bucket, "offset", lineStart)
			bf.lines++
			continue
		}
		if !terminated {
			// Valid JSON but the trailing newline was lost to a torn write.
			// Repair in place so the next append does not glue onto it.
			s.opts.Logger.Warn("bus: bucket file missing final newline, repairing", "bucket", bucket)
			if f, ferr := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600); ferr != nil {
				s.opts.Logger.Warn("bus: bucket newline repair failed", "bucket", bucket, "error", ferr)
			} else {
				if _, werr := f.Write([]byte{'\n'}); werr != nil {
					s.opts.Logger.Warn("bus: bucket newline repair failed", "bucket", bucket, "error", werr)
				}
				f.Close()
			}
		}
		bf.lines++
		if op.Rev > maxRev {
			maxRev = op.Rev
		}
		switch op.Op {
		case "put":
			if op.Key == "" {
				continue
			}
			b.entries[op.Key] = &kvEntry{value: op.Value, rev: op.Rev, expiresAt: op.ExpiresAt}
		case "del":
			delete(b.entries, op.Key)
		default:
			s.opts.Logger.Warn("bus: unknown bucket op, skipping", "bucket", bucket, "op", op.Op)
		}
	}

	// Drop any lease that expired while the coordinator was down: a restart
	// must not resurrect a dead key. Untimed records (jobs/tasks) keep a zero
	// expiresAt and are always retained.
	for k, e := range b.entries {
		if e.expired(now) {
			delete(b.entries, k)
		}
	}

	// Resume the revision counter past the highest rev ever seen, so a
	// post-restart CAS can never collide with a pre-restart revision.
	b.seq = maxRev
	s.kv[bucket] = b
	s.bucketFiles[bucket] = bf
	return nil
}

// persistPut mirrors one applied put to the bucket's op log. Called under s.mu
// — the same critical section as the in-memory mutation — so on-disk order
// always matches apply order. Best-effort: a disk error degrades to
// in-memory-only with a warning rather than failing the put, because the live
// process's memory stays authoritative and a flaky disk must not take the bus
// down with it.
func (s *Server) persistPut(bucket, key string, e *kvEntry) {
	s.appendBucketOp(bucket, bucketOp{Op: "put", Key: key, Value: e.value, Rev: e.rev, ExpiresAt: e.expiresAt})
}

// persistDelete mirrors one applied delete to the bucket's op log.
func (s *Server) persistDelete(bucket, key string) {
	s.appendBucketOp(bucket, bucketOp{Op: "del", Key: key})
}

func (s *Server) appendBucketOp(bucket string, op bucketOp) {
	bf := s.bucketFiles[bucket]
	if bf == nil {
		bf = &bucketFile{}
		s.bucketFiles[bucket] = bf
	}
	if bf.f == nil {
		f, err := os.OpenFile(s.bucketFilePath(bucket), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
		if err != nil {
			s.opts.Logger.Warn("bus: open bucket file failed, op not persisted", "bucket", bucket, "error", err)
			return
		}
		bf.f = f
	}
	line, err := json.Marshal(op)
	if err != nil {
		s.opts.Logger.Warn("bus: marshal bucket op failed, not persisted", "bucket", bucket, "error", err)
		return
	}
	if _, err := bf.f.Write(append(line, '\n')); err != nil {
		// A partial write leaves a torn line; the loader's corrupt-line skip
		// absorbs that on the next start.
		s.opts.Logger.Warn("bus: bucket op not persisted", "bucket", bucket, "error", err)
		return
	}
	bf.lines++
	if b := s.kv[bucket]; b != nil && bf.lines > compactBucketThreshold(len(b.entries)) {
		s.compactBucketLocked(bucket, bf, b)
	}
}

// compactBucketThreshold bounds the op log relative to the live key count. A
// floor keeps a tiny bucket from compacting on every other op (churn), while
// the 2x factor mirrors the stream side: disk retention may exceed the live
// set between compactions but stays bounded.
func compactBucketThreshold(liveKeys int) int {
	const floor = 64
	if t := 2 * liveKeys; t > floor {
		return t
	}
	return floor
}

// compactBucketLocked rewrites the op log down to a snapshot of the live
// bucket (one put per surviving key, no deletes). Like the stream compactor it
// writes a same-dir temp and commits with an atomic rename; a crash
// mid-compact leaves the original intact plus a stale *.tmp the next load
// removes. Failure at any step degrades: the oversized original keeps working
// and the next op retries compaction.
func (s *Server) compactBucketLocked(bucket string, bf *bucketFile, b *kvBucket) {
	tmp, err := os.CreateTemp(s.opts.PersistDir, bucketFilePrefix+bucket+"-*.tmp")
	if err != nil {
		s.opts.Logger.Warn("bus: compact bucket: create temp failed", "bucket", bucket, "error", err)
		return
	}
	w := bufio.NewWriter(tmp)
	ok := true
	written := 0
	for key, e := range b.entries {
		line, err := json.Marshal(bucketOp{Op: "put", Key: key, Value: e.value, Rev: e.rev, ExpiresAt: e.expiresAt})
		if err != nil {
			ok = false
			break
		}
		w.Write(line)
		w.WriteByte('\n')
		written++
	}
	if err := w.Flush(); err != nil {
		ok = false
	}
	if err := tmp.Close(); err != nil {
		ok = false
	}
	if !ok {
		os.Remove(tmp.Name())
		s.opts.Logger.Warn("bus: compact bucket: write temp failed", "bucket", bucket)
		return
	}
	if err := os.Rename(tmp.Name(), s.bucketFilePath(bucket)); err != nil {
		os.Remove(tmp.Name())
		s.opts.Logger.Warn("bus: compact bucket: rename failed", "bucket", bucket, "error", err)
		return
	}
	// The open handle now points at the unlinked old inode; close it and let
	// the next op lazily reopen the compacted file.
	if bf.f != nil {
		bf.f.Close()
		bf.f = nil
	}
	bf.lines = written
}

// loadBucketTmpCleanup removes leftover compaction temps for persisted buckets.
// Called from loadBuckets' caller path indirectly; kept separate so a crash
// mid-compact never leaves *.tmp garbage accumulating. The rename is the commit
// point, so any *.tmp is discardable.
func (s *Server) cleanBucketTemps() {
	if s.opts.PersistDir == "" {
		return
	}
	entries, err := os.ReadDir(s.opts.PersistDir)
	if err != nil {
		return
	}
	for _, de := range entries {
		name := de.Name()
		if strings.HasPrefix(name, bucketFilePrefix) && strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(s.opts.PersistDir, name))
		}
	}
}

// closeBucketFiles closes every open bucket file handle. Called from Stop after
// wg.Wait(), mirroring closeStreamFiles.
func (s *Server) closeBucketFiles() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, bf := range s.bucketFiles {
		if bf.f != nil {
			bf.f.Close()
			bf.f = nil
		}
	}
}
