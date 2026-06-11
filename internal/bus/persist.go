package bus

// Stream persistence: when Options.StreamDir is set, every bounded stream is
// mirrored to <StreamDir>/<stream>.jsonl — one JSON line per StreamEntry —
// so the durable subjects (the P1 blackboard) survive a coordinator restart.
// Stream names are validStoreName-constrained ([A-Za-z0-9_-]{1,64}), so they
// are safe filenames by construction.
//
// Durability class: appends ride the page cache with no fsync, so an
// acknowledged entry survives a *process* crash (the kernel still holds the
// dirty page) but an OS crash or power loss may lose the tail. That stronger
// durability is deliberately out of scope for a local-first bus — the
// blackboard's job is surviving coordinator restarts, not machine failures.
// Crash tolerance is instead pushed to load time: a torn or corrupt line
// degrades (skip + warn), never fails startup.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const streamFileExt = ".jsonl"

// streamFile tracks one stream's on-disk JSONL mirror.
type streamFile struct {
	// f is the append handle, kept open across appends. nil until the first
	// append (lazy create for new streams) and after a compaction swaps the
	// file out from under it.
	f *os.File
	// lines counts physical lines in the file — valid and corrupt alike —
	// because corrupt lines occupy disk too and must count toward the
	// compaction trigger that eventually purges them.
	lines int
}

func (s *Server) streamFilePath(stream string) string {
	return filepath.Join(s.opts.StreamDir, stream+streamFileExt)
}

// loadStreams replays every persisted stream into memory. Called from Start
// before the socket is bound, so no append can race the load. Content
// corruption degrades per line; an unreadable directory or file is a real
// environment problem and fails startup honestly (never fake-success).
func (s *Server) loadStreams() error {
	if err := os.MkdirAll(s.opts.StreamDir, 0o700); err != nil {
		return fmt.Errorf("bus: create stream dir: %w", err)
	}
	entries, err := os.ReadDir(s.opts.StreamDir)
	if err != nil {
		return fmt.Errorf("bus: read stream dir: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		if strings.HasSuffix(name, ".tmp") {
			// Leftover compaction temp from a crash mid-compact. The rename
			// is the commit point, so the original file is still intact and
			// the temp is garbage.
			os.Remove(filepath.Join(s.opts.StreamDir, name))
			continue
		}
		stream, ok := strings.CutSuffix(name, streamFileExt)
		if !ok || !validStoreName(stream) {
			continue // not one of ours; never touch unknown files
		}
		if len(s.streams) >= maxStoreNames {
			s.opts.Logger.Warn("bus: stream file cap reached, ignoring remaining files", "dir", s.opts.StreamDir)
			break
		}
		if err := s.loadStream(stream); err != nil {
			return err
		}
	}
	return nil
}

// loadStream replays one <stream>.jsonl. The file is walked line by line so a
// single corrupt line — torn tail from a crash mid-append, or a damaged line
// mid-file — costs only that line, never the stream (degrade, don't throw).
// In memory only the last MaxStreamLen entries are retained, but seq
// numbering resumes after the highest seq found in the full file history.
func (s *Server) loadStream(stream string) error {
	path := s.streamFilePath(stream)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("bus: load stream %s: %w", stream, err)
	}

	st := &streamBuf{firstSeq: 1, nextSeq: 1}
	sf := &streamFile{}
	var maxSeq uint64
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

		var e StreamEntry
		if err := json.Unmarshal(line, &e); err != nil {
			if !terminated {
				// Torn final line: the process died mid-write. Truncate the
				// fragment away so the next append starts on a fresh line
				// instead of gluing onto it (which would corrupt that entry
				// too). Repair failure degrades — worst case one future
				// entry's line is corrupt and the next load skips it.
				s.opts.Logger.Warn("bus: stream file ends in torn line, truncating", "stream", stream, "offset", lineStart)
				if terr := os.Truncate(path, int64(lineStart)); terr != nil {
					s.opts.Logger.Warn("bus: truncate torn stream line failed", "stream", stream, "error", terr)
				}
				break
			}
			// Corrupt but newline-framed line mid-file: skip it. It stays on
			// disk (harmlessly re-skipped on every load) until the next
			// compaction rewrites the file without it.
			s.opts.Logger.Warn("bus: skipping corrupt stream entry", "stream", stream, "offset", lineStart)
			sf.lines++
			continue
		}
		if !terminated {
			// Valid JSON but the trailing newline was lost to a torn write.
			// Repair in place so the next append does not glue onto it.
			s.opts.Logger.Warn("bus: stream file missing final newline, repairing", "stream", stream)
			if f, ferr := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600); ferr != nil {
				s.opts.Logger.Warn("bus: newline repair failed", "stream", stream, "error", ferr)
			} else {
				if _, werr := f.Write([]byte{'\n'}); werr != nil {
					s.opts.Logger.Warn("bus: newline repair failed", "stream", stream, "error", werr)
				}
				f.Close()
			}
		}
		sf.lines++
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
		st.entries = append(st.entries, e)
		if len(st.entries) > s.opts.MaxStreamLen {
			st.entries = st.entries[1:]
		}
	}

	st.nextSeq = maxSeq + 1
	if len(st.entries) > 0 {
		st.firstSeq = st.entries[0].Seq
	} else {
		st.firstSeq = st.nextSeq
	}
	s.streams[stream] = st
	s.streamFiles[stream] = sf
	return nil
}

// persistAppend mirrors one appended entry to the stream's JSONL file. Called
// under s.mu — the same critical section as the in-memory append — so on-disk
// order always matches seq order. Persistence is best-effort: a disk error
// degrades to in-memory-only with a warning rather than failing the append,
// because the live process's memory remains authoritative and a flaky disk
// must not take the bus down with it.
func (s *Server) persistAppend(stream string, st *streamBuf, e StreamEntry) {
	sf := s.streamFiles[stream]
	if sf == nil {
		sf = &streamFile{}
		s.streamFiles[stream] = sf
	}
	if sf.f == nil {
		f, err := os.OpenFile(s.streamFilePath(stream), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
		if err != nil {
			s.opts.Logger.Warn("bus: open stream file failed, entry not persisted", "stream", stream, "error", err)
			return
		}
		sf.f = f
	}
	b, err := json.Marshal(e)
	if err != nil {
		s.opts.Logger.Warn("bus: marshal stream entry failed, not persisted", "stream", stream, "error", err)
		return
	}
	if _, err := sf.f.Write(append(b, '\n')); err != nil {
		// A partial write leaves a torn line mid-file; the loader's
		// corrupt-line skip absorbs that on the next start.
		s.opts.Logger.Warn("bus: stream entry not persisted", "stream", stream, "error", err)
		return
	}
	sf.lines++
	if sf.lines > 2*s.opts.MaxStreamLen {
		s.compactStreamLocked(stream, sf, st)
	}
}

// compactStreamLocked rewrites the file down to the retained in-memory window
// (the last MaxStreamLen entries). Disk retention may exceed memory retention
// between compactions but is bounded at 2*MaxStreamLen lines — bounded from
// day one, like the in-memory side (audit Avoid #8). The rewrite goes to a
// temp file in the same directory and commits with an atomic rename; a crash
// mid-compact leaves the original intact plus a stale *.tmp that the next
// load removes. Failure at any step degrades: the oversized original keeps
// working and the next append retries the compaction.
func (s *Server) compactStreamLocked(stream string, sf *streamFile, st *streamBuf) {
	tmp, err := os.CreateTemp(s.opts.StreamDir, stream+"-*.tmp")
	if err != nil {
		s.opts.Logger.Warn("bus: compact stream: create temp failed", "stream", stream, "error", err)
		return
	}
	w := bufio.NewWriter(tmp)
	ok := true
	for _, e := range st.entries {
		b, err := json.Marshal(e)
		if err != nil {
			ok = false
			break
		}
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		ok = false
	}
	if err := tmp.Close(); err != nil {
		ok = false
	}
	if !ok {
		os.Remove(tmp.Name())
		s.opts.Logger.Warn("bus: compact stream: write temp failed", "stream", stream)
		return
	}
	// Windows cannot replace a file while our append handle is still open.
	// Close before the rename everywhere; the next append lazily reopens the
	// compacted file.
	if sf.f != nil {
		if err := sf.f.Close(); err != nil {
			s.opts.Logger.Warn("bus: compact stream: close old file failed", "stream", stream, "error", err)
		}
		sf.f = nil
	}
	if err := os.Rename(tmp.Name(), s.streamFilePath(stream)); err != nil {
		os.Remove(tmp.Name())
		s.opts.Logger.Warn("bus: compact stream: rename failed", "stream", stream, "error", err)
		return
	}
	sf.lines = len(st.entries)
}

// closeStreamFiles closes every open stream file handle. Called from Stop
// after wg.Wait(), so no connection goroutine can race an append against the
// close; the lock is held anyway for the race detector's peace of mind.
func (s *Server) closeStreamFiles() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sf := range s.streamFiles {
		if sf.f != nil {
			sf.f.Close()
			sf.f = nil
		}
	}
}
