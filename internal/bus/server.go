// Package bus is the local bus/store spine for P0: pub/sub with NATS-style
// subjects, KV buckets with revision-CAS and TTL leases, and bounded streams.
//
// Topology is a star: the coordinator embeds one Server on a permissioned
// unix socket; sidecars, the dashboard, and tests connect with Client. This
// deliberately defers a full NATS/JetStream broker until lateral peer comms
// or multi-host genuinely require it (DECISIONS.md: "Transport reopened").
// The spine invariants survive a future transport swap: one versioned
// envelope on the wire, one authority per fact (a KV record), typed results.
package bus

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Options tune the server. Zero values take defaults.
type Options struct {
	// MaxStreamLen bounds every stream's retained entries (default 1000).
	// Retention is bounded from day one (audit Avoid #8).
	MaxStreamLen int
	// JanitorInterval is how often expired KV keys are purged (default
	// 100ms). Reads also lazily treat expired keys as absent, so TTL
	// correctness does not depend on janitor timing.
	JanitorInterval time.Duration
	// OutboundDepth bounds each connection's outbound push queue (default
	// 256). A consumer that falls this far behind is disconnected rather
	// than buffered in unbounded RAM (audit Avoid #8).
	OutboundDepth int
}

func (o Options) withDefaults() Options {
	if o.MaxStreamLen <= 0 {
		o.MaxStreamLen = 1000
	}
	if o.JanitorInterval <= 0 {
		o.JanitorInterval = 100 * time.Millisecond
	}
	if o.OutboundDepth <= 0 {
		o.OutboundDepth = 256
	}
	return o
}

// Server is the bus/store. One per mesh, embedded in the coordinator.
type Server struct {
	path string
	opts Options

	mu      sync.Mutex
	ln      net.Listener
	closed  bool
	kv      map[string]*kvBucket
	streams map[string]*streamBuf
	conns   map[*serverConn]struct{}

	janitorDone chan struct{}
	wg          sync.WaitGroup
}

type kvBucket struct {
	seq     uint64 // bucket-wide monotonic revision counter
	entries map[string]*kvEntry
}

type kvEntry struct {
	value     json.RawMessage
	rev       uint64
	expiresAt time.Time // zero = no TTL
}

func (e *kvEntry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

type streamBuf struct {
	firstSeq uint64
	nextSeq  uint64
	entries  []StreamEntry
}

// NewServer creates a bus server that will listen on the given unix socket.
func NewServer(socketPath string, opts Options) *Server {
	return &Server{
		path:        socketPath,
		opts:        opts.withDefaults(),
		kv:          make(map[string]*kvBucket),
		streams:     make(map[string]*streamBuf),
		conns:       make(map[*serverConn]struct{}),
		janitorDone: make(chan struct{}),
	}
}

// Start binds the unix socket (owner-only permissions, no TCP surface) and
// begins serving. It returns once listening; Stop shuts down deterministically.
func (s *Server) Start() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("bus: create socket dir: %w", err)
	}
	ln, err := listenUnix(s.path)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return ErrClosed
	}
	s.ln = ln
	s.mu.Unlock()

	s.wg.Add(2)
	go s.acceptLoop(ln)
	go s.janitor()
	return nil
}

// listenUnix binds path, recovering a stale socket file left by a dead
// process: if nothing accepts a connection there, the file is removed.
func listenUnix(path string) (net.Listener, error) {
	if _, err := os.Stat(path); err == nil {
		conn, err := net.DialTimeout("unix", path, 250*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil, fmt.Errorf("bus: socket %s already in use", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("bus: remove stale socket %s: %w", path, err)
		}
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("bus: listen %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, fmt.Errorf("bus: chmod socket: %w", err)
	}
	return ln, nil
}

// Stop closes the listener and every connection, then waits for all server
// goroutines to exit. Safe to call once; deterministic for tests.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	ln := s.ln
	conns := make([]*serverConn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	close(s.janitorDone)
	if ln != nil {
		ln.Close()
	}
	for _, c := range conns {
		c.close()
	}
	s.wg.Wait()
	os.Remove(s.path)
}

func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed
		}
		sc := &serverConn{
			s:    s,
			c:    conn,
			out:  make(chan []byte, s.opts.OutboundDepth),
			done: make(chan struct{}),
			subs: make(map[string]string),
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.conns[sc] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(2)
		go sc.readLoop()
		go sc.writeLoop()
	}
}

func (s *Server) janitor() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.opts.JanitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.janitorDone:
			return
		case now := <-ticker.C:
			s.mu.Lock()
			for _, b := range s.kv {
				for key, e := range b.entries {
					if e.expired(now) {
						delete(b.entries, key)
					}
				}
			}
			s.mu.Unlock()
		}
	}
}

// --- per-connection handling -------------------------------------------------

type serverConn struct {
	s    *Server
	c    net.Conn
	out  chan []byte
	done chan struct{}

	closeOnce sync.Once

	mu      sync.Mutex
	subs    map[string]string // subID → pattern
	nextSub uint64
}

func (sc *serverConn) close() {
	sc.closeOnce.Do(func() {
		close(sc.done)
		sc.c.Close()
		sc.s.mu.Lock()
		delete(sc.s.conns, sc)
		sc.s.mu.Unlock()
	})
}

func (sc *serverConn) readLoop() {
	defer sc.s.wg.Done()
	defer sc.close()

	scanner := bufio.NewScanner(sc.c)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var f frame
		if err := json.Unmarshal(line, &f); err != nil {
			sc.respond(frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: "unparseable frame"}})
			continue
		}
		sc.respond(sc.handle(f))
	}
}

func (sc *serverConn) writeLoop() {
	defer sc.s.wg.Done()
	for {
		select {
		case <-sc.done:
			return
		case b := <-sc.out:
			if _, err := sc.c.Write(append(b, '\n')); err != nil {
				sc.close()
				return
			}
		}
	}
}

// respond queues a frame; if the consumer is too slow, it is disconnected
// (bounded queue + honest backpressure, never unbounded buffering).
func (sc *serverConn) respond(f frame) {
	b, err := json.Marshal(f)
	if err != nil {
		return
	}
	select {
	case sc.out <- b:
	default:
		sc.close()
	}
}

func (sc *serverConn) handle(f frame) frame {
	if f.V != protocolVersion {
		return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: fmt.Sprintf("protocol version %d", f.V)}}
	}
	switch f.Op {
	case opPing:
		return frame{ID: f.ID, OK: true}
	case opPub:
		return sc.handlePub(f)
	case opSub:
		return sc.handleSub(f)
	case opUnsub:
		return sc.handleUnsub(f)
	case opKVGet, opKVPut, opKVDelete, opKVList:
		return sc.s.handleKV(f)
	case opStreamAppend, opStreamRead:
		return sc.s.handleStream(f)
	default:
		return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: fmt.Sprintf("unknown op %q", f.Op)}}
	}
}

func (sc *serverConn) handlePub(f frame) frame {
	if !ValidSubject(f.Subject) {
		return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: fmt.Sprintf("invalid subject %q", f.Subject)}}
	}
	s := sc.s
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.mu.Lock()
		for subID, pattern := range c.subs {
			if Match(pattern, f.Subject) {
				push, err := json.Marshal(frame{Op: opMsg, Sub: subID, Subject: f.Subject, Data: f.Data})
				if err != nil {
					continue
				}
				select {
				case c.out <- push:
				default:
					// Slow consumer: close it after releasing its lock.
					defer c.close()
				}
			}
		}
		c.mu.Unlock()
	}
	return frame{ID: f.ID, OK: true}
}

func (sc *serverConn) handleSub(f frame) frame {
	if !ValidPattern(f.Pattern) {
		return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: fmt.Sprintf("invalid pattern %q", f.Pattern)}}
	}
	sc.mu.Lock()
	sc.nextSub++
	subID := fmt.Sprintf("s%d", sc.nextSub)
	sc.subs[subID] = f.Pattern
	sc.mu.Unlock()
	return frame{ID: f.ID, OK: true, Sub: subID}
}

func (sc *serverConn) handleUnsub(f frame) frame {
	sc.mu.Lock()
	delete(sc.subs, f.Sub)
	sc.mu.Unlock()
	return frame{ID: f.ID, OK: true}
}

// --- KV ----------------------------------------------------------------------

func (s *Server) handleKV(f frame) frame {
	if !validStoreName(f.Bucket) {
		return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: fmt.Sprintf("invalid bucket %q", f.Bucket)}}
	}
	if f.Op != opKVList && f.Key == "" {
		return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: "missing key"}}
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.kv[f.Bucket]
	if b == nil {
		// Buckets are created on first use, but the count of distinct names
		// is capped — lazily creating one per arbitrary peer-supplied name
		// would be an unbounded-memory vector.
		if len(s.kv) >= maxStoreNames {
			return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: "too many buckets"}}
		}
		b = &kvBucket{entries: make(map[string]*kvEntry)}
		s.kv[f.Bucket] = b
	}

	switch f.Op {
	case opKVGet:
		e, ok := b.entries[f.Key]
		if !ok || e.expired(now) {
			return frame{ID: f.ID, OK: true, Found: false}
		}
		return frame{ID: f.ID, OK: true, Found: true, Value: e.value, NewRev: e.rev}

	case opKVPut:
		cur, exists := b.entries[f.Key]
		if exists && cur.expired(now) {
			delete(b.entries, f.Key)
			exists = false
		}
		// CAS semantics: nil = unconditional, 0 = create-only, >0 = revision match.
		switch {
		case f.Rev == nil:
		case *f.Rev == 0:
			if exists {
				return frame{ID: f.ID, Err: &frameError{Code: errCodeCASLost, Message: "key exists"}}
			}
		default:
			if !exists || cur.rev != *f.Rev {
				return frame{ID: f.ID, Err: &frameError{Code: errCodeCASLost, Message: "revision mismatch"}}
			}
		}
		b.seq++
		e := &kvEntry{value: f.Value, rev: b.seq}
		if f.TTLMs > 0 {
			e.expiresAt = now.Add(time.Duration(f.TTLMs) * time.Millisecond)
		}
		b.entries[f.Key] = e
		return frame{ID: f.ID, OK: true, NewRev: e.rev}

	case opKVDelete:
		delete(b.entries, f.Key) // idempotent
		return frame{ID: f.ID, OK: true}

	case opKVList:
		keys := make(map[string]KVValue, len(b.entries))
		for k, e := range b.entries {
			if e.expired(now) {
				continue
			}
			keys[k] = KVValue{Value: e.value, Rev: e.rev}
		}
		return frame{ID: f.ID, OK: true, Keys: keys}
	}
	return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: "unreachable"}}
}

// --- streams -------------------------------------------------------------------

func (s *Server) handleStream(f frame) frame {
	if !validStoreName(f.Stream) {
		return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: fmt.Sprintf("invalid stream %q", f.Stream)}}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	st := s.streams[f.Stream]
	if st == nil {
		if len(s.streams) >= maxStoreNames {
			return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: "too many streams"}}
		}
		st = &streamBuf{firstSeq: 1, nextSeq: 1}
		s.streams[f.Stream] = st
	}

	switch f.Op {
	case opStreamAppend:
		entry := StreamEntry{Seq: st.nextSeq, TS: time.Now().UTC(), Data: f.Data}
		st.entries = append(st.entries, entry)
		st.nextSeq++
		if over := len(st.entries) - s.opts.MaxStreamLen; over > 0 {
			// Slide in place: reuses the backing array instead of
			// reallocating ~MaxStreamLen entries on every append at capacity.
			n := copy(st.entries, st.entries[over:])
			st.entries = st.entries[:n]
			st.firstSeq = st.entries[0].Seq
		}
		return frame{ID: f.ID, OK: true, Seq: entry.Seq}

	case opStreamRead:
		from := f.From
		if from < st.firstSeq {
			from = st.firstSeq
		}
		var out []StreamEntry
		for _, e := range st.entries {
			if e.Seq >= from {
				out = append(out, e)
			}
		}
		return frame{ID: f.ID, OK: true, Entries: out}
	}
	return frame{ID: f.ID, Err: &frameError{Code: errCodeBadRequest, Message: "unreachable"}}
}
