package bus

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// ClientOptions tune the bus client. Zero values take defaults.
type ClientOptions struct {
	DialTimeout    time.Duration // default 2s
	RequestTimeout time.Duration // default 5s
	ReconnectMin   time.Duration // default 50ms
	ReconnectMax   time.Duration // default 1s
	// OnReconnect runs (in its own goroutine) after the client re-dials and
	// re-establishes its subscriptions. Sidecars use it to re-register, which
	// is how the registry repopulates after a coordinator restart.
	OnReconnect func()
}

func (o ClientOptions) withDefaults() ClientOptions {
	if o.DialTimeout <= 0 {
		o.DialTimeout = 2 * time.Second
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 5 * time.Second
	}
	if o.ReconnectMin <= 0 {
		o.ReconnectMin = 50 * time.Millisecond
	}
	if o.ReconnectMax <= 0 {
		o.ReconnectMax = time.Second
	}
	return o
}

// Client is a bus connection. Safe for concurrent use. It reconnects with
// backoff after a dropped connection and re-establishes subscriptions; while
// disconnected, operations fail fast with ErrDisconnected.
type Client struct {
	path string
	opts ClientOptions

	mu      sync.Mutex
	conn    net.Conn
	closed  bool
	nextReq uint64
	pending map[string]chan frame
	subs    map[*Subscription]struct{}
	subIDs  map[string]*Subscription // server-assigned subID → subscription

	wmu     sync.Mutex // serializes frame writes onto the connection
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// Subscription is a live subject-pattern subscription.
type Subscription struct {
	c       *Client
	pattern string
	fn      func(envelope.Envelope)
	msgs    chan frame
	done    chan struct{}
	once    sync.Once
	dropped uint64 // messages dropped because the handler fell behind
}

// Dial connects to the bus server socket.
func Dial(socketPath string, opts ClientOptions) (*Client, error) {
	c := &Client{
		path:    socketPath,
		opts:    opts.withDefaults(),
		pending: make(map[string]chan frame),
		subs:    make(map[*Subscription]struct{}),
		subIDs:  make(map[string]*Subscription),
		closeCh: make(chan struct{}),
	}
	conn, err := net.DialTimeout("unix", socketPath, c.opts.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("bus: dial %s: %w", socketPath, err)
	}
	c.conn = conn
	c.wg.Add(1)
	go c.readLoop(conn)
	return c, nil
}

// Close shuts the client down. Pending requests fail with ErrClosed and all
// subscription goroutines exit.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.closeCh)
	conn := c.conn
	c.conn = nil
	c.failPendingLocked()
	subs := make([]*Subscription, 0, len(c.subs))
	for s := range c.subs {
		subs = append(subs, s)
	}
	c.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
	for _, s := range subs {
		s.stop()
	}
	c.wg.Wait()
}

// failPendingLocked aborts every in-flight request; callers see a closed
// channel and map it to ErrClosed/ErrDisconnected.
func (c *Client) failPendingLocked() {
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
}

func (c *Client) readLoop(conn net.Conn) {
	defer c.wg.Done()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var f frame
		if err := json.Unmarshal(line, &f); err != nil {
			continue // protocol garbage; drop the line, keep the conn
		}
		if f.Op == opMsg {
			c.dispatch(f)
			continue
		}
		if f.ID == "" {
			continue
		}
		c.mu.Lock()
		ch, ok := c.pending[f.ID]
		if ok {
			delete(c.pending, f.ID)
		}
		c.mu.Unlock()
		if ok {
			ch <- f // buffered(1); never blocks
		}
	}
	c.handleDisconnect(conn)
}

func (c *Client) dispatch(f frame) {
	c.mu.Lock()
	sub := c.subIDs[f.Sub]
	c.mu.Unlock()
	if sub == nil {
		return
	}
	select {
	case sub.msgs <- f:
	default:
		c.mu.Lock()
		sub.dropped++
		c.mu.Unlock()
	}
}

func (c *Client) handleDisconnect(conn net.Conn) {
	c.mu.Lock()
	if c.closed || c.conn != conn {
		c.mu.Unlock()
		return
	}
	c.conn = nil
	c.failPendingLocked()
	c.mu.Unlock()

	c.wg.Add(1)
	go c.reconnectLoop()
}

func (c *Client) reconnectLoop() {
	defer c.wg.Done()
	backoff := c.opts.ReconnectMin
	sleep := func() bool { // false = client closed
		select {
		case <-c.closeCh:
			return false
		case <-time.After(backoff):
			backoff *= 2
			if backoff > c.opts.ReconnectMax {
				backoff = c.opts.ReconnectMax
			}
			return true
		}
	}
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		conn, err := net.DialTimeout("unix", c.path, c.opts.DialTimeout)
		if err != nil {
			if !sleep() {
				return
			}
			continue
		}

		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			conn.Close()
			return
		}
		c.conn = conn
		c.subIDs = make(map[string]*Subscription)
		subs := make([]*Subscription, 0, len(c.subs))
		for s := range c.subs {
			subs = append(subs, s)
		}
		c.mu.Unlock()

		c.wg.Add(1)
		go c.readLoop(conn)

		ok := true
		for _, s := range subs {
			if err := c.sendSub(s); err != nil {
				ok = false
				break
			}
		}
		if !ok {
			// Subscriptions could not be re-established. Detach the conn
			// first so its readLoop's handleDisconnect no-ops instead of
			// spawning a second reconnect loop, then retry from here.
			c.mu.Lock()
			if c.conn == conn {
				c.conn = nil
				c.failPendingLocked()
			}
			c.mu.Unlock()
			conn.Close()
			if !sleep() {
				return
			}
			continue
		}
		if c.opts.OnReconnect != nil {
			go c.opts.OnReconnect()
		}
		return
	}
}

// request sends one frame and waits for its correlated response.
func (c *Client) request(f frame) (frame, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return frame{}, ErrClosed
	}
	conn := c.conn
	if conn == nil {
		c.mu.Unlock()
		return frame{}, ErrDisconnected
	}
	c.nextReq++
	f.V = protocolVersion
	f.ID = fmt.Sprintf("r%d", c.nextReq)
	ch := make(chan frame, 1)
	c.pending[f.ID] = ch
	c.mu.Unlock()

	b, err := json.Marshal(f)
	if err != nil {
		c.dropPending(f.ID)
		return frame{}, fmt.Errorf("bus: marshal request: %w", err)
	}
	c.wmu.Lock()
	_, err = conn.Write(append(b, '\n'))
	c.wmu.Unlock()
	if err != nil {
		c.dropPending(f.ID)
		return frame{}, ErrDisconnected
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return frame{}, ErrDisconnected
		}
		if resp.Err != nil {
			return frame{}, errorFromFrame(resp.Err)
		}
		return resp, nil
	case <-time.After(c.opts.RequestTimeout):
		c.dropPending(f.ID)
		return frame{}, fmt.Errorf("bus: request %s timed out after %s", f.Op, c.opts.RequestTimeout)
	}
}

func (c *Client) dropPending(id string) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

// Ping round-trips the connection.
func (c *Client) Ping() error {
	_, err := c.request(frame{Op: opPing})
	return err
}

// Publish validates and publishes an envelope to its subject. This is the
// single publish edge: an envelope that fails validation never reaches the
// wire.
func (c *Client) Publish(env envelope.Envelope) error {
	data, err := envelope.Encode(env)
	if err != nil {
		return err
	}
	_, err = c.request(frame{Op: opPub, Subject: env.Subject, Data: data})
	return err
}

// Subscribe registers fn for every envelope published to a matching subject.
// fn runs on a single goroutine per subscription (ordered). Envelopes that
// fail decode are dropped — a malformed peer message must never crash a
// consumer (degrade-don't-throw at the boundary).
func (c *Client) Subscribe(pattern string, fn func(envelope.Envelope)) (*Subscription, error) {
	sub := &Subscription{
		c:       c,
		pattern: pattern,
		fn:      fn,
		msgs:    make(chan frame, 256),
		done:    make(chan struct{}),
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	c.subs[sub] = struct{}{}
	c.mu.Unlock()

	if err := c.sendSub(sub); err != nil {
		c.mu.Lock()
		delete(c.subs, sub)
		c.mu.Unlock()
		return nil, err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-sub.done:
				return
			case f := <-sub.msgs:
				env, err := envelope.Decode(f.Data)
				if err != nil {
					continue
				}
				sub.fn(env)
			}
		}
	}()
	return sub, nil
}

// sendSub registers sub with the server and records the server-assigned id.
func (c *Client) sendSub(sub *Subscription) error {
	resp, err := c.request(frame{Op: opSub, Pattern: sub.pattern})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.subIDs[resp.Sub] = sub
	c.mu.Unlock()
	return nil
}

// Unsubscribe stops the subscription. Best-effort on the wire; local
// resources are always released.
func (s *Subscription) Unsubscribe() {
	c := s.c
	c.mu.Lock()
	delete(c.subs, s)
	var serverID string
	for id, sub := range c.subIDs {
		if sub == s {
			serverID = id
			delete(c.subIDs, id)
			break
		}
	}
	c.mu.Unlock()

	if serverID != "" {
		c.request(frame{Op: opUnsub, Sub: serverID}) //nolint:errcheck // best-effort
	}
	s.stop()
}

func (s *Subscription) stop() {
	s.once.Do(func() { close(s.done) })
}

// --- KV ----------------------------------------------------------------------

// PutOptions control KV writes.
type PutOptions struct {
	// CAS: nil = unconditional put; pointer to 0 = create-only (fails with
	// ErrCASLost if the key exists); pointer to rev>0 = succeed only if the
	// current revision matches.
	CAS *uint64
	// TTL: if >0 the key expires unless re-put before the deadline (lease).
	TTL time.Duration
}

// CreateOnly is a convenience CAS value for single-winner claims.
func CreateOnly() *uint64 { r := uint64(0); return &r }

// Rev is a convenience CAS value for revision-guarded updates.
func Rev(r uint64) *uint64 { return &r }

// KVPut writes a value. Returns the new revision. ErrCASLost means another
// writer legitimately won (map to ClaimLost, do not retry); any other error
// is transport/store failure (map to ClaimError, retry is reasonable).
func (c *Client) KVPut(bucket, key string, value any, opts PutOptions) (uint64, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return 0, fmt.Errorf("bus: marshal value: %w", err)
	}
	f := frame{Op: opKVPut, Bucket: bucket, Key: key, Value: raw, Rev: opts.CAS}
	if opts.TTL > 0 {
		f.TTLMs = opts.TTL.Milliseconds()
	}
	resp, err := c.request(f)
	if err != nil {
		return 0, err
	}
	return resp.NewRev, nil
}

// KVGet reads a key. found=false (with nil error) means absent or expired.
func (c *Client) KVGet(bucket, key string) (KVValue, bool, error) {
	resp, err := c.request(frame{Op: opKVGet, Bucket: bucket, Key: key})
	if err != nil {
		return KVValue{}, false, err
	}
	if !resp.Found {
		return KVValue{}, false, nil
	}
	return KVValue{Value: resp.Value, Rev: resp.NewRev}, true, nil
}

// KVDelete removes a key (idempotent).
func (c *Client) KVDelete(bucket, key string) error {
	_, err := c.request(frame{Op: opKVDelete, Bucket: bucket, Key: key})
	return err
}

// KVList returns all live (non-expired) keys in a bucket.
func (c *Client) KVList(bucket string) (map[string]KVValue, error) {
	resp, err := c.request(frame{Op: opKVList, Bucket: bucket})
	if err != nil {
		return nil, err
	}
	if resp.Keys == nil {
		return map[string]KVValue{}, nil
	}
	return resp.Keys, nil
}

// --- streams -------------------------------------------------------------------

// StreamAppend appends a record to a bounded stream and returns its sequence.
func (c *Client) StreamAppend(stream string, data any) (uint64, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("bus: marshal stream data: %w", err)
	}
	resp, err := c.request(frame{Op: opStreamAppend, Stream: stream, Data: raw})
	if err != nil {
		return 0, err
	}
	return resp.Seq, nil
}

// StreamRead returns retained entries with seq >= from (0 = all retained).
func (c *Client) StreamRead(stream string, from uint64) ([]StreamEntry, error) {
	resp, err := c.request(frame{Op: opStreamRead, Stream: stream, From: from})
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}
