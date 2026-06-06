// Package dashboard is the read-only observer: it taps mesh.> on the bus,
// snapshots the registry, and bridges both to a browser.
//
// P0 deviation from issue #10: the browser bridge is SSE (EventSource), not
// WebSocket — same observer semantics, zero dependencies, native in every
// browser. P4 (the production dashboard) can revisit.
//
// The dashboard never publishes to the mesh: its bus client only subscribes
// and reads KV. Disconnecting it affects nothing.
package dashboard

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/claim"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

//go:embed dashboard.html
var indexHTML []byte

// rosterInterval is how often the registry snapshot is refreshed and pushed.
const rosterInterval = 1 * time.Second

// Dashboard serves the live observer UI.
type Dashboard struct {
	cfg  config.Config
	addr string
	log  *slog.Logger

	bus     *bus.Client
	httpSrv *http.Server
	ln      net.Listener

	mu      sync.Mutex
	clients map[chan []byte]struct{}
	roster  []agentcard.RegistryRecord
	claims  []claim.Held
	// noteSeq tracks the last replayed seq per notes stream, so each tick
	// broadcasts only new blackboard entries (replay and live are one path).
	noteSeq map[string]uint64

	stop chan struct{}
	wg   sync.WaitGroup
}

// New creates a dashboard listening on addr (default cfg.DashboardAddr).
func New(cfg config.Config, addr string, log *slog.Logger) *Dashboard {
	if addr == "" {
		addr = cfg.DashboardAddr
	}
	if log == nil {
		log = slog.Default()
	}
	return &Dashboard{
		cfg:     cfg,
		addr:    addr,
		log:     log,
		clients: make(map[chan []byte]struct{}),
		noteSeq: make(map[string]uint64),
		stop:    make(chan struct{}),
	}
}

// Start connects the tap and begins serving HTTP.
func (d *Dashboard) Start() error {
	cli, err := bus.Dial(d.cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		return fmt.Errorf("dashboard: connect bus: %w", err)
	}
	d.bus = cli

	// Tap everything; forward each envelope to connected browsers.
	if _, err := cli.Subscribe(envelope.PatternAll, d.onEvent); err != nil {
		cli.Close()
		return fmt.Errorf("dashboard: subscribe: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", d.serveIndex)
	mux.HandleFunc("GET /events", d.serveSSE)
	mux.HandleFunc("GET /api/roster", d.serveRoster)
	mux.HandleFunc("GET /api/claims", d.serveClaims)
	mux.HandleFunc("GET /api/notes", d.serveNotes)

	ln, err := net.Listen("tcp", d.addr)
	if err != nil {
		cli.Close()
		return fmt.Errorf("dashboard: listen %s: %w", d.addr, err)
	}
	d.ln = ln
	d.httpSrv = &http.Server{Handler: mux}

	d.wg.Add(2)
	go func() {
		defer d.wg.Done()
		d.httpSrv.Serve(ln) //nolint:errcheck // closed on Stop
	}()
	go d.rosterLoop()

	d.log.Info("dashboard started", "addr", d.Addr())
	return nil
}

// Addr returns the bound listen address (useful when addr was :0).
func (d *Dashboard) Addr() string {
	if d.ln == nil {
		return d.addr
	}
	return d.ln.Addr().String()
}

// Stop shuts down HTTP and the bus tap.
func (d *Dashboard) Stop() {
	select {
	case <-d.stop:
		return
	default:
		close(d.stop)
	}
	if d.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		d.httpSrv.Shutdown(ctx) //nolint:errcheck
		cancel()
	}
	d.wg.Wait()
	if d.bus != nil {
		d.bus.Close()
	}
}

// onEvent forwards one tapped envelope to all SSE clients.
func (d *Dashboard) onEvent(env envelope.Envelope) {
	msg, err := json.Marshal(map[string]any{"type": "event", "envelope": env})
	if err != nil {
		return
	}
	d.broadcast(msg)
}

// rosterLoop periodically reads the authoritative registry and pushes the
// snapshot. Polling the KV (rather than deriving counts from events) keeps
// the dashboard a pure reader of the one authority.
func (d *Dashboard) rosterLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(rosterInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			roster, err := d.fetchRoster()
			if err != nil {
				continue // bus reconnecting; try next tick
			}
			d.mu.Lock()
			d.roster = roster
			d.mu.Unlock()
			msg, err := json.Marshal(map[string]any{"type": "roster", "agents": roster})
			if err != nil {
				continue
			}
			d.broadcast(msg)
			d.tickClaims()
			d.tickNotes(roster)
		}
	}
}

// tickClaims snapshots the authoritative claims KV and pushes it. The KV is
// the lock — claim events on the tap show attempts, this shows truth (TTL
// expiry and coordinator reclaim included).
func (d *Dashboard) tickClaims() {
	held, err := claim.ListAll(d.bus)
	if err != nil {
		return
	}
	d.mu.Lock()
	d.claims = held
	d.mu.Unlock()
	if msg, err := json.Marshal(map[string]any{"type": "claims", "claims": held}); err == nil {
		d.broadcast(msg)
	}
}

// tickNotes tails every visible repo's blackboard stream and broadcasts new
// entries as ordinary note events — replayed history and live notes reach
// browsers through one path, reading the one durable authority. Repos are
// discovered from agent cards and live claims (the bus has no list-streams
// op, deliberately: streams are named by the shared subject taxonomy).
func (d *Dashboard) tickNotes(roster []agentcard.RegistryRecord) {
	repos := map[string]bool{envelope.DefaultRepo: true}
	for _, rec := range roster {
		if envelope.ValidRepo(rec.Card.Repo) {
			repos[rec.Card.Repo] = true
		}
	}
	d.mu.Lock()
	for _, h := range d.claims {
		if envelope.ValidRepo(h.Record.Repo) {
			repos[h.Record.Repo] = true
		}
	}
	d.mu.Unlock()

	for repo := range repos {
		stream := envelope.StreamNotes(repo)
		d.mu.Lock()
		from := d.noteSeq[stream] + 1
		d.mu.Unlock()
		entries, err := d.bus.StreamRead(stream, from)
		if err != nil {
			continue
		}
		for _, e := range entries {
			env, err := envelope.Decode(e.Data)
			if err != nil || env.Kind != envelope.KindNote {
				continue // malformed record never breaks the observer
			}
			if msg, merr := json.Marshal(map[string]any{"type": "event", "envelope": env}); merr == nil {
				d.broadcast(msg)
			}
			d.mu.Lock()
			if e.Seq > d.noteSeq[stream] {
				d.noteSeq[stream] = e.Seq
			}
			d.mu.Unlock()
		}
	}
}

func (d *Dashboard) fetchRoster() ([]agentcard.RegistryRecord, error) {
	keys, err := d.bus.KVList(envelope.BucketRegistry)
	if err != nil {
		return nil, err
	}
	roster := make([]agentcard.RegistryRecord, 0, len(keys))
	for _, kv := range keys {
		var rec agentcard.RegistryRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		roster = append(roster, rec)
	}
	sort.Slice(roster, func(i, j int) bool { return roster[i].Card.Name < roster[j].Card.Name })
	return roster, nil
}

func (d *Dashboard) broadcast(msg []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for ch := range d.clients {
		select {
		case ch <- msg:
		default: // slow browser: drop the frame, never block the tap
		}
	}
}

// --- HTTP handlers ---------------------------------------------------------------

func (d *Dashboard) serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML) //nolint:errcheck
}

func (d *Dashboard) serveRoster(w http.ResponseWriter, _ *http.Request) {
	d.mu.Lock()
	roster := d.roster
	d.mu.Unlock()
	if roster == nil {
		// First tick may not have run yet; read directly.
		if fresh, err := d.fetchRoster(); err == nil {
			roster = fresh
		} else {
			roster = []agentcard.RegistryRecord{}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"agents": roster}) //nolint:errcheck
}

// serveClaims returns the authoritative claims snapshot (one source of
// truth: the claims KV).
func (d *Dashboard) serveClaims(w http.ResponseWriter, _ *http.Request) {
	held, err := claim.ListAll(d.bus)
	if err != nil {
		d.mu.Lock()
		held = d.claims // last good snapshot; bus may be reconnecting
		d.mu.Unlock()
	}
	if held == nil {
		held = []claim.Held{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"claims": held}) //nolint:errcheck
}

// serveNotes replays a repo's blackboard stream (?repo=R, default repo when
// omitted) as decoded note envelopes.
func (d *Dashboard) serveNotes(w http.ResponseWriter, r *http.Request) {
	repo := r.URL.Query().Get("repo")
	if repo == "" {
		repo = envelope.DefaultRepo
	}
	if !envelope.ValidRepo(repo) {
		http.Error(w, "invalid repo id", http.StatusBadRequest)
		return
	}
	entries, err := d.bus.StreamRead(envelope.StreamNotes(repo), 0)
	if err != nil {
		http.Error(w, "bus unavailable", http.StatusServiceUnavailable)
		return
	}
	notes := make([]envelope.Envelope, 0, len(entries))
	for _, e := range entries {
		env, err := envelope.Decode(e.Data)
		if err != nil || env.Kind != envelope.KindNote {
			continue
		}
		notes = append(notes, env)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"repo": repo, "notes": notes}) //nolint:errcheck
}

func (d *Dashboard) serveSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 64)
	d.mu.Lock()
	d.clients[ch] = struct{}{}
	roster := d.roster
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.clients, ch)
		d.mu.Unlock()
	}()

	// Initial snapshot so the page renders without waiting a tick.
	if roster != nil {
		if msg, err := json.Marshal(map[string]any{"type": "roster", "agents": roster}); err == nil {
			fmt.Fprintf(w, "data: %s\n\n", msg) //nolint:errcheck
			flusher.Flush()
		}
	}

	for {
		select {
		case <-d.stop:
			return
		case <-r.Context().Done():
			return
		case msg := <-ch:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
