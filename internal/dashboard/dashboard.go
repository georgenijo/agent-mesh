// Package dashboard is the live observer with a write path for job intake.
//
// P0/P1/P2 base: the dashboard taps mesh.> on the bus, snapshots the
// registry, and bridges both to a browser via SSE. The dashboard bus client
// is read-only for presence/claims/notes — it only subscribes and reads KV.
//
// P4 addition (issue #47): a minimal coordinator write API on the dashboard
// HTTP server. POST /api/jobs is the one write endpoint: it calls
// job.Store.Create through the dashboard's own bus connection (same bus
// client already used for reads), protected by a local bearer token written to
// MESH_DIR/dashboard.token on start. Observer endpoints (GET /, /events,
// /api/roster, /api/claims, /api/notes) remain unauthenticated. One
// authority per fact: the POST delegates to job.Store — the same machinery
// `mesh submit` uses — and never maintains parallel job state.
//
// Security (issue #61): all dashboard routes are wrapped with
// hostCheckMiddleware, which validates the Host header against the loopback
// allow-list (localhost, 127.0.0.1, [::1], with optional port). Any other
// Host value — including a missing one that Go fills from a non-loopback
// forwarded address — is rejected with 403. This closes the DNS-rebinding
// vector: a malicious page on evil.example that re-points to 127.0.0.1 sends
// Host: evil.example, which the middleware rejects before any handler fires.
package dashboard

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/claim"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/observe"
)

//go:embed dashboard.html
var indexHTML []byte

// maxClaimLog bounds the in-memory claim history ring. Generous enough to be
// useful on a busy mesh, small enough that replaying it to a new browser on
// connect is cheap.
const maxClaimLog = 200

// maxJobSnapshot and maxTaskSnapshot bound the in-memory P3 state rings.
// The dashboard is a pure observer: it reflects what the KV emits, bounded
// so replaying to a new browser on connect is cheap.
const (
	maxJobSnapshot  = 500
	maxTaskSnapshot = 1000
)

// jobSnap is one job row as carried in the "jobs" SSE frame.
type jobSnap struct {
	ID     string            `json:"id"`
	Repo   string            `json:"repo"`
	Source string            `json:"source"`
	Title  string            `json:"title"`
	State  envelope.JobState `json:"state"`
	TS     time.Time         `json:"ts"`
}

// taskSnap is one task row as carried in the "tasks" SSE frame.
type taskSnap struct {
	ID    string             `json:"id"`
	Job   string             `json:"job"`
	Role  string             `json:"role"`
	Title string             `json:"title"`
	State envelope.TaskState `json:"state"`
	TS    time.Time          `json:"ts"`
}

// workerSnap is one worker-run entry as carried in the "workers" SSE frame.
type workerSnap struct {
	Task    string                   `json:"task"`
	Job     string                   `json:"job"`
	Result  envelope.WorkerResult    `json:"result"`
	Code    envelope.WorkerErrorCode `json:"code,omitempty"`
	CostUSD float64                  `json:"costUSD,omitempty"`
	Reason  string                   `json:"reason,omitempty"`
	TS      time.Time                `json:"ts"`
}

// triageSnap is one triage-attempt entry as carried in the "triage" SSE frame.
type triageSnap struct {
	Job    string                   `json:"job"`
	Result envelope.TriageResult    `json:"result"`
	Tasks  int                      `json:"tasks,omitempty"`
	Code   envelope.TriageErrorCode `json:"code,omitempty"`
	Reason string                   `json:"reason,omitempty"`
	TS     time.Time                `json:"ts"`
}

// fleetSnap carries the last observed fleet state, as carried in the "fleet" SSE frame.
type fleetSnap struct {
	State     envelope.FleetState     `json:"state"`
	Code      envelope.FleetPauseCode `json:"code,omitempty"`
	Reason    string                  `json:"reason,omitempty"`
	SpentUSD  float64                 `json:"spentUSD,omitempty"`
	BudgetUSD float64                 `json:"budgetUSD,omitempty"`
	TS        time.Time               `json:"ts"`
}

// claimLogEntry is one observed claim lifecycle event for the history panel.
// Result is the wire ClaimResult (claimed|lost|error) for takes, or the
// synthetic "released" for a claim that left the authoritative KV snapshot.
type claimLogEntry struct {
	From   string    `json:"from"`
	Path   string    `json:"path"`
	Repo   string    `json:"repo"`
	Result string    `json:"result"`
	TS     time.Time `json:"ts"`
}

// Dashboard serves the live observer UI with a write path for job intake.
type Dashboard struct {
	cfg  config.Config
	addr string
	log  *slog.Logger

	// rosterEvery is how often the registry snapshot is refreshed and
	// pushed. New sets the production default; white-box tests tighten it
	// before Start so lifecycle transitions land between ticks.
	rosterEvery time.Duration

	// jobToken is the local bearer token that guards POST /api/jobs.
	// Generated fresh on each Start, written to cfg.DashboardTokenFile(),
	// removed on Stop. Observer endpoints remain unauthenticated.
	jobToken string

	bus     *bus.Client
	httpSrv *http.Server
	ln      net.Listener

	mu      sync.Mutex
	clients map[chan []byte]struct{}
	roster  []agentcard.RegistryRecord
	claims  []claim.Held
	// claimLog is a bounded, newest-last ring of observed claim lifecycle
	// events — derived observability, NOT an authority. Claim takes
	// (claimed|lost|error) are recorded from the mesh.claim.<repo> tap with
	// their real envelope timestamp; releases are synthesized by diffing the
	// authoritative claims-KV snapshot each tick (which also catches TTL
	// expiry and coordinator reclaim-on-death). Replayed to each new browser
	// so the Claim History panel survives a page refresh. Resets on dashboard
	// restart — the KV remains the one authority for who currently holds what.
	claimLog []claimLogEntry
	// noteSeq tracks the last replayed seq per notes stream, so each tick
	// broadcasts only new blackboard entries (replay and live are one path).
	noteSeq map[string]uint64

	// P3 in-memory state: populated from the SSE tap (KindJob/KindTask/
	// KindWorker/KindFleet/KindTriage). These are derived observability —
	// the jobs/tasks KV records are the authorities. Replayed to each new
	// browser on SSE connect so the panels survive a page refresh.
	// All four are held under mu.
	jobs    map[string]jobSnap  // job id -> latest job snapshot
	tasks   map[string]taskSnap // task id -> latest task snapshot
	workers []workerSnap        // bounded ring, newest last
	triages []triageSnap        // bounded ring, newest last
	fleet   *fleetSnap          // nil until the first KindFleet is observed

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
		cfg:         cfg,
		addr:        addr,
		log:         log,
		rosterEvery: 1 * time.Second,
		clients:     make(map[chan []byte]struct{}),
		noteSeq:     make(map[string]uint64),
		jobs:        make(map[string]jobSnap),
		tasks:       make(map[string]taskSnap),
		stop:        make(chan struct{}),
	}
}

// Start connects the tap and begins serving HTTP.
func (d *Dashboard) Start() error {
	// Generate a fresh local bearer token for the write API. The token is
	// written to MESH_DIR/dashboard.token so scripts and `curl` can read it;
	// the UI page receives it via GET /api/write-token (loopback-only, same
	// trust boundary as the rest of the dashboard).
	token, err := generateToken()
	if err != nil {
		return fmt.Errorf("dashboard: generate write token: %w", err)
	}
	d.jobToken = token
	if err := os.WriteFile(d.cfg.DashboardTokenFile(), []byte(token+"\n"), 0o600); err != nil {
		return fmt.Errorf("dashboard: write token file: %w", err)
	}

	cli, err := bus.Dial(d.cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		os.Remove(d.cfg.DashboardTokenFile()) //nolint:errcheck
		return fmt.Errorf("dashboard: connect bus: %w", err)
	}
	d.bus = cli

	// Tap everything; forward each envelope to connected browsers.
	if _, err := cli.Subscribe(envelope.PatternAll, d.onEvent); err != nil {
		cli.Close()
		os.Remove(d.cfg.DashboardTokenFile()) //nolint:errcheck
		return fmt.Errorf("dashboard: subscribe: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", d.serveIndex)
	mux.HandleFunc("GET /classic", d.serveClassicIndex)
	mux.HandleFunc("GET /events", d.serveSSE)
	mux.HandleFunc("GET /api/roster", d.serveRoster)
	mux.HandleFunc("GET /api/claims", d.serveClaims)
	mux.HandleFunc("GET /api/notes", d.serveNotes)
	mux.HandleFunc("GET /api/runlog", d.serveRunLog)
	mux.HandleFunc("GET /api/jobs", d.serveListJobs)
	mux.HandleFunc("POST /api/jobs", d.serveCreateJob)
	mux.HandleFunc("GET /api/write-token", d.serveWriteToken)
	mountWebUI(mux)

	ln, err := observe.ListenWithFallback(d.addr, d.log)

	if err != nil {
		cli.Close()
		os.Remove(d.cfg.DashboardTokenFile()) //nolint:errcheck
		return fmt.Errorf("dashboard: listen %s: %w", d.addr, err)
	}
	d.ln = ln

	// Run files for the ops plane and `mesh up`: pid first, then the real
	// bound address atomically. The addr file is the readiness gate — once it
	// exists the listener is accepting and the pidfile is complete.
	if err := observe.WriteRunFiles(d.cfg.DashboardPID(), d.cfg.DashboardAddrFile(), d.Addr()); err != nil {
		ln.Close()
		cli.Close()
		os.Remove(d.cfg.DashboardTokenFile()) //nolint:errcheck
		return fmt.Errorf("dashboard: %w", err)
	}
	d.httpSrv = &http.Server{Handler: hostCheckMiddleware(mux)}

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
	observe.RemoveRunFiles(d.cfg.DashboardPID(), d.cfg.DashboardAddrFile())
	os.Remove(d.cfg.DashboardTokenFile()) //nolint:errcheck
}

// onEvent forwards one tapped envelope to all SSE clients, records claim
// takes into the history ring, and updates the P3 in-memory snapshots.
func (d *Dashboard) onEvent(env envelope.Envelope) {
	switch env.Kind {
	case envelope.KindClaim:
		d.recordClaim(env)
	case envelope.KindJob:
		d.recordJob(env)
	case envelope.KindTask:
		d.recordTask(env)
	case envelope.KindWorker:
		d.recordWorker(env)
	case envelope.KindTriage:
		d.recordTriage(env)
	case envelope.KindFleet:
		d.recordFleet(env)
	}
	msg, err := json.Marshal(map[string]any{"type": "event", "envelope": env})
	if err != nil {
		return
	}
	d.broadcast(msg)
}

// recordJob upserts a job snapshot from a KindJob envelope and broadcasts a
// "jobs" frame so all connected browsers see the update immediately.
func (d *Dashboard) recordJob(env envelope.Envelope) {
	var p envelope.JobPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		return
	}
	snap := jobSnap{
		ID:     p.ID,
		Repo:   p.Repo,
		Source: p.Source,
		Title:  p.Title,
		State:  p.State,
		TS:     env.TS,
	}
	d.mu.Lock()
	d.jobs[p.ID] = snap
	// Evict oldest when over the cap (iterate once; map order is random but
	// bounded by maxJobSnapshot so the loop body runs at most once per call).
	for len(d.jobs) > maxJobSnapshot {
		var oldest string
		var oldestTS time.Time
		for id, j := range d.jobs {
			if oldest == "" || j.TS.Before(oldestTS) {
				oldest = id
				oldestTS = j.TS
			}
		}
		delete(d.jobs, oldest)
	}
	snapshot := d.jobsSnapshot()
	d.mu.Unlock()
	d.broadcastJobs(snapshot)
}

// recordTask upserts a task snapshot from a KindTask envelope.
func (d *Dashboard) recordTask(env envelope.Envelope) {
	var p envelope.TaskPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		return
	}
	snap := taskSnap{
		ID:    p.ID,
		Job:   p.Job,
		Role:  p.Role,
		Title: p.Title,
		State: p.State,
		TS:    env.TS,
	}
	d.mu.Lock()
	d.tasks[p.ID] = snap
	for len(d.tasks) > maxTaskSnapshot {
		var oldest string
		var oldestTS time.Time
		for id, t := range d.tasks {
			if oldest == "" || t.TS.Before(oldestTS) {
				oldest = id
				oldestTS = t.TS
			}
		}
		delete(d.tasks, oldest)
	}
	snapshot := d.tasksSnapshot()
	d.mu.Unlock()
	d.broadcastTasks(snapshot)
}

// recordWorker appends a worker-run snapshot to the bounded ring.
func (d *Dashboard) recordWorker(env envelope.Envelope) {
	var p envelope.WorkerPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		return
	}
	snap := workerSnap{
		Task:    p.Task,
		Job:     p.Job,
		Result:  p.Result,
		Code:    p.Code,
		CostUSD: p.CostUSD,
		Reason:  p.Reason,
		TS:      env.TS,
	}
	d.mu.Lock()
	d.workers = append(d.workers, snap)
	if len(d.workers) > maxClaimLog { // reuse the same cap
		d.workers = d.workers[len(d.workers)-maxClaimLog:]
	}
	snapshot := append([]workerSnap(nil), d.workers...)
	d.mu.Unlock()
	if msg, err := json.Marshal(map[string]any{"type": "workers", "workers": snapshot}); err == nil {
		d.broadcast(msg)
	}
}

// recordTriage appends a triage-attempt snapshot to the bounded ring.
func (d *Dashboard) recordTriage(env envelope.Envelope) {
	var p envelope.TriagePayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		return
	}
	snap := triageSnap{
		Job:    p.Job,
		Result: p.Result,
		Tasks:  p.Tasks,
		Code:   p.Code,
		Reason: p.Reason,
		TS:     env.TS,
	}
	d.mu.Lock()
	d.triages = append(d.triages, snap)
	if len(d.triages) > maxClaimLog {
		d.triages = d.triages[len(d.triages)-maxClaimLog:]
	}
	snapshot := append([]triageSnap(nil), d.triages...)
	d.mu.Unlock()
	if msg, err := json.Marshal(map[string]any{"type": "triage", "triages": snapshot}); err == nil {
		d.broadcast(msg)
	}
}

// recordFleet stores the latest fleet-state snapshot and broadcasts it.
func (d *Dashboard) recordFleet(env envelope.Envelope) {
	var p envelope.FleetPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		return
	}
	snap := fleetSnap{
		State:     p.State,
		Code:      p.Code,
		Reason:    p.Reason,
		SpentUSD:  p.SpentUSD,
		BudgetUSD: p.BudgetUSD,
		TS:        env.TS,
	}
	d.mu.Lock()
	d.fleet = &snap
	d.mu.Unlock()
	if msg, err := json.Marshal(map[string]any{"type": "fleet", "fleet": snap}); err == nil {
		d.broadcast(msg)
	}
}

// jobsSnapshot returns a sorted slice of current job snapshots. Must be called
// with mu held.
func (d *Dashboard) jobsSnapshot() []jobSnap {
	out := make([]jobSnap, 0, len(d.jobs))
	for _, j := range d.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].TS.Before(out[k].TS) })
	return out
}

// tasksSnapshot returns a sorted slice of current task snapshots. Must be
// called with mu held.
func (d *Dashboard) tasksSnapshot() []taskSnap {
	out := make([]taskSnap, 0, len(d.tasks))
	for _, t := range d.tasks {
		out = append(out, t)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].TS.Before(out[k].TS) })
	return out
}

// broadcastJobs broadcasts a "jobs" snapshot frame to all SSE clients.
func (d *Dashboard) broadcastJobs(jobs []jobSnap) {
	if jobs == nil {
		jobs = []jobSnap{}
	}
	if msg, err := json.Marshal(map[string]any{"type": "jobs", "jobs": jobs}); err == nil {
		d.broadcast(msg)
	}
}

// broadcastTasks broadcasts a "tasks" snapshot frame to all SSE clients.
func (d *Dashboard) broadcastTasks(tasks []taskSnap) {
	if tasks == nil {
		tasks = []taskSnap{}
	}
	if msg, err := json.Marshal(map[string]any{"type": "tasks", "tasks": tasks}); err == nil {
		d.broadcast(msg)
	}
}

// recordClaim appends a claim take (claimed|lost|error) to the history ring,
// stamped with the envelope's own timestamp. A malformed payload is dropped —
// one bad event never corrupts the log.
func (d *Dashboard) recordClaim(env envelope.Envelope) {
	var p envelope.ClaimPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		return
	}
	d.addClaimLog(claimLogEntry{
		From:   env.From,
		Path:   p.Path,
		Repo:   p.Repo,
		Result: string(p.Result),
		TS:     env.TS,
	})
}

// addClaimLog appends entries to the bounded ring and broadcasts the whole log
// as one frame. Broadcasting the full (bounded) log keeps clients free of
// merge/ordering logic; on a local mesh the volume is trivial.
func (d *Dashboard) addClaimLog(entries ...claimLogEntry) {
	if len(entries) == 0 {
		return
	}
	d.mu.Lock()
	d.claimLog = append(d.claimLog, entries...)
	if len(d.claimLog) > maxClaimLog {
		d.claimLog = d.claimLog[len(d.claimLog)-maxClaimLog:]
	}
	snapshot := append([]claimLogEntry(nil), d.claimLog...)
	d.mu.Unlock()
	if msg, err := json.Marshal(map[string]any{"type": "claimlog", "entries": snapshot}); err == nil {
		d.broadcast(msg)
	}
}

// rosterLoop periodically reads the authoritative registry and pushes the
// snapshot. Polling the KV (rather than deriving counts from events) keeps
// the dashboard a pure reader of the one authority.
func (d *Dashboard) rosterLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.rosterEvery)
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
	prev := d.claims
	d.claims = held
	d.mu.Unlock()

	// Synthesize "released" history entries for claims that left the snapshot
	// (manual release, TTL expiry, or coordinator reclaim-on-death) or whose
	// holder changed. The new holder's own "claimed" take arrives via the tap,
	// so only the departing holder needs synthesizing here.
	newByKey := make(map[string]claim.Held, len(held))
	for _, h := range held {
		newByKey[claim.Key(h.Repo, h.Path)] = h
	}
	var releases []claimLogEntry
	now := time.Now().UTC()
	for _, p := range prev {
		cur, ok := newByKey[claim.Key(p.Repo, p.Path)]
		if !ok || cur.Agent != p.Agent {
			releases = append(releases, claimLogEntry{
				From: p.Agent, Path: p.Path, Repo: p.Repo, Result: "released", TS: now,
			})
		}
	}
	d.addClaimLog(releases...)

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
	// Root redirects to the #31 production dashboard (/ui/): the actively-built
	// UI with the live work queue, the submit-job form (#47), and claim
	// history. The P0 observer (dashboard.html) predates the autonomous
	// pipeline and can't render jobs/tasks; it stays reachable at /classic.
	target := "/ui/"
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	http.Redirect(w, r, target, http.StatusFound)
}

// serveClassicIndex serves the frozen P0 observer page for reference.
func (d *Dashboard) serveClassicIndex(w http.ResponseWriter, _ *http.Request) {
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
		writeJSONError(w, `{"error":"bad_request","message":"invalid repo id"}`, http.StatusBadRequest)
		return
	}
	entries, err := d.bus.StreamRead(envelope.StreamNotes(repo), 0)
	if err != nil {
		writeJSONError(w, `{"error":"unavailable","message":"bus unavailable"}`, http.StatusServiceUnavailable)
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

// --- Write API (P4, issue #47) ---------------------------------------------------

// maxJobTitleLen and maxJobBodyLen mirror meshapi.MaxJobTitleLen/MaxJobBodyLen.
// Duplicated here to avoid a meshapi import cycle (meshapi imports job; we are
// downstream of both).
const (
	maxJobTitleLen = 4096
	maxJobBodyLen  = 1 << 19 // 512 KiB
)

// jobCreateRequest is the POST /api/jobs request body.
type jobCreateRequest struct {
	Repo  string `json:"repo"`
	Title string `json:"title"`
	Body  string `json:"body,omitempty"`
}

// checkWriteAuth returns false and writes 401 if the request does not carry
// the expected bearer token. Called only on mutating endpoints.
func (d *Dashboard) checkWriteAuth(w http.ResponseWriter, r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) {
		writeJSONError(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return false
	}
	if strings.TrimPrefix(auth, prefix) != d.jobToken {
		writeJSONError(w, `{"error":"forbidden"}`, http.StatusForbidden)
		return false
	}
	return true
}

// serveCreateJob handles POST /api/jobs — the dashboard write path for job
// intake. It delegates entirely to job.Store, which is the one authority for
// jobs (same as `mesh submit`). No fake-success: any store or validation
// failure returns a typed error JSON body.
func (d *Dashboard) serveCreateJob(w http.ResponseWriter, r *http.Request) {
	if !d.checkWriteAuth(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxJobBodyLen+maxJobTitleLen+4096)
	var req jobCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeJSONError(w, `{"error":"bad_request","message":"body too large"}`, http.StatusBadRequest)
		} else {
			writeJSONError(w, `{"error":"bad_request","message":"invalid JSON body"}`, http.StatusBadRequest)
		}
		return
	}
	req.Repo = strings.TrimSpace(req.Repo)
	req.Title = strings.TrimSpace(req.Title)
	if req.Repo == "" {
		writeJSONError(w, `{"error":"bad_request","message":"repo is required"}`, http.StatusBadRequest)
		return
	}
	if req.Title == "" {
		writeJSONError(w, `{"error":"bad_request","message":"title is required"}`, http.StatusBadRequest)
		return
	}
	if len(req.Title) > maxJobTitleLen {
		writeJSONError(w, fmt.Sprintf(`{"error":"bad_request","message":"title exceeds %d bytes"}`, maxJobTitleLen), http.StatusBadRequest)
		return
	}
	if len(req.Body) > maxJobBodyLen {
		writeJSONError(w, fmt.Sprintf(`{"error":"bad_request","message":"body exceeds %d bytes"}`, maxJobBodyLen), http.StatusBadRequest)
		return
	}

	store := job.NewStore(d.bus)
	rec, err := store.Create(job.Record{
		Repo:   req.Repo,
		Source: job.SourceManual,
		Title:  req.Title,
		Body:   req.Body,
	})
	if err != nil {
		d.log.Warn("dashboard: create job failed", "err", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{"error": "unavailable", "message": err.Error()}) //nolint:errcheck
		return
	}

	// Publish a KindJob observability event so the SSE tap (and any other
	// mesh.> subscriber) sees the intake exactly as `mesh submit` does.
	env, err := envelope.New(envelope.KindJob, "dashboard", envelope.SubjectJob(rec.ID), &envelope.JobPayload{
		ID: rec.ID, Repo: rec.Repo, Source: rec.Source, Title: rec.Title, State: rec.State,
	})
	if err == nil {
		d.bus.Publish(env) //nolint:errcheck // best-effort: the KV write is the authority
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
		"job":   rec.ID,
		"repo":  rec.Repo,
		"state": string(rec.State),
	})
}

// serveListJobs returns all jobs from the authoritative jobs KV, oldest first.
// Unauthenticated read-only endpoint (same posture as /api/roster).
func (d *Dashboard) serveListJobs(w http.ResponseWriter, _ *http.Request) {
	store := job.NewStore(d.bus)
	jobs, err := store.List()
	if err != nil {
		writeJSONError(w, `{"error":"unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	if jobs == nil {
		jobs = []job.Record{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"jobs": jobs}) //nolint:errcheck
}

// serveWriteToken returns the local write-API bearer token as JSON.
// This endpoint is intentionally served over the same loopback-bound listener
// as the rest of the dashboard — the same trust boundary that already gives
// the browser access to every mesh.> event. It is not more privileged than
// reading the token file directly. Observer endpoints have always been
// unauthenticated on loopback; this adds a convenient UI path for the form.
func (d *Dashboard) serveWriteToken(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"token": d.jobToken}) //nolint:errcheck
}

// serveRunLog serves a worker's live per-task transcript (runs/<task>.jsonl),
// written when MESH_WORKER_STREAM is on. Read-only and unauthenticated, the same
// posture as the other observer endpoints. The task id is constrained to safe
// filename characters so the path cannot escape RunsDir. An absent transcript
// (run not started, or streaming off) is an empty body, not an error — the UI
// polls this and renders whatever is there.
func (d *Dashboard) serveRunLog(w http.ResponseWriter, r *http.Request) {
	task := r.URL.Query().Get("task")
	if !validRunTaskID(task) {
		writeJSONError(w, `{"error":"bad_request","message":"invalid task id"}`, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	data, err := os.ReadFile(filepath.Join(d.cfg.RunsDir(), task+".jsonl"))
	if err != nil {
		return // no transcript yet → empty body
	}
	w.Write(data) //nolint:errcheck
}

// validRunTaskID accepts only the safe filename characters a task id uses
// (UUIDv7: hex + hyphen), bounding length, so the transcript path cannot escape
// RunsDir via separators or "..".
func validRunTaskID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// writeJSONError writes a JSON error response with the correct Content-Type.
// It keeps the JSON shapes identical to the inline literals previously passed
// to http.Error, but corrects the Content-Type to application/json.
func writeJSONError(w http.ResponseWriter, body string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(body)) //nolint:errcheck
}

// isLoopbackHost reports whether host (the hostname part of the HTTP Host
// header, with any port already stripped) is one of the canonical loopback
// names accepted by the dashboard.
//
// Accepted: "localhost", "127.0.0.1", "::1", "[::1]".
// The bracketed form "[::1]" is how net.SplitHostPort leaves the host when
// the original Host header was "[::1]:port", and is included for robustness
// even though Go's own http.Server normalises IPv6 addresses.
func isLoopbackHost(host string) bool {
	switch host {
	case "localhost", "127.0.0.1", "::1", "[::1]":
		return true
	}
	return false
}

// hostCheckMiddleware is an HTTP middleware that validates the Host header
// against the loopback allow-list before passing the request to next.
//
// DNS-rebinding attack: a malicious page on evil.example that re-points its
// domain to 127.0.0.1 becomes same-origin in the browser's view, but the
// browser still sends Host: evil.example. Checking the Host header here
// rejects such requests before any handler fires, at the cost of one string
// comparison per request (negligible on a local server).
//
// If the Host header is absent or its hostname cannot be parsed the request
// is also rejected — an absent Host is allowed by HTTP/1.0 but not by
// HTTP/1.1, and the dashboard is only ever accessed on loopback where a
// well-behaved client always supplies it.
func hostCheckMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if host == "" {
			writeJSONError(w, `{"error":"forbidden","message":"missing Host header"}`, http.StatusForbidden)
			return
		}
		// Strip the port if present. net.SplitHostPort returns an error for
		// bare hostnames (no colon) which we treat as the full hostname.
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if !isLoopbackHost(host) {
			writeJSONError(w, `{"error":"forbidden","message":"Host not allowed"}`, http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// generateToken creates a 32-byte random hex bearer token.
func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// WriteToken exposes the bearer token for tests and the write-token API
// endpoint; production code should read the token file instead.
func (d *Dashboard) WriteToken() string { return d.jobToken }

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
	claimLog := append([]claimLogEntry(nil), d.claimLog...)
	jobsSnap := d.jobsSnapshot()
	tasksSnap := d.tasksSnapshot()
	workersSnap := append([]workerSnap(nil), d.workers...)
	triagesSnap := append([]triageSnap(nil), d.triages...)
	fleetSnap := d.fleet
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

	// Replay the claim history ring so the Claim History panel is populated on
	// connect (survives a browser refresh). Always sent, even when empty, so
	// the client renders its empty state deterministically.
	if claimLog == nil {
		claimLog = []claimLogEntry{}
	}
	if msg, err := json.Marshal(map[string]any{"type": "claimlog", "entries": claimLog}); err == nil {
		fmt.Fprintf(w, "data: %s\n\n", msg) //nolint:errcheck
		flusher.Flush()
	}

	// Send initial P3 snapshots. Each is always sent (even empty) so the
	// browser renders its honest empty state without waiting for live traffic.
	if jobsSnap == nil {
		jobsSnap = []jobSnap{}
	}
	if msg, err := json.Marshal(map[string]any{"type": "jobs", "jobs": jobsSnap}); err == nil {
		fmt.Fprintf(w, "data: %s\n\n", msg) //nolint:errcheck
		flusher.Flush()
	}
	if tasksSnap == nil {
		tasksSnap = []taskSnap{}
	}
	if msg, err := json.Marshal(map[string]any{"type": "tasks", "tasks": tasksSnap}); err == nil {
		fmt.Fprintf(w, "data: %s\n\n", msg) //nolint:errcheck
		flusher.Flush()
	}
	if workersSnap == nil {
		workersSnap = []workerSnap{}
	}
	if msg, err := json.Marshal(map[string]any{"type": "workers", "workers": workersSnap}); err == nil {
		fmt.Fprintf(w, "data: %s\n\n", msg) //nolint:errcheck
		flusher.Flush()
	}
	if triagesSnap == nil {
		triagesSnap = []triageSnap{}
	}
	if msg, err := json.Marshal(map[string]any{"type": "triage", "triages": triagesSnap}); err == nil {
		fmt.Fprintf(w, "data: %s\n\n", msg) //nolint:errcheck
		flusher.Flush()
	}
	if fleetSnap != nil {
		if msg, err := json.Marshal(map[string]any{"type": "fleet", "fleet": *fleetSnap}); err == nil {
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
