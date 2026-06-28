// Package coordinator is the control plane: it embeds the bus server and
// reduces register/leave/heartbeat/status events into the authoritative
// registry KV bucket, with two-tier lease eviction (live → away → evicted).
//
// It is a pure reducer over bus events — it decides *who exists*, never
// carries payloads, and stays out of the data path. On restart its state is
// rebuilt from the documented startup source: every sidecar's bus client
// reconnects and re-registers (see bus.ClientOptions.OnReconnect).
package coordinator

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/claim"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/cost"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/scheduler"
	"github.com/georgenijo/agent-mesh/internal/triage"
	"github.com/georgenijo/agent-mesh/internal/worker"
)

// coordinatorID is the From id the coordinator uses on the bus.
const coordinatorID = "coordinator"

// AuditEntry is one record appended to the unified audit stream
// (envelope.StreamAudit) — the #29 policy/audit substrate. The coordinator is
// its sole writer (one authority per fact), so the record shape lives here, in
// the package that owns the stream, while the typed Kind vocabulary lives in
// internal/envelope (AuditCategory) beside the other enums.
//
// The original presence/claim fields (Kind/ID/Event/Path/Repo/TS) are
// unchanged and frozen — existing reducer/sweep/claims paths and their tests
// depend on them. #29 adds optional correlation fields so a single ordered
// read of the stream can reconstruct how a ticket/job/task reached its state:
// Ticket/Job/Task tie an entry to a work unit, Role/By/State/Result carry the
// typed lifecycle detail, and Detail is free-text context (never parsed — taps
// discriminate on the typed fields). Every added field is omitempty, so a
// presence/claim entry serializes byte-identically to before (golden-pinned).
type AuditEntry struct {
	Kind  envelope.AuditCategory `json:"kind"`  // presence|claim|ticket|ask|answer|job|task|triage|worker|fleet
	ID    string                 `json:"id"`    // the agent or actor; empty for actorless events
	Event string                 `json:"event"` // the lifecycle verb/state within the category
	Path  string                 `json:"path,omitempty"`
	Repo  string                 `json:"repo,omitempty"`
	TS    time.Time              `json:"ts"`

	// #29 correlation fields — all omitempty, additive.
	Ticket string `json:"ticket,omitempty"` // ask-ticket id (ticket/ask/answer)
	Job    string `json:"job,omitempty"`    // job id (job/task/triage/worker)
	Task   string `json:"task,omitempty"`   // task id (task/worker)
	Role   string `json:"role,omitempty"`   // role addressed/owning (ask/task)
	By     string `json:"by,omitempty"`     // actor that caused a transition, when distinct from ID
	State  string `json:"state,omitempty"`  // resulting lifecycle state (ticket/job/task/fleet)
	Result string `json:"result,omitempty"` // typed outcome (claim/triage/worker result)
	Detail string `json:"detail,omitempty"` // free-text context; never parsed
}

// Coordinator runs the bus server and the registry reducer.
type Coordinator struct {
	cfg config.Config
	log *slog.Logger

	// WorkerJoin joins one #26 worker agent to the mesh (an embedded
	// per-worker sidecar). It is INJECTED — by cmd/meshd in production, by
	// tests directly — because this package must not import internal/sidecar:
	// the sidecar package's own tests import the coordinator, and Go forbids
	// the cycle (the same seam pattern as the expert loop's ExpertFunc).
	// Required before Start when cfg.WorkerCLI is set; ignored otherwise.
	WorkerJoin worker.JoinFunc

	srv *bus.Server
	cli *bus.Client

	// triager is the #24 triage loop — nil unless cfg.PlannerCLI is set
	// (an autostarted coordinator must never spawn LLM processes unless the
	// operator opted in).
	triager *triage.Triager

	// scheduler is the #25 dependency-gated worker scheduler — nil unless
	// cfg.WorkerCLI is set, under the same opt-in rule as the triager.
	scheduler *scheduler.Scheduler

	// reviewer is the #80 review-gating transport (scheduler→expert bus round
	// trip) — nil unless BOTH cfg.WorkerCLI and cfg.ReviewRole are set.
	reviewer *scheduler.BusReviewer

	// experts is the #117 autonomous expert spawner — nil unless
	// cfg.AutoExperts is set. It watches role-asks / review requests and
	// launches a resident expert when one targets an un-owned role.
	experts *expertSpawner

	// mu serializes every registry read-modify-write: events arrive on
	// independent subscription goroutines, and the KV store has no
	// transactions across get+put.
	mu sync.Mutex

	stop chan struct{}
	wg   sync.WaitGroup
}

// New creates a coordinator for the given config.
func New(cfg config.Config, log *slog.Logger) *Coordinator {
	if log == nil {
		log = slog.Default()
	}
	return &Coordinator{cfg: cfg, log: log, stop: make(chan struct{})}
}

// Start binds the bus socket, subscribes the reducer, and starts the
// presence janitor.
func (c *Coordinator) Start() error {
	if err := c.cfg.EnsureDirs(); err != nil {
		return err
	}
	// StreamDir makes the bus's durable subjects (the blackboard, the audit
	// trail) survive a coordinator restart — the registry deliberately does
	// not: it repopulates from sidecar re-registration.
	//
	// PersistBuckets makes the jobs (#23) and tasks (#24) KV records durable
	// too (#65). Unlike registry/claims — TTL leases a live owner re-asserts —
	// a job/task has no live owner to re-establish it, so without this a
	// coordinator restart would lose every open job and the persisted task
	// DAG. The bus loads these buckets in Start() *before* binding the socket,
	// so they are whole before the triage loop / #25 scheduler (started below,
	// only after the bus is up) can sweep them. Registry/claims stay in-memory
	// by omission — explicit non-goal (DECISIONS.md: lease + re-establishment).
	//
	// BucketTriageAttempts (#64) is persisted for the same reason: it holds the
	// triage retry attempt count + next-retry deadline, so a job mid-backoff
	// resumes its schedule across a restart instead of restarting from attempt 0
	// (and so a still-open job is not re-hammered on the next lifetime's sweep).
	c.srv = bus.NewServer(c.cfg.BusSocket(), bus.Options{
		StreamDir:      c.cfg.StreamsDir(),
		PersistDir:     c.cfg.BucketsDir(),
		PersistBuckets: []string{envelope.BucketJobs, envelope.BucketTasks, envelope.BucketTriageAttempts, envelope.BucketCostLedger},
		Logger:         c.log,
	})
	if err := c.srv.Start(); err != nil {
		return fmt.Errorf("coordinator: start bus: %w", err)
	}
	cli, err := bus.Dial(c.cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		c.srv.Stop()
		return fmt.Errorf("coordinator: dial own bus: %w", err)
	}
	c.cli = cli

	// One subscription, one dispatch goroutine: register/leave/heartbeat/
	// status for the same agent MUST be reduced in publish order. Separate
	// subscriptions would each get their own delivery goroutine, losing
	// cross-subject ordering (a leave could be reduced before its own
	// register, stranding a "live" record).
	if _, err := cli.Subscribe(envelope.PatternAll, c.reduce); err != nil {
		c.Stop()
		return fmt.Errorf("coordinator: subscribe %s: %w", envelope.PatternAll, err)
	}

	c.wg.Add(1)
	go c.janitor()

	// Triage loop (#24): opt-in via MESH_PLANNER_CLI. It runs on its own
	// goroutine with its own sweep — a planner turn takes seconds-to-minutes
	// and must never sit on the reducer or janitor path. A triage failure is
	// a typed event on the job, never a coordinator crash.
	if c.cfg.PlannerCLI != "" {
		tri, err := triage.New(cli, triage.Options{
			PlannerCLI:  c.cfg.PlannerCLI,
			Model:       c.cfg.PlannerModel,
			Timeout:     c.cfg.TriageTimeout,
			Interval:    sweepInterval(c.cfg.HeartbeatInterval),
			WorkDir:     c.cfg.MeshDir,           // clean cwd: no CLAUDE.md context tax (M0 spike)
			MaxAttempts: c.cfg.TriageMaxAttempts, // #64 retry/backoff policy
			Backoff:     c.cfg.TriageBackoff,
			Log:         c.log,
		})
		if err != nil {
			c.Stop()
			return fmt.Errorf("coordinator: triage: %w", err)
		}
		c.triager = tri
		c.triager.Start()
		c.log.Info("triage enabled", "planner", c.cfg.PlannerCLI)
	}

	// Scheduler (#25): opt-in via MESH_WORKER_CLI, same rule as triage — a
	// bare `mesh join` coordinator must never start spawning workers. The
	// driver is the #26 worker runtime: worktree-per-worker isolation with an
	// embedded per-worker sidecar (mesh CLI access inside the run). It
	// requires MESH_REPOS_DIR — refusing to start beats letting workers guess
	// which directory tree they may rewrite. The scheduler itself only knows
	// the Driver seam.
	if c.cfg.WorkerCLI != "" {
		drv, err := worker.NewDriver(cli, c.cfg, c.WorkerJoin, c.log)
		if err != nil {
			c.Stop()
			return fmt.Errorf("coordinator: worker driver: %w", err)
		}
		sopts := scheduler.Options{
			Driver:      drv,
			BudgetUSD:   c.cfg.BudgetUSD,
			MaxParallel: c.cfg.MaxWorkers,
			Interval:    sweepInterval(c.cfg.HeartbeatInterval),
			Log:         c.log,
			CostLedger:  cost.New(cli),
		}
		// Review gating (#80): opt-in via MESH_REVIEW_ROLE, same posture as
		// the planner/worker knobs — unset means a worker success transitions
		// the task to done with no review, exactly as before. Set, every
		// successful worker diff is routed to the expert serving that role and
		// the task's terminal state is gated on the typed verdict.
		if c.cfg.ReviewRole != "" {
			rev, err := scheduler.NewBusReviewer(cli, scheduler.ReviewerOptions{
				Role:     c.cfg.ReviewRole,
				ReposDir: c.cfg.ReposDir,
				Timeout:  c.cfg.ReviewTimeout,
				Log:      c.log,
			})
			if err != nil {
				c.Stop()
				return fmt.Errorf("coordinator: reviewer: %w", err)
			}
			c.reviewer = rev
			sopts.Reviewer = rev
		}
		sch, err := scheduler.New(cli, sopts)
		if err != nil {
			c.Stop()
			return fmt.Errorf("coordinator: scheduler: %w", err)
		}
		c.scheduler = sch
		c.scheduler.Start()
		c.log.Info("scheduler enabled", "worker", c.cfg.WorkerCLI,
			"reposDir", c.cfg.ReposDir, "budgetUSD", c.cfg.BudgetUSD, "maxWorkers", c.cfg.MaxWorkers)
		if c.reviewer != nil {
			c.log.Info("review gating enabled", "role", c.cfg.ReviewRole, "timeout", c.cfg.ReviewTimeout)
		}
	} else if c.cfg.ReviewRole != "" {
		c.log.Warn("MESH_REVIEW_ROLE set but MESH_WORKER_CLI unset; review gating inactive")
	}

	// Autonomous experts (#117): opt-in via MESH_AUTO_EXPERTS, same posture as
	// the planner/worker knobs — an autostarted coordinator never spawns LLM
	// processes unless armed. A setup failure degrades to "no auto-experts"
	// (logged), never a coordinator crash: presence/asks must keep working.
	if c.cfg.AutoExperts {
		sp, err := newExpertSpawner(cli, c.cfg, c.log)
		if err != nil {
			c.log.Warn("auto-experts requested but disabled", "err", err)
		} else if err := sp.start(); err != nil {
			c.log.Warn("auto-experts requested but disabled", "err", err)
		} else {
			c.experts = sp
			c.log.Info("auto-experts enabled")
		}
	}

	if err := writeCoordinatorPID(c.cfg.CoordinatorPID()); err != nil {
		c.Stop()
		return fmt.Errorf("coordinator: write pid file: %w", err)
	}
	c.log.Info("coordinator started", "bus", c.cfg.BusSocket())
	return nil
}

// Stop shuts down the janitor, the bus client, and the bus server.
func (c *Coordinator) Stop() {
	select {
	case <-c.stop:
		return // already stopped
	default:
		close(c.stop)
	}
	c.wg.Wait()
	if c.triager != nil {
		c.triager.Stop()
	}
	if c.scheduler != nil {
		c.scheduler.Stop()
	}
	if c.reviewer != nil {
		c.reviewer.Close()
	}
	if c.experts != nil {
		c.experts.stop()
	}
	if c.cli != nil {
		c.cli.Close()
	}
	if c.srv != nil {
		c.srv.Stop()
	}
	os.Remove(c.cfg.CoordinatorPID()) //nolint:errcheck
}

func writeCoordinatorPID(path string) error {
	return os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600)
}

// --- reducer -------------------------------------------------------------------

// reduce dispatches every tapped envelope to the matching presence handler.
// Runs on the subscription's single delivery goroutine, so events for one
// agent are reduced in publish order. Presence kinds mutate the registry; the
// rest (P1+ announce/claim/ask/answer/ticket/job/task/...) are not control-plane
// events but ARE fanned into the unified audit log (#29) — the coordinator is
// the one component that taps every subject, so it is the natural single writer
// of the audit trail (see auditObserved).
func (c *Coordinator) reduce(env envelope.Envelope) {
	switch env.Kind {
	case envelope.KindRegister:
		c.handleRegister(env)
	case envelope.KindLeave:
		c.handleLeave(env)
	case envelope.KindHeartbeat:
		c.handleHeartbeat(env)
	case envelope.KindStatus:
		c.handleStatus(env)
	default:
		// Non-presence lifecycle event: record it in the audit log. Presence is
		// audited at its mutation site (register/leave/away/evict) so the entry
		// reflects the reduced outcome, not just the wire event; claims are
		// audited where the coordinator reclaims them. Everything else is
		// observed here. Heartbeats are intentionally not audited (every-5s
		// noise, no lifecycle meaning).
		c.auditObserved(env)
	}
}

// fromMatches enforces the one authority rule for presence mutations: an
// envelope may only mutate the record of the agent that sent it. Without
// this, any bus client could forge a leave/heartbeat/status for a peer's id
// and evict or resurrect it.
func (c *Coordinator) fromMatches(env envelope.Envelope, id, kind string) bool {
	if env.From != id {
		c.log.Warn("drop "+kind+" with mismatched sender", "from", env.From, "id", id)
		return false
	}
	return true
}

func (c *Coordinator) handleRegister(env envelope.Envelope) {
	var p envelope.RegisterPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		c.log.Warn("drop malformed register", "err", err)
		return
	}
	if !c.fromMatches(env, p.Card.ID, "register") {
		return
	}
	now := time.Now().UTC()

	c.mu.Lock()
	defer c.mu.Unlock()
	prev, found := c.getRecord(p.Card.ID)
	rec := agentcard.RegistryRecord{
		Card:         p.Card,
		State:        agentcard.PresenceLive,
		RegisteredAt: now,
		LastSeen:     now,
	}
	if found {
		// Re-register (sidecar reconnect): keep observed status history AND
		// the original registration time — resetting RegisteredAt would
		// re-arm the grace window and let a flapping agent evade eviction.
		rec.RegisteredAt = prev.RegisteredAt
		rec.LastStatus, rec.LastStatusAt = prev.LastStatus, prev.LastStatusAt
	}
	c.putRecord(rec)
	c.audit(p.Card.ID, "registered")
}

func (c *Coordinator) handleLeave(env envelope.Envelope) {
	var p envelope.LeavePayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		c.log.Warn("drop malformed leave", "err", err)
		return
	}
	// Only the agent itself may leave. The coordinator's own evict publish
	// (From=coordinator) is an announcement to observers, not a command —
	// the record was already deleted by sweep, so skipping it here is right.
	if env.From != p.ID {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, found := c.getRecord(p.ID); !found {
		return // already gone
	}
	if err := c.cli.KVDelete(envelope.BucketRegistry, p.ID); err != nil {
		c.log.Warn("registry delete failed", "id", p.ID, "err", err)
		return
	}
	c.audit(p.ID, "left")
	// A graceful leave frees the agent's claims immediately — the sidecar is
	// gone, so nothing will renew them, and waiting out the TTL would block
	// peers from paths nobody is editing.
	c.releaseClaims(p.ID, "released")
}

func (c *Coordinator) handleHeartbeat(env envelope.Envelope) {
	var p envelope.HeartbeatPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		c.log.Warn("drop malformed heartbeat", "err", err)
		return
	}
	if !c.fromMatches(env, p.ID, "heartbeat") {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, found := c.getRecord(p.ID)
	if !found {
		// Heartbeat from an agent we don't know (e.g. coordinator restarted
		// and the sidecar has not re-registered yet). Ignore: registration
		// is the only way in; the sidecar re-registers on reconnect.
		return
	}
	rec.LastSeen = time.Now().UTC()
	// Heartbeats carry the agent's latest status text so it survives a
	// coordinator restart (the explicit status envelope may predate us).
	if p.Status != "" && p.Status != rec.LastStatus {
		rec.LastStatus, rec.LastStatusAt = p.Status, env.TS
	}
	if rec.State == agentcard.PresenceAway {
		rec.State = agentcard.PresenceLive
		c.audit(p.ID, "recovered")
	}
	c.putRecord(rec)
}

func (c *Coordinator) handleStatus(env envelope.Envelope) {
	var p envelope.StatusPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		c.log.Warn("drop malformed status", "err", err)
		return
	}
	if !c.fromMatches(env, p.ID, "status") {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, found := c.getRecord(p.ID)
	if !found {
		return
	}
	now := time.Now().UTC()
	rec.LastStatus, rec.LastStatusAt = p.Text, now
	rec.LastSeen = now
	if rec.State == agentcard.PresenceAway {
		rec.State = agentcard.PresenceLive
		c.audit(p.ID, "recovered")
	}
	c.putRecord(rec)
}

// --- two-tier presence janitor --------------------------------------------------

// sweepInterval derives a loop cadence from the heartbeat interval with a
// floor, shared by the presence janitor and the triage loop.
func sweepInterval(heartbeat time.Duration) time.Duration {
	interval := heartbeat / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	return interval
}

func (c *Coordinator) janitor() {
	defer c.wg.Done()
	ticker := time.NewTicker(sweepInterval(c.cfg.HeartbeatInterval))
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.sweep()
		}
	}
}

func (c *Coordinator) sweep() {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	keys, err := c.cli.KVList(envelope.BucketRegistry)
	if err != nil {
		return
	}
	for id, kv := range keys {
		var rec agentcard.RegistryRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			c.log.Warn("registry record unparseable; evicting", "id", id)
			c.cli.KVDelete(envelope.BucketRegistry, id) //nolint:errcheck
			continue
		}
		// Registration grace: a fresh agent is never reaped before it has a
		// chance to beat (audit Steal #5).
		if now.Sub(rec.RegisteredAt) < c.cfg.RegistrationGrace {
			continue
		}
		silentFor := now.Sub(rec.LastSeen)
		switch {
		case silentFor > c.cfg.EvictAfter:
			// If a delayed sweep jumps straight past the away window, still
			// emit the full two-tier sequence in the audit trail.
			if rec.State == agentcard.PresenceLive {
				c.audit(id, "away")
			}
			if err := c.cli.KVDelete(envelope.BucketRegistry, id); err != nil {
				continue
			}
			c.audit(id, "evicted")
			// Reclaim the dead agent's claims before announcing the leave,
			// so anything reacting to mesh.leave already sees the paths free.
			// This sweep is the prompt reclaim path (locked decision: TTL
			// leases with reclaim-on-death); the claims-bucket TTL is only
			// the backstop for when the coordinator itself is down.
			c.releaseClaims(id, "reclaimed")
			c.publishEvict(id)
		case silentFor > c.cfg.AwayAfter && rec.State == agentcard.PresenceLive:
			rec.State = agentcard.PresenceAway
			c.putRecord(rec)
			c.audit(id, "away")
		}
	}
}

// publishEvict announces an eviction as a leave event (ARCHITECTURE §6:
// "miss N beats → mark dead → PUB mesh.leave").
func (c *Coordinator) publishEvict(id string) {
	env, err := envelope.New(envelope.KindLeave, coordinatorID, envelope.SubjectLeave,
		&envelope.LeavePayload{ID: id, Reason: "evicted"})
	if err != nil {
		return
	}
	if err := c.cli.Publish(env); err != nil {
		c.log.Warn("publish evict failed", "id", id, "err", err)
	}
}

// --- registry KV helpers (callers hold c.mu) ------------------------------------

func (c *Coordinator) getRecord(id string) (agentcard.RegistryRecord, bool) {
	kv, found, err := c.cli.KVGet(envelope.BucketRegistry, id)
	if err != nil || !found {
		return agentcard.RegistryRecord{}, false
	}
	var rec agentcard.RegistryRecord
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return agentcard.RegistryRecord{}, false
	}
	return rec, true
}

func (c *Coordinator) putRecord(rec agentcard.RegistryRecord) {
	// Every presence record is a TTL lease (locked decision): the janitor
	// does the two-tier away/evict, and the store-level TTL is the backstop
	// that self-expires a record even if the reducer/janitor wedges. Renewed
	// on every write, i.e. at least once per heartbeat. The backstop must
	// outlast every legitimate silent window — both the eviction threshold
	// and the registration grace (a fresh record may not be rewritten until
	// the first heartbeat lands).
	opts := bus.PutOptions{TTL: 2 * (c.cfg.EvictAfter + c.cfg.RegistrationGrace)}
	if _, err := c.cli.KVPut(envelope.BucketRegistry, rec.Card.ID, rec, opts); err != nil {
		c.log.Warn("registry put failed", "id", rec.Card.ID, "err", err)
	}
}

func (c *Coordinator) audit(id, event string) {
	entry := AuditEntry{Kind: envelope.AuditPresence, ID: id, Event: event, TS: time.Now().UTC()}
	if _, err := c.cli.StreamAppend(envelope.StreamAudit, entry); err != nil {
		c.log.Warn("audit append failed", "id", id, "event", event, "err", err)
	}
}

// releaseClaims frees every claim held by a departed agent and audits each
// path released. Best-effort by design: a claim that cannot be freed here is
// collected by the claims-bucket TTL, so a failure must degrade, not wedge
// the reducer/sweep.
func (c *Coordinator) releaseClaims(id, event string) {
	for _, rec := range claim.ReleaseAllOwnedBy(c.cli, id) {
		entry := AuditEntry{Kind: envelope.AuditClaim, ID: id, Event: event, Path: rec.Path, Repo: rec.Repo, TS: time.Now().UTC()}
		if _, err := c.cli.StreamAppend(envelope.StreamAudit, entry); err != nil {
			c.log.Warn("claim audit append failed", "id", id, "event", event, "path", rec.Path, "err", err)
		}
	}
}
