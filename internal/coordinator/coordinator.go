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
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/claim"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// coordinatorID is the From id the coordinator uses on the bus.
const coordinatorID = "coordinator"

// AuditEntry is one transition record appended to the audit stream.
// Presence entries track an agent's lifecycle; claim entries track the
// coordinator freeing a departed agent's claims (Path/Repo identify which).
type AuditEntry struct {
	Kind  string    `json:"kind"`  // "presence" | "claim"
	ID    string    `json:"id"`    // the agent
	Event string    `json:"event"` // presence: registered|left|away|recovered|evicted; claim: released|reclaimed
	Path  string    `json:"path,omitempty"`
	Repo  string    `json:"repo,omitempty"`
	TS    time.Time `json:"ts"`
}

// Coordinator runs the bus server and the registry reducer.
type Coordinator struct {
	cfg config.Config
	log *slog.Logger

	srv *bus.Server
	cli *bus.Client

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
	c.srv = bus.NewServer(c.cfg.BusSocket(), bus.Options{StreamDir: c.cfg.StreamsDir()})
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
	if c.cli != nil {
		c.cli.Close()
	}
	if c.srv != nil {
		c.srv.Stop()
	}
}

// --- reducer -------------------------------------------------------------------

// reduce dispatches every tapped envelope to the matching presence handler.
// Runs on the subscription's single delivery goroutine, so events for one
// agent are reduced in publish order. Non-presence kinds (P1+ announce/claim/
// note/...) are not control-plane events and are ignored here.
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

func (c *Coordinator) janitor() {
	defer c.wg.Done()
	interval := c.cfg.HeartbeatInterval / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
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
	entry := AuditEntry{Kind: "presence", ID: id, Event: event, TS: time.Now().UTC()}
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
		entry := AuditEntry{Kind: "claim", ID: id, Event: event, Path: rec.Path, Repo: rec.Repo, TS: time.Now().UTC()}
		if _, err := c.cli.StreamAppend(envelope.StreamAudit, entry); err != nil {
			c.log.Warn("claim audit append failed", "id", id, "event", event, "path", rec.Path, "err", err)
		}
	}
}
