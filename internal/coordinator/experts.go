package coordinator

// Autonomous on-demand experts (#117). When MESH_AUTO_EXPERTS is armed, the
// coordinator watches the two role-owner triggers — role-addressed asks
// (mesh.ask.role.<role>) and #80 review requests (mesh.review-req.<role>) — and
// the instant one targets a role NO live agent fills, it launches a resident
// `meshd --mode expert --role <role>` itself. The asker's sidecar no longer
// short-circuits a role-ask with no owner (see ensureResponder), so the ticket
// is created and the ask published for the coordinator to observe here.
//
// The triggering message is published a beat before the fresh expert is
// listening (sidecar.Start registers, then subscribes), so it would otherwise
// be missed. The spawner buffers the trigger and RE-DELIVERS it once the expert
// is live and (for reviews) has signalled subscription readiness — the cold
// first ask/review gets answered, not just later ones. On a ready-wait timeout
// it still re-delivers immediately and schedules spaced follow-up retries.
// Re-delivery is safe: an already-accepted ticket rejects a duplicate via its
// CAS guard; duplicate review requests are correlated by task id.
//
// This is the control-plane spawn seam the architecture always called for ("the
// Expert pool is on-demand responder agents the coordinator spawns when no live
// agent owns a topic"). It stays out of the data path: the question/answer
// payloads still travel directly between the asker's and expert's sidecars.

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

const (
	askRolePrefix   = "mesh.ask.role."
	reviewReqPrefix = "mesh.review-req."

	// autoExpertSpawnWait bounds how long we wait for a freshly launched expert
	// to register as a live owner of its role before giving up. On timeout the
	// asker's ticket simply TTL-expires unanswered — honest, never a fake answer.
	// Generous: spawn + runtime child start + first register is seconds.
	autoExpertSpawnWait = 30 * time.Second
	// autoExpertPollInterval is the registry poll cadence while waiting for live
	// and for the review-subscription readiness signal.
	autoExpertPollInterval = 250 * time.Millisecond
)

// Package-level vars (not consts) so unit tests can shorten the readiness
// window and retry schedule without waiting on production timeouts.
var (
	// autoExpertReadyWait bounds how long we wait for the expert to signal that
	// its review subscription is active (BucketExpertReady/<name> written by
	// ServeReviews). This replaces the old fixed 500ms settle: the expert writes
	// the key only after its subscription is established, so re-delivery is safe
	// the moment the key appears (#125). On timeout we re-deliver anyway and
	// schedule spaced follow-up re-delivers (see autoExpertRedeliverBackoff).
	autoExpertReadyWait = 10 * time.Second
	// autoExpertRedeliverBackoff is the schedule of follow-up re-delivers after a
	// ready-wait timeout, measured from the first (immediate) re-deliver. Duplicate
	// review requests are OK — the scheduler correlates by task id.
	autoExpertRedeliverBackoff = []time.Duration{1 * time.Second, 3 * time.Second}
)

// expertSpawner owns autonomous expert lifecycle for one coordinator: the
// trigger subscriptions, the per-expert spawn state, and teardown of the
// children it launched.
//
// Pool support (#123): for the review role, up to cfg.ReviewPoolSize resident
// experts are maintained so multiple diffs can be reviewed concurrently. The
// spawner keeps one entry per expert (keyed by name, unique across the pool),
// which allows multiple entries for the same role.
type expertSpawner struct {
	cli   *bus.Client
	cfg   config.Config
	log   *slog.Logger
	meshd string // resolved meshd binary path (the daemon we re-exec in expert mode)

	// launchFunc starts a meshd --mode expert child. Defaults to e.launch;
	// replaced in tests to avoid real process spawning.
	launchFunc func(role, name string) (*os.Process, error)

	mu sync.Mutex
	// experts is keyed by expert name (unique per instance) rather than role,
	// so the pool can hold N entries for the same role.
	experts map[string]*roleExpert
	seq     int // monotonic suffix so a re-spawn never collides with an away ghost
	stopped bool
	subs    []*bus.Subscription
	wg      sync.WaitGroup
}

// roleExpert tracks the spawn lifecycle for a single role. It exists from the
// first un-owned trigger until the role's expert is live (or the spawn failed).
type roleExpert struct {
	role    string
	name    string
	proc    *os.Process         // the launched meshd --mode expert; nil until started
	live    bool                // true once the expert registered as a live role owner
	pending []envelope.Envelope // triggers buffered during spawn, re-delivered on live
}

// newExpertSpawner resolves the meshd binary up front so a misconfigured deploy
// fails loudly at arm time rather than silently never spawning.
func newExpertSpawner(cli *bus.Client, cfg config.Config, log *slog.Logger) (*expertSpawner, error) {
	meshd, err := findMeshd()
	if err != nil {
		return nil, err
	}
	sp := &expertSpawner{
		cli:     cli,
		cfg:     cfg,
		log:     log,
		meshd:   meshd,
		experts: map[string]*roleExpert{},
	}
	sp.launchFunc = sp.launch
	return sp, nil
}

// start subscribes to the two role-owner trigger subjects.
func (e *expertSpawner) start() error {
	ask, err := e.cli.Subscribe(envelope.PatternAsks, e.handle)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", envelope.PatternAsks, err)
	}
	rev, err := e.cli.Subscribe(reviewReqPrefix+">", e.handle)
	if err != nil {
		ask.Unsubscribe()
		return fmt.Errorf("subscribe %s>: %w", reviewReqPrefix, err)
	}
	e.subs = append(e.subs, ask, rev)
	return nil
}

// handle runs on a bus delivery goroutine. It is the single decision point:
// is this an un-owned role trigger, and if so, is a spawn already underway?
func (e *expertSpawner) handle(env envelope.Envelope) {
	role := roleFromSubject(env.Subject)
	if role == "" || !agentcard.ValidName(role) {
		return
	}
	// React only to the genuine trigger kinds, not answers/verdicts/tickets that
	// may share a subject tree.
	switch {
	case strings.HasPrefix(env.Subject, askRolePrefix):
		if env.Kind != envelope.KindAsk {
			return
		}
	case strings.HasPrefix(env.Subject, reviewReqPrefix):
		if env.Kind != envelope.KindReviewRequest {
			return
		}
	default:
		return
	}

	// For the review pool (#123): check whether the pool is at full capacity
	// (live + in-flight ≥ target) before falling through to the live-owner check.
	target := e.targetForRole(role)
	if target > 1 {
		// Pool role: react to the trigger by ensuring the full pool is spawned.
		// If the pool is already at capacity (live or spawning), nothing to do.
		e.mu.Lock()
		if e.stopped {
			e.mu.Unlock()
			return
		}
		active := e.activeCountForRole(role)
		if active >= target {
			// Pool is full; if any member is live, the request will be delivered
			// normally. If all are still spawning, buffer in any pending member.
			if anyLive := e.anyLiveForRole(role); anyLive {
				e.mu.Unlock()
				return
			}
			if first := e.firstPendingForRole(role); first != nil {
				first.pending = append(first.pending, env)
			}
			e.mu.Unlock()
			return
		}
		// Pool is below target: spawn enough to fill it. The first new expert
		// gets the buffered trigger; extras are pre-emptive (no pending).
		firstNew := true
		for active < target {
			e.seq++
			var pending []envelope.Envelope
			if firstNew {
				pending = []envelope.Envelope{env}
				firstNew = false
			}
			re := &roleExpert{role: role, name: expertName(role, e.seq), pending: pending}
			e.experts[re.name] = re
			e.wg.Add(1)
			go e.spawnAndServe(re)
			active++
		}
		e.mu.Unlock()
		return
	}

	// Single-expert path (target == 1, the pre-#123 behaviour).
	if e.hasLiveOwner(role) {
		return // a live agent already fills this role; normal delivery handles it
	}

	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return
	}
	// Find an existing entry for this role (there can be at most one for target=1).
	re := e.firstForRole(role)
	if re == nil {
		e.seq++
		re = &roleExpert{role: role, name: expertName(role, e.seq), pending: []envelope.Envelope{env}}
		e.experts[re.name] = re
		e.wg.Add(1)
		go e.spawnAndServe(re)
		e.mu.Unlock()
		return
	}
	if re.live {
		// Expert came up but this trigger landed in the register→subscribe gap;
		// re-send it. (hasLiveOwner above is usually true in this case and we'd
		// have returned, but cover the race.)
		e.mu.Unlock()
		e.redeliver(env)
		return
	}
	re.pending = append(re.pending, env) // still spawning: buffer for re-delivery
	e.mu.Unlock()
}

// spawnAndServe launches the expert, waits for it to become a live agent and
// signal review-subscription readiness, then re-delivers everything buffered
// during the cold start. The readiness poll replaces the old fixed settle delay
// (#125): we wait for BucketExpertReady/<name> (written by ServeReviews after
// its subscription is active) rather than sleeping a fixed interval that can
// expire before the subscription is established.
func (e *expertSpawner) spawnAndServe(re *roleExpert) {
	defer e.wg.Done()

	proc, err := e.launchFunc(re.role, re.name)
	if err != nil {
		e.log.Warn("auto-expert: launch failed", "role", re.role, "name", re.name, "err", err)
		e.forgetByName(re.name) // let a later trigger retry the spawn
		return
	}
	e.mu.Lock()
	re.proc = proc
	e.mu.Unlock()
	if proc != nil {
		e.log.Info("auto-expert: spawned", "role", re.role, "name", re.name, "pid", proc.Pid)
	}

	deadline := time.Now().Add(autoExpertSpawnWait)
	for {
		if e.stopping() {
			return
		}
		// Wait for THIS specific expert to register as live (not just any agent
		// for the role, since the pool may have multiple experts coming up).
		if e.hasLiveAgent(re.name) {
			break
		}
		if time.Now().After(deadline) {
			e.log.Warn("auto-expert: never became live; pending asks will expire",
				"role", re.role, "name", re.name)
			e.forgetByName(re.name)
			return
		}
		time.Sleep(autoExpertPollInterval)
	}

	// Wait for the expert's ServeReviews subscription to be active (#125).
	// The expert writes BucketExpertReady/<name> after subscribing; we poll
	// for it rather than sleeping a fixed delay that may be shorter than the
	// runtime startup time. On timeout we re-deliver anyway and schedule
	// spaced follow-up re-delivers so a late subscription still catches the
	// cold first request.
	readyDeadline := time.Now().Add(autoExpertReadyWait)
	ready := false
	for {
		if e.stopping() {
			return
		}
		if e.hasExpertReady(re.name) {
			ready = true
			break
		}
		if time.Now().After(readyDeadline) {
			e.log.Warn("auto-expert: review subscription not confirmed in time; re-delivering anyway",
				"role", re.role, "name", re.name)
			break
		}
		time.Sleep(autoExpertPollInterval)
	}

	// Re-check at the moment of re-deliver: the ready key may have landed in
	// the last poll interval, or we may still be in the timeout path.
	readyAtRedeliver := ready || e.hasExpertReady(re.name)

	e.mu.Lock()
	re.live = true
	pending := re.pending
	re.pending = nil
	e.mu.Unlock()

	e.log.Info("auto-expert: live; re-delivering",
		"role", re.role, "name", re.name, "pending", len(pending), "ready", readyAtRedeliver)
	for _, env := range pending {
		e.redeliver(env)
	}

	// Ready-timeout hardening: if the readiness key was still missing when we
	// re-delivered, schedule 2–3 spaced retries of the same pending envelopes.
	// Idempotent — duplicate KindReviewRequest is OK (scheduler correlates by
	// task id). Gated on !stopping() so coordinator teardown does not linger.
	if !readyAtRedeliver && len(pending) > 0 {
		e.scheduleReadyTimeoutRedelivers(re, pending)
	}
}

// scheduleReadyTimeoutRedelivers re-publishes pending triggers at the offsets
// in autoExpertRedeliverBackoff (measured from now / the first re-deliver).
// Runs on a spawn-tracked goroutine so stop() waits for it to drain.
func (e *expertSpawner) scheduleReadyTimeoutRedelivers(re *roleExpert, pending []envelope.Envelope) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		start := time.Now()
		for i, at := range autoExpertRedeliverBackoff {
			if e.sleepOrStop(at - time.Since(start)) {
				return
			}
			e.log.Info("auto-expert: ready-timeout re-deliver retry",
				"role", re.role, "name", re.name, "attempt", i+1,
				"of", len(autoExpertRedeliverBackoff), "pending", len(pending))
			for _, env := range pending {
				e.redeliver(env)
			}
		}
	}()
}

// sleepOrStop sleeps until d elapses or the spawner is stopping. Returns true
// if stopping (caller should abort), false if the full duration elapsed.
func (e *expertSpawner) sleepOrStop(d time.Duration) bool {
	if d <= 0 {
		return e.stopping()
	}
	deadline := time.Now().Add(d)
	for {
		if e.stopping() {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		sleep := remaining
		if sleep > autoExpertPollInterval {
			sleep = autoExpertPollInterval
		}
		time.Sleep(sleep)
	}
}

// launch starts `meshd --mode expert` as a child of this coordinator (its own
// process group, so stop() can signal it), inheriting the environment so
// MESH_EXPERT_CLI and friends carry through. Output is captured to a per-expert
// log under $MESH_DIR/logs.
func (e *expertSpawner) launch(role, name string) (*os.Process, error) {
	logDir := filepath.Join(e.cfg.MeshDir, "logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	logFile, err := os.OpenFile(filepath.Join(logDir, name+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	defer logFile.Close()

	// --mesh-dir is the ops-plane ownership marker (matches autostart): it lets
	// `mesh ops down` verify a pid belongs to THIS mesh by its argv.
	cmd := exec.Command(e.meshd,
		"--mode", "expert",
		"--name", name,
		"--role", role,
		"--mesh-dir", e.cfg.MeshDir,
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start meshd: %w", err)
	}
	return cmd.Process, nil
}

// redeliver re-publishes a buffered trigger so the now-listening expert's
// handleIncomingAsk / ServeReviews picks it up. Idempotent at the ticket layer.
func (e *expertSpawner) redeliver(env envelope.Envelope) {
	if err := e.cli.Publish(env); err != nil {
		e.log.Warn("auto-expert: re-deliver failed", "subject", env.Subject, "err", err)
	}
}

// hasLiveOwner reports whether any registry record is a live agent filling role.
func (e *expertSpawner) hasLiveOwner(role string) bool {
	keys, err := e.cli.KVList(envelope.BucketRegistry)
	if err != nil {
		return false
	}
	for _, kv := range keys {
		var rec agentcard.RegistryRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		if rec.State == agentcard.PresenceLive && rec.Card.Role == role {
			return true
		}
	}
	return false
}

// hasLiveAgent reports whether the registry has a live entry for the named agent.
func (e *expertSpawner) hasLiveAgent(name string) bool {
	kvs, err := e.cli.KVList(envelope.BucketRegistry)
	if err != nil {
		return false
	}
	for _, kv := range kvs {
		var rec agentcard.RegistryRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		if rec.State == agentcard.PresenceLive && rec.Card.Name == name {
			return true
		}
	}
	return false
}

// hasExpertReady reports whether the expert has written its review-readiness
// marker (BucketExpertReady/<name>), meaning ServeReviews has subscribed (#125).
func (e *expertSpawner) hasExpertReady(name string) bool {
	_, found, err := e.cli.KVGet(envelope.BucketExpertReady, name)
	return err == nil && found
}

// targetForRole returns the desired number of experts for role. For the
// review role this is cfg.ReviewPoolSize; for all other roles it is 1.
func (e *expertSpawner) targetForRole(role string) int {
	if role == e.cfg.ReviewRole && e.cfg.ReviewPoolSize > 1 {
		return e.cfg.ReviewPoolSize
	}
	return 1
}

// activeCountForRole counts entries in the experts map for role (must hold mu).
func (e *expertSpawner) activeCountForRole(role string) int {
	count := 0
	for _, re := range e.experts {
		if re.role == role {
			count++
		}
	}
	return count
}

// anyLiveForRole reports whether any expert entry for role is marked live
// (must hold mu).
func (e *expertSpawner) anyLiveForRole(role string) bool {
	for _, re := range e.experts {
		if re.role == role && re.live {
			return true
		}
	}
	return false
}

// firstForRole returns the first expert entry for role, or nil (must hold mu).
func (e *expertSpawner) firstForRole(role string) *roleExpert {
	for _, re := range e.experts {
		if re.role == role {
			return re
		}
	}
	return nil
}

// firstPendingForRole returns the first non-live expert entry for role, or nil
// (must hold mu).
func (e *expertSpawner) firstPendingForRole(role string) *roleExpert {
	for _, re := range e.experts {
		if re.role == role && !re.live {
			return re
		}
	}
	return nil
}

func (e *expertSpawner) forgetByName(name string) {
	e.mu.Lock()
	delete(e.experts, name)
	e.mu.Unlock()
}

func (e *expertSpawner) stopping() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stopped
}

// stop unsubscribes the triggers and SIGTERMs every expert this coordinator
// launched (a graceful leave — the expert traps the signal and deregisters),
// then waits for the spawn goroutines to drain. Experts orphaned by a hard
// coordinator crash are NOT reaped here (a restarted coordinator forgets them);
// they re-register and are reused, and idle reaping is the separate #105.
func (e *expertSpawner) stop() {
	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return
	}
	e.stopped = true
	subs := e.subs
	e.subs = nil
	var procs []*os.Process
	for _, re := range e.experts {
		if re.proc != nil {
			procs = append(procs, re.proc)
		}
	}
	e.mu.Unlock()

	for _, s := range subs {
		s.Unsubscribe()
	}
	for _, p := range procs {
		_ = p.Signal(syscall.SIGTERM)
	}
	e.wg.Wait()
}

// findMeshd locates the meshd binary to re-exec in expert mode: $MESH_MESHD if
// set, else the currently running executable — which, in the coordinator, IS
// meshd. Mirrors autostart.FindMeshd's trust posture (no bare $PATH search) but
// is inlined here to keep the coordinator off the autostart→observe import path.
func findMeshd() (string, error) {
	if p := os.Getenv(config.EnvMeshdBin); p != "" {
		return p, nil
	}
	self, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate meshd: %w (set %s)", err, config.EnvMeshdBin)
	}
	return self, nil
}

func roleFromSubject(subject string) string {
	if r := strings.TrimPrefix(subject, askRolePrefix); r != subject {
		return r
	}
	if r := strings.TrimPrefix(subject, reviewReqPrefix); r != subject {
		return r
	}
	return ""
}

// expertName is a stable-ish, registry-valid id for a spawned expert. The seq
// suffix keeps a re-spawn from colliding with an away-but-not-yet-evicted ghost
// of a prior expert for the same role.
func expertName(role string, seq int) string {
	name := fmt.Sprintf("expert-%s-%d", role, seq)
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}
