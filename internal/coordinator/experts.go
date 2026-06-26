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
// is live — the cold first ask gets answered, not just later ones. Re-delivery
// is safe: an already-accepted ticket rejects a duplicate via its CAS guard.
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
	// autoExpertPollInterval is the registry poll cadence while waiting for live.
	autoExpertPollInterval = 250 * time.Millisecond
	// autoExpertSettle covers the window between an agent registering (live) and
	// activating its ask subscription — sidecar.Start registers first, then
	// subscribes. We observe "live" by polling (already well past the
	// sub-millisecond gap) and settle a touch more before re-delivering.
	autoExpertSettle = 500 * time.Millisecond
)

// expertSpawner owns autonomous expert lifecycle for one coordinator: the
// trigger subscriptions, the per-role spawn state, and teardown of the children
// it launched.
type expertSpawner struct {
	cli   *bus.Client
	cfg   config.Config
	log   *slog.Logger
	meshd string // resolved meshd binary path (the daemon we re-exec in expert mode)

	mu      sync.Mutex
	roles   map[string]*roleExpert
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
	return &expertSpawner{
		cli:   cli,
		cfg:   cfg,
		log:   log,
		meshd: meshd,
		roles: map[string]*roleExpert{},
	}, nil
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

	if e.hasLiveOwner(role) {
		return // a live agent already fills this role; normal delivery handles it
	}

	e.mu.Lock()
	if e.stopped {
		e.mu.Unlock()
		return
	}
	re := e.roles[role]
	if re == nil {
		e.seq++
		re = &roleExpert{role: role, name: expertName(role, e.seq), pending: []envelope.Envelope{env}}
		e.roles[role] = re
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

// spawnAndServe launches the expert, waits for it to become a live role owner,
// then re-delivers everything buffered during the cold start.
func (e *expertSpawner) spawnAndServe(re *roleExpert) {
	defer e.wg.Done()

	proc, err := e.launch(re.role, re.name)
	if err != nil {
		e.log.Warn("auto-expert: launch failed", "role", re.role, "name", re.name, "err", err)
		e.forget(re.role) // let a later ask retry the spawn
		return
	}
	e.mu.Lock()
	re.proc = proc
	e.mu.Unlock()
	e.log.Info("auto-expert: spawned", "role", re.role, "name", re.name, "pid", proc.Pid)

	deadline := time.Now().Add(autoExpertSpawnWait)
	for {
		if e.stopping() {
			return
		}
		if e.hasLiveOwner(re.role) {
			break
		}
		if time.Now().After(deadline) {
			e.log.Warn("auto-expert: never became live; pending asks will expire",
				"role", re.role, "name", re.name)
			e.forget(re.role)
			return
		}
		time.Sleep(autoExpertPollInterval)
	}
	time.Sleep(autoExpertSettle)

	e.mu.Lock()
	re.live = true
	pending := re.pending
	re.pending = nil
	e.mu.Unlock()

	e.log.Info("auto-expert: live; re-delivering", "role", re.role, "name", re.name, "pending", len(pending))
	for _, env := range pending {
		e.redeliver(env)
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

func (e *expertSpawner) forget(role string) {
	e.mu.Lock()
	delete(e.roles, role)
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
	for _, re := range e.roles {
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
