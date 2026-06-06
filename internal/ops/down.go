package ops

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/observe"
)

// TargetKind is what a teardown target is.
type TargetKind string

const (
	KindCoordinator TargetKind = "coordinator"
	KindSidecar     TargetKind = "sidecar"
	KindChild       TargetKind = "child"
	KindService     TargetKind = "service" // dashboard / observe HTTP daemons
)

// TargetSource is the fact source a target pid came from.
type TargetSource string

const (
	SourcePidfile  TargetSource = "pidfile"
	SourceRegistry TargetSource = "registry"
	SourceRuntime  TargetSource = "runtime" // sidecar runtime IPC
)

// KillOutcome is the per-target result of a teardown.
type KillOutcome string

const (
	KillTerminated        KillOutcome = "terminated"         // exited after SIGTERM
	KillKilled            KillOutcome = "killed"             // required SIGKILL
	KillAlreadyDead       KillOutcome = "already_dead"       // nothing to signal
	KillSkippedUnverified KillOutcome = "skipped_unverified" // ownership unproven; never signaled
	KillFailed            KillOutcome = "failed"             // signal error or alive after SIGKILL
)

// DownTarget is one process in the teardown set.
type DownTarget struct {
	PID     int            `json:"pid"`
	Kind    TargetKind     `json:"kind"`
	Name    string         `json:"name,omitempty"`
	Sources []TargetSource `json:"sources"`
	Outcome KillOutcome    `json:"outcome"`
	Detail  string         `json:"detail,omitempty"`
}

// DownReport is the `mesh ops down` contract.
type DownReport struct {
	Meta    observe.Meta `json:"meta"`
	Targets []DownTarget `json:"targets"`
	Clean   bool         `json:"clean"` // final verify: zero targets left running
}

// DownOptions tunes the teardown.
type DownOptions struct {
	TermTimeout time.Duration // SIGTERM grace before SIGKILL escalation (default 5s)
}

const (
	defaultTermTimeout = 5 * time.Second
	killVerifyTimeout  = time.Second
	downPollInterval   = 50 * time.Millisecond
)

// Down gracefully tears down every mesh-owned process under cfg.MeshDir:
// SIGTERM → poll → SIGKILL after the timeout → verify zero alive.
//
// The pid set is the union of the three fact sources (pidfiles ∪ registry KV
// ∪ runtime IPC) as gathered by observe.Collect. Before any signal, each
// daemon pid is ownership-verified by matching `--mesh-dir <dir>` in its argv
// via ps — never by process name: multiple meshes coexist per machine and a
// name match would kill them all (DECISIONS 2026-06-05). Child processes
// carry no marker; their ownership is transitive — the pid was reported
// moments ago by this mesh's own sidecar over its socket. The sub-second
// report-to-signal pid-reuse window is the accepted residual risk.
//
// Sidecars are terminated before the coordinator so their SIGTERM handler
// (graceful leave) can still publish on the bus.
func Down(cfg config.Config, opts DownOptions) (DownReport, error) {
	if opts.TermTimeout <= 0 {
		opts.TermTimeout = defaultTermTimeout
	}

	snap, err := observe.Collect(cfg)
	if err != nil {
		return DownReport{}, err
	}
	rep := DownReport{Meta: snap.Meta, Targets: gatherTargets(snap)}

	// Partition into the signal plan. Outcomes for dead/unverified targets
	// are final before anything is signaled.
	var sidecars, children, coordinators []*DownTarget
	alive := aliveByPS(targetPIDs(rep.Targets))
	for i := range rep.Targets {
		t := &rep.Targets[i]
		if !alive[t.PID] {
			t.Outcome = KillAlreadyDead
			continue
		}
		if t.Kind != KindChild {
			ok, detail := argvOwned(t.PID, cfg.MeshDir)
			if !ok {
				t.Outcome = KillSkippedUnverified
				t.Detail = detail
				continue
			}
		}
		switch t.Kind {
		case KindCoordinator:
			coordinators = append(coordinators, t)
		case KindChild:
			children = append(children, t)
		default:
			sidecars = append(sidecars, t)
		}
	}

	deadline := time.Now().Add(opts.TermTimeout)
	signalAll(append(children, sidecars...), syscall.SIGTERM)
	pollUntilDead(append(children, sidecars...), deadline)
	signalAll(coordinators, syscall.SIGTERM)
	stragglers := pollUntilDead(append(append(children, sidecars...), coordinators...), deadline)

	// Escalate. SIGKILL cannot be caught; anything still alive after the
	// verify window is reported as failed.
	signalAll(stragglers, syscall.SIGKILL)
	for _, t := range pollUntilDead(stragglers, time.Now().Add(killVerifyTimeout)) {
		t.Outcome = KillFailed
		t.Detail = "still alive after SIGKILL"
	}
	for _, t := range stragglers {
		if t.Outcome == "" {
			t.Outcome = KillKilled
		}
	}

	rep.Clean = true
	finalAlive := aliveByPS(targetPIDs(rep.Targets))
	for i := range rep.Targets {
		t := &rep.Targets[i]
		if finalAlive[t.PID] {
			rep.Clean = false
		} else if t.Outcome == "" {
			t.Outcome = KillTerminated
		}
	}
	return rep, nil
}

// gatherTargets builds the deduped, source-tagged pid set from a snapshot.
func gatherTargets(snap observe.Snapshot) []DownTarget {
	byPID := map[int]*DownTarget{}
	var order []int

	add := func(pid int, kind TargetKind, name string, src TargetSource) {
		// Guard rails: never target init, ourselves, or our parent (a test
		// runner's pid can end up in a planted pidfile).
		if pid <= 1 || pid == os.Getpid() || pid == os.Getppid() {
			return
		}
		t, ok := byPID[pid]
		if !ok {
			byPID[pid] = &DownTarget{PID: pid, Kind: kind, Name: name, Sources: []TargetSource{src}}
			order = append(order, pid)
			return
		}
		for _, s := range t.Sources {
			if s == src {
				return
			}
		}
		t.Sources = append(t.Sources, src)
	}

	if snap.Coordinator.PID > 0 {
		add(snap.Coordinator.PID, KindCoordinator, "coordinator", SourcePidfile)
	}
	// Services (dashboard/observe) ride the pre-coordinator SIGTERM bucket via
	// the partition default and are argv-ownership-verified like sidecars —
	// `mesh up` spawns them with the --mesh-dir marker.
	for _, svc := range snap.Services {
		if svc.PID > 0 {
			add(svc.PID, KindService, svc.Name, SourcePidfile)
		}
	}
	for _, sc := range snap.Sidecars {
		if sc.PIDFilePID > 0 {
			add(sc.PIDFilePID, KindSidecar, sc.Name, SourcePidfile)
		}
		if sc.Registry != nil && sc.Registry.Card.PID > 0 {
			add(sc.Registry.Card.PID, KindSidecar, sc.Name, SourceRegistry)
		}
		if sc.SocketDialable && sc.PID > 0 {
			add(sc.PID, KindSidecar, sc.Name, SourceRuntime)
		}
	}
	for _, ch := range snap.Children {
		if ch.State == "running" && ch.PID > 0 {
			add(ch.PID, KindChild, ch.Sidecar+": "+ch.Cmd, SourceRuntime)
		}
	}

	out := make([]DownTarget, 0, len(order))
	for _, pid := range order {
		out = append(out, *byPID[pid])
	}
	return out
}

func targetPIDs(targets []DownTarget) []int {
	pids := make([]int, 0, len(targets))
	for _, t := range targets {
		pids = append(pids, t.PID)
	}
	return pids
}

func signalAll(targets []*DownTarget, sig syscall.Signal) {
	for _, t := range targets {
		if err := syscall.Kill(t.PID, sig); err != nil && err != syscall.ESRCH {
			t.Outcome = KillFailed
			t.Detail = fmt.Sprintf("signal %s: %v", sig, err)
		}
	}
}

// pollUntilDead waits for the targets to exit and returns the survivors.
// Targets already carrying a terminal outcome (failed signal) are skipped.
func pollUntilDead(targets []*DownTarget, deadline time.Time) []*DownTarget {
	pending := make([]*DownTarget, 0, len(targets))
	for _, t := range targets {
		if t.Outcome == "" {
			pending = append(pending, t)
		}
	}
	for len(pending) > 0 {
		pids := make([]int, len(pending))
		for i, t := range pending {
			pids[i] = t.PID
		}
		alive := aliveByPS(pids)
		next := pending[:0]
		for _, t := range pending {
			if alive[t.PID] {
				next = append(next, t)
			}
		}
		pending = next
		if len(pending) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(downPollInterval)
	}
	return pending
}

// aliveByPS reports which pids are actually running, via one batched ps
// call. Unlike a signal-0 probe it treats unreaped zombies as dead: a zombie
// holds no sockets and runs no code — reaping it is its parent's job, not a
// teardown failure.
func aliveByPS(pids []int) map[int]bool {
	out := make(map[int]bool, len(pids))
	if len(pids) == 0 {
		return out
	}
	args := []string{"-o", "pid=,state=", "-p"}
	strs := make([]string, len(pids))
	for i, pid := range pids {
		strs[i] = strconv.Itoa(pid)
	}
	args = append(args, strings.Join(strs, ","))
	raw, err := exec.Command("ps", args...).Output()
	if err != nil {
		// ps exits non-zero when none of the pids exist; with partial
		// matches it still prints the live ones, so parse what we got.
		if _, ok := err.(*exec.ExitError); !ok {
			// ps itself unavailable: fall back to signal-0.
			for _, pid := range pids {
				out[pid] = observe.PIDAlive(pid)
			}
			return out
		}
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		if !strings.HasPrefix(fields[1], "Z") {
			out[pid] = true
		}
	}
	return out
}

// argvOwned verifies that the process's argv carries this mesh's ownership
// marker: a `--mesh-dir <dir>` (or `--mesh-dir=<dir>`, single or double
// dash) token pair matching cfg.MeshDir, raw or symlink-resolved (darwin
// tempdirs live under /var → /private/var). Token-exact comparison kills the
// /tmp/mesh1-vs-/tmp/mesh10 prefix-collision class.
func argvOwned(pid int, meshDir string) (bool, string) {
	raw, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	if err != nil {
		return false, "argv unreadable: " + err.Error()
	}
	want := map[string]struct{}{meshDir: {}}
	if resolved, err := filepath.EvalSymlinks(meshDir); err == nil {
		want[resolved] = struct{}{}
	}
	fields := strings.Fields(string(raw))
	for i, f := range fields {
		name, inline, hasInline := strings.Cut(strings.TrimLeft(f, "-"), "=")
		if name != "mesh-dir" {
			continue
		}
		cand := inline
		if !hasInline {
			if i+1 >= len(fields) {
				continue
			}
			cand = fields[i+1]
		}
		if _, ok := want[cand]; ok {
			return true, ""
		}
		if resolved, err := filepath.EvalSymlinks(cand); err == nil {
			if _, ok := want[resolved]; ok {
				return true, ""
			}
		}
	}
	return false, "argv lacks --mesh-dir " + meshDir + "; not signaled"
}
