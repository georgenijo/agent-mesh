// Package ops implements the actuator verbs of the runtime ops plane:
// doctor (classify), down (teardown), clean (janitor). It consumes
// internal/observe facts and only ever acts on mesh-owned artifacts —
// pidfile/argv-verified processes and paths under MESH_DIR. Scope stops at
// inspect + teardown + janitor: anything that *starts* processes belongs to
// the coordinator (DECISIONS 2026-06-05).
//
// It is deliberately separate from internal/observe, which stays a read-only
// collector (and is served over HTTP by `meshd --mode observe`): nothing
// reachable from the observe plane can kill or unlink.
package ops

import (
	"fmt"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/observe"
)

// HealthState classifies one runtime entity (typed per the envelope results
// rule — never free text in a machine contract).
type HealthState string

const (
	StateHealthy     HealthState = "healthy"      // all fact sources agree
	StateOrphan      HealthState = "orphan"       // process alive, registry doesn't know it
	StateStaleSocket HealthState = "stale_socket" // socket file present, nothing serving it
	StateDeadPidfile HealthState = "dead_pidfile" // pidfile present, pid dead
)

// Verdict is the aggregate doctor outcome.
type Verdict string

const (
	VerdictClean Verdict = "clean"
	VerdictDirty Verdict = "dirty"
)

// Finding classifies one entity (coordinator or sidecar).
type Finding struct {
	Entity string      `json:"entity"` // "coordinator" or the agent name
	State  HealthState `json:"state"`
	PID    int         `json:"pid,omitempty"`
	Detail string      `json:"detail,omitempty"` // human hint, not machine contract
}

// DoctorReport is the `mesh ops doctor` contract.
type DoctorReport struct {
	Meta      observe.Meta `json:"meta"`
	Verdict   Verdict      `json:"verdict"`
	Findings  []Finding    `json:"findings"`
	Anomalies []string     `json:"anomalies,omitempty"`
}

// Diagnose is a pure classification over a snapshot: no re-collection, no
// side effects. The verdict is dirty iff any finding is unhealthy or the
// snapshot carries anomalies (e.g. a registry ghost with no on-disk artifact
// to classify still dirties the verdict via its drift anomaly).
func Diagnose(snap observe.Snapshot) DoctorReport {
	rep := DoctorReport{
		Meta:      snap.Meta,
		Verdict:   VerdictClean,
		Anomalies: snap.Anomalies,
	}

	if f, ok := diagnoseCoordinator(snap.Coordinator); ok {
		rep.Findings = append(rep.Findings, f)
	}
	for _, sc := range snap.Sidecars {
		rep.Findings = append(rep.Findings, diagnoseSidecar(sc))
	}

	for _, f := range rep.Findings {
		if f.State != StateHealthy {
			rep.Verdict = VerdictDirty
		}
	}
	if len(snap.Anomalies) > 0 {
		rep.Verdict = VerdictDirty
	}
	return rep
}

// diagnoseCoordinator classifies the control plane. A mesh dir with no
// coordinator artifacts at all (fresh or fully torn down) yields no finding —
// the lock file alone is harmless flock residue, recreated O_CREATE.
func diagnoseCoordinator(c observe.CoordinatorInfo) (Finding, bool) {
	f := Finding{Entity: "coordinator", PID: c.PID}
	switch {
	case c.PID > 0 && !c.PIDAlive:
		f.State = StateDeadPidfile
		f.Detail = "coordinator.pid points at a dead process"
	case c.BusSocketPresent && !c.BusDialable && !c.PIDAlive:
		f.State = StateStaleSocket
		f.Detail = "bus.sock exists but nothing serves it"
	case c.PIDAlive && !c.BusDialable:
		f.State = StateOrphan
		f.Detail = "coordinator process alive but the bus is not dialable"
	case c.PIDAlive || c.BusDialable:
		f.State = StateHealthy
	default:
		return Finding{}, false // nothing to classify
	}
	return f, true
}

// diagnoseSidecar classifies one sidecar entity; first match wins.
func diagnoseSidecar(sc observe.SidecarInfo) Finding {
	f := Finding{Entity: sc.Name, PID: sc.PID}
	registered := sc.Registry != nil
	switch {
	case sc.PIDAlive && !registered:
		// Covers live orphan_socket and orphan_pidfile: a running daemon the
		// registry no longer knows — the class pidfiles exist to expose.
		f.State = StateOrphan
		f.Detail = "process alive but absent from the registry"
	case sc.SocketPresent && !sc.SocketDialable && !sc.PIDAlive:
		f.State = StateStaleSocket
		f.Detail = "socket file exists but nothing serves it"
	case sc.PIDFilePID != 0 && !sc.PIDAlive:
		f.State = StateDeadPidfile
		f.Detail = fmt.Sprintf("%s points at a dead process", sc.PIDFile)
	case sc.SocketDialable && sc.PIDAlive && registered &&
		sc.Registry.State == agentcard.PresenceLive && len(sc.Drift) == 0:
		f.State = StateHealthy
	default:
		// Drifted but not matching a teardown class (e.g. presence_mismatch,
		// away-state registry record): report the strongest hint we have.
		if len(sc.Drift) > 0 {
			f.State = StateOrphan
			f.Detail = "drift: " + driftCSV(sc.Drift)
		} else {
			f.State = StateHealthy
		}
	}
	return f
}

func driftCSV(items []observe.Drift) string {
	out := ""
	for i, d := range items {
		if i > 0 {
			out += ","
		}
		out += string(d)
	}
	return out
}
