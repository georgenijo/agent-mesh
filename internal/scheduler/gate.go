package scheduler

import (
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// Gate is the scheduler's per-node view of a task: the persisted TaskState
// (the frozen wire contract, the one authority) refined by the node's
// dependency situation. COMPUTED, never persisted — the richer
// queued/runnable/blocked/skipped vocabulary maps onto the persisted states:
//
//	pending   → queued (waiting on deps) | runnable (all deps done) |
//	            blocked (a dep failed/cancelled/escalated; about to be skipped)
//	running   → running
//	done      → done
//	failed    → failed
//	cancelled → skipped (the typed terminal state for a skipped dependent)
//	escalated → escalated (terminal, awaiting human input; no retry)
type Gate string

const (
	GateQueued    Gate = "queued"
	GateRunnable  Gate = "runnable"
	GateRunning   Gate = "running"
	GateDone      Gate = "done"
	GateFailed    Gate = "failed"
	GateBlocked   Gate = "blocked"
	GateSkipped   Gate = "skipped"
	GateEscalated Gate = "escalated"
)

// GateOf computes a task's gate from its persisted state plus its
// dependencies' persisted states. byID must contain every task of the job
// (the persisted DAG read via Store.ListByJob).
func GateOf(rec task.Record, byID map[string]task.Record) Gate {
	switch rec.State {
	case envelope.TaskRunning:
		return GateRunning
	case envelope.TaskDone:
		return GateDone
	case envelope.TaskFailed:
		return GateFailed
	case envelope.TaskCancelled:
		return GateSkipped
	case envelope.TaskEscalated:
		return GateEscalated
	}
	// Pending: derived from dependencies.
	if _, blocked := blockingDep(rec, byID); blocked {
		return GateBlocked
	}
	for _, dep := range rec.DependsOn {
		if byID[dep].State != envelope.TaskDone {
			return GateQueued
		}
	}
	return GateRunnable
}

// blockingDep returns a dependency that can never complete (failed,
// cancelled, escalated, or missing from the persisted DAG — a referential
// break that must never spawn work).
func blockingDep(rec task.Record, byID map[string]task.Record) (string, bool) {
	for _, dep := range rec.DependsOn {
		d, ok := byID[dep]
		if !ok {
			return dep, true
		}
		if d.State == envelope.TaskFailed || d.State == envelope.TaskCancelled || d.State == envelope.TaskEscalated {
			return dep, true
		}
	}
	return "", false
}
