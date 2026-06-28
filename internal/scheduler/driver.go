package scheduler

import (
	"context"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// The worker-driver seam (#25 ↔ #26). The scheduler decides WHEN a task runs
// and what its outcome means for the DAG; the driver decides HOW a task runs.
// #25 ships a fake driver in tests and the provisional CLIDriver; #26 (worker
// runtime: worktree-per-worker isolation, diff collection, richer prompts)
// supplies the real driver behind this same interface — swapping it is
// injection at coordinator wiring, never a scheduler change.

// Driver creates one Worker per task dispatch.
type Driver interface {
	// Spawn allocates whatever the task's worker needs (process, worktree,
	// session) without blocking on the work itself. A Spawn error is a typed
	// spawn_failed outcome; once Spawn succeeds the scheduler guarantees
	// Teardown is called exactly once, no matter how Run ends.
	Spawn(ctx context.Context, t task.Record) (Worker, error)
}

// Worker executes exactly one task.
type Worker interface {
	// Run executes the task to completion (or ctx cancellation) and returns a
	// typed Result. A non-nil error means the run could not produce a typed
	// result at all (crash, transport failure) and maps to worker_failed.
	Run(ctx context.Context) (Result, error)
	// Teardown releases the worker's resources. Called exactly once per
	// spawned worker by the scheduler — drivers must not self-teardown.
	Teardown() error
}

// Result is the typed outcome of one worker run. Never fake-success: a zero
// Code means the run's success discriminators all passed; any failure carries
// a typed envelope.WorkerErrorCode the scheduler's policy switches on.
type Result struct {
	Code      envelope.WorkerErrorCode // empty = success
	Summary   string                   // model text / diff summary (opaque)
	CostUSD   float64                  // the run's total_cost_usd (reported even for failed runs)
	SessionID string                   // runtime session, for traceability
	Branch    string                   // worker output branch holding the committed work (success only); the scheduler records it on the task so dependents inherit it
	Model     string                   // model name used for this run (for per-model cost breakdown); empty = unknown
	Agent     string                   // mesh agent name of the worker (w-<id>); empty = unknown
}

// Succeeded reports a typed success.
func (r Result) Succeeded() bool { return r.Code == "" }
