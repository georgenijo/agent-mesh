package envelope

import "fmt"

// Scheduler-plane wire vocabulary (#25): subjects, result enums, and payloads
// for the worker-outcome and fleet-state observability events. Additive to
// the frozen contract — JobState/TaskState are untouched; the scheduler's
// richer per-node view (queued/runnable/blocked/skipped) is COMPUTED from a
// task's dependencies plus its persisted TaskState, never persisted (see
// internal/scheduler.Gate).

// SubjectWorker names the worker-outcome observability event (KindWorker,
// #25), one per worker run on a task. The tasks KV record is the authority
// for task state; this event only lets taps see how a run ended (typed
// result, failure class, cost) without polling the KV.
func SubjectWorker(task string) string { return "mesh.worker." + task }

// SubjectFleet is the scheduler fleet-state event subject (KindFleet, #25).
// Fixed: the fleet is a per-mesh singleton, not a per-job fact.
const SubjectFleet = "mesh.fleet"

// PatternWorkers matches every worker-outcome event.
const PatternWorkers = "mesh.worker.>"

// WorkerResult is the typed outcome of one worker run on a task (#25).
// Never fake-success: only a run whose result envelope passed the documented
// success discriminators maps to ok.
type WorkerResult string

const (
	WorkerOK    WorkerResult = "ok"    // run completed; task may transition to done
	WorkerError WorkerResult = "error" // typed failure; Code says why
)

// ValidWorkerResult reports whether r is a recognized worker result.
func ValidWorkerResult(r WorkerResult) bool { return r == WorkerOK || r == WorkerError }

// WorkerErrorCode classifies why a worker run failed. Wire contract: it
// travels in WorkerPayload so dashboards and audit consumers can discriminate
// failure classes without parsing prose. The scheduler's policy hangs off
// these classes (locked decision, DECISIONS.md 2026-06-09 fleet billing
// posture): rate_limited backs off and retries, billing_error pauses the
// fleet, everything else fails the task.
type WorkerErrorCode string

const (
	// WorkerSpawnFailed: the driver could not create a worker for the task
	// (missing binary, worktree setup failure, resource exhaustion).
	WorkerSpawnFailed WorkerErrorCode = "spawn_failed"
	// WorkerFailed: the worker ran but did not produce a typed success
	// (is_error, non-success subtype, malformed result, crash, timeout).
	WorkerFailed WorkerErrorCode = "worker_failed"
	// WorkerRateLimited: the run hit a rate-limit/overloaded signal. The task
	// is NOT failed — the scheduler backs off and re-dispatches.
	WorkerRateLimited WorkerErrorCode = "rate_limited"
	// WorkerBillingError: the run hit a billing/credit-exhaustion signal. The
	// task is NOT failed — the scheduler pauses the whole fleet.
	WorkerBillingError WorkerErrorCode = "billing_error"
	// WorkerInternal: recording the outcome failed (store/bus error, lost CAS).
	WorkerInternal WorkerErrorCode = "internal"
)

var workerErrorCodes = map[WorkerErrorCode]bool{
	WorkerSpawnFailed:  true,
	WorkerFailed:       true,
	WorkerRateLimited:  true,
	WorkerBillingError: true,
	WorkerInternal:     true,
}

// ValidWorkerErrorCode reports whether c is a recognized worker error code.
func ValidWorkerErrorCode(c WorkerErrorCode) bool { return workerErrorCodes[c] }

// FleetState is the scheduler fleet lifecycle vocabulary (#25).
type FleetState string

const (
	FleetRunning FleetState = "running" // scheduler is dispatching runnable tasks
	FleetPaused  FleetState = "paused"  // nothing new spawns until reset
)

// ValidFleetState reports whether s is a recognized fleet state.
func ValidFleetState(s FleetState) bool { return s == FleetRunning || s == FleetPaused }

// FleetPauseCode classifies why the fleet paused. Locked decision: on either
// code, jobs and tasks stay queued/pending in their KV records — never failed.
type FleetPauseCode string

const (
	// FleetBudgetExhausted: accumulated run cost reached the configured
	// MESH_BUDGET_USD cap.
	FleetBudgetExhausted FleetPauseCode = "budget_exhausted"
	// FleetBillingError: a worker run reported a billing/credit error.
	FleetBillingError FleetPauseCode = "billing_error"
)

var fleetPauseCodes = map[FleetPauseCode]bool{
	FleetBudgetExhausted: true,
	FleetBillingError:    true,
}

// ValidFleetPauseCode reports whether c is a recognized fleet pause code.
func ValidFleetPauseCode(c FleetPauseCode) bool { return fleetPauseCodes[c] }

// WorkerPayload is the worker-outcome observability event (#25, KindWorker):
// one per worker run on a task. An observability tap only — the tasks KV
// record (internal/task) is the authority for task state. CostUSD is the
// run's reported total_cost_usd (0 when the runtime did not report one).
type WorkerPayload struct {
	Task    string          `json:"task"`
	Job     string          `json:"job"`
	Result  WorkerResult    `json:"result"`
	Code    WorkerErrorCode `json:"code,omitempty"`
	CostUSD float64         `json:"costUSD,omitempty"`
	Reason  string          `json:"reason,omitempty"`
}

func (p WorkerPayload) validate() error {
	if err := requireField("task", p.Task); err != nil {
		return err
	}
	if err := requireField("job", p.Job); err != nil {
		return err
	}
	if !ValidWorkerResult(p.Result) {
		return fmt.Errorf("unknown worker result %q", p.Result)
	}
	if p.Result == WorkerError && !ValidWorkerErrorCode(p.Code) {
		return fmt.Errorf("worker error without a valid code (got %q)", p.Code)
	}
	if p.Result == WorkerOK && p.Code != "" {
		return fmt.Errorf("worker ok must not carry an error code (got %q)", p.Code)
	}
	return nil
}

// FleetPayload is the scheduler fleet-state event (#25, KindFleet). SpentUSD
// is the cost accumulated this coordinator lifetime; BudgetUSD is the
// configured cap (0 = unlimited, in which case only billing_error pauses).
type FleetPayload struct {
	State     FleetState     `json:"state"`
	Code      FleetPauseCode `json:"code,omitempty"`
	Reason    string         `json:"reason,omitempty"`
	SpentUSD  float64        `json:"spentUSD,omitempty"`
	BudgetUSD float64        `json:"budgetUSD,omitempty"`
}

func (p FleetPayload) validate() error {
	if !ValidFleetState(p.State) {
		return fmt.Errorf("unknown fleet state %q", p.State)
	}
	if p.State == FleetPaused && !ValidFleetPauseCode(p.Code) {
		return fmt.Errorf("fleet paused without a valid code (got %q)", p.Code)
	}
	if p.State == FleetRunning && p.Code != "" {
		return fmt.Errorf("fleet running must not carry a pause code (got %q)", p.Code)
	}
	return nil
}
