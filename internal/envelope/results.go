package envelope

// Typed result enums. Every operation returns an explicit typed state —
// never a fake-success payload, never a boolean that conflates "lost the
// race" with "transport error" (audit Avoid #2/#4).

// ClaimResult is the outcome of a CAS claim attempt.
type ClaimResult string

const (
	ClaimClaimed ClaimResult = "claimed" // this caller won the claim
	ClaimLost    ClaimResult = "lost"    // another caller legitimately won
	ClaimError   ClaimResult = "error"   // transport/store failure; retryable
)

// ReleaseResult is the outcome of a release attempt. Release is
// delete-if-owner: releasing an already-gone claim is an idempotent success
// (the fact is already true), and a claim held by someone else is not_owner —
// never release-by-force.
type ReleaseResult string

const (
	ReleaseReleased ReleaseResult = "released"  // freed, or already gone
	ReleaseNotOwner ReleaseResult = "not_owner" // someone else holds it; nothing deleted
	ReleaseError    ReleaseResult = "error"     // transport/store failure; retryable
)

// AskResult is the outcome of polling an ask ticket.
//
// Locked CLI exit-code mapping (ARCHITECTURE.md §4): AskPending → exit 3
// (no-answer-yet), AskNoSuchTicket → exit 4. Pinned here so the CLI verbs
// (#18) consume the mapping instead of reinventing it.
type AskResult string

const (
	AskAnswered     AskResult = "answered"
	AskPending      AskResult = "pending"
	AskTimedOut     AskResult = "timed_out"
	AskExpired      AskResult = "expired"
	AskNoSuchTicket AskResult = "no_such_ticket"
)

// TicketState is the lifecycle vocabulary of an ask ticket. The vocabulary is
// wire contract — frozen here; the legal-transition table and reducer are the
// ticket FSM's (#17). The tickets KV record is the one authority for a
// ticket's current state.
type TicketState string

const (
	TicketOpen      TicketState = "open"      // created, not yet routed
	TicketRouted    TicketState = "routed"    // coordinator picked a responder
	TicketAccepted  TicketState = "accepted"  // responder took it
	TicketAnswered  TicketState = "answered"  // answer recorded
	TicketClosed    TicketState = "closed"    // asker collected the answer
	TicketExpired   TicketState = "expired"   // TTL ran out unanswered
	TicketCancelled TicketState = "cancelled" // asker withdrew it
)

var ticketStates = map[TicketState]bool{
	TicketOpen:      true,
	TicketRouted:    true,
	TicketAccepted:  true,
	TicketAnswered:  true,
	TicketClosed:    true,
	TicketExpired:   true,
	TicketCancelled: true,
}

// ValidTicketState reports whether s is a recognized ticket state.
func ValidTicketState(s TicketState) bool { return ticketStates[s] }

// JobState is the lifecycle vocabulary of an autonomous Job — the top-level
// work unit created by `mesh submit` (#23). The vocabulary is wire contract,
// frozen here; the jobs KV record (internal/job) is the one authority for a
// job's current state. #23 only mints JobOpen at creation; triage (#24) and
// the scheduler/worker (#25/#26) drive the later transitions.
type JobState string

const (
	JobOpen      JobState = "open"      // created, not yet triaged
	JobTriaged   JobState = "triaged"   // decomposed into tasks
	JobScheduled JobState = "scheduled" // tasks dispatched
	JobRunning   JobState = "running"   // a worker is executing it
	JobDone      JobState = "done"      // completed successfully
	JobFailed    JobState = "failed"    // terminal failure
	JobCancelled JobState = "cancelled" // withdrawn
)

var jobStates = map[JobState]bool{
	JobOpen:      true,
	JobTriaged:   true,
	JobScheduled: true,
	JobRunning:   true,
	JobDone:      true,
	JobFailed:    true,
	JobCancelled: true,
}

// ValidJobState reports whether s is a recognized job state.
func ValidJobState(s JobState) bool { return jobStates[s] }

// TaskState is the lifecycle vocabulary of a Task — one DAG node minted by
// triage (#24). The vocabulary is wire contract, frozen here; the tasks KV
// record (internal/task) is the one authority for a task's current state.
// #24 only mints TaskPending; the scheduler (#25) and worker (#26) drive the
// later transitions. Readiness is not a stored state: a task is runnable when
// it is pending and every DependsOn task is done — derived, never persisted.
type TaskState string

const (
	TaskPending   TaskState = "pending"   // created by triage, not yet dispatched
	TaskRunning   TaskState = "running"   // a worker is executing it
	TaskDone      TaskState = "done"      // completed successfully
	TaskFailed    TaskState = "failed"    // terminal failure
	TaskCancelled TaskState = "cancelled" // withdrawn (e.g. job cancelled)
)

var taskStates = map[TaskState]bool{
	TaskPending:   true,
	TaskRunning:   true,
	TaskDone:      true,
	TaskFailed:    true,
	TaskCancelled: true,
}

// ValidTaskState reports whether s is a recognized task state.
func ValidTaskState(s TaskState) bool { return taskStates[s] }

// TriageResult is the outcome of one planner triage attempt (#24). Typed,
// never fake-success: only a validated, persisted DAG maps to ok.
type TriageResult string

const (
	TriageOK    TriageResult = "ok"    // DAG validated and persisted; job triaged
	TriageError TriageResult = "error" // typed failure; Code says why
)

// ValidTriageResult reports whether r is a recognized triage result.
func ValidTriageResult(r TriageResult) bool { return r == TriageOK || r == TriageError }

// TriageErrorCode classifies why a triage attempt failed. Wire contract:
// it travels in TriagePayload so dashboards and audit consumers can
// discriminate failure classes without parsing prose.
type TriageErrorCode string

const (
	// TriagePlannerUnavailable: the planner CLI could not run to completion
	// (missing binary, spawn failure, timeout, crash).
	TriagePlannerUnavailable TriageErrorCode = "planner_unavailable"
	// TriagePlannerFailed: the planner ran but its result envelope was not a
	// success (is_error, non-success subtype, api_error_status, or stdout that
	// is not the documented JSON result shape).
	TriagePlannerFailed TriageErrorCode = "planner_failed"
	// TriageBadPlan: the planner answered successfully but the result text is
	// not a parseable plan document (strict JSON validation; never scraped).
	TriageBadPlan TriageErrorCode = "bad_plan"
	// TriageInvalidDAG: the plan parsed but failed DAG validation (cycle,
	// duplicate/missing node id, unknown dependency, unknown role, bounds).
	TriageInvalidDAG TriageErrorCode = "invalid_dag"
	// TriageInternal: persisting tasks or transitioning the job failed
	// (store/bus error, lost CAS).
	TriageInternal TriageErrorCode = "internal"
)

var triageErrorCodes = map[TriageErrorCode]bool{
	TriagePlannerUnavailable: true,
	TriagePlannerFailed:      true,
	TriageBadPlan:            true,
	TriageInvalidDAG:         true,
	TriageInternal:           true,
}

// ValidTriageErrorCode reports whether c is a recognized triage error code.
func ValidTriageErrorCode(c TriageErrorCode) bool { return triageErrorCodes[c] }

// AuditCategory groups an audit-log entry by the domain whose lifecycle it
// records. The audit stream (envelope.StreamAudit, written only by the
// coordinator) is the unified policy/audit substrate (#29): the coordinator
// taps mesh.> and fans the major lifecycle events of every domain into it, so a
// single ordered read reconstructs how a ticket / job / claim reached its
// current state. The vocabulary is wire contract — observers (dashboard, ops,
// e2e) discriminate on these typed strings, never on prose. Frozen here beside
// the other enums; the audit record shape (AuditEntry) lives in the coordinator
// package, its sole writer (one authority per fact).
type AuditCategory string

const (
	AuditPresence AuditCategory = "presence" // agent join/leave/away/recover/evict (predates #29)
	AuditClaim    AuditCategory = "claim"    // CAS file-claim attempt or coordinator reclaim
	AuditTicket   AuditCategory = "ticket"   // ask-ticket FSM transition
	AuditAsk      AuditCategory = "ask"      // an ask was opened (role/direct routed)
	AuditAnswer   AuditCategory = "answer"   // an ask ticket was answered
	AuditJob      AuditCategory = "job"      // autonomous job lifecycle transition
	AuditTask     AuditCategory = "task"     // DAG-node task lifecycle transition
	AuditTriage   AuditCategory = "triage"   // a planner triage attempt resolved
	AuditWorker   AuditCategory = "worker"   // a worker run on a task resolved
	AuditFleet    AuditCategory = "fleet"    // scheduler fleet pause/resume
)

var auditCategories = map[AuditCategory]bool{
	AuditPresence: true,
	AuditClaim:    true,
	AuditTicket:   true,
	AuditAsk:      true,
	AuditAnswer:   true,
	AuditJob:      true,
	AuditTask:     true,
	AuditTriage:   true,
	AuditWorker:   true,
	AuditFleet:    true,
}

// ValidAuditCategory reports whether c is a recognized audit category.
func ValidAuditCategory(c AuditCategory) bool { return auditCategories[c] }
