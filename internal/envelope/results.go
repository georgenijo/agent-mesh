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
