package envelope

import "regexp"

// Subject taxonomy, KV bucket and stream names — the shared spine every
// component must agree on (docs/components.md "The shared spine"). Defined
// here, beside the envelope codec, so the whole wire contract lives in one
// package and no component hand-rolls a subject string.

// Fixed subjects.
const (
	SubjectRegister = "mesh.register"
	SubjectLeave    = "mesh.leave"
)

// Parameterized subjects.
func SubjectHeartbeat(id string) string  { return "mesh.heartbeat." + id }
func SubjectStatus(id string) string     { return "mesh.status." + id }
func SubjectAnnounce(repo string) string { return "mesh.announce." + repo }

// SubjectNote names a note envelope's subject. Notes are not published —
// they are appended to the durable per-repo stream (the one authority for
// blackboard history) — but every envelope carries a subject, and replay
// consumers see this one.
func SubjectNote(repo string) string { return "mesh.note." + repo }

// SubjectClaim names the observability event published after a claim
// attempt. The claims KV record is the lock — the one authority; this event
// only lets taps (dashboard, e2e) watch contention without polling the KV.
func SubjectClaim(repo string) string { return "mesh.claim." + repo }

// Ask/answer subjects (P2). An ask is addressed either to a role (the
// coordinator routes it) or to a specific agent id (direct to that agent's
// inbox); the answer travels on the ticket's own subject back to the asker's
// sidecar.
func SubjectAskRole(role string) string  { return "mesh.ask.role." + role }
func SubjectAskID(id string) string      { return "mesh.ask.id." + id }
func SubjectInbox(id string) string      { return "mesh.inbox." + id }
func SubjectAnswer(ticket string) string { return "mesh.answer." + ticket }

// SubjectTicket names the ticket-FSM transition event (KindTicket). The
// tickets KV record is the authority for ticket state; this event only lets
// taps (dashboard, e2e) watch the lifecycle without polling the KV.
func SubjectTicket(ticket string) string { return "mesh.ticket." + ticket }

// SubjectJob names the job observability event (KindJob, #23). The jobs KV
// record is the authority for job state; this event only lets taps (dashboard,
// e2e) watch intake/lifecycle without polling the KV. mesh.job.* matches the
// mesh.> tap.
func SubjectJob(id string) string { return "mesh.job." + id }

// Subscription patterns.
const (
	PatternAll        = "mesh.>"
	PatternHeartbeats = "mesh.heartbeat.>"
	PatternStatuses   = "mesh.status.>"
	PatternAnnounces  = "mesh.announce.>"
	PatternClaims     = "mesh.claim.>"
	PatternAsks       = "mesh.ask.>"
	PatternAnswers    = "mesh.answer.>"
	PatternTickets    = "mesh.ticket.>"
	PatternJobs       = "mesh.job.>"
)

// KV buckets. One authority per fact: the registry bucket is the single
// source of truth for "who exists and in what presence state"; only the
// coordinator writes it. The claims bucket is the single source of truth for
// "who holds which path" — the CAS record is the lock, announce is advisory.
// The tickets bucket is the single source of truth for ticket state —
// mesh.ticket.<ticket> events are derived observability.
// The jobs bucket is the single source of truth for autonomous work-unit
// state; mesh.job.<id> events are derived observability.
const (
	BucketRegistry = "registry"
	BucketClaims   = "claims"
	BucketTickets  = "tickets"
	BucketJobs     = "jobs"
)

// Streams (bounded).
const (
	StreamAudit   = "audit"
	StreamTickets = "ticket-events"
	StreamJobs    = "job-events"
)

// StreamNotes is the per-repo durable blackboard stream name.
func StreamNotes(repo string) string { return "notes-" + repo }

// repoRE constrains repo ids: they become subject tokens
// (mesh.announce.<repo>) and stream names (notes-<repo>), so dots, wildcards,
// slashes, and whitespace are forbidden, and length is bounded so derived
// store names stay within the bus's 64-char name limit. A repo id is a label
// chosen at join/claim time, not a filesystem path.
//
// Role and agent-id subject segments (mesh.ask.role.<role>, mesh.inbox.<id>,
// …) must satisfy the same character class; that is enforced where those
// identities are minted (join/agent-card validation), not here.
var repoRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,48}$`)

// ValidRepo reports whether s is a legal repo id.
func ValidRepo(s string) bool { return repoRE.MatchString(s) }

// DefaultRepo is the repo identity used when an agent does not set one.
const DefaultRepo = "default"
