package envelope

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
)

// Typed payloads, one per kind. Decode helpers are co-located with the
// envelope codec so the whole wire contract lives in this package.

// RegisterPayload announces an agent joining the mesh.
type RegisterPayload struct {
	Card agentcard.Card `json:"card"`
}

func (p RegisterPayload) validate() error { return p.Card.Validate() }

// LeavePayload announces a graceful departure.
type LeavePayload struct {
	ID     string `json:"id"`
	Reason string `json:"reason,omitempty"`
}

func (p LeavePayload) validate() error { return requireField("id", p.ID) }

// HeartbeatPayload renews an agent's presence lease. The envelope TS is the
// beat time; Status optionally carries the agent's latest status text.
type HeartbeatPayload struct {
	ID     string `json:"id"`
	Status string `json:"status,omitempty"`
}

func (p HeartbeatPayload) validate() error { return requireField("id", p.ID) }

// StatusPayload is a fire-and-forget "what I'm doing now" line.
type StatusPayload struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

func (p StatusPayload) validate() error { return requireField("id", p.ID) }

// AnnouncePayload broadcasts edit intent for conflict avoidance (P1).
// Advisory only: a real edit additionally takes a CAS claim (#12).
type AnnouncePayload struct {
	ID     string   `json:"id"`
	Intent string   `json:"intent"`
	Paths  []string `json:"paths,omitempty"`
	Repo   string   `json:"repo,omitempty"`
}

func (p AnnouncePayload) validate() error {
	if err := requireField("id", p.ID); err != nil {
		return err
	}
	return requireField("intent", p.Intent)
}

// ClaimPayload records a CAS file/ticket claim event (P1).
type ClaimPayload struct {
	ID     string      `json:"id"`
	Path   string      `json:"path"`
	Repo   string      `json:"repo,omitempty"`
	Result ClaimResult `json:"result,omitempty"`
}

func (p ClaimPayload) validate() error {
	if err := requireField("id", p.ID); err != nil {
		return err
	}
	return requireField("path", p.Path)
}

// AskPayload opens an async ask ticket (P2).
type AskPayload struct {
	Ticket string `json:"ticket"`
	Role   string `json:"role,omitempty"`
	To     string `json:"to,omitempty"`
	Q      string `json:"q"`
	Ctx    string `json:"ctx,omitempty"`
}

func (p AskPayload) validate() error {
	if err := requireField("ticket", p.Ticket); err != nil {
		return err
	}
	return requireField("q", p.Q)
}

// AnswerPayload resolves an ask ticket (P2).
type AnswerPayload struct {
	Ticket string `json:"ticket"`
	Answer string `json:"answer"`
}

func (p AnswerPayload) validate() error {
	if err := requireField("ticket", p.Ticket); err != nil {
		return err
	}
	return requireField("answer", p.Answer)
}

// NoteKind classifies a blackboard note (#15). Open-ended enough for P1;
// anything unrecognized is rejected at the publish edge, not at read time.
const (
	NoteKindDecision = "decision"
	NoteKindContext  = "context"
	NoteKindSummary  = "summary"
	NoteKindOther    = "other"
)

var noteKinds = map[string]bool{
	NoteKindDecision: true,
	NoteKindContext:  true,
	NoteKindSummary:  true,
	NoteKindOther:    true,
}

// ValidNoteKind reports whether k is a recognized note kind.
func ValidNoteKind(k string) bool { return noteKinds[k] }

// NotePayload appends a decision to the durable blackboard (P1). ID is the
// author agent id (sender-bound: must equal the envelope From). The envelope
// TS is the note timestamp.
type NotePayload struct {
	ID       string `json:"id"`
	Decision string `json:"decision"`
	Repo     string `json:"repo,omitempty"`
	Kind     string `json:"kind,omitempty"`   // decision|context|summary|other; empty = decision
	Ticket   string `json:"ticket,omitempty"` // optional ask-ticket linkage (P2)
}

func (p NotePayload) validate() error {
	if err := requireField("id", p.ID); err != nil {
		return err
	}
	if err := requireField("decision", p.Decision); err != nil {
		return err
	}
	if p.Kind != "" && !ValidNoteKind(p.Kind) {
		return fmt.Errorf("unknown note kind %q", p.Kind)
	}
	return nil
}

// TicketPayload is the ticket-FSM transition event (P2, KindTicket). An
// observability tap only: the tickets KV record is the authority for ticket
// state; this event lets the dashboard and e2e watch the lifecycle without
// polling the KV. By is the agent that caused the transition; Reason carries
// detail for expired/cancelled.
type TicketPayload struct {
	Ticket string      `json:"ticket"`
	State  TicketState `json:"state"`
	By     string      `json:"by,omitempty"`
	Reason string      `json:"reason,omitempty"`
}

func (p TicketPayload) validate() error {
	if err := requireField("ticket", p.Ticket); err != nil {
		return err
	}
	if !ValidTicketState(p.State) {
		return fmt.Errorf("unknown ticket state %q", p.State)
	}
	return nil
}

// JobPayload is the autonomous work-unit observability event (#23, KindJob).
// An observability tap only: the jobs KV record (internal/job) is the
// authority for job state; this event lets the dashboard and e2e watch intake
// without polling the KV. It carries just enough to render a job row.
type JobPayload struct {
	ID     string   `json:"id"`
	Repo   string   `json:"repo"`
	Source string   `json:"source"` // manual | github
	Title  string   `json:"title"`
	State  JobState `json:"state"`
}

func (p JobPayload) validate() error {
	for field, val := range map[string]string{
		"id": p.ID, "repo": p.Repo, "source": p.Source, "title": p.Title,
	} {
		if err := requireField(field, val); err != nil {
			return err
		}
	}
	if !ValidJobState(p.State) {
		return fmt.Errorf("unknown job state %q", p.State)
	}
	return nil
}

// TaskPayload is the DAG-node observability event (#24, KindTask). An
// observability tap only: the tasks KV record (internal/task) is the
// authority for task state. It carries just enough to render a task row;
// dependencies and acceptance live in the KV record.
type TaskPayload struct {
	ID    string    `json:"id"`
	Job   string    `json:"job"`
	Role  string    `json:"role"`
	Title string    `json:"title"`
	State TaskState `json:"state"`
}

func (p TaskPayload) validate() error {
	for field, val := range map[string]string{
		"id": p.ID, "job": p.Job, "role": p.Role, "title": p.Title,
	} {
		if err := requireField(field, val); err != nil {
			return err
		}
	}
	if !ValidTaskState(p.State) {
		return fmt.Errorf("unknown task state %q", p.State)
	}
	return nil
}

// TriagePayload is the planner-outcome event (#24, KindTriage): one per
// triage attempt. Result is typed ok|error; on error, Code classifies the
// failure and Reason carries human-readable detail. Tasks is the number of
// DAG nodes persisted (0 on error).
type TriagePayload struct {
	Job    string          `json:"job"`
	Result TriageResult    `json:"result"`
	Tasks  int             `json:"tasks,omitempty"`
	Code   TriageErrorCode `json:"code,omitempty"`
	Reason string          `json:"reason,omitempty"`
}

func (p TriagePayload) validate() error {
	if err := requireField("job", p.Job); err != nil {
		return err
	}
	if !ValidTriageResult(p.Result) {
		return fmt.Errorf("unknown triage result %q", p.Result)
	}
	if p.Result == TriageError && !ValidTriageErrorCode(p.Code) {
		return fmt.Errorf("triage error without a valid code (got %q)", p.Code)
	}
	if p.Result == TriageOK && p.Code != "" {
		return fmt.Errorf("triage ok must not carry an error code (got %q)", p.Code)
	}
	return nil
}

// payloadKinds maps each kind to its expected payload validator, so DecodeInto
// can reject a payload that does not match the envelope's kind.
type validator interface{ validate() error }

func payloadFor(kind Kind) validator {
	switch kind {
	case KindRegister:
		return &RegisterPayload{}
	case KindLeave:
		return &LeavePayload{}
	case KindHeartbeat:
		return &HeartbeatPayload{}
	case KindStatus:
		return &StatusPayload{}
	case KindAnnounce:
		return &AnnouncePayload{}
	case KindClaim:
		return &ClaimPayload{}
	case KindAsk:
		return &AskPayload{}
	case KindAnswer:
		return &AnswerPayload{}
	case KindNote:
		return &NotePayload{}
	case KindTicket:
		return &TicketPayload{}
	case KindJob:
		return &JobPayload{}
	case KindTask:
		return &TaskPayload{}
	case KindTriage:
		return &TriagePayload{}
	case KindWorker:
		return &WorkerPayload{}
	case KindFleet:
		return &FleetPayload{}
	case KindReview:
		return &ReviewPayload{}
	default:
		return nil
	}
}

// DecodeInto unmarshals the envelope payload into v, verifying that the
// envelope kind matches the payload type and that required payload fields are
// present. v must be a pointer to one of the payload structs above.
func DecodeInto(env Envelope, v validator) error {
	want := payloadFor(env.Kind)
	if want == nil {
		return &DecodeError{Code: CodeUnknownKind, Detail: fmt.Sprintf("kind %q", env.Kind)}
	}
	if fmt.Sprintf("%T", want) != fmt.Sprintf("%T", v) {
		return &DecodeError{
			Code:   CodeKindMismatch,
			Detail: fmt.Sprintf("envelope kind %q does not carry %T", env.Kind, v),
		}
	}
	if err := json.Unmarshal(env.Payload, v); err != nil {
		return &DecodeError{Code: CodeUnparseable, Detail: err.Error()}
	}
	if err := v.validate(); err != nil {
		return &DecodeError{Code: CodeInvalidPayload, Detail: err.Error()}
	}
	return nil
}

func requireField(name, val string) error {
	if strings.TrimSpace(val) == "" {
		return fmt.Errorf("missing %s", name)
	}
	return nil
}
