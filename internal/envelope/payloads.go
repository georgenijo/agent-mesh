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
type AnnouncePayload struct {
	ID     string   `json:"id"`
	Intent string   `json:"intent"`
	Paths  []string `json:"paths,omitempty"`
	Repo   string   `json:"repo,omitempty"`
}

func (p AnnouncePayload) validate() error { return requireField("id", p.ID) }

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

// NotePayload appends a decision to the durable blackboard (P1).
type NotePayload struct {
	Decision string `json:"decision"`
	Repo     string `json:"repo,omitempty"`
}

func (p NotePayload) validate() error { return requireField("decision", p.Decision) }

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
