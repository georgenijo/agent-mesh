// Package envelope is the wire contract for every Agent Mesh message.
//
// One versioned envelope, encode and decode co-located in this package, so no
// component can invent its own format (audit Avoid #6; decision "One versioned
// envelope, one authority per fact"). Free text only ever travels as opaque
// payload content; routing metadata is typed.
package envelope

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// SchemaVersion is the current wire schema version. Decode rejects any other
// version with a typed error — forward-compat negotiation is v1+ work.
const SchemaVersion = 1

// MaxPayloadBytes bounds a single envelope payload. The bus frame protocol
// caps a line at 4MB and kills the connection past it; rejecting oversized
// payloads here, at the publish edge, turns that silent disconnect into a
// typed error before anything reaches the wire.
const MaxPayloadBytes = 1 << 20 // 1 MiB

// Kind discriminates the payload type carried by an envelope.
type Kind string

// Core kinds. P0 uses register/leave/heartbeat/status; the rest are defined
// now so the contract is stable before P1/P2 build on it.
const (
	KindRegister  Kind = "register"
	KindLeave     Kind = "leave"
	KindHeartbeat Kind = "heartbeat"
	KindStatus    Kind = "status"
	KindAnnounce  Kind = "announce"
	KindClaim     Kind = "claim"
	KindAsk       Kind = "ask"
	KindAnswer    Kind = "answer"
	KindNote      Kind = "note"
	// KindTicket is the ticket-FSM transition event — an observability tap.
	// The tickets KV record is the authority for ticket state, mirroring how
	// KindClaim events relate to the claims bucket.
	KindTicket Kind = "ticket"
)

var knownKinds = map[Kind]bool{
	KindRegister:  true,
	KindLeave:     true,
	KindHeartbeat: true,
	KindStatus:    true,
	KindAnnounce:  true,
	KindClaim:     true,
	KindAsk:       true,
	KindAnswer:    true,
	KindNote:      true,
	KindTicket:    true,
}

// Envelope is the single wire shape. Payload is kind-specific (payloads.go).
type Envelope struct {
	SchemaVersion int             `json:"schemaVersion"`
	Kind          Kind            `json:"kind"`
	ID            string          `json:"id"`
	From          string          `json:"from"`
	To            string          `json:"to,omitempty"`
	Subject       string          `json:"subject"`
	TS            time.Time       `json:"ts"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

// DecodeErrorCode classifies why a decode failed. Typed, never a bare panic
// or prose-only error (audit Steal #10 discipline / Avoid #4).
type DecodeErrorCode string

const (
	CodeUnparseable        DecodeErrorCode = "unparseable"
	CodeUnsupportedVersion DecodeErrorCode = "unsupported_version"
	CodeUnknownKind        DecodeErrorCode = "unknown_kind"
	CodeMissingField       DecodeErrorCode = "missing_field"
	CodeKindMismatch       DecodeErrorCode = "kind_mismatch"
	CodeInvalidPayload     DecodeErrorCode = "invalid_payload"
	CodePayloadTooLarge    DecodeErrorCode = "payload_too_large"
)

// DecodeError is the typed error returned for any malformed envelope.
type DecodeError struct {
	Code   DecodeErrorCode
	Detail string
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("envelope: %s: %s", e.Code, e.Detail)
}

// IsDecodeError reports whether err is a DecodeError with the given code.
func IsDecodeError(err error, code DecodeErrorCode) bool {
	de, ok := err.(*DecodeError)
	return ok && de.Code == code
}

// New builds a validated envelope of the given kind around a typed payload.
// A typed payload (anything implementing the package's validator interface)
// is validated at this publish edge: a malformed payload returns a typed
// CodeInvalidPayload error before anything reaches the wire.
func New(kind Kind, from, subject string, payload any) (Envelope, error) {
	if v, ok := payload.(validator); ok {
		if err := v.validate(); err != nil {
			return Envelope{}, &DecodeError{Code: CodeInvalidPayload, Detail: err.Error()}
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("envelope: marshal payload: %w", err)
	}
	env := Envelope{
		SchemaVersion: SchemaVersion,
		Kind:          kind,
		ID:            NewID(),
		From:          from,
		Subject:       subject,
		TS:            time.Now().UTC(),
		Payload:       raw,
	}
	if err := env.validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Encode serializes a validated envelope to JSON.
func Encode(env Envelope) ([]byte, error) {
	if err := env.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(env)
}

// Decode parses and strictly validates an envelope. Malformed input returns a
// typed *DecodeError — it never panics. Unknown top-level JSON fields are
// tolerated (forward compat); unknown schemaVersion or kind is rejected
// explicitly.
func Decode(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, &DecodeError{Code: CodeUnparseable, Detail: err.Error()}
	}
	if err := env.validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

func (e Envelope) validate() error {
	if e.SchemaVersion != SchemaVersion {
		return &DecodeError{
			Code:   CodeUnsupportedVersion,
			Detail: fmt.Sprintf("schemaVersion %d, want %d", e.SchemaVersion, SchemaVersion),
		}
	}
	if !knownKinds[e.Kind] {
		return &DecodeError{Code: CodeUnknownKind, Detail: fmt.Sprintf("kind %q", e.Kind)}
	}
	for field, val := range map[string]string{
		"id":      e.ID,
		"from":    e.From,
		"subject": e.Subject,
	} {
		if strings.TrimSpace(val) == "" {
			return &DecodeError{Code: CodeMissingField, Detail: field}
		}
	}
	if e.TS.IsZero() {
		return &DecodeError{Code: CodeMissingField, Detail: "ts"}
	}
	if len(e.Payload) > MaxPayloadBytes {
		return &DecodeError{
			Code:   CodePayloadTooLarge,
			Detail: fmt.Sprintf("payload %d bytes exceeds %d", len(e.Payload), MaxPayloadBytes),
		}
	}
	return nil
}
