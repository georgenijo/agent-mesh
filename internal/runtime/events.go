package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Stream event types observed in the runtime-proxy spike
// (docs/spikes/runtime-proxy.md "Observed event shapes"). The set is OPEN:
// the CLI's stream format will evolve, so unknown types and unknown fields
// are tolerated and skipped, never treated as fatal.
const (
	EventSystem    = "system"           // subtypes: init, hook_started, hook_response, ...
	EventAssistant = "assistant"        // assistant message payload
	EventResult    = "result"           // terminal turn-completion marker
	EventRateLimit = "rate_limit_event" // rate-limit metadata (status "allowed", ...)
)

// SubtypeInit is the system event that carries the session id at startup.
const SubtypeInit = "init"

// ResultSuccess is the one result subtype that may map to TurnAnswered.
const ResultSuccess = "success"

// Event is one decoded stdout line from the resident child. Only the fields
// the proxy routes on are typed; Raw preserves the entire original line so
// nothing the CLI emits is lost.
type Event struct {
	Type      string
	Subtype   string
	SessionID string
	Result    *ResultEvent    // non-nil iff Type == EventResult
	Raw       json.RawMessage // the full original line
}

// ResultEvent is the terminal turn result, fields per the spike's observed
// `result` shape. Unknown fields are tolerated; structured sub-objects the
// proxy does not interpret (usage, modelUsage, ...) are kept raw.
type ResultEvent struct {
	Type              string          `json:"type"`
	Subtype           string          `json:"subtype"`
	IsError           bool            `json:"is_error"`
	Result            string          `json:"result"` // the model's text output
	SessionID         string          `json:"session_id"`
	NumTurns          int             `json:"num_turns"` // NOT a session-reuse proof (see spike)
	DurationMS        int64           `json:"duration_ms"`
	DurationAPIMS     int64           `json:"duration_api_ms"`
	StopReason        string          `json:"stop_reason"`
	TerminalReason    string          `json:"terminal_reason"`
	APIErrorStatus    json.RawMessage `json:"api_error_status"`
	Usage             json.RawMessage `json:"usage"`
	ModelUsage        json.RawMessage `json:"modelUsage"`
	PermissionDenials json.RawMessage `json:"permission_denials"`
	Raw               json.RawMessage `json:"-"`

	// degraded is set by ParseEvent when a success-discriminator field
	// (subtype / is_error / api_error_status) had an unexpected JSON type
	// and silently kept its zero value. Degrade-don't-throw is safe for
	// every field except the discriminators themselves: a zero-valued
	// IsError must never turn an error result into a success.
	degraded bool
}

// HasAPIError reports a non-null api_error_status — the spike's
// transport/API error (rate-limit) signal.
func (r *ResultEvent) HasAPIError() bool {
	s := strings.TrimSpace(string(r.APIErrorStatus))
	return s != "" && s != "null"
}

// Succeeded applies the spike's never-fake-success mapping rule: a result is
// a success only when subtype is "success" AND is_error is false AND
// api_error_status is null. Anything else — including a result whose
// discriminator fields could not be decoded and so cannot be trusted — maps
// to a typed error result.
func (r *ResultEvent) Succeeded() bool {
	return !r.degraded && r.Subtype == ResultSuccess && !r.IsError && !r.HasAPIError()
}

// ParseEvent decodes one stdout line into a typed event. It is deliberately
// tolerant: unknown event types and unknown fields pass through untouched,
// and a field with an unexpected type degrades to its zero value instead of
// rejecting the line. The one exception is a result event's success
// discriminators (subtype / is_error / api_error_status): if any of those is
// type-degraded the result is marked untrustworthy and Succeeded() returns
// false — never-fake-success. Only a line that is not a JSON object at all
// returns an error (wrapping ErrMalformedEvent) — the read loop skips those.
func ParseEvent(line []byte) (Event, error) {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return Event{}, fmt.Errorf("%w: not a JSON object", ErrMalformedEvent)
	}

	var head struct {
		Type      string `json:"type"`
		Subtype   string `json:"subtype"`
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(trimmed, &head); err != nil {
		var typeErr *json.UnmarshalTypeError
		if !errors.As(err, &typeErr) {
			return Event{}, fmt.Errorf("%w: %v", ErrMalformedEvent, err)
		}
		// An oddly-typed field degrades; whatever decoded is kept.
	}

	raw := make(json.RawMessage, len(trimmed))
	copy(raw, trimmed)
	ev := Event{Type: head.Type, Subtype: head.Subtype, SessionID: head.SessionID, Raw: raw}

	if head.Type == EventResult {
		var res ResultEvent
		if err := json.Unmarshal(raw, &res); err != nil {
			var typeErr *json.UnmarshalTypeError
			if !errors.As(err, &typeErr) {
				return Event{}, fmt.Errorf("%w: %v", ErrMalformedEvent, err)
			}
		}
		// Re-decode just the success discriminators strictly: encoding/json
		// reports only the FIRST type error and keeps going, so the tolerant
		// decode above can silently zero is_error (fake success) while
		// reporting some unrelated field. A struct holding only the
		// discriminators can fail only on the discriminators.
		var disc struct {
			Subtype        string          `json:"subtype"`
			IsError        bool            `json:"is_error"`
			APIErrorStatus json.RawMessage `json:"api_error_status"`
		}
		if err := json.Unmarshal(raw, &disc); err != nil {
			res.degraded = true
		}
		res.Raw = raw
		ev.Result = &res
	}
	return ev, nil
}

// userMessage is the one stdin shape the resident contract accepts. The
// spike verified that message.role is required — flatter shapes
// ({"type":"user","content":...}) are rejected by the CLI with a parse error.
type userMessage struct {
	Type    string          `json:"type"`
	Message userMessageBody `json:"message"`
}

type userMessageBody struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// EncodeUserMessage builds one stream-json stdin line (trailing newline
// included). Per the spike's adapter rule: exactly one JSON object per line,
// written and flushed as a unit.
func EncodeUserMessage(content string) ([]byte, error) {
	if content == "" {
		return nil, errors.New("runtime: empty user message")
	}
	b, err := json.Marshal(userMessage{
		Type:    "user",
		Message: userMessageBody{Role: "user", Content: content},
	})
	if err != nil {
		return nil, fmt.Errorf("runtime: marshal user message: %w", err)
	}
	return append(b, '\n'), nil
}
