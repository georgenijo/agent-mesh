package runtime

import (
	"errors"
	"testing"
)

func TestEncodeUserMessage(t *testing.T) {
	got, err := EncodeUserMessage("hello there")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// The exact spike-verified shape: message.role is required by the CLI.
	want := `{"type":"user","message":{"role":"user","content":"hello there"}}` + "\n"
	if string(got) != want {
		t.Fatalf("encoded line = %q, want %q", got, want)
	}

	if _, err := EncodeUserMessage(""); err == nil {
		t.Fatal("empty message must not encode")
	}
}

func TestParseEventInit(t *testing.T) {
	line := `{"type":"system","subtype":"init","session_id":"s-123","model":"opus","cwd":"/tmp","tools":["Bash"]}`
	ev, err := ParseEvent([]byte(line))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ev.Type != EventSystem || ev.Subtype != SubtypeInit || ev.SessionID != "s-123" {
		t.Fatalf("parsed = %+v", ev)
	}
	if ev.Result != nil {
		t.Fatal("init must not carry a result")
	}
	if string(ev.Raw) != line {
		t.Fatalf("raw not preserved: %q", ev.Raw)
	}
}

func TestParseEventResultClassification(t *testing.T) {
	cases := []struct {
		name      string
		line      string
		succeeded bool
	}{
		{
			name:      "success",
			line:      `{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s1","api_error_status":null}`,
			succeeded: true,
		},
		{
			name:      "is_error true",
			line:      `{"type":"result","subtype":"success","is_error":true,"result":"x","session_id":"s1"}`,
			succeeded: false,
		},
		{
			name:      "non-success subtype",
			line:      `{"type":"result","subtype":"error_during_execution","is_error":false,"session_id":"s1"}`,
			succeeded: false,
		},
		{
			name:      "api_error_status non-null",
			line:      `{"type":"result","subtype":"success","is_error":false,"result":"x","api_error_status":429}`,
			succeeded: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := ParseEvent([]byte(tc.line))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if ev.Result == nil {
				t.Fatal("result event parsed without Result")
			}
			if got := ev.Result.Succeeded(); got != tc.succeeded {
				t.Fatalf("Succeeded() = %v, want %v", got, tc.succeeded)
			}
		})
	}
}

func TestParseEventTolerance(t *testing.T) {
	// Unknown event types pass through.
	ev, err := ParseEvent([]byte(`{"type":"wormhole_event","payload":[1,2,3]}`))
	if err != nil {
		t.Fatalf("unknown type rejected: %v", err)
	}
	if ev.Type != "wormhole_event" {
		t.Fatalf("type = %q", ev.Type)
	}

	// Oddly-typed known fields degrade instead of rejecting the line.
	ev, err = ParseEvent([]byte(`{"type":42,"weird":true}`))
	if err != nil {
		t.Fatalf("odd-typed field rejected: %v", err)
	}
	if ev.Type != "" {
		t.Fatalf("degraded type = %q, want empty", ev.Type)
	}

	// A result with an oddly-typed text field still classifies.
	ev, err = ParseEvent([]byte(`{"type":"result","subtype":"success","is_error":false,"result":42,"session_id":"s1"}`))
	if err != nil {
		t.Fatalf("odd-typed result rejected: %v", err)
	}
	if ev.Result == nil || !ev.Result.Succeeded() {
		t.Fatalf("result = %+v", ev.Result)
	}
}

func TestParseEventMalformed(t *testing.T) {
	for _, line := range []string{
		"this is not json",
		`["a","top-level","array"]`,
		"",
		"   ",
		`{"type":"system"`, // truncated object
	} {
		if _, err := ParseEvent([]byte(line)); !errors.Is(err, ErrMalformedEvent) {
			t.Fatalf("ParseEvent(%q) error = %v, want ErrMalformedEvent", line, err)
		}
	}
}
