package envelope

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
)

func validCard() agentcard.Card {
	return agentcard.Card{ID: "codex-7", Name: "codex-7", Role: "auth", Caps: []string{"go", "backend"}}
}

// TestRoundTripEveryKind encodes and decodes one envelope per core kind and
// checks the typed payload survives.
func TestRoundTripEveryKind(t *testing.T) {
	cases := []struct {
		kind    Kind
		payload validator
		decoded validator
	}{
		{KindRegister, &RegisterPayload{Card: validCard()}, &RegisterPayload{}},
		{KindLeave, &LeavePayload{ID: "codex-7", Reason: "done"}, &LeavePayload{}},
		{KindHeartbeat, &HeartbeatPayload{ID: "codex-7", Status: "building"}, &HeartbeatPayload{}},
		{KindStatus, &StatusPayload{ID: "codex-7", Text: "building RRULE builder"}, &StatusPayload{}},
		{KindAnnounce, &AnnouncePayload{ID: "codex-7", Intent: "editing EventForm.tsx", Paths: []string{"a.tsx"}}, &AnnouncePayload{}},
		{KindClaim, &ClaimPayload{ID: "codex-7", Path: "a.tsx", Result: ClaimClaimed}, &ClaimPayload{}},
		{KindAsk, &AskPayload{Ticket: "T1", Role: "auth", Q: "RLS recursion fix?"}, &AskPayload{}},
		{KindAnswer, &AnswerPayload{Ticket: "T1", Answer: "use is_admin() SECURITY DEFINER"}, &AnswerPayload{}},
		{KindNote, &NotePayload{Decision: "events store UTC", Repo: "stbasils"}, &NotePayload{}},
	}

	for _, tc := range cases {
		t.Run(string(tc.kind), func(t *testing.T) {
			env, err := New(tc.kind, "codex-7", "mesh.test."+string(tc.kind), tc.payload)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			data, err := Encode(env)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got.Kind != tc.kind || got.SchemaVersion != SchemaVersion {
				t.Fatalf("got kind=%q v=%d", got.Kind, got.SchemaVersion)
			}
			if err := DecodeInto(got, tc.decoded); err != nil {
				t.Fatalf("DecodeInto: %v", err)
			}
		})
	}
}

func TestDecodeMalformedIsTypedNeverPanics(t *testing.T) {
	cases := []struct {
		name string
		data string
		code DecodeErrorCode
	}{
		{"garbage", "not json at all", CodeUnparseable},
		{"empty object", "{}", CodeUnsupportedVersion},
		{"future version", `{"schemaVersion":99,"kind":"status","id":"x","from":"a","subject":"s","ts":"2026-06-05T00:00:00Z"}`, CodeUnsupportedVersion},
		{"unknown kind", `{"schemaVersion":1,"kind":"telepathy","id":"x","from":"a","subject":"s","ts":"2026-06-05T00:00:00Z"}`, CodeUnknownKind},
		{"missing id", `{"schemaVersion":1,"kind":"status","from":"a","subject":"s","ts":"2026-06-05T00:00:00Z"}`, CodeMissingField},
		{"missing from", `{"schemaVersion":1,"kind":"status","id":"x","subject":"s","ts":"2026-06-05T00:00:00Z"}`, CodeMissingField},
		{"zero ts", `{"schemaVersion":1,"kind":"status","id":"x","from":"a","subject":"s"}`, CodeMissingField},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode([]byte(tc.data))
			if err == nil {
				t.Fatal("want typed error, got nil")
			}
			if !IsDecodeError(err, tc.code) {
				t.Fatalf("want code %q, got %v", tc.code, err)
			}
		})
	}
}

func TestDecodeToleratesUnknownTopLevelFields(t *testing.T) {
	data := `{"schemaVersion":1,"kind":"status","id":"x","from":"a","subject":"s",` +
		`"ts":"2026-06-05T00:00:00Z","payload":{"id":"a","text":"hi"},"futureField":42}`
	env, err := Decode([]byte(data))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	var p StatusPayload
	if err := DecodeInto(env, &p); err != nil {
		t.Fatalf("DecodeInto: %v", err)
	}
	if p.Text != "hi" {
		t.Fatalf("payload text = %q", p.Text)
	}
}

func TestDecodeIntoKindMismatch(t *testing.T) {
	env, err := New(KindStatus, "a", "mesh.status.a", &StatusPayload{ID: "a", Text: "x"})
	if err != nil {
		t.Fatal(err)
	}
	var p RegisterPayload
	err = DecodeInto(env, &p)
	if !IsDecodeError(err, CodeKindMismatch) {
		t.Fatalf("want kind_mismatch, got %v", err)
	}
}

func TestDecodeIntoInvalidPayload(t *testing.T) {
	env, err := New(KindRegister, "a", "mesh.register", &RegisterPayload{}) // empty card
	if err != nil {
		t.Fatal(err)
	}
	var p RegisterPayload
	err = DecodeInto(env, &p)
	if !IsDecodeError(err, CodeInvalidPayload) {
		t.Fatalf("want invalid_payload, got %v", err)
	}
}

func TestOversizedPayloadIsTypedAtPublishEdge(t *testing.T) {
	huge := strings.Repeat("x", MaxPayloadBytes+1)
	_, err := New(KindStatus, "a", "mesh.status.a", &StatusPayload{ID: "a", Text: huge})
	if !IsDecodeError(err, CodePayloadTooLarge) {
		t.Fatalf("want payload_too_large, got %v", err)
	}
}

func TestNewIDTimeOrderedAndUnique(t *testing.T) {
	const n = 1000
	seen := make(map[string]bool, n)
	var prevMillis string
	for i := 0; i < n; i++ {
		id := NewID()
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
		// UUIDv7 string sorts by time at millisecond granularity: the
		// timestamp prefix must be non-decreasing.
		millis := id[:13] // first 12 hex chars + dash cover the 48-bit ts
		if prevMillis != "" && millis < prevMillis {
			t.Fatalf("timestamp prefix went backwards: %s < %s", millis, prevMillis)
		}
		prevMillis = millis
		if id[14] != '7' {
			t.Fatalf("id %s is not version 7", id)
		}
	}
}

func TestEnvelopeJSONShape(t *testing.T) {
	env, err := New(KindStatus, "codex-7", "mesh.status.codex-7", &StatusPayload{ID: "codex-7", Text: "x"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := Encode(env)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schemaVersion", "kind", "id", "from", "subject", "ts", "payload"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("encoded envelope missing %q: %s", key, data)
		}
	}
	if _, ok := m["to"]; ok {
		t.Fatal("empty to should be omitted")
	}
	ts, ok := m["ts"].(string)
	if !ok || !strings.Contains(ts, "T") {
		t.Fatalf("ts not RFC3339: %v", m["ts"])
	}
	if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
		t.Fatalf("ts parse: %v", err)
	}
}
