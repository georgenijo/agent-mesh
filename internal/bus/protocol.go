package bus

import (
	"encoding/json"
	"time"
)

// Wire protocol: newline-delimited JSON frames over a unix socket, one
// persistent multiplexed connection per client. This protocol is private to
// the bus package — components use the typed Client API, never raw frames.

const protocolVersion = 1

// Frame ops.
const (
	opPub          = "pub"
	opSub          = "sub"
	opUnsub        = "unsub"
	opKVGet        = "kv.get"
	opKVPut        = "kv.put"
	opKVDelete     = "kv.del"
	opKVList       = "kv.list"
	opStreamAppend = "stream.append"
	opStreamRead   = "stream.read"
	opPing         = "ping"
	opMsg          = "msg" // server → client push for subscriptions
)

// Error codes carried in responses.
const (
	errCodeCASLost      = "cas_lost"
	errCodeNoSuchKey    = "no_such_key"
	errCodeBadRequest   = "bad_request"
	errCodeSlowConsumer = "slow_consumer"
)

// frame is the single wire shape for requests, responses, and pushes.
// Loose by design: this is an internal point-to-point protocol, and the
// typed Client/Server APIs are the real contract.
type frame struct {
	V  int    `json:"v,omitempty"`
	ID string `json:"id,omitempty"`
	Op string `json:"op,omitempty"`

	// Request fields.
	Subject string          `json:"subject,omitempty"`
	Pattern string          `json:"pattern,omitempty"`
	Sub     string          `json:"sub,omitempty"` // subscription id (unsub + push)
	Bucket  string          `json:"bucket,omitempty"`
	Key     string          `json:"key,omitempty"`
	Value   json.RawMessage `json:"value,omitempty"`
	Rev     *uint64         `json:"rev,omitempty"` // nil=unconditional, 0=create-only, >0=CAS
	TTLMs   int64           `json:"ttlMs,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Stream  string          `json:"stream,omitempty"`
	From    uint64          `json:"from,omitempty"`

	// Response fields.
	OK      bool               `json:"ok,omitempty"`
	Err     *frameError        `json:"error,omitempty"`
	NewRev  uint64             `json:"newRev,omitempty"`
	Found   bool               `json:"found,omitempty"`
	Keys    map[string]KVValue `json:"keys,omitempty"`
	Entries []StreamEntry      `json:"entries,omitempty"`
	Seq     uint64             `json:"seq,omitempty"`
}

type frameError struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// KVValue is a stored value with its revision.
type KVValue struct {
	Value json.RawMessage `json:"value"`
	Rev   uint64          `json:"rev"`
}

// StreamEntry is one appended record in a bounded stream.
type StreamEntry struct {
	Seq  uint64          `json:"seq"`
	TS   time.Time       `json:"ts"`
	Data json.RawMessage `json:"data"`
}
