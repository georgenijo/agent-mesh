// Package answercache is the exact-match answer reuse store (Feature 6 / #29):
// when an expert has already answered the same role+q(+ctx), drainInbox can
// replay that answer without another LLM turn. Tickets KV remains the ticket
// FSM authority; this bucket is a best-effort cache with TTL leases.
package answercache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// Entry is one cached successful answer for a role-ask key.
type Entry struct {
	Role         string    `json:"role"`
	Q            string    `json:"q"`
	Ctx          string    `json:"ctx,omitempty"`
	Answer       string    `json:"answer"`
	AnsweredBy   string    `json:"answeredBy,omitempty"`
	AnsweredAt   time.Time `json:"answeredAt,omitzero"`
	SourceTicket string    `json:"sourceTicket,omitempty"`
	Hits         int       `json:"hits,omitempty"`
}

// keyMaterial is the stable JSON shape hashed into a cache key. Field order is
// fixed by the struct so encoding/json produces a canonical byte sequence.
type keyMaterial struct {
	Role string `json:"role"`
	Q    string `json:"q"`
	Ctx  string `json:"ctx"`
}

// Key returns the SHA-256 hex digest of the stable JSON key material for
// role+q(+ctx). Strings are trimmed. When includeCtx is false, ctx is forced
// empty so asks that differ only in context share one entry.
func Key(role, q, ctx string, includeCtx bool) string {
	m := keyMaterial{
		Role: strings.TrimSpace(role),
		Q:    strings.TrimSpace(q),
	}
	if includeCtx {
		m.Ctx = strings.TrimSpace(ctx)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		// Unreachable for plain strings; degrade to a distinct empty key rather
		// than panic at the publish edge.
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Store reads and writes envelope.BucketAnswerCache via the bus KV.
type Store struct {
	cli        *bus.Client
	ttl        time.Duration
	includeCtx bool
}

// New wraps cli as an answer-cache store. ttl is applied on every Put (lease);
// includeCtx controls whether ask context participates in the key.
func New(cli *bus.Client, ttl time.Duration, includeCtx bool) *Store {
	return &Store{cli: cli, ttl: ttl, includeCtx: includeCtx}
}

// Get looks up a cached answer for role+q(+ctx). A missing key, expired lease,
// or corrupt JSON is a miss (found=false, nil error) — the cache degrades
// rather than wedging the expert loop.
func (s *Store) Get(role, q, ctx string) (*Entry, bool) {
	if s == nil || s.cli == nil {
		return nil, false
	}
	key := Key(role, q, ctx, s.includeCtx)
	if key == "" {
		return nil, false
	}
	kv, found, err := s.cli.KVGet(envelope.BucketAnswerCache, key)
	if err != nil || !found {
		return nil, false
	}
	var e Entry
	if err := json.Unmarshal(kv.Value, &e); err != nil {
		return nil, false
	}
	if strings.TrimSpace(e.Answer) == "" {
		return nil, false
	}
	return &e, true
}

// Put stores entry under its role+q(+ctx) key with the store's TTL. Empty role
// or empty answer is skipped (no-op, nil error) — never cache a useless entry.
func (s *Store) Put(entry Entry) error {
	if s == nil || s.cli == nil {
		return nil
	}
	entry.Role = strings.TrimSpace(entry.Role)
	entry.Q = strings.TrimSpace(entry.Q)
	entry.Ctx = strings.TrimSpace(entry.Ctx)
	entry.Answer = strings.TrimSpace(entry.Answer)
	if entry.Role == "" || entry.Answer == "" {
		return nil
	}
	key := Key(entry.Role, entry.Q, entry.Ctx, s.includeCtx)
	if key == "" {
		return nil
	}
	_, err := s.cli.KVPut(envelope.BucketAnswerCache, key, entry, bus.PutOptions{TTL: s.ttl})
	return err
}
