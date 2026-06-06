// Package meshapi defines the verb names and typed argument/result shapes
// exchanged between the `mesh` CLI and its sidecar over the unix socket.
//
// One shared definition so the CLI and sidecar cannot drift apart — the same
// rule as the envelope package, applied to the local IPC hop.
package meshapi

import (
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// Verbs implemented in P0 + P1. P2 adds ask/poll/inbox/answer here.
const (
	VerbPing     = "ping"
	VerbJoin     = "join"
	VerbLeave    = "leave"
	VerbWho      = "who"
	VerbStatus   = "status"
	VerbClaim    = "claim"
	VerbRelease  = "release"
	VerbAnnounce = "announce"
	VerbNote     = "note"
	VerbContext  = "context"
)

// MaxStatusLen bounds a status line in bytes. Status text is fanned out to
// every subscriber and stored in the registry record; an unbounded value
// could blow the bus frame limit and silently kill connections.
const MaxStatusLen = 4096

// JoinArgs asks the sidecar to (re-)register this agent.
type JoinArgs struct {
	Card agentcard.Card `json:"card"`
}

// JoinResult reports the registered identity.
type JoinResult struct {
	Card     agentcard.Card `json:"card"`
	Rejoined bool           `json:"rejoined"` // true if the sidecar was already joined
}

// LeaveArgs asks the sidecar to deregister and shut down.
type LeaveArgs struct {
	Reason string `json:"reason,omitempty"`
}

// LeaveResult confirms departure.
type LeaveResult struct {
	ID string `json:"id"`
}

// StatusArgs posts a status line.
type StatusArgs struct {
	Text string `json:"text"`
}

// StatusResult confirms the publish.
type StatusResult struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

// WhoResult is the current roster as read from the authoritative registry.
type WhoResult struct {
	Agents []agentcard.RegistryRecord `json:"agents"`
}

// PingResult reports sidecar identity and health; used by autostart waits.
type PingResult struct {
	ID     string `json:"id"`
	Joined bool   `json:"joined"`
	Bus    bool   `json:"bus"` // bus connection currently healthy
}

// --- P1: claims -----------------------------------------------------------------

// MaxNoteLen bounds a note in bytes, MaxIntentLen an announce intent — both
// are fanned out / durably stored, so unbounded values are rejected at the
// socket edge like status text is.
const (
	MaxNoteLen   = 16384
	MaxIntentLen = 4096
)

// ClaimArgs asks the sidecar to take a CAS claim on a path. Repo defaults to
// the agent's card repo, else "default".
type ClaimArgs struct {
	Path string `json:"path"`
	Repo string `json:"repo,omitempty"`
}

// ClaimVerbResult is the typed outcome of a claim attempt. Result is always
// set; Owner/Since describe the current holder (the caller when claimed; the
// winner when lost) so a loser can see who owns the claim.
type ClaimVerbResult struct {
	Result envelope.ClaimResult `json:"result"` // claimed | lost | error
	Path   string               `json:"path"`   // normalized path actually claimed
	Repo   string               `json:"repo"`
	Owner  string               `json:"owner,omitempty"`
	Since  time.Time            `json:"since,omitempty"`
}

// ReleaseArgs releases a claim previously taken by this agent.
type ReleaseArgs struct {
	Path string `json:"path"`
	Repo string `json:"repo,omitempty"`
}

// ReleaseResultKind is the typed outcome of a release attempt.
type ReleaseResultKind string

const (
	ReleaseReleased ReleaseResultKind = "released"  // freed (or already gone)
	ReleaseNotOwner ReleaseResultKind = "not_owner" // someone else holds it
	ReleaseError    ReleaseResultKind = "error"     // transport/store failure
)

// ReleaseVerbResult reports a release outcome.
type ReleaseVerbResult struct {
	Result ReleaseResultKind `json:"result"`
	Path   string            `json:"path"`
	Repo   string            `json:"repo"`
	Owner  string            `json:"owner,omitempty"` // holder, when not_owner
}

// --- P1: announce ----------------------------------------------------------------

// AnnounceArgs broadcasts advisory edit intent.
type AnnounceArgs struct {
	Intent string   `json:"intent"`
	Paths  []string `json:"paths,omitempty"`
	Repo   string   `json:"repo,omitempty"`
}

// AnnounceResult confirms the publish.
type AnnounceResult struct {
	ID     string   `json:"id"`
	Repo   string   `json:"repo"`
	Intent string   `json:"intent"`
	Paths  []string `json:"paths,omitempty"`
}

// --- P1: blackboard ----------------------------------------------------------------

// NoteArgs appends a note to the repo's durable blackboard stream.
type NoteArgs struct {
	Text   string `json:"text"`
	Repo   string `json:"repo,omitempty"`
	Kind   string `json:"kind,omitempty"` // decision|context|summary|other
	Ticket string `json:"ticket,omitempty"`
}

// NoteResult confirms the durable append.
type NoteResult struct {
	Seq  uint64 `json:"seq"` // stream sequence of the appended note
	Repo string `json:"repo"`
}

// ContextArgs replays the repo's blackboard history.
type ContextArgs struct {
	Repo string `json:"repo,omitempty"`
}

// ContextNote is one replayed blackboard entry.
type ContextNote struct {
	Seq    uint64    `json:"seq"`
	TS     time.Time `json:"ts"`
	Author string    `json:"author"`
	Text   string    `json:"text"`
	Kind   string    `json:"kind"`
	Ticket string    `json:"ticket,omitempty"`
}

// ContextResult is the replayed blackboard history, oldest first.
type ContextResult struct {
	Repo  string        `json:"repo"`
	Notes []ContextNote `json:"notes"`
}
