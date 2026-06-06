// Package meshapi defines the verb names and typed argument/result shapes
// exchanged between the `mesh` CLI and its sidecar over the unix socket.
//
// One shared definition so the CLI and sidecar cannot drift apart — the same
// rule as the envelope package, applied to the local IPC hop.
package meshapi

import "github.com/georgenijo/agent-mesh/internal/agentcard"

// Verbs implemented in P0. P1/P2 add announce/claim/note/ask/... here.
const (
	VerbPing   = "ping"
	VerbJoin   = "join"
	VerbLeave  = "leave"
	VerbWho    = "who"
	VerbStatus = "status"
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
