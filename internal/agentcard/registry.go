package agentcard

import "time"

// PresenceState is the two-tier liveness state of a registered agent:
// live → away (missed beats; degraded but still listed) → evicted (removed).
// Two tiers separate "temporarily unreachable" from "gone" (audit Steal #5).
type PresenceState string

const (
	PresenceLive PresenceState = "live"
	PresenceAway PresenceState = "away"
	// PresenceEvicted appears in transition events only; evicted agents are
	// deleted from the registry, never stored with this state.
	PresenceEvicted PresenceState = "evicted"
)

// RegistryRecord is the authoritative registry entry for one agent. The
// coordinator is the only writer; sidecars and the dashboard read it (one
// authority per fact). `mesh who` renders exactly this record.
type RegistryRecord struct {
	Card         Card          `json:"card"`
	State        PresenceState `json:"state"`
	RegisteredAt time.Time     `json:"registeredAt"`
	LastSeen     time.Time     `json:"lastSeen"`
	LastStatus   string        `json:"lastStatus,omitempty"`
	LastStatusAt time.Time     `json:"lastStatusAt,omitempty"`
}
