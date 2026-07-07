package envelope

import "time"

// Settings wire vocabulary (v1 settings screen): the KindSettings observability
// event and its EFFECTIVE-config projection payload. Additive to the frozen
// contract — nothing here reshapes an existing payload.
//
// The settings KV bucket (BucketSettings, singleton key settings.KeyCurrent) is
// the desired-config authority; the coordinator overlays it onto its Config on
// every Start. KindSettings is a derived observability tap on mesh.settings: it
// carries the coordinator's EFFECTIVE (post-overlay) config so a dashboard can
// render the "Effective" column and reflect a live hot-apply across tabs. It is
// NON-secret by construction — it never carries a credential or API key (the
// no-API-key-in-core lock), only the operator-facing knobs.

// SubjectSettings is the fixed settings observability subject. The settings
// bucket is a per-mesh singleton, not a per-entity fact, so the subject is
// fixed (like SubjectFleet). PatternAll (mesh.>) catches it.
const SubjectSettings = "mesh.settings"

// SettingsPayload is the coordinator's EFFECTIVE config projection (KindSettings).
// An observability tap only: the settings KV record (internal/settings) is the
// authority for the DESIRED (staged) config; this event carries the resolved
// effective values (env > staged > default) so the dashboard renders the live
// picture without polling. Durations are rendered as Go duration strings.
//
// Rev/UpdatedAt/UpdatedBy mirror the staged record's provenance so a tap can see
// which staged revision produced this effective snapshot.
type SettingsPayload struct {
	Rev       uint64    `json:"rev"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
	UpdatedBy string    `json:"updatedBy,omitempty"`

	// Hot knobs (apply within one scheduler sweep).
	BudgetUSD         float64 `json:"budgetUSD"`
	MaxWorkers        int     `json:"maxWorkers"`
	ReDispatchBackoff string  `json:"reDispatchBackoff"`

	// Restart-coordinator knobs.
	WorkerCLI         string `json:"workerCLI"`
	WorkerModel       string `json:"workerModel"`
	PlannerCLI        string `json:"plannerCLI"`
	PlannerModel      string `json:"plannerModel"`
	ExpertCLI         string `json:"expertCLI"`
	WorkerTimeout     string `json:"workerTimeout"`
	TriageTimeout     string `json:"triageTimeout"`
	TriageBackoff     string `json:"triageBackoff"`
	TriageMaxAttempts int    `json:"triageMaxAttempts"`
	ReviewRole        string `json:"reviewRole"`
	ReviewPoolSize    int    `json:"reviewPoolSize"`
	ReviewRetries     int    `json:"reviewRetries"`
	ReviewTimeout     string `json:"reviewTimeout"`
	KeepWorktrees     string `json:"keepWorktrees"`
	AutoExperts       bool   `json:"autoExperts"`
	AuditFanout       bool   `json:"auditFanout"`
	ExpertIdleTTL     string `json:"expertIdleTTL"`
	JobsAddr          string `json:"jobsAddr"`

	// Restart-fleet knobs (every daemon must restart).
	HeartbeatInterval string `json:"heartbeatInterval"`
	AwayAfter         string `json:"awayAfter"`
	EvictAfter        string `json:"evictAfter"`
	ClaimTTL          string `json:"claimTTL"`
	DashboardAddr     string `json:"dashboardAddr"`
	ObserveAddr       string `json:"observeAddr"`
}

// validate accepts any projection: the payload is a derived snapshot, never a
// command, so there is no required field. The staged authority (settings.Store)
// is where invariants are enforced.
func (p SettingsPayload) validate() error { return nil }
