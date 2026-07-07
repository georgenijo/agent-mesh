// Package settings owns the desired-config Record — the authoritative KV shape
// behind the v1 settings screen. It mirrors internal/job: the record shape and
// its CAS/validation live here (one authority per fact), while the envelope
// package owns the wire vocabulary (KindSettings, SubjectSettings, BucketSettings,
// StreamSettings, SettingsPayload).
//
// Authority & precedence. A settings Record is the DESIRED config an operator
// staged; the coordinator overlays it onto its env-loaded Config on every Start
// (autostart included) via Overlay. Precedence is env > settings > default,
// mirroring `mesh up`'s setIfUnset: an explicit env var always wins, a staged
// value fills where env is unset, and the compiled default is the floor. Every
// knob field is a POINTER so nil (absent → fall through to env/default) stays
// distinct from a non-nil staged value — the three-way unset/empty/value
// distinction WorkerModel/PlannerModel need.
//
// A settings Record is NOT a lease: no TTL. Like a job, it persists until an
// operator changes it. The bucket is durable (persisted alongside jobs/tasks).
package settings

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// KeyCurrent is the singleton key the settings bucket stores its one record
// under. The bucket is a per-mesh singleton, not a keyed collection.
const KeyCurrent = "current"

var (
	// ErrBadRecord means the staged record failed validation (a field is
	// unparseable, out of range, or the resolved config violates an invariant).
	ErrBadRecord = errors.New("settings: bad record")
	// ErrCASLost means another writer moved the record since the caller's read
	// (stale stagedRev). The caller must refetch and retry — never a silent
	// overwrite. Maps to HTTP 409.
	ErrCASLost = errors.New("settings: cas lost")
)

// Record is the authoritative settings-bucket entry (envelope.BucketSettings,
// key KeyCurrent). Every knob is a pointer: nil = absent (fall through to
// env/default), non-nil = staged. Duration knobs are Go duration strings
// (e.g. "30s") so the wire shape is human-legible and the three-way distinction
// survives JSON round-trips. Rev/UpdatedAt/UpdatedBy are provenance the Store
// stamps on each Put.
type Record struct {
	Rev       uint64    `json:"rev"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
	UpdatedBy string    `json:"updatedBy,omitempty"`

	// Hot knobs (apply within one scheduler sweep via the mutex-guarded setters).
	BudgetUSD         *float64 `json:"budgetUSD,omitempty"`
	MaxWorkers        *int     `json:"maxWorkers,omitempty"`
	ReDispatchBackoff *string  `json:"reDispatchBackoff,omitempty"`

	// Restart-coordinator knobs.
	WorkerCLI         *string `json:"workerCLI,omitempty"`
	WorkerModel       *string `json:"workerModel,omitempty"`
	PlannerCLI        *string `json:"plannerCLI,omitempty"`
	PlannerModel      *string `json:"plannerModel,omitempty"`
	ExpertCLI         *string `json:"expertCLI,omitempty"`
	WorkerTimeout     *string `json:"workerTimeout,omitempty"`
	TriageTimeout     *string `json:"triageTimeout,omitempty"`
	TriageBackoff     *string `json:"triageBackoff,omitempty"`
	TriageMaxAttempts *int    `json:"triageMaxAttempts,omitempty"`
	ReviewRole        *string `json:"reviewRole,omitempty"`
	ReviewPoolSize    *int    `json:"reviewPoolSize,omitempty"`
	ReviewRetries     *int    `json:"reviewRetries,omitempty"`
	ReviewTimeout     *string `json:"reviewTimeout,omitempty"`
	KeepWorktrees     *string `json:"keepWorktrees,omitempty"`
	AutoExperts       *bool   `json:"autoExperts,omitempty"`
	AuditFanout       *bool   `json:"auditFanout,omitempty"`
	ExpertIdleTTL     *string `json:"expertIdleTTL,omitempty"`
	JobsAddr          *string `json:"jobsAddr,omitempty"`

	// Restart-fleet knobs (every daemon must restart; Advanced section).
	HeartbeatInterval *string `json:"heartbeatInterval,omitempty"`
	AwayAfter         *string `json:"awayAfter,omitempty"`
	EvictAfter        *string `json:"evictAfter,omitempty"`
	ClaimTTL          *string `json:"claimTTL,omitempty"`
	DashboardAddr     *string `json:"dashboardAddr,omitempty"`
	ObserveAddr       *string `json:"observeAddr,omitempty"`
}

// ApplyClass is how a knob change reaches a running mesh.
type ApplyClass string

const (
	// ApplyHot: effect within one scheduler sweep via a mutex-guarded setter.
	ApplyHot ApplyClass = "hot"
	// ApplyRestartCoordinator: persisted, overlaid on the coordinator's next Start.
	ApplyRestartCoordinator ApplyClass = "restart-coordinator"
	// ApplyRestartFleet: every daemon (coordinator + every sidecar/expert) must restart.
	ApplyRestartFleet ApplyClass = "restart-fleet"
)

// FieldMeta describes one knob: its apply class, backing env var, whether it
// arms real spend / opens a port (arming requires operator confirmation), and a
// UI label. The single source the API uses to route hot-vs-persist and the UI
// reads to render badges and env-lock icons.
type FieldMeta struct {
	Field      string     `json:"field"`
	ApplyClass ApplyClass `json:"applyClass"`
	EnvName    string     `json:"envName"`
	Arming     bool       `json:"arming"`
	Label      string     `json:"label"`
}

// meta is the ordered apply-class table. Order is grouped by apply class then by
// the operator workflow the UI renders top-to-bottom.
var meta = []FieldMeta{
	// Hot.
	{"budgetUSD", ApplyHot, config.EnvBudgetUSD, false, "Budget (USD)"},
	{"maxWorkers", ApplyHot, config.EnvMaxWorkers, false, "Max workers"},
	{"reDispatchBackoff", ApplyHot, config.EnvReDispatchBackoff, false, "Re-dispatch backoff"},
	// Restart-coordinator.
	{"workerCLI", ApplyRestartCoordinator, config.EnvWorkerCLI, true, "Worker CLI"},
	{"workerModel", ApplyRestartCoordinator, config.EnvWorkerModel, false, "Worker model"},
	{"plannerCLI", ApplyRestartCoordinator, config.EnvPlannerCLI, true, "Planner CLI"},
	{"plannerModel", ApplyRestartCoordinator, config.EnvPlannerModel, false, "Planner model"},
	{"expertCLI", ApplyRestartCoordinator, config.EnvExpertCLI, false, "Expert CLI"},
	{"workerTimeout", ApplyRestartCoordinator, config.EnvWorkerTimeout, false, "Worker timeout"},
	{"triageTimeout", ApplyRestartCoordinator, config.EnvTriageTimeout, false, "Triage timeout"},
	{"triageBackoff", ApplyRestartCoordinator, config.EnvTriageBackoff, false, "Triage backoff"},
	{"triageMaxAttempts", ApplyRestartCoordinator, config.EnvTriageMaxAttempts, false, "Triage max attempts"},
	{"reviewRole", ApplyRestartCoordinator, config.EnvReviewRole, true, "Review role"},
	{"reviewPoolSize", ApplyRestartCoordinator, config.EnvReviewPoolSize, false, "Review pool size"},
	{"reviewRetries", ApplyRestartCoordinator, config.EnvReviewRetries, false, "Review retries"},
	{"reviewTimeout", ApplyRestartCoordinator, config.EnvReviewTimeout, false, "Review timeout"},
	{"keepWorktrees", ApplyRestartCoordinator, config.EnvKeepWorktrees, false, "Keep worktrees"},
	{"autoExperts", ApplyRestartCoordinator, config.EnvAutoExperts, true, "Auto-experts"},
	{"auditFanout", ApplyRestartCoordinator, config.EnvAuditFanout, false, "Audit fan-out"},
	{"expertIdleTTL", ApplyRestartCoordinator, config.EnvExpertIdleTTL, false, "Expert idle TTL"},
	{"jobsAddr", ApplyRestartCoordinator, config.EnvJobsAddr, true, "Jobs ingress addr"},
	// Restart-fleet.
	{"heartbeatInterval", ApplyRestartFleet, config.EnvHeartbeatInterval, false, "Heartbeat interval"},
	{"awayAfter", ApplyRestartFleet, config.EnvAwayAfter, false, "Away after"},
	{"evictAfter", ApplyRestartFleet, config.EnvEvictAfter, false, "Evict after"},
	{"claimTTL", ApplyRestartFleet, config.EnvClaimTTL, false, "Claim TTL"},
	{"dashboardAddr", ApplyRestartFleet, config.EnvDashboardAddr, false, "Dashboard addr"},
	{"observeAddr", ApplyRestartFleet, config.EnvObserveAddr, false, "Observe addr"},
}

// Meta returns the apply-class table (a fresh copy so callers can't mutate it).
func Meta() []FieldMeta {
	out := make([]FieldMeta, len(meta))
	copy(out, meta)
	return out
}

// ArmingFields returns the field keys whose change arms real spend / opens a
// port and therefore requires explicit confirmation.
func ArmingFields() []string {
	var out []string
	for _, m := range meta {
		if m.Arming {
			out = append(out, m.Field)
		}
	}
	return out
}

// Store reads and writes the authoritative settings KV record.
type Store struct {
	cli *bus.Client
	now func() time.Time
}

func NewStore(cli *bus.Client) Store {
	return Store{cli: cli, now: func() time.Time { return time.Now().UTC() }}
}

func (s Store) withNow(now func() time.Time) Store {
	s.now = now
	return s
}

// Get reads the singleton record. found=false (nil error) means nothing staged
// yet. rev is the KV store revision — the caller presents it back as stagedRev
// so a concurrent write loses the CAS race (ErrCASLost) instead of clobbering.
func (s Store) Get() (rec Record, rev uint64, found bool, err error) {
	kv, found, err := s.cli.KVGet(envelope.BucketSettings, KeyCurrent)
	if err != nil || !found {
		return Record{}, 0, found, err
	}
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return Record{}, 0, false, fmt.Errorf("%w: %v", ErrBadRecord, err)
	}
	return rec, kv.Rev, true, nil
}

// Put stages a new desired-config record. Create-only on the first write,
// revision-CAS thereafter (a stale casRev → ErrCASLost). It re-validates the
// record's own fields AND the resolved cross-field invariants before writing
// (never a silent bad write), increments Rev, stamps UpdatedAt, and appends a
// who/what/old→new/when change Event to StreamSettings for replay.
func (s Store) Put(rec Record, casRev uint64) (Record, error) {
	if s.now == nil {
		s.now = func() time.Time { return time.Now().UTC() }
	}
	if err := ValidateRecord(rec); err != nil {
		return Record{}, err
	}
	prev, kvRev, found, err := s.Get()
	if err != nil {
		return Record{}, err
	}
	rec.UpdatedAt = s.now()
	if !found {
		if casRev != 0 {
			return Record{}, ErrCASLost // caller thinks there's a prior; there isn't
		}
		rec.Rev = 1
		if _, err := s.cli.KVPut(envelope.BucketSettings, KeyCurrent, rec, bus.PutOptions{CAS: bus.CreateOnly()}); err != nil {
			if errors.Is(err, bus.ErrCASLost) {
				return Record{}, ErrCASLost
			}
			return Record{}, err
		}
	} else {
		if casRev != kvRev {
			return Record{}, ErrCASLost // stale UI view; refetch + diff
		}
		rec.Rev = prev.Rev + 1
		if _, err := s.cli.KVPut(envelope.BucketSettings, KeyCurrent, rec, bus.PutOptions{CAS: bus.Rev(kvRev)}); err != nil {
			if errors.Is(err, bus.ErrCASLost) {
				return Record{}, ErrCASLost
			}
			return Record{}, err
		}
	}
	_ = s.appendEvent(prev, rec, found)
	return rec, nil
}

// Change is one field's old→new transition for the audit Event.
type Change struct {
	Field string `json:"field"`
	Old   string `json:"old"`
	New   string `json:"new"`
}

// Event records one settings change (who/what/old→new/when).
type Event struct {
	Rev     uint64    `json:"rev"`
	By      string    `json:"by,omitempty"`
	At      time.Time `json:"at"`
	Changes []Change  `json:"changes,omitempty"`
}

func (s Store) appendEvent(prev, next Record, hadPrev bool) error {
	ev := Event{Rev: next.Rev, By: next.UpdatedBy, At: next.UpdatedAt}
	if !hadPrev {
		prev = Record{}
	}
	ev.Changes = diff(prev, next)
	_, err := s.cli.StreamAppend(envelope.StreamSettings, ev)
	return err
}

// diff reports the knob fields whose staged value changed, as old→new strings.
// Reflection over the pointer knob fields keeps this compact; "«unset»" marks a
// nil pointer so an unset→value transition is legible.
func diff(prev, next Record) []Change {
	var out []Change
	pv := reflect.ValueOf(prev)
	nv := reflect.ValueOf(next)
	t := pv.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Type.Kind() != reflect.Ptr {
			continue // Rev/UpdatedAt/UpdatedBy are provenance, not knobs
		}
		o := ptrString(pv.Field(i))
		n := ptrString(nv.Field(i))
		if o != n {
			out = append(out, Change{Field: jsonName(f), Old: o, New: n})
		}
	}
	return out
}

func ptrString(v reflect.Value) string {
	if v.IsNil() {
		return "«unset»"
	}
	return fmt.Sprintf("%v", v.Elem().Interface())
}

func jsonName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	for i := 0; i < len(tag); i++ {
		if tag[i] == ',' {
			return tag[:i]
		}
	}
	if tag != "" {
		return tag
	}
	return f.Name
}

// ValidateRecord checks a staged record: every set duration parses, every enum
// is legal, every numeric knob is in range, and the resolved config (all staged
// pointers applied onto the compiled defaults) passes config.Validate. Errors
// name the offending field/invariant so the dashboard can surface a typed 400.
func ValidateRecord(rec Record) error {
	for _, d := range []struct {
		field string
		p     *string
	}{
		{"reDispatchBackoff", rec.ReDispatchBackoff},
		{"workerTimeout", rec.WorkerTimeout},
		{"triageTimeout", rec.TriageTimeout},
		{"triageBackoff", rec.TriageBackoff},
		{"reviewTimeout", rec.ReviewTimeout},
		{"expertIdleTTL", rec.ExpertIdleTTL},
		{"heartbeatInterval", rec.HeartbeatInterval},
		{"awayAfter", rec.AwayAfter},
		{"evictAfter", rec.EvictAfter},
		{"claimTTL", rec.ClaimTTL},
	} {
		if d.p != nil {
			if _, err := time.ParseDuration(*d.p); err != nil {
				return fmt.Errorf("%w: %s: not a duration (%v)", ErrBadRecord, d.field, err)
			}
		}
	}
	if rec.KeepWorktrees != nil {
		switch *rec.KeepWorktrees {
		case config.KeepWorktreesOnFailure, config.KeepWorktreesAlways, config.KeepWorktreesNever:
		default:
			return fmt.Errorf("%w: keepWorktrees: want on-failure|always|never", ErrBadRecord)
		}
	}
	if rec.BudgetUSD != nil {
		if b := *rec.BudgetUSD; b < 0 || math.IsNaN(b) || math.IsInf(b, 0) {
			return fmt.Errorf("%w: budgetUSD: want a non-negative finite amount", ErrBadRecord)
		}
	}
	if rec.MaxWorkers != nil && *rec.MaxWorkers <= 0 {
		return fmt.Errorf("%w: maxWorkers: want a positive integer", ErrBadRecord)
	}
	if rec.TriageMaxAttempts != nil && *rec.TriageMaxAttempts <= 0 {
		return fmt.Errorf("%w: triageMaxAttempts: want a positive integer", ErrBadRecord)
	}
	if rec.ReviewPoolSize != nil && *rec.ReviewPoolSize < 1 {
		return fmt.Errorf("%w: reviewPoolSize: want >= 1", ErrBadRecord)
	}
	if rec.ReviewRetries != nil && *rec.ReviewRetries < 0 {
		return fmt.Errorf("%w: reviewRetries: want a non-negative integer", ErrBadRecord)
	}
	if rec.ReviewRole != nil && *rec.ReviewRole != "" && !envelope.ValidRole(*rec.ReviewRole) {
		return fmt.Errorf("%w: reviewRole: not a valid role token", ErrBadRecord)
	}
	// Cross-field invariants on the fully-resolved config (staged applied onto
	// the compiled defaults, env ignored: the staged intent must be self-consistent
	// even where an env var currently masks it).
	resolved := overlay(defaultConfig(), rec, false)
	if err := config.Validate(resolved); err != nil {
		return fmt.Errorf("%w: %v", ErrBadRecord, err)
	}
	return nil
}

// Overlay applies a staged record onto cfg with env-wins precedence: for each
// knob, if its backing env var is present the cfg value (env already won in
// config.Load) is kept; otherwise a non-nil staged pointer is applied. Pure and
// stdlib-only. settings→config only; config never imports settings, so no cycle.
func Overlay(cfg config.Config, rec Record) config.Config {
	return overlay(cfg, rec, true)
}

func overlay(cfg config.Config, rec Record, honorEnv bool) config.Config {
	// Hot.
	overlayFloat(&cfg.BudgetUSD, config.EnvBudgetUSD, rec.BudgetUSD, honorEnv)
	overlayInt(&cfg.MaxWorkers, config.EnvMaxWorkers, rec.MaxWorkers, honorEnv)
	overlayDur(&cfg.Backoff, config.EnvReDispatchBackoff, rec.ReDispatchBackoff, honorEnv)
	// Restart-coordinator.
	overlayStr(&cfg.WorkerCLI, config.EnvWorkerCLI, rec.WorkerCLI, honorEnv)
	overlayStr(&cfg.WorkerModel, config.EnvWorkerModel, rec.WorkerModel, honorEnv)
	overlayStr(&cfg.PlannerCLI, config.EnvPlannerCLI, rec.PlannerCLI, honorEnv)
	overlayStr(&cfg.PlannerModel, config.EnvPlannerModel, rec.PlannerModel, honorEnv)
	overlayStr(&cfg.ExpertCLI, config.EnvExpertCLI, rec.ExpertCLI, honorEnv)
	overlayDur(&cfg.WorkerTimeout, config.EnvWorkerTimeout, rec.WorkerTimeout, honorEnv)
	overlayDur(&cfg.TriageTimeout, config.EnvTriageTimeout, rec.TriageTimeout, honorEnv)
	overlayDur(&cfg.TriageBackoff, config.EnvTriageBackoff, rec.TriageBackoff, honorEnv)
	overlayInt(&cfg.TriageMaxAttempts, config.EnvTriageMaxAttempts, rec.TriageMaxAttempts, honorEnv)
	overlayStr(&cfg.ReviewRole, config.EnvReviewRole, rec.ReviewRole, honorEnv)
	overlayInt(&cfg.ReviewPoolSize, config.EnvReviewPoolSize, rec.ReviewPoolSize, honorEnv)
	overlayInt(&cfg.ReviewRetries, config.EnvReviewRetries, rec.ReviewRetries, honorEnv)
	overlayDur(&cfg.ReviewTimeout, config.EnvReviewTimeout, rec.ReviewTimeout, honorEnv)
	overlayStr(&cfg.KeepWorktrees, config.EnvKeepWorktrees, rec.KeepWorktrees, honorEnv)
	overlayBool(&cfg.AutoExperts, config.EnvAutoExperts, rec.AutoExperts, honorEnv)
	overlayBool(&cfg.AuditFanout, config.EnvAuditFanout, rec.AuditFanout, honorEnv)
	overlayDur(&cfg.ExpertIdleTTL, config.EnvExpertIdleTTL, rec.ExpertIdleTTL, honorEnv)
	overlayStr(&cfg.JobsAddr, config.EnvJobsAddr, rec.JobsAddr, honorEnv)
	// Restart-fleet.
	overlayDur(&cfg.HeartbeatInterval, config.EnvHeartbeatInterval, rec.HeartbeatInterval, honorEnv)
	overlayDur(&cfg.AwayAfter, config.EnvAwayAfter, rec.AwayAfter, honorEnv)
	overlayDur(&cfg.EvictAfter, config.EnvEvictAfter, rec.EvictAfter, honorEnv)
	overlayDur(&cfg.ClaimTTL, config.EnvClaimTTL, rec.ClaimTTL, honorEnv)
	overlayStr(&cfg.DashboardAddr, config.EnvDashboardAddr, rec.DashboardAddr, honorEnv)
	overlayStr(&cfg.ObserveAddr, config.EnvObserveAddr, rec.ObserveAddr, honorEnv)
	return cfg
}

func envMasked(env string, honorEnv bool) bool {
	if !honorEnv {
		return false
	}
	_, ok := os.LookupEnv(env)
	return ok
}

func overlayStr(dst *string, env string, p *string, honorEnv bool) {
	if envMasked(env, honorEnv) || p == nil {
		return
	}
	*dst = *p
}

func overlayInt(dst *int, env string, p *int, honorEnv bool) {
	if envMasked(env, honorEnv) || p == nil {
		return
	}
	*dst = *p
}

func overlayFloat(dst *float64, env string, p *float64, honorEnv bool) {
	if envMasked(env, honorEnv) || p == nil {
		return
	}
	*dst = *p
}

func overlayBool(dst *bool, env string, p *bool, honorEnv bool) {
	if envMasked(env, honorEnv) || p == nil {
		return
	}
	*dst = *p
}

func overlayDur(dst *time.Duration, env string, p *string, honorEnv bool) {
	if envMasked(env, honorEnv) || p == nil {
		return
	}
	if d, err := time.ParseDuration(*p); err == nil {
		*dst = d
	}
}

// defaultConfig returns the compiled config defaults — the same values
// config.Load seeds before applying env — with ClaimTTL derived. Used as the
// base for validation and for the dashboard's "Default" column. Kept in sync
// with config.Load's default block.
func defaultConfig() config.Config {
	c := config.Config{
		HeartbeatInterval: config.DefaultHeartbeatInterval,
		AwayAfter:         config.DefaultAwayAfter,
		EvictAfter:        config.DefaultEvictAfter,
		RegistrationGrace: config.DefaultRegistrationGrace,
		DashboardAddr:     config.DefaultDashboardAddr,
		ObserveAddr:       config.DefaultObserveAddr,
		ExpertCLI:         config.DefaultExpertCLI,
		PlannerModel:      config.DefaultPlannerModel,
		TriageTimeout:     config.DefaultTriageTimeout,
		TriageMaxAttempts: config.DefaultTriageMaxAttempts,
		TriageBackoff:     config.DefaultTriageBackoff,
		WorkerModel:       config.DefaultWorkerModel,
		WorkerTimeout:     config.DefaultWorkerTimeout,
		MaxWorkers:        config.DefaultMaxWorkers,
		ReviewTimeout:     config.DefaultReviewTimeout,
		ReviewPoolSize:    config.DefaultReviewPoolSize,
		ReviewRetries:     config.DefaultReviewRetries,
		KeepWorktrees:     config.KeepWorktreesOnFailure,
		AuditFanout:       true,
		ExpertIdleTTL:     config.DefaultExpertIdleTTL,
		Backoff:           config.DefaultReDispatchBackoff,
	}
	c.ClaimTTL = 2 * (c.EvictAfter + c.RegistrationGrace)
	return c
}

// Defaults returns the compiled config defaults as a Record projection — the
// "Default" column source for the settings UI. Every knob is set (no nils).
func Defaults() envelope.SettingsPayload {
	return Project(defaultConfig(), Record{})
}

// Project renders a resolved config as the non-secret SettingsPayload the
// KindSettings tap carries (the "Effective" column). Provenance comes from rec.
// Durations render as Go duration strings. Never carries a credential.
func Project(cfg config.Config, rec Record) envelope.SettingsPayload {
	return envelope.SettingsPayload{
		Rev:               rec.Rev,
		UpdatedAt:         rec.UpdatedAt,
		UpdatedBy:         rec.UpdatedBy,
		BudgetUSD:         cfg.BudgetUSD,
		MaxWorkers:        cfg.MaxWorkers,
		ReDispatchBackoff: cfg.Backoff.String(),
		WorkerCLI:         cfg.WorkerCLI,
		WorkerModel:       cfg.WorkerModel,
		PlannerCLI:        cfg.PlannerCLI,
		PlannerModel:      cfg.PlannerModel,
		ExpertCLI:         cfg.ExpertCLI,
		WorkerTimeout:     cfg.WorkerTimeout.String(),
		TriageTimeout:     cfg.TriageTimeout.String(),
		TriageBackoff:     cfg.TriageBackoff.String(),
		TriageMaxAttempts: cfg.TriageMaxAttempts,
		ReviewRole:        cfg.ReviewRole,
		ReviewPoolSize:    cfg.ReviewPoolSize,
		ReviewRetries:     cfg.ReviewRetries,
		ReviewTimeout:     cfg.ReviewTimeout.String(),
		KeepWorktrees:     cfg.KeepWorktrees,
		AutoExperts:       cfg.AutoExperts,
		AuditFanout:       cfg.AuditFanout,
		ExpertIdleTTL:     cfg.ExpertIdleTTL.String(),
		JobsAddr:          cfg.JobsAddr,
		HeartbeatInterval: cfg.HeartbeatInterval.String(),
		AwayAfter:         cfg.AwayAfter.String(),
		EvictAfter:        cfg.EvictAfter.String(),
		ClaimTTL:          cfg.ClaimTTL.String(),
		DashboardAddr:     cfg.DashboardAddr,
		ObserveAddr:       cfg.ObserveAddr,
	}
}

// EnvOverridden returns the backing env-var names that are currently set — the
// knobs env pins so a staged value can never take effect. The UI renders an
// env-lock icon for each. honorEnvLookup is os.LookupEnv semantics.
func EnvOverridden() []string {
	var out []string
	for _, m := range meta {
		if _, ok := os.LookupEnv(m.EnvName); ok {
			out = append(out, m.EnvName)
		}
	}
	return out
}

// armsDelta reports the arming field keys whose staged value differs between
// prev and next — the confirmation gate's input. A create (no prev) that sets
// an arming field counts as arming it.
func armsDelta(prev Record, hadPrev bool, next Record) []string {
	if !hadPrev {
		prev = Record{}
	}
	changed := map[string]bool{}
	for _, c := range diff(prev, next) {
		changed[c.Field] = true
	}
	var out []string
	for _, m := range meta {
		if m.Arming && changed[m.Field] {
			out = append(out, m.Field)
		}
	}
	return out
}

// ChangedFields reports the knob field keys whose staged value differs between
// prev and next — the dashboard maps these to apply classes to tell the UI what
// applied now (hot) vs what needs a restart.
func ChangedFields(prev, next Record) []string {
	var out []string
	for _, c := range diff(prev, next) {
		out = append(out, c.Field)
	}
	return out
}

// MetaFor returns the FieldMeta for a knob field key.
func MetaFor(field string) (FieldMeta, bool) {
	for _, m := range meta {
		if m.Field == field {
			return m, true
		}
	}
	return FieldMeta{}, false
}

// ArmingDelta reports which arming knobs the proposed record changes relative to
// what is currently staged. The dashboard uses it to decide whether to demand a
// confirmation before writing. Reads the current record through the store.
func (s Store) ArmingDelta(next Record) ([]string, error) {
	prev, _, found, err := s.Get()
	if err != nil {
		return nil, err
	}
	return armsDelta(prev, found, next), nil
}
