package dashboard

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/settings"
)

// maxSettingsBody bounds the POST /api/settings request body. A settings record
// is small (a few dozen short fields); this is generous headroom.
const maxSettingsBody = 64 << 10 // 64 KiB

// settingsRejection is the last write the settings store refused, surfaced in
// GET /api/settings so the UI can show why a stage failed (never a silent
// no-op — the never-fake-success discipline).
type settingsRejection struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// settingsUpdateRequest is the POST /api/settings body: a partial settings
// Record (only the knobs the operator changed) plus the stagedRev the UI last
// saw (revision CAS) and an explicit confirm for arming changes.
type settingsUpdateRequest struct {
	settings.Record
	StagedRev uint64 `json:"stagedRev"`
	Confirm   bool   `json:"confirm"`
}

// recordSettings updates the effective snapshot from a coordinator KindSettings
// tap and broadcasts a "settings" frame so a live hot-apply (or an edit in
// another tab) reflects everywhere. Only the coordinator's authoritative
// republish (From=coordinator) sets the effective column; the dashboard's own
// write-path poke (From=dashboard) is ignored here — it only triggers the
// coordinator to republish the real effective config.
func (d *Dashboard) recordSettings(env envelope.Envelope) {
	if env.From != "coordinator" {
		return
	}
	var p envelope.SettingsPayload
	if err := envelope.DecodeInto(env, &p); err != nil {
		return
	}
	d.mu.Lock()
	d.effectiveSettings = &p
	d.mu.Unlock()
	if msg, err := json.Marshal(map[string]any{"type": "settings", "settings": d.settingsResponse()}); err == nil {
		d.broadcast(msg)
	}
}

// settingsResponse assembles the GET /api/settings body (also the SSE settings
// frame): the staged record + its rev, the last effective projection, the
// compiled defaults, the apply-class meta table, the env-pinned knobs, and the
// last rejection. It reads the staged record fresh from the authority.
func (d *Dashboard) settingsResponse() map[string]any {
	store := settings.NewStore(d.bus)
	rec, rev, found, err := store.Get()
	var staged any
	if err == nil && found {
		staged = rec
	}
	d.mu.Lock()
	eff := d.effectiveSettings
	rej := d.lastRejection
	d.mu.Unlock()
	return map[string]any{
		"staged":        staged,
		"stagedRev":     rev,
		"effective":     eff,
		"defaults":      settings.Defaults(),
		"meta":          settings.Meta(),
		"envOverridden": settings.EnvOverridden(),
		"lastRejection": rej,
	}
}

// serveGetSettings returns the three-column settings state. Unauthenticated
// read, same posture as GET /api/roster (loopback-only via the host middleware).
func (d *Dashboard) serveGetSettings(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(d.settingsResponse()) //nolint:errcheck
}

// serveUpdateSettings handles POST /api/settings — the dashboard write path for
// staging config. It mirrors serveCreateJob: token-gated, MaxBytesReader,
// strict-decode, and delegates WHOLESALE to settings.Store.Put (the one
// authority). Typed errors: 400 (validation, naming the field), 401/403 (auth),
// 409 (stale stagedRev OR arming-without-confirm), 503 (unavailable). Never
// fake-success.
func (d *Dashboard) serveUpdateSettings(w http.ResponseWriter, r *http.Request) {
	if !d.checkWriteAuth(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxSettingsBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req settingsUpdateRequest
	if err := dec.Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeJSONError(w, `{"error":"bad_request","message":"body too large"}`, http.StatusBadRequest)
		} else {
			writeJSONError(w, fmt.Sprintf(`{"error":"bad_request","message":%q}`, "invalid JSON body: "+err.Error()), http.StatusBadRequest)
		}
		return
	}
	store := settings.NewStore(d.bus)

	// Merge-patch semantics: the UI (and any curl caller) POSTs only the fields
	// it wants to change, so the staged record is prev + patch — an absent field
	// keeps its previously staged value. Without this a partial POST would
	// silently wipe every knob it didn't mention (Store.Put is a deliberate
	// full-record CAS replace). The CAS on stagedRev below still rejects a write
	// racing another editor, so the merge base cannot be stale without a 409.
	prev, _, _, gerr := store.Get()
	if gerr != nil {
		writeJSONError(w, `{"error":"unavailable","message":"bus unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	rec := settings.Merge(prev, req.Record)

	// Field + self-consistency validation, naming the offending field. This is
	// the same check settings.Store.Put runs (against the compiled defaults, env
	// ignored); hoisted here so an obvious field error surfaces its typed message
	// before the apply-parity check below reports env conflicts.
	if verr := settings.ValidateRecord(rec); verr != nil {
		d.setRejection(verr.Error())
		writeJSONError(w, fmt.Sprintf(`{"error":"bad_request","message":%q}`, verr.Error()), http.StatusBadRequest)
		return
	}

	// Apply-parity validation (never fake success). The coordinator applies a
	// staged record by overlaying it onto its ENV-loaded config with env-wins
	// precedence (settings.Overlay honorEnv=true), then runs config.Validate — and
	// drops the WHOLE record if that fails. settings.ValidateRecord above resolves
	// against the compiled DEFAULTS (env ignored), so a record can pass the write
	// (201) yet be rejected at apply time — e.g. a staged awayAfter below an
	// env-pinned heartbeat — giving the operator a false success that never takes
	// effect. Validate against the SAME env-honoring resolution here so such a
	// record is refused now with a surfaced 400 naming the violated invariant.
	//
	// Only meaningful when the base config the coordinator overlays onto is itself
	// valid. In production d.cfg is the config.Load()-derived config the
	// coordinator also runs (ClaimTTL derived, invariants satisfied), so this
	// always runs and precisely mirrors the coordinator: any failure is the
	// staged record's doing. A bare/partial base (e.g. a test fixture) is skipped
	// — the coordinator, which owns the real valid base, remains the final gate.
	if config.Validate(d.cfg) == nil {
		if verr := config.Validate(settings.Overlay(d.cfg, rec)); verr != nil {
			msg := settings.ErrBadRecord.Error() + ": conflicts with the coordinator's effective config: " + verr.Error()
			d.setRejection(msg)
			writeJSONError(w, fmt.Sprintf(`{"error":"bad_request","message":%q}`, msg), http.StatusBadRequest)
			return
		}
	}

	// Arming gate: a change that arms real subscription spend / opens a port
	// requires an explicit confirm. Rejected with 409 confirmation_required and
	// the arming field list so the UI can name the consequence before re-POSTing.
	// The delta is computed on the MERGED record, so an arming knob that was
	// already staged (and confirmed) does not demand re-confirmation on an
	// unrelated later edit.
	arming, aerr := store.ArmingDelta(rec)
	if aerr != nil {
		writeJSONError(w, `{"error":"unavailable","message":"bus unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	if len(arming) > 0 && !req.Confirm {
		body, _ := json.Marshal(map[string]any{"error": "confirmation_required", "arming": arming})
		writeJSONError(w, string(body), http.StatusConflict)
		return
	}

	saved, err := store.Put(rec, req.StagedRev)
	if err != nil {
		switch {
		case errors.Is(err, settings.ErrBadRecord):
			d.setRejection(err.Error())
			writeJSONError(w, fmt.Sprintf(`{"error":"bad_request","message":%q}`, err.Error()), http.StatusBadRequest)
		case errors.Is(err, settings.ErrCASLost):
			writeJSONError(w, `{"error":"conflict","message":"stagedRev is stale — refetch and reapply"}`, http.StatusConflict)
		default:
			writeJSONError(w, fmt.Sprintf(`{"error":"unavailable","message":%q}`, err.Error()), http.StatusServiceUnavailable)
		}
		return
	}
	d.clearRejection()

	// Best-effort KindSettings poke so the coordinator re-reads the bucket,
	// hot-applies, and republishes its authoritative effective snapshot. The KV
	// write above is the authority; this publish is only the tap trigger.
	poke := envelope.SettingsPayload{Rev: saved.Rev, UpdatedBy: saved.UpdatedBy}
	if env, perr := envelope.New(envelope.KindSettings, "dashboard", envelope.SubjectSettings, &poke); perr == nil {
		d.bus.Publish(env) //nolint:errcheck
	}

	// Report which apply classes the delta touched so the UI shows what applied
	// now (hot) vs what needs a restart.
	applied := map[string]bool{
		string(settings.ApplyHot):                false,
		string(settings.ApplyRestartCoordinator): false,
		string(settings.ApplyRestartFleet):       false,
	}
	for _, f := range settings.ChangedFields(prev, saved) {
		if m, ok := settings.MetaFor(f); ok {
			applied[string(m.ApplyClass)] = true
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
		"applied": applied,
		"staged":  saved,
		"rev":     saved.Rev,
	})
}

func (d *Dashboard) setRejection(msg string) {
	d.mu.Lock()
	d.lastRejection = &settingsRejection{Message: msg}
	d.mu.Unlock()
}

func (d *Dashboard) clearRejection() {
	d.mu.Lock()
	d.lastRejection = nil
	d.mu.Unlock()
}
