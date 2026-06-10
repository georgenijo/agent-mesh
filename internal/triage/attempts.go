package triage

// Retry/backoff policy for triage (#64). This file owns three things:
//
//  1. The TRANSIENT vs PERMANENT classification of a typed TriageErrorCode.
//  2. The durable per-job attempt bookkeeping (envelope.BucketTriageAttempts),
//     so attempt count + next-retry deadline survive a coordinator restart
//     instead of resetting (the bucket is persisted alongside jobs/tasks, #65).
//  3. The bounded exponential backoff schedule.
//
// The job record (job.Record, golden-pinned) is never reshaped: the policy
// state lives in its own bucket keyed by job id. A job stays `open` while it
// backs off — no new JobState is invented — and the loop simply skips it until
// its nextRetryAt elapses, so a down planner is never hammered every tick.

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// transientErr reports whether a typed triage failure is worth retrying.
//
// TRANSIENT (retry under the cap) — the failure may not recur:
//   - planner_unavailable: the CLI could not run to completion (missing binary
//     during a deploy, spawn failure, timeout under load, a crash). The next
//     attempt may find it healthy.
//   - planner_failed WITH an api_error_status: the planner reached the API but
//     it returned a transient error (rate-limit 429, overloaded 529, 5xx). This
//     is exactly the "possibly planner_failed with api_error_status" the issue
//     calls out as retryable. A backoff-then-retry is the right move.
//   - internal: a store/bus/CAS hiccup persisting tasks or transitioning the
//     job. The plan itself may be fine; the next sweep can re-attempt. Capped
//     (transient-with-cap) so a genuinely wedged store still fails the job
//     rather than looping forever — documented call.
//
// PERMANENT (fail fast, no retry) — retrying the same prompt burns a planner
// turn (= money) for nothing:
//   - planner_failed WITHOUT an api_error_status: the planner emitted prose, a
//     non-result object, a plain error subtype, or is_error. That output is
//     deterministic for this prompt; a retry would reproduce it.
//   - bad_plan: the success text is not a parseable plan document.
//   - invalid_dag: the plan parsed but failed DAG validation (cycle, unknown
//     role, missing/duplicate node id, bounds).
func transientErr(err error) bool {
	var te *Error
	if !errors.As(err, &te) {
		// Untyped error: treated as TriageInternal (CodeOf), which is transient.
		return true
	}
	switch te.Code {
	case envelope.TriagePlannerUnavailable, envelope.TriageInternal:
		return true
	case envelope.TriagePlannerFailed:
		return te.apiError // only a transient API blip is retryable
	default: // TriageBadPlan, TriageInvalidDAG
		return false
	}
}

// maxTriageBackoff caps the exponential schedule so a long-lived job in backoff
// never waits longer than this between attempts (and 2^N can't overflow the
// duration). The default base (30s) over the default cap (4 attempts) never
// reaches this; it only bites if an operator sets a large base or attempt cap.
const maxTriageBackoff = 30 * time.Minute

// backoffFor returns the delay before the Nth-attempt retry: base*2^(attempts-1),
// clamped to maxTriageBackoff. attempts is the number already made (>=1).
func backoffFor(base time.Duration, attempts int) time.Duration {
	if base <= 0 {
		base = 30 * time.Second
	}
	d := base
	for i := 1; i < attempts && d < maxTriageBackoff; i++ {
		d *= 2
	}
	if d > maxTriageBackoff {
		d = maxTriageBackoff
	}
	return d
}

// attemptRecord is the durable per-job retry bookkeeping. It is policy state,
// not a job authority: it records how many planner turns a job has consumed,
// the last typed code, and the earliest time the loop may retry. Keyed by job
// id in envelope.BucketTriageAttempts.
type attemptRecord struct {
	Job         string                   `json:"job"`
	Attempts    int                      `json:"attempts"`              // planner invocations made so far
	LastCode    envelope.TriageErrorCode `json:"lastCode,omitempty"`    // last typed failure
	NextRetryAt time.Time                `json:"nextRetryAt,omitempty"` // earliest retry; zero = retry now
	UpdatedAt   time.Time                `json:"updatedAt"`
}

// attemptStore reads and writes the durable triage-attempt bucket. The bus KV
// is the one authority; there is no in-memory mirror, so the schedule survives
// a coordinator restart by construction.
type attemptStore struct {
	cli *bus.Client
}

func newAttemptStore(cli *bus.Client) attemptStore { return attemptStore{cli: cli} }

// get returns the attempt record for a job, or a zero record (Attempts 0) when
// none is persisted yet. The bool reports whether a record was found.
func (s attemptStore) get(job string) (attemptRecord, bool, error) {
	kv, found, err := s.cli.KVGet(envelope.BucketTriageAttempts, job)
	if err != nil || !found {
		return attemptRecord{Job: job}, false, err
	}
	var rec attemptRecord
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		// A corrupt record degrades to "no record": the job is re-attempted
		// from zero rather than wedged. Best-effort policy state, not authority.
		return attemptRecord{Job: job}, false, nil
	}
	return rec, true, nil
}

// put writes the attempt record unconditionally. Only the single loop goroutine
// writes this bucket (the sweep is sequential), so there is no CAS race to lose.
func (s attemptStore) put(rec attemptRecord) error {
	rec.UpdatedAt = time.Now().UTC()
	_, err := s.cli.KVPut(envelope.BucketTriageAttempts, rec.Job, rec, bus.PutOptions{})
	return err
}

// clear removes a job's attempt record (it left the open state). Missing is fine.
func (s attemptStore) clear(job string) error {
	err := s.cli.KVDelete(envelope.BucketTriageAttempts, job)
	if errors.Is(err, bus.ErrNoSuchKey) {
		return nil
	}
	return err
}
