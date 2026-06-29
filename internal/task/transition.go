package task

// Task lifecycle transitions (#25). Mirrors internal/job's legality table +
// CAS Transition: the tasks KV record stays the one authority, every move is
// re-read + revision-guarded, and an Event is appended for replay. #24 mints
// pending; the scheduler drives the rest. The TaskState vocabulary itself is
// frozen wire contract in internal/envelope — the scheduler's richer gating
// view (queued/runnable/blocked/skipped) is computed, never persisted.

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

var (
	ErrNoSuchTask    = errors.New("task: no such task")
	ErrBadTransition = errors.New("task: illegal transition")
)

// transitions is the legal-successor table for the task lifecycle.
// pending→running is the scheduler dispatch; pending→failed covers a dispatch
// that could never start; pending→cancelled is the skip of a dependent whose
// dependency failed (or a doomed job's fail-fast). Terminal states (done,
// failed, cancelled, escalated) have no successors.
var transitions = map[envelope.TaskState][]envelope.TaskState{
	envelope.TaskPending: {envelope.TaskRunning, envelope.TaskFailed, envelope.TaskCancelled},
	envelope.TaskRunning: {envelope.TaskDone, envelope.TaskFailed, envelope.TaskCancelled, envelope.TaskEscalated},
}

// CanTransition reports whether from→to is a legal task transition.
func CanTransition(from, to envelope.TaskState) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Redispatch atomically decrements the retry counter, stores reviewer feedback,
// and resets a running task back to pending — ready for re-dispatch on the
// scheduler's next sweep (#85). It bypasses the legality table because
// running→pending is only valid through this path (re-dispatch after
// request_changes), keeping the general Transition path clean. The caller
// (scheduler loop goroutine, the single writer) guarantees the task is
// currently running.
func (s Store) Redispatch(id, feedback string, retriesLeft int) (Record, error) {
	kv, found, err := s.cli.KVGet(envelope.BucketTasks, id)
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, fmt.Errorf("%w: %s", ErrNoSuchTask, id)
	}
	var rec Record
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return Record{}, fmt.Errorf("%w: %s: %v", ErrBadRecord, id, err)
	}
	if rec.State != envelope.TaskRunning {
		return Record{}, fmt.Errorf("%w: task %s is %s, not running", ErrBadTransition, id, rec.State)
	}
	rec.State = envelope.TaskPending
	rec.ReviewFeedback = feedback
	rec.RetriesLeft = retriesLeft
	rec.Redispatched = true
	if _, err := s.cli.KVPut(envelope.BucketTasks, id, rec, bus.PutOptions{CAS: bus.Rev(kv.Rev)}); err != nil {
		if errors.Is(err, bus.ErrCASLost) {
			return Record{}, ErrCASLost
		}
		return Record{}, err
	}
	ev := Event{
		ID:     id,
		Job:    rec.Job,
		From:   envelope.TaskRunning,
		To:     envelope.TaskPending,
		By:     "scheduler",
		At:     s.now(),
		Reason: "review requested changes; re-dispatching",
	}
	s.cli.StreamAppend(envelope.StreamTasks, ev) //nolint:errcheck
	return rec, nil
}

// Escalate transitions a running task to TaskEscalated and records the
// escalation reason/question. It uses the general Transition table (running→
// escalated is legal) and additionally writes EscalationReason onto the record
// so the scheduler and human operators can see why the task was paused. The
// caller (scheduler loop goroutine) guarantees the task is currently running.
func (s Store) Escalate(id, reason, by string) (Record, error) {
	kv, found, err := s.cli.KVGet(envelope.BucketTasks, id)
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, fmt.Errorf("%w: %s", ErrNoSuchTask, id)
	}
	var rec Record
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return Record{}, fmt.Errorf("%w: %s: %v", ErrBadRecord, id, err)
	}
	if rec.State != envelope.TaskRunning {
		return Record{}, fmt.Errorf("%w: task %s is %s, not running", ErrBadTransition, id, rec.State)
	}
	rec.State = envelope.TaskEscalated
	rec.EscalationReason = reason
	if _, err := s.cli.KVPut(envelope.BucketTasks, id, rec, bus.PutOptions{CAS: bus.Rev(kv.Rev)}); err != nil {
		if errors.Is(err, bus.ErrCASLost) {
			return Record{}, ErrCASLost
		}
		return Record{}, err
	}
	ev := Event{
		ID:     id,
		Job:    rec.Job,
		From:   envelope.TaskRunning,
		To:     envelope.TaskEscalated,
		By:     by,
		At:     s.now(),
		Reason: reason,
	}
	s.cli.StreamAppend(envelope.StreamTasks, ev) //nolint:errcheck
	return rec, nil
}

// Transition moves a task from one state to the next under revision CAS: the
// record is re-read, the from-state and table legality are checked, and the
// write is guarded by the read revision — a concurrent writer loses exactly
// one of the two races (ErrCASLost), never both-win. An Event is appended for
// replay. The KV record stays the one authority; publishing the derived
// KindTask envelope is the caller's concern.
func (s Store) Transition(id string, from, to envelope.TaskState, by, reason string) (Record, error) {
	if !CanTransition(from, to) {
		return Record{}, fmt.Errorf("%w: %s -> %s", ErrBadTransition, from, to)
	}
	kv, found, err := s.cli.KVGet(envelope.BucketTasks, id)
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, fmt.Errorf("%w: %s", ErrNoSuchTask, id)
	}
	var rec Record
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return Record{}, fmt.Errorf("%w: %s: %v", ErrBadRecord, id, err)
	}
	if rec.State != from {
		return Record{}, fmt.Errorf("%w: task %s is %s, not %s", ErrBadTransition, id, rec.State, from)
	}
	rec.State = to
	if _, err := s.cli.KVPut(envelope.BucketTasks, id, rec, bus.PutOptions{CAS: bus.Rev(kv.Rev)}); err != nil {
		if errors.Is(err, bus.ErrCASLost) {
			return Record{}, ErrCASLost
		}
		return Record{}, err
	}
	// Best-effort replay record: the KV authority already moved.
	ev := Event{ID: id, Job: rec.Job, From: from, To: to, By: by, At: s.now(), Reason: reason}
	s.cli.StreamAppend(envelope.StreamTasks, ev) //nolint:errcheck
	return rec, nil
}
