// Package task owns the Task record — the authoritative KV shape for one DAG
// node in the autonomous work hierarchy (Job → Task → ask-Ticket). Triage
// (#24) decomposes a Job (internal/job) into Tasks; the scheduler (#25) reads
// the persisted DAG back from the tasks bucket and dispatches nodes whose
// dependencies are done; the worker (#26) executes them.
//
// Record shapes live in their domain package as the single authority (the
// same role job.Record plays for jobs and ticket.Record for the P2 async
// ask). The envelope package owns the rest of the task vocabulary: KindTask,
// TaskPayload, TaskState, the mesh.task.<id> subject, BucketTasks, and
// StreamTasks.
//
// Like a Job — and unlike a claim or a ticket — a Task is NOT a lease: no
// ExpiresAt, no TTL. It persists until its lifecycle reaches a terminal
// state.
package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

var (
	ErrBadRecord = errors.New("task: bad record")
	ErrCASLost   = errors.New("task: cas lost")
)

// Record is the authoritative tasks-bucket entry, keyed by ID in
// envelope.BucketTasks. Task ids are envelope.NewID() UUIDv7s, so they are
// valid mesh.task.<id> subject tokens. DependsOn holds task IDs (not planner
// node ids) — resolved at mint time by FromPlan — so the scheduler never
// needs the node-id mapping. Node preserves the planner's node id for
// traceability back to the plan.
type Record struct {
	ID          string             `json:"id"`
	Job         string             `json:"job"`
	Node        string             `json:"node"`
	Title       string             `json:"title"`
	Description string             `json:"description,omitempty"`
	Role        string             `json:"role"`
	DependsOn   []string           `json:"dependsOn,omitempty"`
	Files       []string           `json:"files,omitempty"`
	Acceptance  []string           `json:"acceptance,omitempty"`
	State       envelope.TaskState `json:"state"`
	CreatedAt   time.Time          `json:"createdAt"`
	// Branch is the worker output branch (mesh/worker/<id>[-N]) that holds this
	// task's committed work, recorded by the scheduler on success. A dependent
	// task's worker bases its worktree on the merge of its deps' Branches, so
	// the DAG carries code forward, not just execution order (#26). Empty until
	// a worker succeeds (and stays empty for a task that committed nothing).
	Branch string `json:"branch,omitempty"`
	// RetriesLeft is the re-dispatch budget remaining for this task (#85).
	// Decremented by the scheduler each time a reviewer returns request_changes
	// and the task is re-dispatched. When it reaches zero the next
	// request_changes verdict fails the task instead of re-dispatching.
	// Zero value on first dispatch; initialized to MESH_REVIEW_RETRIES by the
	// scheduler on the first request_changes verdict.
	RetriesLeft int `json:"retriesLeft,omitempty"`
	// ReviewFeedback carries the last reviewer's notes from a request_changes
	// verdict (#85). Injected into the worker prompt on re-dispatch so the
	// worker addresses the reviewer's concerns. Empty until first re-dispatch.
	ReviewFeedback string `json:"reviewFeedback,omitempty"`
	// Redispatched is set the first time the task is re-dispatched after a
	// request_changes (#85). It is the authoritative "has been re-dispatched"
	// flag: retry accounting must not infer this from ReviewFeedback emptiness,
	// because a reviewer may return request_changes with empty notes — which
	// would otherwise re-initialize the budget every round (unbounded retries).
	Redispatched bool `json:"redispatched,omitempty"`
	// EscalationReason is the worker's specific question or explanation of why
	// the task could not be completed without human input (#141). Set when the
	// task transitions to TaskEscalated; empty for all other states.
	EscalationReason string `json:"escalationReason,omitempty"`
}

// Event records one task transition, appended to the task-events stream.
// The tasks KV record stays the current-state authority; events let tests and
// observers deterministically replay how a task reached that state. #24 only
// emits To: TaskPending.
type Event struct {
	ID     string             `json:"id"`
	Job    string             `json:"job"`
	From   envelope.TaskState `json:"from,omitempty"`
	To     envelope.TaskState `json:"to"`
	By     string             `json:"by,omitempty"`
	At     time.Time          `json:"at"`
	Reason string             `json:"reason,omitempty"`
}

// Store writes the authoritative tasks KV bucket.
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

// CreateAll persists one job's freshly-minted tasks. Each record is written
// create-only (CAS) — fresh UUIDv7 ids never collide — and a TaskPending
// event is appended per task for replay. It fails on the first store error;
// triage treats any failure as a typed internal triage error, so partial
// writes for a job that never reaches triaged are inert (the scheduler only
// reads tasks of triaged jobs).
func (s Store) CreateAll(recs []Record) error {
	for _, rec := range recs {
		if err := validateNew(rec); err != nil {
			return err
		}
		if _, err := s.cli.KVPut(envelope.BucketTasks, rec.ID, rec, bus.PutOptions{CAS: bus.CreateOnly()}); err != nil {
			if errors.Is(err, bus.ErrCASLost) {
				return ErrCASLost
			}
			return err
		}
		ev := Event{ID: rec.ID, Job: rec.Job, To: envelope.TaskPending, At: rec.CreatedAt}
		if _, err := s.cli.StreamAppend(envelope.StreamTasks, ev); err != nil {
			return err
		}
	}
	return nil
}

// Get reads a single task record by id.
func (s Store) Get(id string) (Record, bool, error) {
	kv, found, err := s.cli.KVGet(envelope.BucketTasks, id)
	if err != nil || !found {
		return Record{}, found, err
	}
	var rec Record
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return Record{}, false, err
	}
	return rec, true, nil
}

// SetBranch records the worker output branch holding this task's committed
// work, so a dependent task's worker can base its worktree on it (#26
// dependency inheritance). It leaves State untouched (not a lifecycle move)
// and is idempotent. CAS on the read revision; the scheduler is the only
// post-creation writer of task records, so this never contends in practice.
func (s Store) SetBranch(id, branch string) error {
	kv, found, err := s.cli.KVGet(envelope.BucketTasks, id)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: %s", ErrNoSuchTask, id)
	}
	var rec Record
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrBadRecord, id, err)
	}
	if rec.Branch == branch {
		return nil
	}
	rec.Branch = branch
	if _, err := s.cli.KVPut(envelope.BucketTasks, id, rec, bus.PutOptions{CAS: bus.Rev(kv.Rev)}); err != nil {
		if errors.Is(err, bus.ErrCASLost) {
			return ErrCASLost
		}
		return err
	}
	return nil
}

// ListByJob returns every task of one job in stable plan order (creation
// order; ties broken by id). This is the scheduler's read path for the
// persisted DAG.
func (s Store) ListByJob(job string) ([]Record, error) {
	keys, err := s.cli.KVList(envelope.BucketTasks)
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0, len(keys))
	for _, kv := range keys {
		var rec Record
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		if rec.Job == job {
			records = append(records, rec)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if !records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].CreatedAt.Before(records[j].CreatedAt)
		}
		return records[i].ID < records[j].ID
	})
	return records, nil
}

func validateNew(rec Record) error {
	for field, val := range map[string]string{
		"id": rec.ID, "job": rec.Job, "node": rec.Node,
		"title": rec.Title, "role": rec.Role,
	} {
		if strings.TrimSpace(val) == "" {
			return fmt.Errorf("%w: missing %s", ErrBadRecord, field)
		}
	}
	if rec.State != envelope.TaskPending {
		return fmt.Errorf("%w: new task state must be pending", ErrBadRecord)
	}
	if rec.CreatedAt.IsZero() {
		return fmt.Errorf("%w: missing createdAt", ErrBadRecord)
	}
	return nil
}
