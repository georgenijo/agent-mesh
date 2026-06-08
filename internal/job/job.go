// Package job owns the Job record — the authoritative KV shape behind the
// autonomous work hierarchy (Job → Task → ask-Ticket). A Job is the top-level
// intake created by `mesh submit` (#23); triage (#24) decomposes it into Tasks.
// Record shapes live in their domain package as the single authority (the same
// role agentcard.RegistryRecord plays for presence, claim.Record for claims,
// and ticket.Record for the P2 async ask). The envelope package owns the rest
// of the job vocabulary: KindJob, JobPayload, JobState, the mesh.job.<id>
// subject, BucketJobs, and StreamJobs.
//
// A Job is NOT a lease: there is no ExpiresAt and no TTL. Unlike a claim or a
// ticket, a job persists until its lifecycle reaches a terminal state. #23 only
// mints JobOpen; the later transitions are #24–#26's.
package job

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
	ErrBadRecord = errors.New("job: bad record")
	ErrNoSuchJob = errors.New("job: no such job")
	ErrCASLost   = errors.New("job: cas lost")
)

// SourceManual and SourceGitHub are the two recognized job sources.
const (
	SourceManual = "manual"
	SourceGitHub = "github"
)

var sources = map[string]bool{SourceManual: true, SourceGitHub: true}

// Record is the authoritative jobs-bucket entry, keyed by ID in
// envelope.BucketJobs. Job ids are envelope.NewID() UUIDv7s, so they are valid
// mesh.job.<id> subject tokens. The KV record is the one authority for job
// state; mesh.job.<id> envelopes (KindJob) are derived observability events.
type Record struct {
	ID        string            `json:"id"`
	Repo      string            `json:"repo"`
	Source    string            `json:"source"`              // "manual" | "github"
	SourceRef string            `json:"sourceRef,omitempty"` // e.g. "owner/repo#123" or issue URL
	Title     string            `json:"title"`
	Body      string            `json:"body,omitempty"`
	State     envelope.JobState `json:"state"` // always JobOpen at creation
	CreatedAt time.Time         `json:"createdAt"`
}

// Event records one job transition. The jobs KV record is still the
// current-state authority; events let tests and observers deterministically
// replay how a job reached that state. #23 only emits To: JobOpen.
type Event struct {
	ID     string            `json:"id"`
	From   envelope.JobState `json:"from,omitempty"`
	To     envelope.JobState `json:"to"`
	By     string            `json:"by,omitempty"`
	At     time.Time         `json:"at"`
	Reason string            `json:"reason,omitempty"`
}

// Store writes the authoritative jobs KV bucket.
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

// Create records a new Job. Each submit creates a NEW job with a new id — there
// is no dedup here (dedup is #29 policy). The record is written create-only
// (CAS), so a fresh UUIDv7 id never collides; a JobOpen event is appended to the
// job-events stream for replay.
func (s Store) Create(rec Record) (Record, error) {
	if s.now == nil {
		s.now = func() time.Time { return time.Now().UTC() }
	}
	if rec.ID == "" {
		rec.ID = envelope.NewID()
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = s.now()
	}
	rec.State = envelope.JobOpen
	if err := validateNew(rec); err != nil {
		return Record{}, err
	}
	if _, err := s.cli.KVPut(envelope.BucketJobs, rec.ID, rec, bus.PutOptions{CAS: bus.CreateOnly()}); err != nil {
		if errors.Is(err, bus.ErrCASLost) {
			return Record{}, ErrCASLost
		}
		return Record{}, err
	}
	_ = s.append(Event{ID: rec.ID, To: envelope.JobOpen, At: rec.CreatedAt})
	return rec, nil
}

// Get reads a single job record by id.
func (s Store) Get(id string) (Record, bool, error) {
	kv, found, err := s.cli.KVGet(envelope.BucketJobs, id)
	if err != nil || !found {
		return Record{}, found, err
	}
	var rec Record
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return Record{}, false, err
	}
	return rec, true, nil
}

// List returns every job, oldest first. The scheduler and dashboard read
// through this.
func (s Store) List() ([]Record, error) {
	keys, err := s.cli.KVList(envelope.BucketJobs)
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0, len(keys))
	for _, kv := range keys {
		var rec Record
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].CreatedAt.Before(records[j].CreatedAt) })
	return records, nil
}

func (s Store) append(ev Event) error {
	if ev.At.IsZero() {
		ev.At = s.now()
	}
	_, err := s.cli.StreamAppend(envelope.StreamJobs, ev)
	return err
}

func validateNew(rec Record) error {
	if strings.TrimSpace(rec.ID) == "" {
		return fmt.Errorf("%w: missing id", ErrBadRecord)
	}
	for field, val := range map[string]string{"repo": rec.Repo, "title": rec.Title, "source": rec.Source} {
		if strings.TrimSpace(val) == "" {
			return fmt.Errorf("%w: missing %s", ErrBadRecord, field)
		}
	}
	if !sources[rec.Source] {
		return fmt.Errorf("%w: unknown source %q (want manual|github)", ErrBadRecord, rec.Source)
	}
	if rec.State != envelope.JobOpen {
		return fmt.Errorf("%w: new job state must be open", ErrBadRecord)
	}
	return nil
}
