package coordinator

// Durability across a coordinator restart (#65): jobs (#23) and tasks (#24)
// are authoritative KV records with no live owner to re-establish them, unlike
// the registry (sidecars re-register) and claims (holders re-establish). The
// coordinator persists their buckets, so a Stop/Start does not lose an open
// job or a triaged task DAG. These tests drive the records through the real
// job.Store / task.Store paths, bounce the coordinator, and assert the records
// (and the revision counter) survive — and that registry presence does NOT
// (the explicit non-goal: it self-heals by re-registration).

import (
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// freshBus dials a new bus client on the coordinator's socket (after a restart
// the previous client's connection is gone; a fresh dial is the simplest
// honest reconnect). Closed via Cleanup.
func freshBus(t *testing.T, cfg config.Config) *bus.Client {
	t.Helper()
	cli, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cli.Close)
	return cli
}

func TestJobsAndTasksSurviveCoordinatorRestart(t *testing.T) {
	cfg := fastConfig(t)

	// --- first lifetime: create a job and a triaged DAG through the real stores
	c1 := New(cfg, nil)
	if err := c1.Start(); err != nil {
		t.Fatal(err)
	}
	cli1, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	jobs1 := job.NewStore(cli1)
	tasks1 := task.NewStore(cli1)

	rec, err := jobs1.Create(job.Record{Repo: "demo", Source: job.SourceManual, Title: "fix the thing"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	taskRecs := []task.Record{
		{ID: envelope.NewID(), Job: rec.ID, Node: "impl", Title: "implement", Role: "builder", State: envelope.TaskPending, CreatedAt: now},
		{ID: envelope.NewID(), Job: rec.ID, Node: "review", Title: "review", Role: "reviewer", State: envelope.TaskPending, CreatedAt: now},
	}
	if err := tasks1.CreateAll(taskRecs); err != nil {
		t.Fatal(err)
	}
	// Drive a real lifecycle transition so the durable record is not just the
	// open state: open→triaged, exactly as triage commits it.
	if _, err := jobs1.Transition(rec.ID, envelope.JobOpen, envelope.JobTriaged, "test", "tasks: 2"); err != nil {
		t.Fatal(err)
	}

	cli1.Close()
	c1.Stop() // hard bounce: socket and in-memory KV are gone

	// --- second lifetime: same MeshDir, fresh process state
	c2 := New(cfg, nil)
	if err := c2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c2.Stop)
	cli2 := freshBus(t, cfg)
	jobs2 := job.NewStore(cli2)
	tasks2 := task.NewStore(cli2)

	got, found, err := jobs2.Get(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("job did not survive coordinator restart")
	}
	if got.Title != "fix the thing" || got.Repo != "demo" || got.State != envelope.JobTriaged {
		t.Fatalf("job replayed as %+v, want title/repo/state preserved and state=triaged", got)
	}

	dag, err := tasks2.ListByJob(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(dag) != 2 {
		t.Fatalf("task DAG replayed %d nodes, want 2", len(dag))
	}
	// Both DAG nodes survived with their fields intact. Order is by CreatedAt
	// then id (ListByJob); these two share a CreatedAt, so assert on content by
	// node id, not slice position.
	byNode := map[string]task.Record{}
	for _, tr := range dag {
		byNode[tr.Node] = tr
	}
	if byNode["impl"].Role != "builder" {
		t.Fatalf("impl node replayed with role %q, want builder", byNode["impl"].Role)
	}
	if byNode["review"].Role != "reviewer" {
		t.Fatalf("review node replayed with role %q, want reviewer", byNode["review"].Role)
	}

	// The job's revision counter resumed past its pre-restart value: a CAS
	// transition off the replayed record must succeed (no rev collision).
	if _, err := jobs2.Transition(rec.ID, envelope.JobTriaged, envelope.JobScheduled, "test", "post-restart"); err != nil {
		t.Fatalf("CAS transition on replayed job failed (rev did not resume?): %v", err)
	}
}

// TestRegistryDoesNotSurviveRestart pins the non-goal: presence is a TTL lease
// that self-heals by re-registration, so it must NOT be durable.
func TestRegistryDoesNotSurviveRestart(t *testing.T) {
	cfg := fastConfig(t)

	c1 := New(cfg, nil)
	if err := c1.Start(); err != nil {
		t.Fatal(err)
	}
	cli1, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	register(t, cli1, "agent-1")
	waitFor(t, 2*time.Second, "agent-1 registered", func() bool {
		_, found := getRecord(t, cli1, "agent-1")
		return found
	})
	cli1.Close()
	c1.Stop()

	c2 := New(cfg, nil)
	if err := c2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c2.Stop)
	cli2 := freshBus(t, cfg)
	if _, found := getRecord(t, cli2, "agent-1"); found {
		t.Fatal("registry record survived restart; presence must self-heal by re-registration, not durability")
	}
}

// TestTriageReSweepsStillOpenJobAfterRestart confirms the self-healing sweep: a
// job left open by the first lifetime (e.g. the coordinator died before triaging
// it — here, triage was simply off) is durable AND triaged by the new
// coordinator's sweep. The first lifetime made no attempt, so there is no #64
// attempt record to resume; the fresh sweep triages from a clean slate. (The
// resume-mid-backoff case is unit-tested in internal/triage.)
func TestTriageReSweepsStillOpenJobAfterRestart(t *testing.T) {
	cfg := fastConfig(t)
	cfg.TriageTimeout = 10 * time.Second

	// First lifetime: NO planner (triage off). A submitted job stays open and
	// durable — exactly the "coordinator died before triage" case.
	c1 := New(cfg, nil)
	if err := c1.Start(); err != nil {
		t.Fatal(err)
	}
	cli1, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := job.NewStore(cli1).Create(job.Record{Repo: "demo", Source: job.SourceManual, Title: "triage me"})
	if err != nil {
		t.Fatal(err)
	}
	cli1.Close()
	c1.Stop()

	// Second lifetime: planner ON. The durable still-open job must be swept and
	// driven to triaged by the fresh coordinator.
	cfg.PlannerCLI = plannerScript(t, coordPlanJSON)
	c2 := New(cfg, nil)
	if err := c2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c2.Stop)
	cli2 := freshBus(t, cfg)
	jobs2 := job.NewStore(cli2)
	waitJobState(t, jobs2, rec.ID, envelope.JobTriaged)

	dag, err := task.NewStore(cli2).ListByJob(rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(dag) != 2 {
		t.Fatalf("re-swept job produced %d tasks, want 2", len(dag))
	}
}
