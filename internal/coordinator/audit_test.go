package coordinator

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

var updateGolden = flag.Bool("update", false, "rewrite audit golden files from the current record shape")

// auditConfig is fastConfig with the #29 bus-observed audit fan-out enabled
// (config.Load defaults it on; the fastConfig literal leaves it off so the
// pre-#29 presence/claim tests stay deterministic).
func auditConfig(t *testing.T) config.Config {
	c := fastConfig(t)
	c.AuditFanout = true
	return c
}

// readAudit drains every audit entry of the given category from the stream.
func readAudit(t *testing.T, cli *bus.Client, kind envelope.AuditCategory) []AuditEntry {
	t.Helper()
	entries, err := cli.StreamRead(envelope.StreamAudit, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []AuditEntry
	for _, e := range entries {
		var a AuditEntry
		if err := json.Unmarshal(e.Data, &a); err != nil {
			t.Fatal(err)
		}
		if a.Kind == kind {
			out = append(out, a)
		}
	}
	return out
}

func waitAudit(t *testing.T, cli *bus.Client, kind envelope.AuditCategory, n int, timeout time.Duration) []AuditEntry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := readAudit(t, cli, kind)
		if len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("audit %q never reached %d entries (have %d)", kind, n, len(readAudit(t, cli, kind)))
	return nil
}

func pub(t *testing.T, cli *bus.Client, env envelope.Envelope, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}
}

// TestAuditReconstructsTicketLifecycle is the #29 acceptance: a single ordered
// read of the audit stream reconstructs a ticket's major lifecycle — the ask
// that opened it, its FSM transitions, and the answer that resolved it.
func TestAuditReconstructsTicketLifecycle(t *testing.T) {
	cfg := auditConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	const ticket = "01976f00-0000-7000-8000-0000000000aa"

	// Asker opens a role-routed ask.
	env, err := envelope.New(envelope.KindAsk, "asker", envelope.SubjectAskRole("auth"),
		&envelope.AskPayload{Ticket: ticket, Role: "auth", Q: "RLS fix?"})
	pub(t, cli, env, err)

	// Coordinator routes it (FSM transition tap), responder accepts, answers, asker closes.
	for _, st := range []envelope.TicketState{envelope.TicketRouted, envelope.TicketAccepted, envelope.TicketAnswered, envelope.TicketClosed} {
		env, err := envelope.New(envelope.KindTicket, "coordinator", envelope.SubjectTicket(ticket),
			&envelope.TicketPayload{Ticket: ticket, State: st, By: "responder"})
		pub(t, cli, env, err)
	}
	env, err = envelope.New(envelope.KindAnswer, "responder", envelope.SubjectAnswer(ticket),
		&envelope.AnswerPayload{Ticket: ticket, Answer: "use SECURITY DEFINER"})
	pub(t, cli, env, err)

	// Drain the whole stream once, ordered, and reconstruct this ticket's story.
	deadline := time.Now().Add(2 * time.Second)
	var story []string
	for time.Now().Before(deadline) {
		entries, err := cli.StreamRead(envelope.StreamAudit, 0)
		if err != nil {
			t.Fatal(err)
		}
		story = story[:0]
		for _, e := range entries {
			var a AuditEntry
			if err := json.Unmarshal(e.Data, &a); err != nil {
				t.Fatal(err)
			}
			if a.Ticket == ticket {
				story = append(story, string(a.Kind)+":"+a.Event)
			}
		}
		if len(story) == 6 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	want := []string{"ask:opened", "ticket:routed", "ticket:accepted", "ticket:answered", "ticket:closed", "answer:answered"}
	if len(story) != len(want) {
		t.Fatalf("ticket story = %v, want %v", story, want)
	}
	for i := range want {
		if story[i] != want[i] {
			t.Fatalf("ticket story = %v, want %v", story, want)
		}
	}
}

// TestAuditDedupsInboxEcho confirms a role ask routed to an inbox is logged
// exactly once (on its origin subject), not twice.
func TestAuditDedupsInboxEcho(t *testing.T) {
	cfg := auditConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	const ticket = "01976f00-0000-7000-8000-0000000000bb"
	// Origin ask.
	env, err := envelope.New(envelope.KindAsk, "asker", envelope.SubjectAskRole("auth"),
		&envelope.AskPayload{Ticket: ticket, Role: "auth", Q: "q?"})
	pub(t, cli, env, err)
	// Routed inbox copy — same kind, must NOT produce a second audit entry.
	env, err = envelope.New(envelope.KindAsk, "asker", envelope.SubjectInbox("expert"),
		&envelope.AskPayload{Ticket: ticket, Role: "auth", Q: "q?"})
	pub(t, cli, env, err)

	got := waitAudit(t, cli, envelope.AuditAsk, 1, 2*time.Second)
	// Give a beat for any (incorrect) second entry to land before asserting count.
	time.Sleep(100 * time.Millisecond)
	got = readAudit(t, cli, envelope.AuditAsk)
	var forTicket int
	for _, a := range got {
		if a.Ticket == ticket {
			forTicket++
		}
	}
	if forTicket != 1 {
		t.Fatalf("ask audit entries for ticket = %d, want 1 (inbox echo must be deduped)", forTicket)
	}
}

// TestAuditFanoutWorkHierarchy covers job/task/triage/worker/fleet fan-out —
// the autonomous work hierarchy's major lifecycle events all land in the log.
func TestAuditFanoutWorkHierarchy(t *testing.T) {
	cfg := auditConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	const jobID = "01976f00-0000-7000-8000-0000000000c1"
	const taskID = "01976f00-0000-7000-8000-0000000000c2"

	env, err := envelope.New(envelope.KindJob, "submitter", envelope.SubjectJob(jobID),
		&envelope.JobPayload{ID: jobID, Repo: "demo", Source: "manual", Title: "add X", State: envelope.JobOpen})
	pub(t, cli, env, err)
	env, err = envelope.New(envelope.KindTask, "triager", envelope.SubjectTask(taskID),
		&envelope.TaskPayload{ID: taskID, Job: jobID, Role: "builder", Title: "do X", State: envelope.TaskPending})
	pub(t, cli, env, err)
	env, err = envelope.New(envelope.KindTriage, "triager", envelope.SubjectTriage(jobID),
		&envelope.TriagePayload{Job: jobID, Result: envelope.TriageOK, Tasks: 1})
	pub(t, cli, env, err)
	env, err = envelope.New(envelope.KindWorker, "scheduler", envelope.SubjectWorker(taskID),
		&envelope.WorkerPayload{Task: taskID, Job: jobID, Result: envelope.WorkerError, Code: envelope.WorkerRateLimited, Reason: "429"})
	pub(t, cli, env, err)
	env, err = envelope.New(envelope.KindFleet, "scheduler", envelope.SubjectFleet,
		&envelope.FleetPayload{State: envelope.FleetPaused, Code: envelope.FleetBudgetExhausted, Reason: "cap", SpentUSD: 5, BudgetUSD: 5})
	pub(t, cli, env, err)

	jobs := waitAudit(t, cli, envelope.AuditJob, 1, 2*time.Second)
	if jobs[0].Job != jobID || jobs[0].State != string(envelope.JobOpen) {
		t.Fatalf("job audit = %+v", jobs[0])
	}
	tasks := waitAudit(t, cli, envelope.AuditTask, 1, 2*time.Second)
	if tasks[0].Task != taskID || tasks[0].Job != jobID || tasks[0].Role != "builder" {
		t.Fatalf("task audit = %+v", tasks[0])
	}
	tri := waitAudit(t, cli, envelope.AuditTriage, 1, 2*time.Second)
	if tri[0].Job != jobID || tri[0].Result != string(envelope.TriageOK) {
		t.Fatalf("triage audit = %+v", tri[0])
	}
	wk := waitAudit(t, cli, envelope.AuditWorker, 1, 2*time.Second)
	// On error the failure class travels in State so taps discriminate without prose.
	if wk[0].Task != taskID || wk[0].Result != string(envelope.WorkerError) || wk[0].State != string(envelope.WorkerRateLimited) {
		t.Fatalf("worker audit = %+v", wk[0])
	}
	fl := waitAudit(t, cli, envelope.AuditFleet, 1, 2*time.Second)
	if fl[0].State != string(envelope.FleetPaused) || fl[0].Result != string(envelope.FleetBudgetExhausted) {
		t.Fatalf("fleet audit = %+v", fl[0])
	}
}

// TestAuditClaimAttempt confirms agent-initiated claim attempts are audited
// (distinct from the coordinator's reclaim-on-death claim audit).
func TestAuditClaimAttempt(t *testing.T) {
	cfg := auditConfig(t)
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	env, err := envelope.New(envelope.KindClaim, "ag", envelope.SubjectClaim("demo"),
		&envelope.ClaimPayload{ID: "ag", Path: "src/x.go", Repo: "demo", Result: envelope.ClaimClaimed})
	pub(t, cli, env, err)

	got := waitAudit(t, cli, envelope.AuditClaim, 1, 2*time.Second)
	var attempt *AuditEntry
	for i := range got {
		if got[i].Event == "attempt" {
			attempt = &got[i]
		}
	}
	if attempt == nil {
		t.Fatalf("no claim attempt audit entry; got %+v", got)
	}
	if attempt.Path != "src/x.go" || attempt.Result != string(envelope.ClaimClaimed) {
		t.Fatalf("claim attempt audit = %+v", *attempt)
	}
}

// TestAuditFanoutDisabled confirms MESH_AUDIT_FANOUT=off suppresses the
// bus-observed fan-out while the always-on presence audits still flow.
func TestAuditFanoutDisabled(t *testing.T) {
	cfg := fastConfig(t) // AuditFanout zero-value = false
	startCoordinator(t, cfg)
	cli := dialBus(t, cfg)

	// Presence audit must still appear (it is emitted at the mutation site).
	register(t, cli, "p1")

	// A job event must NOT be fanned in.
	const jobID = "01976f00-0000-7000-8000-0000000000d1"
	env, err := envelope.New(envelope.KindJob, "submitter", envelope.SubjectJob(jobID),
		&envelope.JobPayload{ID: jobID, Repo: "demo", Source: "manual", Title: "x", State: envelope.JobOpen})
	pub(t, cli, env, err)

	// Wait for the presence "registered" entry, then assert no job entry exists.
	deadline := time.Now().Add(2 * time.Second)
	var sawPresence bool
	for time.Now().Before(deadline) && !sawPresence {
		for _, a := range readAudit(t, cli, envelope.AuditPresence) {
			if a.ID == "p1" && a.Event == "registered" {
				sawPresence = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawPresence {
		t.Fatal("presence audit suppressed by AuditFanout=off; it must always flow")
	}
	if jobs := readAudit(t, cli, envelope.AuditJob); len(jobs) != 0 {
		t.Fatalf("job audit fanned in with AuditFanout=off: %+v", jobs)
	}
}

// TestAuditEntryGolden pins the serialized audit-record shape. A presence/claim
// entry must encode byte-identically to the pre-#29 shape (the new correlation
// fields are omitempty); a fully-populated entry pins the additive fields.
func TestAuditEntryGolden(t *testing.T) {
	ts := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	cases := map[string]AuditEntry{
		"presence": {Kind: envelope.AuditPresence, ID: "codex-7", Event: "registered", TS: ts},
		"claim":    {Kind: envelope.AuditClaim, ID: "codex-7", Event: "reclaimed", Path: "src/x.go", Repo: "demo", TS: ts},
		"ticket": {Kind: envelope.AuditTicket, ID: "responder", Event: "answered", TS: ts,
			Ticket: "01976f00-0000-7000-8000-00000000007e", State: "answered", By: "responder", Detail: "within TTL"},
		"worker": {Kind: envelope.AuditWorker, ID: "scheduler", Event: "error", TS: ts,
			Task: "01976f00-0000-7000-8000-0000000000a1", Job: "01976f00-0000-7000-8000-00000000007e",
			Result: "error", State: "rate_limited", Detail: "api_error_status 429"},
	}
	for name, entry := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("testdata", "audit-"+name+".json")
			got, err := json.MarshalIndent(entry, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			if *updateGolden {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v (run with -update to generate)", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("audit record drifted from golden\ngolden: %s\ngot:    %s", want, got)
			}
		})
	}
}
