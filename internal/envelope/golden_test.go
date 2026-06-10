package envelope

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
)

// update rewrites the golden files from the current contract:
//
//	go test ./internal/envelope -run TestGolden -update
var update = flag.Bool("update", false, "rewrite golden files from the current contract")

// Pinned identity for goldens — a fixed UUIDv7 literal and a fixed timestamp,
// so regenerated files differ only when the contract differs.
const (
	goldenEnvID  = "01976f00-0000-7000-8000-000000000001"
	goldenTicket = "01976f00-0000-7000-8000-00000000007e"
)

var goldenTS = time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC)

type goldenCase struct {
	kind    Kind
	to      string
	subject string
	payload validator // representative payload: every optional field set
	decoded validator // fresh instance for DecodeInto
}

func goldenCases() []goldenCase {
	fullCard := agentcard.Card{
		ID: "codex-7", Name: "codex-7", Role: "auth",
		Caps: []string{"go", "backend"}, Repo: "demo", CWD: "/repo", Model: "opus", PID: 4242,
	}
	return []goldenCase{
		{KindRegister, "", SubjectRegister,
			&RegisterPayload{Card: fullCard}, &RegisterPayload{}},
		{KindLeave, "", SubjectLeave,
			&LeavePayload{ID: "codex-7", Reason: "done"}, &LeavePayload{}},
		{KindHeartbeat, "", SubjectHeartbeat("codex-7"),
			&HeartbeatPayload{ID: "codex-7", Status: "building"}, &HeartbeatPayload{}},
		{KindStatus, "", SubjectStatus("codex-7"),
			&StatusPayload{ID: "codex-7", Text: "building RRULE builder"}, &StatusPayload{}},
		{KindAnnounce, "", SubjectAnnounce("demo"),
			&AnnouncePayload{ID: "codex-7", Intent: "editing EventForm.tsx",
				Paths: []string{"src/EventForm.tsx", "src/api.ts"}, Repo: "demo"}, &AnnouncePayload{}},
		{KindClaim, "", SubjectClaim("demo"),
			&ClaimPayload{ID: "codex-7", Path: "src/EventForm.tsx", Repo: "demo",
				Result: ClaimClaimed}, &ClaimPayload{}},
		{KindAsk, "claude-2", SubjectAskRole("auth"),
			&AskPayload{Ticket: goldenTicket, Role: "auth", To: "claude-2",
				Q: "RLS recursion fix?", Ctx: "policy on users joins itself"}, &AskPayload{}},
		{KindAnswer, "codex-7", SubjectAnswer(goldenTicket),
			&AnswerPayload{Ticket: goldenTicket,
				Answer: "use is_admin() SECURITY DEFINER"}, &AnswerPayload{}},
		{KindNote, "", SubjectNote("demo"),
			&NotePayload{ID: "codex-7", Decision: "events store UTC", Repo: "demo",
				Kind: NoteKindDecision, Ticket: goldenTicket}, &NotePayload{}},
		{KindTicket, "", SubjectTicket(goldenTicket),
			&TicketPayload{Ticket: goldenTicket, State: TicketAnswered, By: "claude-2",
				Reason: "answered within TTL"}, &TicketPayload{}},
		{KindJob, "", SubjectJob(goldenTicket),
			&JobPayload{ID: goldenTicket, Repo: "demo", Source: "manual",
				Title: "add RRULE builder", State: JobOpen}, &JobPayload{}},
		{KindTask, "", SubjectTask("01976f00-0000-7000-8000-0000000000a1"),
			&TaskPayload{ID: "01976f00-0000-7000-8000-0000000000a1", Job: goldenTicket,
				Role: "builder", Title: "implement RRULE builder", State: TaskPending}, &TaskPayload{}},
		{KindTriage, "", SubjectTriage(goldenTicket),
			&TriagePayload{Job: goldenTicket, Result: TriageError, Tasks: 0,
				Code: TriageInvalidDAG, Reason: "cycle: t1 -> t2 -> t1"}, &TriagePayload{}},
		{KindWorker, "", SubjectWorker("01976f00-0000-7000-8000-0000000000a1"),
			&WorkerPayload{Task: "01976f00-0000-7000-8000-0000000000a1", Job: goldenTicket,
				Result: WorkerError, Code: WorkerRateLimited, CostUSD: 0.0421,
				Reason: "api_error_status 429"}, &WorkerPayload{}},
		{KindFleet, "", SubjectFleet,
			&FleetPayload{State: FleetPaused, Code: FleetBudgetExhausted,
				Reason: "spent 5.25 of 5.00 USD", SpentUSD: 5.25, BudgetUSD: 5}, &FleetPayload{}},
	}
}

// TestGolden freezes the encoded wire format, one committed golden per kind.
// Any json-tag rename, dropped field, or SchemaVersion drift fails here
// loudly instead of surfacing in downstream behavior tests.
func TestGolden(t *testing.T) {
	for _, tc := range goldenCases() {
		t.Run(string(tc.kind), func(t *testing.T) {
			path := filepath.Join("testdata", string(tc.kind)+".v1.json")
			if *update {
				writeGolden(t, path, tc)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v (run with -update to generate)", err)
			}

			env, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode golden: %v", err)
			}
			if err := DecodeInto(env, tc.decoded); err != nil {
				t.Fatalf("DecodeInto: %v", err)
			}

			encoded, err := Encode(env)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !jsonEqual(t, encoded, data) {
				t.Errorf("re-encoded envelope drifted from golden\ngolden: %s\ngot:    %s", data, encoded)
			}

			// Envelope.Payload is raw bytes, so the envelope compare above
			// passes the payload through verbatim. Re-marshal the typed
			// payload too, so a renamed or dropped payload field fails here.
			payloadJSON, err := json.Marshal(tc.decoded)
			if err != nil {
				t.Fatalf("marshal decoded payload: %v", err)
			}
			var goldenEnv struct {
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(data, &goldenEnv); err != nil {
				t.Fatalf("unmarshal golden payload: %v", err)
			}
			if !jsonEqual(t, payloadJSON, goldenEnv.Payload) {
				t.Errorf("re-marshaled payload drifted from golden\ngolden: %s\ngot:    %s", goldenEnv.Payload, payloadJSON)
			}
		})
	}
}

func writeGolden(t *testing.T, path string, tc goldenCase) {
	t.Helper()
	raw, err := json.Marshal(tc.payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	env := Envelope{
		SchemaVersion: SchemaVersion,
		Kind:          tc.kind,
		ID:            goldenEnvID,
		From:          "codex-7",
		To:            tc.to,
		Subject:       tc.subject,
		TS:            goldenTS,
		Payload:       raw,
	}
	data, err := Encode(env)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, data, "", "  "); err != nil {
		t.Fatalf("indent: %v", err)
	}
	pretty.WriteByte('\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir testdata: %v", err)
	}
	if err := os.WriteFile(path, pretty.Bytes(), 0o644); err != nil {
		t.Fatalf("write golden: %v", err)
	}
}

// jsonEqual compares two JSON documents canonically (key order and
// whitespace insignificant).
func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return reflect.DeepEqual(av, bv)
}

// TestContractStrings pins every exported subject helper, pattern, bucket,
// stream, and enum literal against hardcoded strings. These are wire
// contract: a constant rename is a wire break and must fail a test, not a
// review.
func TestContractStrings(t *testing.T) {
	cases := []struct{ name, got, want string }{
		// Fixed subjects.
		{"SubjectRegister", SubjectRegister, "mesh.register"},
		{"SubjectLeave", SubjectLeave, "mesh.leave"},
		// Parameterized subjects.
		{"SubjectHeartbeat", SubjectHeartbeat("codex-7"), "mesh.heartbeat.codex-7"},
		{"SubjectStatus", SubjectStatus("codex-7"), "mesh.status.codex-7"},
		{"SubjectAnnounce", SubjectAnnounce("demo"), "mesh.announce.demo"},
		{"SubjectClaim", SubjectClaim("demo"), "mesh.claim.demo"},
		{"SubjectNote", SubjectNote("demo"), "mesh.note.demo"},
		{"SubjectAskRole", SubjectAskRole("auth"), "mesh.ask.role.auth"},
		{"SubjectAskID", SubjectAskID("codex-7"), "mesh.ask.id.codex-7"},
		{"SubjectInbox", SubjectInbox("codex-7"), "mesh.inbox.codex-7"},
		{"SubjectAnswer", SubjectAnswer("T1"), "mesh.answer.T1"},
		{"SubjectTicket", SubjectTicket("T1"), "mesh.ticket.T1"},
		{"SubjectJob", SubjectJob("J1"), "mesh.job.J1"},
		{"SubjectTask", SubjectTask("T1"), "mesh.task.T1"},
		{"SubjectTriage", SubjectTriage("J1"), "mesh.triage.J1"},
		{"SubjectWorker", SubjectWorker("T1"), "mesh.worker.T1"},
		{"SubjectFleet", SubjectFleet, "mesh.fleet"},
		// Patterns.
		{"PatternAll", PatternAll, "mesh.>"},
		{"PatternHeartbeats", PatternHeartbeats, "mesh.heartbeat.>"},
		{"PatternStatuses", PatternStatuses, "mesh.status.>"},
		{"PatternAnnounces", PatternAnnounces, "mesh.announce.>"},
		{"PatternClaims", PatternClaims, "mesh.claim.>"},
		{"PatternAsks", PatternAsks, "mesh.ask.>"},
		{"PatternAnswers", PatternAnswers, "mesh.answer.>"},
		{"PatternTickets", PatternTickets, "mesh.ticket.>"},
		{"PatternJobs", PatternJobs, "mesh.job.>"},
		{"PatternTasks", PatternTasks, "mesh.task.>"},
		{"PatternTriage", PatternTriage, "mesh.triage.>"},
		{"PatternWorkers", PatternWorkers, "mesh.worker.>"},
		// Buckets and streams.
		{"BucketRegistry", BucketRegistry, "registry"},
		{"BucketClaims", BucketClaims, "claims"},
		{"BucketTickets", BucketTickets, "tickets"},
		{"BucketJobs", BucketJobs, "jobs"},
		{"BucketTasks", BucketTasks, "tasks"},
		{"StreamAudit", StreamAudit, "audit"},
		{"StreamTickets", StreamTickets, "ticket-events"},
		{"StreamJobs", StreamJobs, "job-events"},
		{"StreamTasks", StreamTasks, "task-events"},
		{"StreamNotes", StreamNotes("demo"), "notes-demo"},
		// Repo identity.
		{"DefaultRepo", DefaultRepo, "default"},
		// Result enums.
		{"ClaimClaimed", string(ClaimClaimed), "claimed"},
		{"ClaimLost", string(ClaimLost), "lost"},
		{"ClaimError", string(ClaimError), "error"},
		{"ReleaseReleased", string(ReleaseReleased), "released"},
		{"ReleaseNotOwner", string(ReleaseNotOwner), "not_owner"},
		{"ReleaseError", string(ReleaseError), "error"},
		{"AskAnswered", string(AskAnswered), "answered"},
		{"AskPending", string(AskPending), "pending"},
		{"AskTimedOut", string(AskTimedOut), "timed_out"},
		{"AskExpired", string(AskExpired), "expired"},
		{"AskNoSuchTicket", string(AskNoSuchTicket), "no_such_ticket"},
		// Ticket states.
		{"TicketOpen", string(TicketOpen), "open"},
		{"TicketRouted", string(TicketRouted), "routed"},
		{"TicketAccepted", string(TicketAccepted), "accepted"},
		{"TicketAnswered", string(TicketAnswered), "answered"},
		{"TicketClosed", string(TicketClosed), "closed"},
		{"TicketExpired", string(TicketExpired), "expired"},
		{"TicketCancelled", string(TicketCancelled), "cancelled"},
		// Job states.
		{"JobOpen", string(JobOpen), "open"},
		{"JobTriaged", string(JobTriaged), "triaged"},
		{"JobScheduled", string(JobScheduled), "scheduled"},
		{"JobRunning", string(JobRunning), "running"},
		{"JobDone", string(JobDone), "done"},
		{"JobFailed", string(JobFailed), "failed"},
		{"JobCancelled", string(JobCancelled), "cancelled"},
		// Task states.
		{"TaskPending", string(TaskPending), "pending"},
		{"TaskRunning", string(TaskRunning), "running"},
		{"TaskDone", string(TaskDone), "done"},
		{"TaskFailed", string(TaskFailed), "failed"},
		{"TaskCancelled", string(TaskCancelled), "cancelled"},
		// Triage results and error codes.
		{"TriageOK", string(TriageOK), "ok"},
		{"TriageError", string(TriageError), "error"},
		{"TriagePlannerUnavailable", string(TriagePlannerUnavailable), "planner_unavailable"},
		{"TriagePlannerFailed", string(TriagePlannerFailed), "planner_failed"},
		{"TriageBadPlan", string(TriageBadPlan), "bad_plan"},
		{"TriageInvalidDAG", string(TriageInvalidDAG), "invalid_dag"},
		{"TriageInternal", string(TriageInternal), "internal"},
		// Worker results and error codes.
		{"WorkerOK", string(WorkerOK), "ok"},
		{"WorkerError", string(WorkerError), "error"},
		{"WorkerSpawnFailed", string(WorkerSpawnFailed), "spawn_failed"},
		{"WorkerFailed", string(WorkerFailed), "worker_failed"},
		{"WorkerRateLimited", string(WorkerRateLimited), "rate_limited"},
		{"WorkerBillingError", string(WorkerBillingError), "billing_error"},
		{"WorkerInternal", string(WorkerInternal), "internal"},
		// Fleet states and pause codes.
		{"FleetRunning", string(FleetRunning), "running"},
		{"FleetPaused", string(FleetPaused), "paused"},
		{"FleetBudgetExhausted", string(FleetBudgetExhausted), "budget_exhausted"},
		{"FleetBillingError", string(FleetBillingError), "billing_error"},
		// Kinds.
		{"KindRegister", string(KindRegister), "register"},
		{"KindLeave", string(KindLeave), "leave"},
		{"KindHeartbeat", string(KindHeartbeat), "heartbeat"},
		{"KindStatus", string(KindStatus), "status"},
		{"KindAnnounce", string(KindAnnounce), "announce"},
		{"KindClaim", string(KindClaim), "claim"},
		{"KindAsk", string(KindAsk), "ask"},
		{"KindAnswer", string(KindAnswer), "answer"},
		{"KindNote", string(KindNote), "note"},
		{"KindTicket", string(KindTicket), "ticket"},
		{"KindJob", string(KindJob), "job"},
		{"KindTask", string(KindTask), "task"},
		{"KindTriage", string(KindTriage), "triage"},
		{"KindWorker", string(KindWorker), "worker"},
		{"KindFleet", string(KindFleet), "fleet"},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}
