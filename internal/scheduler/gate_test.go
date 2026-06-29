package scheduler

import (
	"testing"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// TestGateOfMapsPersistedStatesAndDeps pins the computed-gating contract: the
// frozen five-state TaskState plus dependency situation folds into the
// scheduler's seven-gate view, and nothing is ever persisted for it.
func TestGateOfMapsPersistedStatesAndDeps(t *testing.T) {
	mk := func(id string, state envelope.TaskState, deps ...string) task.Record {
		return task.Record{ID: id, State: state, DependsOn: deps}
	}
	cases := []struct {
		name string
		rec  task.Record
		dag  []task.Record
		want Gate
	}{
		{"running maps to running", mk("t", envelope.TaskRunning), nil, GateRunning},
		{"done maps to done", mk("t", envelope.TaskDone), nil, GateDone},
		{"failed maps to failed", mk("t", envelope.TaskFailed), nil, GateFailed},
		{"cancelled maps to skipped", mk("t", envelope.TaskCancelled), nil, GateSkipped},
		{"pending with no deps is runnable", mk("t", envelope.TaskPending), nil, GateRunnable},
		{"pending with done deps is runnable",
			mk("t", envelope.TaskPending, "a"),
			[]task.Record{mk("a", envelope.TaskDone)}, GateRunnable},
		{"pending behind a pending dep is queued",
			mk("t", envelope.TaskPending, "a"),
			[]task.Record{mk("a", envelope.TaskPending)}, GateQueued},
		{"pending behind a running dep is queued",
			mk("t", envelope.TaskPending, "a"),
			[]task.Record{mk("a", envelope.TaskRunning)}, GateQueued},
		{"pending behind a failed dep is blocked",
			mk("t", envelope.TaskPending, "a"),
			[]task.Record{mk("a", envelope.TaskFailed)}, GateBlocked},
		{"pending behind a cancelled dep is blocked",
			mk("t", envelope.TaskPending, "a"),
			[]task.Record{mk("a", envelope.TaskCancelled)}, GateBlocked},
		{"pending behind a missing dep is blocked",
			mk("t", envelope.TaskPending, "ghost"), nil, GateBlocked},
		{"one failed dep blocks even when others are done",
			mk("t", envelope.TaskPending, "a", "b"),
			[]task.Record{mk("a", envelope.TaskDone), mk("b", envelope.TaskFailed)}, GateBlocked},
		{"escalated maps to escalated",
			mk("t", envelope.TaskEscalated), nil, GateEscalated},
		{"pending behind an escalated dep is blocked",
			mk("t", envelope.TaskPending, "a"),
			[]task.Record{mk("a", envelope.TaskEscalated)}, GateBlocked},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			byID := map[string]task.Record{tc.rec.ID: tc.rec}
			for _, r := range tc.dag {
				byID[r.ID] = r
			}
			if got := GateOf(tc.rec, byID); got != tc.want {
				t.Fatalf("GateOf = %s, want %s", got, tc.want)
			}
		})
	}
}
