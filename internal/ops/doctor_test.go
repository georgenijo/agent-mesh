package ops

import (
	"testing"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/observe"
)

func liveRecord(name string, pid int) *agentcard.RegistryRecord {
	return &agentcard.RegistryRecord{
		Card:  agentcard.Card{ID: name, Name: name, Role: "builder", PID: pid},
		State: agentcard.PresenceLive,
	}
}

func TestDiagnose(t *testing.T) {
	healthyCoord := observe.CoordinatorInfo{PID: 100, PIDAlive: true, BusSocketPresent: true, BusDialable: true}

	cases := []struct {
		name    string
		snap    observe.Snapshot
		verdict Verdict
		entity  string // finding to inspect ("" = expect no findings)
		state   HealthState
	}{
		{
			name:    "empty mesh dir is clean",
			snap:    observe.Snapshot{},
			verdict: VerdictClean,
		},
		{
			name: "healthy stack",
			snap: observe.Snapshot{
				Coordinator: healthyCoord,
				Sidecars: []observe.SidecarInfo{{
					Name: "alpha", SocketPresent: true, SocketDialable: true,
					PID: 200, PIDAlive: true, Registry: liveRecord("alpha", 200),
				}},
			},
			verdict: VerdictClean,
			entity:  "alpha",
			state:   StateHealthy,
		},
		{
			name: "live process evicted from registry is an orphan",
			snap: observe.Snapshot{
				Coordinator: healthyCoord,
				Sidecars: []observe.SidecarInfo{{
					Name: "ghostly", PIDFilePID: 300, PID: 300, PIDAlive: true,
					Drift: []observe.Drift{observe.DriftOrphanPidfile},
				}},
				Anomalies: []string{"ghostly: orphan_pidfile"},
			},
			verdict: VerdictDirty,
			entity:  "ghostly",
			state:   StateOrphan,
		},
		{
			name: "dead socket residue is a stale socket",
			snap: observe.Snapshot{
				Coordinator: healthyCoord,
				Sidecars: []observe.SidecarInfo{{
					Name: "crashed", SocketPresent: true, SocketDialable: false,
					PID: 400, PIDAlive: false,
					Drift: []observe.Drift{observe.DriftOrphanSocket, observe.DriftStalePID},
				}},
				Anomalies: []string{"crashed: orphan_socket"},
			},
			verdict: VerdictDirty,
			entity:  "crashed",
			state:   StateStaleSocket,
		},
		{
			name: "pidfile pointing at a dead pid",
			snap: observe.Snapshot{
				Coordinator: healthyCoord,
				Sidecars: []observe.SidecarInfo{{
					Name: "zombie", PIDFile: "/m/agents/zombie.pid", PIDFilePID: 500,
					PID: 500, PIDAlive: false,
					Drift: []observe.Drift{observe.DriftDeadPidfile},
				}},
				Anomalies: []string{"zombie: dead_pidfile"},
			},
			verdict: VerdictDirty,
			entity:  "zombie",
			state:   StateDeadPidfile,
		},
		{
			name: "coordinator pidfile dead",
			snap: observe.Snapshot{
				Coordinator: observe.CoordinatorInfo{PID: 600, PIDAlive: false},
			},
			verdict: VerdictDirty,
			entity:  "coordinator",
			state:   StateDeadPidfile,
		},
		{
			name: "coordinator bus socket stale",
			snap: observe.Snapshot{
				Coordinator: observe.CoordinatorInfo{BusSocketPresent: true},
			},
			verdict: VerdictDirty,
			entity:  "coordinator",
			state:   StateStaleSocket,
		},
		{
			name: "coordinator alive but bus undialable is an orphan",
			snap: observe.Snapshot{
				Coordinator: observe.CoordinatorInfo{PID: 700, PIDAlive: true, BusSocketPresent: true},
			},
			verdict: VerdictDirty,
			entity:  "coordinator",
			state:   StateOrphan,
		},
		{
			name: "registry ghost with no artifacts dirties via anomalies only",
			snap: observe.Snapshot{
				Coordinator: healthyCoord,
				Sidecars: []observe.SidecarInfo{{
					Name: "gone", Registry: liveRecord("gone", 0),
					Drift: []observe.Drift{observe.DriftGhostAgent},
				}},
				Anomalies: []string{"gone: ghost_agent"},
			},
			verdict: VerdictDirty,
			entity:  "gone",
			state:   StateOrphan, // best available class; detail carries the drift
		},
		{
			name: "lock file alone yields no coordinator finding",
			snap: observe.Snapshot{
				Coordinator: observe.CoordinatorInfo{LockPresent: true},
			},
			verdict: VerdictClean,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := Diagnose(tc.snap)
			if rep.Verdict != tc.verdict {
				t.Fatalf("verdict = %s, want %s (findings %+v)", rep.Verdict, tc.verdict, rep.Findings)
			}
			if tc.entity == "" {
				if len(rep.Findings) != 0 {
					t.Fatalf("findings = %+v, want none", rep.Findings)
				}
				return
			}
			var found *Finding
			for i := range rep.Findings {
				if rep.Findings[i].Entity == tc.entity {
					found = &rep.Findings[i]
					break
				}
			}
			if found == nil {
				t.Fatalf("no finding for %s: %+v", tc.entity, rep.Findings)
			}
			if found.State != tc.state {
				t.Fatalf("%s state = %s, want %s (%+v)", tc.entity, found.State, tc.state, found)
			}
		})
	}
}
