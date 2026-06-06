package sidecar

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/coordinator"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
	"github.com/georgenijo/agent-mesh/internal/socket"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func fastConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		MeshDir:           testsock.Dir(t),
		HeartbeatInterval: 50 * time.Millisecond,
		AwayAfter:         150 * time.Millisecond,
		EvictAfter:        400 * time.Millisecond,
		RegistrationGrace: 100 * time.Millisecond,
		ClaimTTL:          2 * time.Second,
	}
}

func startMesh(t *testing.T, cfg config.Config, name string) *Sidecar {
	t.Helper()
	coord := coordinator.New(cfg, nil)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord.Stop)
	return startSidecar(t, cfg, name)
}

func startSidecar(t *testing.T, cfg config.Config, name string) *Sidecar {
	t.Helper()
	card := agentcard.Card{Name: name, Role: "builder", Caps: []string{"go", "backend"}}
	sc, err := New(cfg, card, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sc.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sc.Stop)
	return sc
}

func do(t *testing.T, cfg config.Config, agent, verb string, args any) socket.Response {
	t.Helper()
	var raw json.RawMessage
	if args != nil {
		b, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		raw = b
	}
	resp, err := socket.Do(cfg.AgentSocket(agent), socket.Request{Verb: verb, Args: raw}, 2*time.Second)
	if err != nil {
		t.Fatalf("socket.Do %s: %v", verb, err)
	}
	return resp
}

func whoAgents(t *testing.T, cfg config.Config, via string) []agentcard.RegistryRecord {
	t.Helper()
	resp := do(t, cfg, via, meshapi.VerbWho, nil)
	if !resp.OK {
		t.Fatalf("who failed: %+v", resp)
	}
	var res meshapi.WhoResult
	if err := json.Unmarshal(resp.Data, &res); err != nil {
		t.Fatal(err)
	}
	return res.Agents
}

func TestBootRegistersAndWhoSeesStatus(t *testing.T) {
	cfg := fastConfig(t)
	startMesh(t, cfg, "test")

	// Boot registration lands.
	deadline := time.Now().Add(2 * time.Second)
	for {
		agents := whoAgents(t, cfg, "test")
		if len(agents) == 1 && agents[0].Card.Name == "test" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("agent never appeared in who: %+v", agents)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Status round-trips through the bus into the registry.
	resp := do(t, cfg, "test", meshapi.VerbStatus, meshapi.StatusArgs{Text: "working"})
	if !resp.OK {
		t.Fatalf("status failed: %+v", resp)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		agents := whoAgents(t, cfg, "test")
		if len(agents) == 1 && agents[0].LastStatus == "working" {
			if agents[0].State != agentcard.PresenceLive {
				t.Fatalf("state = %s, want live", agents[0].State)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status never reached registry: %+v", agents)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHeartbeatsKeepAgentLivePastAwayWindow(t *testing.T) {
	cfg := fastConfig(t)
	startMesh(t, cfg, "steady")

	// Wait well past away+grace: heartbeats must keep it live.
	time.Sleep(cfg.RegistrationGrace + 3*cfg.AwayAfter)
	agents := whoAgents(t, cfg, "steady")
	if len(agents) != 1 || agents[0].State != agentcard.PresenceLive {
		t.Fatalf("agents = %+v, want one live", agents)
	}
}

func TestJoinVerbIsIdempotentRejoin(t *testing.T) {
	cfg := fastConfig(t)
	startMesh(t, cfg, "test")

	card := agentcard.Card{Name: "test", Role: "reviewer", Caps: []string{"go"}}
	resp := do(t, cfg, "test", meshapi.VerbJoin, meshapi.JoinArgs{Card: card})
	if !resp.OK {
		t.Fatalf("join failed: %+v", resp)
	}
	var res meshapi.JoinResult
	if err := json.Unmarshal(resp.Data, &res); err != nil {
		t.Fatal(err)
	}
	if !res.Rejoined {
		t.Fatal("boot-joined sidecar should report rejoined")
	}

	// Role update propagates to the registry.
	deadline := time.Now().Add(2 * time.Second)
	for {
		agents := whoAgents(t, cfg, "test")
		if len(agents) == 1 && agents[0].Card.Role == "reviewer" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("role update never landed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStatusTooLongRejectedTyped(t *testing.T) {
	cfg := fastConfig(t)
	startMesh(t, cfg, "test")
	huge := make([]byte, meshapi.MaxStatusLen+1)
	for i := range huge {
		huge[i] = 'x'
	}
	resp := do(t, cfg, "test", meshapi.VerbStatus, meshapi.StatusArgs{Text: string(huge)})
	if resp.OK || resp.Code != socket.CodeBadRequest {
		t.Fatalf("want typed bad_request for oversized status, got %+v", resp)
	}
}

func TestJoinWrongAgentRejected(t *testing.T) {
	cfg := fastConfig(t)
	startMesh(t, cfg, "test")
	card := agentcard.Card{Name: "other", Role: "builder"}
	resp := do(t, cfg, "test", meshapi.VerbJoin, meshapi.JoinArgs{Card: card})
	if resp.OK || resp.Code != socket.CodeBadRequest {
		t.Fatalf("want bad_request, got %+v", resp)
	}
}

func TestLeaveDeregistersAndSignalsExit(t *testing.T) {
	cfg := fastConfig(t)
	coord := coordinator.New(cfg, nil)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord.Stop)

	sc := startSidecar(t, cfg, "leaver")
	other := startSidecar(t, cfg, "observer")
	_ = other

	resp := do(t, cfg, "leaver", meshapi.VerbLeave, meshapi.LeaveArgs{Reason: "done"})
	if !resp.OK {
		t.Fatalf("leave failed: %+v", resp)
	}
	select {
	case <-sc.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("leave did not signal daemon exit")
	}
	sc.Stop()

	// Registry drops the leaver (graceful leave, not lease expiry).
	deadline := time.Now().Add(2 * time.Second)
	for {
		agents := whoAgents(t, cfg, "observer")
		if len(agents) == 1 && agents[0].Card.Name == "observer" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leaver still registered: %+v", agents)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Further verbs against the leaver's socket fail typed: not joined /
	// no socket at all are both "exit 5" conditions for the CLI.
	if _, err := socket.Do(cfg.AgentSocket("leaver"), socket.Request{Verb: meshapi.VerbStatus}, 500*time.Millisecond); err == nil {
		resp := do(t, cfg, "leaver", meshapi.VerbStatus, meshapi.StatusArgs{Text: "zombie"})
		if resp.OK {
			t.Fatal("status after leave must fail")
		}
	}
}

func TestStatusBeforeCoordinatorRestartSurvivesViaReregister(t *testing.T) {
	cfg := fastConfig(t)
	coord := coordinator.New(cfg, nil)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	startSidecar(t, cfg, "phoenix")

	// Let it register, then restart the coordinator (registry wiped).
	deadline := time.Now().Add(2 * time.Second)
	for {
		if len(whoAgents(t, cfg, "phoenix")) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	coord.Stop()

	coord2 := coordinator.New(cfg, nil)
	if err := coord2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord2.Stop)

	// The sidecar's bus client reconnects and re-registers: the agent
	// reappears without any manual step. While the reconnect is in flight,
	// `who` correctly fails typed-unavailable — tolerate that window.
	deadline = time.Now().Add(5 * time.Second)
	for {
		resp := do(t, cfg, "phoenix", meshapi.VerbWho, nil)
		if resp.OK {
			var res meshapi.WhoResult
			if err := json.Unmarshal(resp.Data, &res); err != nil {
				t.Fatal(err)
			}
			if len(res.Agents) == 1 && res.Agents[0].Card.Name == "phoenix" {
				return
			}
		} else if resp.Code != socket.CodeUnavailable {
			t.Fatalf("unexpected who failure: %+v", resp)
		}
		if time.Now().After(deadline) {
			t.Fatalf("agent never re-registered after coordinator restart: %+v", resp)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRuntimeVerbReportsSidecarPID(t *testing.T) {
	cfg := fastConfig(t)
	sc := startMesh(t, cfg, "runtime")

	resp := do(t, cfg, "runtime", meshapi.VerbRuntime, nil)
	if !resp.OK {
		t.Fatalf("runtime failed: %+v", resp)
	}
	var rt meshapi.RuntimeResult
	if err := json.Unmarshal(resp.Data, &rt); err != nil {
		t.Fatal(err)
	}
	if rt.SidecarPID <= 0 {
		t.Fatalf("sidecar pid = %d", rt.SidecarPID)
	}
	if rt.Uptime == "" {
		t.Fatal("uptime missing")
	}
	if len(rt.Children) != 0 {
		t.Fatalf("children = %+v, want empty", rt.Children)
	}

	sc.TrackChild("claude -p", 4242)
	resp = do(t, cfg, "runtime", meshapi.VerbRuntime, nil)
	if !resp.OK {
		t.Fatalf("runtime failed: %+v", resp)
	}
	if err := json.Unmarshal(resp.Data, &rt); err != nil {
		t.Fatal(err)
	}
	if len(rt.Children) != 1 || rt.Children[0].PID != 4242 || rt.Children[0].State != "running" {
		t.Fatalf("children = %+v", rt.Children)
	}
	sc.MarkChildExited(4242)
	resp = do(t, cfg, "runtime", meshapi.VerbRuntime, nil)
	if err := json.Unmarshal(resp.Data, &rt); err != nil {
		t.Fatal(err)
	}
	if len(rt.Children) != 1 || rt.Children[0].State != "exited" {
		t.Fatalf("children = %+v", rt.Children)
	}
}
