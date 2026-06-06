// P1 acceptance across real process boundaries (issue #16): the claim race
// has exactly one winner, a hard-killed holder's claim is reclaimed, and a
// blackboard note survives a coordinator restart for a late joiner to replay
// — all through `mesh` → sidecar socket → bus → coordinator, no mocks.
package e2e

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/observe"
)

// coordinator is a restartable coordinator process handle. The P0 helper
// only ever needed kill-at-cleanup; P1 must bounce the coordinator mid-test
// to prove blackboard disk persistence.
type coordinator struct {
	m   *mesh
	cmd *exec.Cmd
}

func (m *mesh) startCoordinatorRestartable() *coordinator {
	m.t.Helper()
	logf, err := os.OpenFile(filepath.Join(m.dir, "coordinator-e2e.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		m.t.Fatal(err)
	}
	cmd := exec.Command(meshdBin, "--mode", "coordinator")
	cmd.Env = m.env
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		m.t.Fatal(err)
	}
	c := &coordinator{m: m, cmd: cmd}
	m.t.Cleanup(func() {
		c.kill()
		logf.Close()
	})
	m.waitDialable(filepath.Join(m.dir, "bus.sock"), 5*time.Second)
	return c
}

func (c *coordinator) kill() {
	if c.cmd == nil || c.cmd.Process == nil {
		return
	}
	c.cmd.Process.Kill() //nolint:errcheck
	c.cmd.Wait()         //nolint:errcheck
	c.cmd = nil
}

// restart hard-kills the coordinator and boots a fresh process on the same
// MESH_DIR — the durable streams must come back from disk, the registry
// repopulates from sidecar re-registration.
func (c *coordinator) restart() {
	c.m.t.Helper()
	c.kill()
	// The killed process never removed its socket; the new one recovers the
	// stale file (bus.listenUnix), but give the OS a beat to release it.
	time.Sleep(50 * time.Millisecond)
	fresh := c.m.startCoordinatorRestartable()
	c.cmd = fresh.cmd
}

// claimResult is the --json shape of a claim verb reply (meshapi.ClaimVerbResult).
type claimResult struct {
	Result string    `json:"result"`
	Path   string    `json:"path"`
	Repo   string    `json:"repo"`
	Owner  string    `json:"owner"`
	Since  time.Time `json:"since"`
}

func (m *mesh) claimJSON(agent, path, repo string) (int, claimResult) {
	m.t.Helper()
	code, stdout, stderr := m.run("claim", path, "--repo", repo, "--json", "--socket", m.agentSocket(agent))
	var res claimResult
	if stdout != "" {
		if err := json.Unmarshal([]byte(stdout), &res); err != nil && code != 5 {
			m.t.Fatalf("claim --json unparseable (exit %d): %v\nstdout: %s\nstderr: %s", code, err, stdout, stderr)
		}
	}
	return code, res
}

func TestP1AcceptanceFlow(t *testing.T) {
	m := newMesh(t)
	coord := m.startCoordinatorRestartable()

	for _, a := range []string{"alpha", "beta"} {
		if code, _, stderr := m.run("join", "--name", a, "--role", "builder", "--repo", "demo"); code != 0 {
			t.Fatalf("join %s exit %d: %s", a, code, stderr)
		}
	}

	// --- claim race: exactly one winner, typed outcomes, loser sees owner ---
	type outcome struct {
		agent string
		code  int
		res   claimResult
	}
	results := make([]outcome, 2)
	var wg sync.WaitGroup
	for i, agent := range []string{"alpha", "beta"} {
		wg.Add(1)
		go func(i int, agent string) {
			defer wg.Done()
			cmd := exec.Command(meshBin, "claim", "src/foo.go", "--repo", "demo", "--json",
				"--socket", m.agentSocket(agent))
			cmd.Env = m.env
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			code := 0
			if ee, ok := err.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else if err != nil {
				t.Errorf("claim run (%s): %v", agent, err)
				return
			}
			var res claimResult
			json.Unmarshal(stdout.Bytes(), &res) //nolint:errcheck // asserted below
			results[i] = outcome{agent: agent, code: code, res: res}
		}(i, agent)
	}
	wg.Wait()

	var winner, loser outcome
	switch {
	case results[0].code == 0 && results[1].code == 6:
		winner, loser = results[0], results[1]
	case results[0].code == 6 && results[1].code == 0:
		winner, loser = results[1], results[0]
	default:
		t.Fatalf("claim race want exits {0,6}, got %s=%d %s=%d",
			results[0].agent, results[0].code, results[1].agent, results[1].code)
	}
	if winner.res.Result != "claimed" || loser.res.Result != "lost" {
		t.Fatalf("typed results wrong: winner=%q loser=%q", winner.res.Result, loser.res.Result)
	}
	if loser.res.Owner != winner.agent {
		t.Fatalf("loser sees owner %q, want %q", loser.res.Owner, winner.agent)
	}
	if loser.res.Since.IsZero() {
		t.Fatal("loser did not see when the claim was acquired")
	}

	// --- release frees the path for the other agent ---
	if code, _, stderr := m.run("release", "src/foo.go", "--repo", "demo",
		"--socket", m.agentSocket(winner.agent)); code != 0 {
		t.Fatalf("release exit %d: %s", code, stderr)
	}
	if code, res := m.claimJSON(loser.agent, "src/foo.go", "demo"); code != 0 || res.Result != "claimed" {
		t.Fatalf("claim after release: exit %d result %q", code, res.Result)
	}

	// --- equivalent path spellings collide on the same claim ---
	if code, res := m.claimJSON(winner.agent, "./src//foo.go", "demo"); code != 6 || res.Owner != loser.agent {
		t.Fatalf("normalized respelling: exit %d owner %q, want 6/%q", code, res.Owner, loser.agent)
	}

	// --- announce is observable on a raw bus tap, fire-and-forget ---
	tap, err := bus.Dial(filepath.Join(m.dir, "bus.sock"), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tap.Close()
	sawAnnounce := make(chan envelope.AnnouncePayload, 4)
	if _, err := tap.Subscribe(envelope.PatternAnnounces, func(env envelope.Envelope) {
		var p envelope.AnnouncePayload
		if envelope.DecodeInto(env, &p) == nil {
			sawAnnounce <- p
		}
	}); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := m.run("announce", "refactoring claims", "--paths", "src/foo.go",
		"--repo", "demo", "--socket", m.agentSocket("alpha")); code != 0 {
		t.Fatalf("announce exit %d: %s", code, stderr)
	}
	select {
	case p := <-sawAnnounce:
		if p.Intent != "refactoring claims" || p.Repo != "demo" {
			t.Fatalf("tap saw announce %+v", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bus tap never saw the announce")
	}

	// --- hard-kill the claim holder: lease reclaim frees the path ---
	agents, _ := m.who(winner.agent)
	rec, ok := findAgent(agents, loser.agent)
	if !ok || rec.Card.PID <= 0 {
		t.Fatalf("no pid for %s", loser.agent)
	}
	if err := syscall.Kill(rec.Card.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	m.eventually(5*time.Second, "killed holder's claim is reclaimed", func() bool {
		code, res := m.claimJSON(winner.agent, "src/foo.go", "demo")
		return code == 0 && res.Result == "claimed"
	})

	// --- blackboard: note → late joiner replays → survives coordinator restart ---
	if code, _, stderr := m.run("note", "events store UTC", "--repo", "demo", "--kind", "decision",
		"--socket", m.agentSocket(winner.agent)); code != 0 {
		t.Fatalf("note exit %d: %s", code, stderr)
	}
	if code, _, stderr := m.run("join", "--name", "late", "--role", "reviewer", "--repo", "demo"); code != 0 {
		t.Fatalf("late join exit %d: %s", code, stderr)
	}

	assertContextHasNote := func(when string) {
		m.t.Helper()
		m.eventually(5*time.Second, "context replays the note ("+when+")", func() bool {
			code, stdout, _ := m.run("context", "--repo", "demo", "--json", "--socket", m.agentSocket("late"))
			if code != 0 {
				return false
			}
			var res struct {
				Notes []struct {
					Author string `json:"author"`
					Text   string `json:"text"`
					Kind   string `json:"kind"`
				} `json:"notes"`
			}
			if json.Unmarshal([]byte(stdout), &res) != nil {
				return false
			}
			for _, n := range res.Notes {
				if n.Text == "events store UTC" && n.Author == winner.agent && n.Kind == "decision" {
					return true
				}
			}
			return false
		})
	}
	assertContextHasNote("before restart")

	// A claim held before the restart must be re-established afterward (F5):
	// the claims KV is in-memory, so the sidecar re-takes on reconnect.
	if code, res := m.claimJSON(winner.agent, "src/bar.go", "demo"); code != 0 || res.Result != "claimed" {
		t.Fatalf("pre-restart claim: exit %d result %q", code, res.Result)
	}

	// Hard-restart the coordinator: registry rebuilds from re-registration,
	// notes must come back from $MESH_DIR/streams (disk persistence), and the
	// pre-restart holder either re-takes its held claim or observes that it
	// legitimately lost to a peer in the restart gap (#43).
	coord.restart()
	assertContextHasNote("after restart")

	m.eventually(5*time.Second, "held claim re-established or loss observed after restart", func() bool {
		code, res := m.claimJSON("late", "src/bar.go", "demo")
		if code == 6 && res.Owner == winner.agent {
			return true // winner's claim survived the bounce
		}
		if code == 0 {
			return m.sawClaimLoss(winner.agent, "src/bar.go", "late")
		}
		return false
	})
}

func (m *mesh) sawClaimLoss(agent, path, owner string) bool {
	m.t.Helper()
	code, stdout, _ := m.run("ops", "--json")
	if code != 0 {
		return false
	}
	var snap observe.Snapshot
	if json.Unmarshal([]byte(stdout), &snap) != nil {
		return false
	}
	for _, sc := range snap.Sidecars {
		if sc.Name != agent {
			continue
		}
		for _, loss := range sc.ClaimLosses {
			if loss.Path == path && loss.Owner == owner && loss.Reason == "reestablish_lost" {
				return true
			}
		}
	}
	return false
}
