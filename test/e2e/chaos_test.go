// Chaos suite (issue #38): the adversarial counterpart to the P1 happy-path
// acceptance (#16). Where TestP1AcceptanceFlow proves a single claim race and a
// graceful note replay, these tests crash things — kill -9 sidecars and the
// coordinator across real process boundaries — and assert the two locked
// recovery decisions actually hold:
//
//   - "Every claim and presence record is a TTL lease with reclaim-on-death"
//   - "JetStream KV revision-CAS is the single claim/lock primitive"
//     (one-CAS-winner, lost-means-lost)
//
// The audit lesson behind the whole suite (docs/audit-multi-agent-pm.md,
// Avoid #3): release-on-cooperative-event-only stranded work when an agent
// hard-crashed and held its claim forever. Nothing else in the tree proves
// either decision survives a kill -9 across separate processes; mock stores on
// a single event loop (Avoid #2) hid exactly this class of bug in the sibling
// project. Every assertion here flows through real `mesh` CLI processes →
// sidecar sockets → bus → coordinator KV.
//
// All scenarios run on the fast-lease env from newMesh (heartbeat 100ms,
// away 400ms, evict 1200ms, grace 300ms) and use eventually/waitFor rather
// than bare sleeps so they stay deterministic under -race and CI load.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
)

// join boots a named agent's sidecar (autostarted by the CLI) and fails the
// test if the join itself fails. Additive chaos helper: the existing harness
// has who/claimJSON/eventually but no single-name join wrapper.
func (m *mesh) join(name, role string) {
	m.t.Helper()
	if code, _, stderr := m.run("join", "--name", name, "--role", role, "--repo", "demo"); code != 0 {
		m.t.Fatalf("join %s exit %d: %s", name, code, stderr)
	}
}

// sidecarPID reads a joined agent's sidecar pid from the authoritative registry
// record (Card.PID), the same idiom presence_test/p1_test use to target a
// kill -9. viaAgent is any live agent whose socket can serve the `who` read.
func (m *mesh) sidecarPID(viaAgent, name string) int {
	m.t.Helper()
	var pid int
	m.eventually(3*time.Second, fmt.Sprintf("registry carries a pid for %s", name), func() bool {
		agents, exit := m.who(viaAgent)
		if exit != 0 {
			return false
		}
		rec, ok := findAgent(agents, name)
		if ok && rec.Card.PID > 0 {
			pid = rec.Card.PID
			return true
		}
		return false
	})
	return pid
}

// killSidecar hard-kills (SIGKILL, no graceful leave) the named agent's
// sidecar by its registry pid — the crash the audit's Avoid #3 is about.
func (m *mesh) killSidecar(viaAgent, name string) {
	m.t.Helper()
	pid := m.sidecarPID(viaAgent, name)
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		m.t.Fatalf("kill sidecar %s (pid %d): %v", name, pid, err)
	}
}

// claimVia starts a real `mesh claim` CLI process against one agent's socket and
// returns (exit, parsed result). Used by the N-way race so every contender is a
// genuinely separate process, not a goroutine on one event loop (Avoid #2).
func (m *mesh) claimRace(agent, path, repo string) (int, claimResult) {
	cmd := exec.Command(meshBin, "claim", path, "--repo", repo, "--json", "--socket", m.agentSocket(agent))
	cmd.Env = m.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		m.t.Errorf("claim race run (%s): %v", agent, err)
		return -1, claimResult{}
	}
	var res claimResult
	json.Unmarshal(stdout.Bytes(), &res) //nolint:errcheck // asserted by caller
	return code, res
}

// --- Item 2: presence-only chaos (no claims needed) ---------------------------

// TestChaosCoordinatorCrashRegistryReconverges is the cross-process proof of the
// sidecar OnReconnect re-register (unit-tested at sidecar_test.go): kill -9 the
// coordinator, restart it on the same MESH_DIR, and both agents must reappear
// live with their roles intact — no manual re-join. The coordinator's KV is
// in-memory (P0 star bus), so the registry rebuilds purely from the sidecars'
// reconnect re-registration.
func TestChaosCoordinatorCrashRegistryReconverges(t *testing.T) {
	m := newMesh(t)
	coord := m.startCoordinatorRestartable()

	m.join("a1", "builder")
	m.join("a2", "reviewer")

	// Baseline: both live before the crash.
	m.eventually(3*time.Second, "both agents live before crash", func() bool {
		agents, exit := m.who("a1")
		if exit != 0 {
			return false
		}
		r1, ok1 := findAgent(agents, "a1")
		r2, ok2 := findAgent(agents, "a2")
		return ok1 && ok2 && r1.State == agentcard.PresenceLive && r2.State == agentcard.PresenceLive
	})

	// kill -9 the coordinator, then boot a fresh one on the same dir.
	coord.restart()

	// The registry reconverges from re-registration alone, roles intact.
	m.eventually(5*time.Second, "both agents reconverge live with roles after coordinator restart", func() bool {
		agents, exit := m.who("a1")
		if exit != 0 {
			return false
		}
		r1, ok1 := findAgent(agents, "a1")
		r2, ok2 := findAgent(agents, "a2")
		return ok1 && ok2 &&
			r1.State == agentcard.PresenceLive && r1.Card.Role == "builder" &&
			r2.State == agentcard.PresenceLive && r2.Card.Role == "reviewer"
	})
}

// TestChaosCrashThenRejoinSameName: kill -9 an agent's sidecar, then immediately
// re-join the same name BEFORE the away/evict windows fire. The result must be
// exactly one live registry record carrying the NEW sidecar pid — no zombie or
// duplicate entry from the dead daemon. (The registry keys on agent id, so a
// re-register overwrites; this pins that a crash+fast-rejoin can't fork it.)
func TestChaosCrashThenRejoinSameName(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	// An observer survives the crash and serves the who reads.
	m.join("watch", "reviewer")
	m.join("test", "builder")

	oldPID := m.sidecarPID("watch", "test")
	if err := syscall.Kill(oldPID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill test sidecar %d: %v", oldPID, err)
	}

	// Re-join the same name at once — the CLI autostarts a fresh sidecar. This
	// races the away window (400ms) on purpose: the new daemon must win the name
	// without the dead one's record lingering as a duplicate.
	if code, _, stderr := m.run("join", "--name", "test", "--role", "builder", "--repo", "demo"); code != 0 {
		t.Fatalf("rejoin test exit %d: %s", code, stderr)
	}

	m.eventually(5*time.Second, "exactly one live record for test, new pid, no zombie", func() bool {
		agents, exit := m.who("watch")
		if exit != 0 {
			return false
		}
		var count int
		var rec agentcard.RegistryRecord
		for _, a := range agents {
			if a.Card.Name == "test" {
				count++
				rec = a
			}
		}
		return count == 1 && rec.State == agentcard.PresenceLive && rec.Card.PID != oldPID && rec.Card.PID > 0
	})

	// And it stays exactly one — the old daemon's lease must not resurrect a
	// second entry once its (stale) presence record would have gone away.
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		agents, exit := m.who("watch")
		if exit != 0 {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		var count int
		for _, a := range agents {
			if a.Card.Name == "test" {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("want exactly one record for test, got %d", count)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// --- Item 3: claim-holder crash releases the claim ----------------------------

// TestChaosClaimHolderCrashReleasesClaim: A holds a claim, B sees `lost` while
// A's lease is alive; kill -9 A's sidecar and B must observe the claim flip to
// `claimed` after eviction. This is the founding failure mode (Avoid #3) made a
// test: a hard-crashed holder must NOT strand the path forever.
func TestChaosClaimHolderCrashReleasesClaim(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	m.join("A", "builder")
	m.join("B", "builder")

	if code, res := m.claimJSON("A", "src/main.go", "r1"); code != 0 || res.Result != "claimed" {
		t.Fatalf("A claim: exit %d result %q", code, res.Result)
	}
	// While A's lease is alive, B loses the same path (typed result, exit 6).
	if code, res := m.claimJSON("B", "src/main.go", "r1"); code != 6 || res.Result != "lost" || res.Owner != "A" {
		t.Fatalf("B pre-crash claim: exit %d result %q owner %q, want 6/lost/A", code, res.Result, res.Owner)
	}

	m.killSidecar("B", "A")

	// After eviction (evict 1200ms in test config) the reclaimed path becomes
	// claimable by B. Asserted on the typed `result` field, not on prose.
	m.eventually(5*time.Second, "crashed holder's claim is reclaimed and B wins it", func() bool {
		code, res := m.claimJSON("B", "src/main.go", "r1")
		return code == 0 && res.Result == "claimed" && res.Owner == "B"
	})
}

// --- Item 4: N-way race, exactly one winner -----------------------------------

// TestChaosClaimRaceSingleWinner is the cross-process re-proof of the audit's
// Steal #1 / Avoid #2: a guarded claim was "atomic" only on a single event loop;
// our agents are separate OS processes. 10 rounds, fresh path each round, 8 real
// `mesh claim` CLI processes started concurrently. Per round the parsed --json
// results must be exactly {claimed:1, lost:7, error:0} — never two winners,
// never zero.
func TestChaosClaimRaceSingleWinner(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	agents := []string{"r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7"}
	for _, a := range agents {
		m.join(a, "builder")
	}

	const rounds = 10
	for round := 0; round < rounds; round++ {
		path := fmt.Sprintf("src/race_%d.go", round)

		type outcome struct {
			code int
			res  claimResult
		}
		results := make([]outcome, len(agents))
		// A start barrier maximizes the overlap: every process is launched and
		// waiting before any of them runs the CAS.
		var ready, fire sync.WaitGroup
		ready.Add(len(agents))
		fire.Add(1)
		var wg sync.WaitGroup
		for i, agent := range agents {
			wg.Add(1)
			go func(i int, agent string) {
				defer wg.Done()
				ready.Done()
				fire.Wait()
				code, res := m.claimRace(agent, path, "r1")
				results[i] = outcome{code: code, res: res}
			}(i, agent)
		}
		ready.Wait()
		fire.Done()
		wg.Wait()

		var claimed, lost, errs int
		for _, o := range results {
			switch {
			case o.code == 0 && o.res.Result == "claimed":
				claimed++
			case o.code == 6 && o.res.Result == "lost":
				lost++
			default:
				errs++
				t.Errorf("round %d: unexpected outcome exit %d result %q owner %q",
					round, o.code, o.res.Result, o.res.Owner)
			}
		}
		if claimed != 1 || lost != len(agents)-1 || errs != 0 {
			t.Fatalf("round %d counts: claimed=%d lost=%d error=%d, want {1, %d, 0}",
				round, claimed, lost, errs, len(agents)-1)
		}
	}
}

// --- Item 5: eviction releases ALL claims -------------------------------------

// TestChaosEvictionReleasesAllclaims: A holds three paths in one repo; kill -9
// A's sidecar; after A is evicted, B must be able to claim ALL three. Pins the
// TTL-lease clause "on eviction the coordinator releases its file claims" —
// every one, not just the first (coordinator.releaseClaims → ReleaseAllOwnedBy).
func TestChaosEvictionReleasesAllClaims(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	m.join("A", "builder")
	m.join("B", "builder")

	paths := []string{"a.go", "b.go", "c.go"}
	for _, p := range paths {
		if code, res := m.claimJSON("A", p, "r1"); code != 0 || res.Result != "claimed" {
			t.Fatalf("A claim %s: exit %d result %q", p, code, res.Result)
		}
	}

	m.killSidecar("B", "A")

	// A must leave the roster (evicted), then every one of its paths is B's.
	m.eventually(5*time.Second, "A evicted from who", func() bool {
		agents, exit := m.who("B")
		if exit != 0 {
			return false
		}
		_, ok := findAgent(agents, "A")
		return !ok
	})
	for _, p := range paths {
		m.eventually(5*time.Second, "B claims "+p+" after A eviction", func() bool {
			code, res := m.claimJSON("B", p, "r1")
			return code == 0 && res.Result == "claimed" && res.Owner == "B"
		})
	}
}

// --- Item 6: coordinator crash with a live claim holder -----------------------

// TestChaosCoordinatorCrashClaimContract pins the documented recovery contract
// (DECISIONS.md "Coordinator-crash recovery contract"): A holds a claim and a
// note, the coordinator is kill -9'd and restarted, and after recovery
//
//	(a) A reappears live — the in-memory registry KV is wiped by the crash and
//	    repopulates purely from the sidecar's reconnect re-register;
//	(b) A's claim is restored — the sidecar re-asserts its held claims on
//	    reconnect (sidecar.OnReconnect → reestablishClaims, create-only CAS,
//	    never a blind overwrite), so B's claim on the same path comes back
//	    `lost`. Because no rival grabs the path in the restart gap, A wins
//	    re-establishment (one-CAS, lost-means-lost, no restart grace per #43);
//	(c) the durable JSONL blackboard note SURVIVES the bounce.
//
// (c) is the post-#41 contract: P1 shipped per-stream JSONL persistence
// ($MESH_DIR/streams/<stream>.jsonl, replayed on Start), so blackboard notes —
// and the audit trail — survive coordinator death. Only the registry KV resets
// (it has no disk backing by design and recovers via re-register). This test
// pins exactly that split: streams durable, registry/claims rebuilt on
// reconnect.
func TestChaosCoordinatorCrashClaimContract(t *testing.T) {
	m := newMesh(t)
	coord := m.startCoordinatorRestartable()

	m.join("A", "builder")
	m.join("B", "builder")

	if code, res := m.claimJSON("A", "src/contract.go", "r1"); code != 0 || res.Result != "claimed" {
		t.Fatalf("A claim: exit %d result %q", code, res.Result)
	}
	// A durable note, written before the crash, must replay after it.
	if code, _, stderr := m.run("note", "decided to ship", "--repo", "demo", "--kind", "decision",
		"--socket", m.agentSocket("A")); code != 0 {
		t.Fatalf("A note exit %d: %s", code, stderr)
	}

	coord.restart()

	// (a) A reappears live via reconnect re-register (registry KV rebuilt).
	m.eventually(5*time.Second, "A reappears live after coordinator restart", func() bool {
		agents, exit := m.who("A")
		if exit != 0 {
			return false
		}
		rec, ok := findAgent(agents, "A")
		return ok && rec.State == agentcard.PresenceLive
	})

	// (b) A's claim re-asserted: B loses the same path after recovery.
	m.eventually(5*time.Second, "A's claim re-asserted; B loses the path after recovery", func() bool {
		code, res := m.claimJSON("B", "src/contract.go", "r1")
		return code == 6 && res.Result == "lost" && res.Owner == "A"
	})

	// (c) the durable JSONL note survives the coordinator's death and replays.
	m.eventually(5*time.Second, "durable note survives coordinator restart", func() bool {
		code, stdout, _ := m.run("context", "--repo", "demo", "--json", "--socket", m.agentSocket("A"))
		if code != 0 {
			return false
		}
		var res struct {
			Notes []struct {
				Text string `json:"text"`
				Kind string `json:"kind"`
			} `json:"notes"`
		}
		if json.Unmarshal([]byte(stdout), &res) != nil {
			return false
		}
		for _, n := range res.Notes {
			if n.Text == "decided to ship" && n.Kind == "decision" {
				return true
			}
		}
		return false
	})
}
