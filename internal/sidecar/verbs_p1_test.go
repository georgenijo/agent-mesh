package sidecar

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/coordinator"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
)

// startSidecarCard starts a sidecar with a fully specified card (repo + cwd),
// which the default startSidecar helper does not set.
func startSidecarCard(t *testing.T, cfg config.Config, card agentcard.Card) *Sidecar {
	t.Helper()
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

func claimVia(t *testing.T, cfg config.Config, agent string, args meshapi.ClaimArgs) meshapi.ClaimVerbResult {
	t.Helper()
	resp := do(t, cfg, agent, meshapi.VerbClaim, args)
	if !resp.OK {
		t.Fatalf("claim failed: %+v", resp)
	}
	var res meshapi.ClaimVerbResult
	if err := json.Unmarshal(resp.Data, &res); err != nil {
		t.Fatal(err)
	}
	return res
}

// TestClaimAbsAndRelCollide is the F1/F2 regression: the hook hands an
// absolute file_path while a manual `mesh claim` passes a repo-relative one.
// Both spellings of one file must land on a single claim key, so the second
// contender loses. Before the sidecar folded absolutes to repo-relative, the
// two keyed differently and BOTH won — a lock two spellings slipped past.
func TestClaimAbsAndRelCollide(t *testing.T) {
	cfg := fastConfig(t)
	repoRoot := filepath.Join(cfg.MeshDir, "tree")

	coord := coordinator.New(cfg, nil)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord.Stop)

	// Both agents work in the same checkout (card.CWD = repoRoot) on repo "demo".
	startSidecarCard(t, cfg, agentcard.Card{Name: "hookagent", Role: "builder", Repo: "demo", CWD: repoRoot})
	startSidecarCard(t, cfg, agentcard.Card{Name: "manual", Role: "builder", Repo: "demo", CWD: repoRoot})

	// Agent "manual" claims the repo-relative spelling first.
	rel := claimVia(t, cfg, "manual", meshapi.ClaimArgs{Path: "src/foo.go"})
	if rel.Result != envelope.ClaimClaimed {
		t.Fatalf("relative claim result = %q, want claimed", rel.Result)
	}

	// Agent "hookagent" claims the ABSOLUTE spelling of the same file (what
	// Claude Code's edit hook passes). It must collide and lose to "manual".
	abs := claimVia(t, cfg, "hookagent", meshapi.ClaimArgs{Path: filepath.Join(repoRoot, "src", "foo.go")})
	if abs.Result != envelope.ClaimLost {
		t.Fatalf("absolute claim result = %q, want lost (path aliasing bypass)", abs.Result)
	}
	if abs.Owner != "manual" {
		t.Fatalf("absolute claim owner = %q, want manual", abs.Owner)
	}
	if abs.Path != rel.Path {
		t.Fatalf("normalized paths differ: abs=%q rel=%q (must be one key)", abs.Path, rel.Path)
	}
}

// TestClaimsReestablishedAfterCoordinatorRestart is the F5/#43 regression:
// the claims KV is in-memory in the coordinator, so a restart wipes it. A
// holder may re-take the claim, or a rival may legitimately win in the
// restart gap. Both are legal; the holder must observe a loss instead of
// silently forgetting it.
func TestClaimsReestablishedAfterCoordinatorRestart(t *testing.T) {
	cfg := fastConfig(t)

	coord := coordinator.New(cfg, nil)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}

	holder := startSidecarCard(t, cfg, agentcard.Card{Name: "holder", Role: "builder", Repo: "demo", CWD: cfg.MeshDir})
	_ = holder
	if res := claimVia(t, cfg, "holder", meshapi.ClaimArgs{Path: "src/foo.go"}); res.Result != envelope.ClaimClaimed {
		t.Fatalf("initial claim = %q, want claimed", res.Result)
	}

	// Hard-stop the coordinator (its embedded bus + in-memory claims KV go
	// with it) and start a fresh one on the same MESH_DIR.
	coord.Stop()
	time.Sleep(50 * time.Millisecond)
	coord2 := coordinator.New(cfg, nil)
	if err := coord2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord2.Stop)

	// After reconnect, either the holder re-establishes first, or "other"
	// wins the restart gap and the holder records an observable loss.
	other := startSidecarCard(t, cfg, agentcard.Card{Name: "other", Role: "builder", Repo: "demo", CWD: cfg.MeshDir})
	_ = other
	deadline := time.Now().Add(5 * time.Second)
	for {
		res := claimVia(t, cfg, "other", meshapi.ClaimArgs{Path: "src/foo.go"})
		if res.Result == envelope.ClaimLost && res.Owner == "holder" {
			break // holder re-established its claim across the restart
		}
		if res.Result == envelope.ClaimClaimed {
			if observedClaimLoss(t, cfg, "holder", "src/foo.go", "other") {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("holder neither re-established nor observed loss after coordinator restart (last result %q owner %q)", res.Result, res.Owner)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func observedClaimLoss(t *testing.T, cfg config.Config, agent, path, owner string) bool {
	t.Helper()
	resp := do(t, cfg, agent, meshapi.VerbRuntime, nil)
	if !resp.OK {
		t.Fatalf("runtime failed: %+v", resp)
	}
	var rt meshapi.RuntimeResult
	if err := json.Unmarshal(resp.Data, &rt); err != nil {
		t.Fatal(err)
	}
	for _, loss := range rt.ClaimLosses {
		if loss.Path == path && loss.Owner == owner && loss.Reason == "reestablish_lost" {
			return true
		}
	}
	return false
}
