package sidecar

import (
	"context"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
)

// TestExpertIdleReaperExitsAfterTTL verifies that an expert with no handled
// asks/reviews exits cleanly and deregisters once IdleTTL elapses (#105).
func TestExpertIdleReaperExitsAfterTTL(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")
	// observer queries the registry after the expert leaves (the expert's own
	// socket is gone after Stop, so we need a second sidecar to check).
	observer := startSidecar(t, cfg, "observer")
	_ = observer

	// Promote the expert to role so it subscribes to role-routed asks.
	resp := do(t, cfg, "expert", meshapi.VerbJoin, meshapi.JoinArgs{
		Card: agentcard.Card{Name: "expert", Role: "auth"},
	})
	if !resp.OK {
		t.Fatalf("re-join as auth failed: %+v", resp)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := ExpertOptions{
		IdleTTL: 100 * time.Millisecond,
	}
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- expert.ServeExpertWithMemory(ctx, func(_ context.Context, _, _ string) (ExpertResult, error) {
			return ExpertResult{}, nil // never called in this test
		}, 20*time.Millisecond, opts)
	}()

	// The idle reaper should trigger: ServeExpertWithMemory calls sc.Leave and
	// returns nil, and also closes sc.Done() so the main loop can clean up.
	select {
	case err := <-loopDone:
		if err != nil {
			t.Fatalf("ServeExpertWithMemory returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expert did not exit after idle TTL elapsed")
	}

	// Done() must be closed (Leave was called and done was signalled).
	select {
	case <-expert.Done():
	case <-time.After(time.Second):
		t.Fatal("expert.Done() not closed after idle exit")
	}

	// The expert should no longer appear as live in the registry (queried via
	// the observer sidecar, since the expert's socket is stopped).
	deadline := time.Now().Add(2 * time.Second)
	for {
		agents := whoAgents(t, cfg, "observer")
		live := false
		for _, rec := range agents {
			if rec.Card.Name == "expert" && rec.State == agentcard.PresenceLive {
				live = true
			}
		}
		if !live {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expert still live in registry after idle deregister")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestExpertIdleReaperResetByActivity verifies that touching activity resets the
// idle clock so the reaper does not fire prematurely (#105).
func TestExpertIdleReaperResetByActivity(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")

	resp := do(t, cfg, "expert", meshapi.VerbJoin, meshapi.JoinArgs{
		Card: agentcard.Card{Name: "expert", Role: "auth"},
	})
	if !resp.OK {
		t.Fatalf("re-join as auth failed: %+v", resp)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := ExpertOptions{
		IdleTTL: 150 * time.Millisecond,
	}
	done := make(chan error, 1)
	go func() {
		done <- expert.ServeExpertWithMemory(ctx, func(_ context.Context, _, _ string) (ExpertResult, error) {
			return ExpertResult{}, nil
		}, 20*time.Millisecond, opts)
	}()

	// Touch activity at 80ms — before the 150ms TTL would fire — then verify
	// the expert is still alive 50ms later (130ms total, reset gives it 150ms more).
	time.Sleep(80 * time.Millisecond)
	expert.touchExpertActivity()

	select {
	case err := <-done:
		t.Fatalf("expert exited too early after activity touch (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
		// Good: expert is still running 130ms after start (80+50), 50ms after reset.
	}

	// Now wait for the TTL to expire from the last touch (150ms - 50ms elapsed = ~100ms more).
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeExpertWithMemory returned error after idle: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expert did not exit after idle TTL elapsed following activity touch")
	}
}

// TestExpertIdleTTLZeroDisablesReaper verifies that IdleTTL=0 prevents the idle
// reaper from ever firing — the expert runs until context cancellation (#105).
func TestExpertIdleTTLZeroDisablesReaper(t *testing.T) {
	cfg := fastConfig(t)
	expert := startMesh(t, cfg, "expert")

	resp := do(t, cfg, "expert", meshapi.VerbJoin, meshapi.JoinArgs{
		Card: agentcard.Card{Name: "expert", Role: "auth"},
	})
	if !resp.OK {
		t.Fatalf("re-join as auth failed: %+v", resp)
	}

	ctx, cancel := context.WithCancel(context.Background())

	opts := ExpertOptions{
		IdleTTL: 0, // disabled
	}
	done := make(chan error, 1)
	go func() {
		done <- expert.ServeExpertWithMemory(ctx, func(_ context.Context, _, _ string) (ExpertResult, error) {
			return ExpertResult{}, nil
		}, 20*time.Millisecond, opts)
	}()

	// Wait 300ms with no activity. The reaper must not fire.
	select {
	case err := <-done:
		t.Fatalf("expert exited with TTL=0 (should be disabled): err=%v", err)
	case <-time.After(300 * time.Millisecond):
		// Good: expert is still running.
	}

	// Cancelling the context must cause a clean exit (context.Canceled).
	cancel()
	select {
	case err := <-done:
		if err != nil && err.Error() != context.Canceled.Error() {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expert did not exit after context cancellation")
	}
}
