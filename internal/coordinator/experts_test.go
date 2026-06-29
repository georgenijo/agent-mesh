package coordinator

import (
	"os"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// TestRedeliverAfterSlowReviewSubscription proves that a review request
// buffered during a cold expert spawn is re-delivered once the expert's
// review subscription becomes active (#125 fix). The old fixed 500ms settle
// was shorter than the typical runtime startup gap (~2.3s), causing the request
// to be lost. This test injects a fake launchFunc (no real meshd spawned) that
// simulates the timing: the expert registers as live quickly but delays its
// subscription + readiness key write.
func TestRedeliverAfterSlowReviewSubscription(t *testing.T) {
	cfg := fastConfig(t)
	cfg.AutoExperts = true
	// Use longer eviction windows so the fake expert (which sends no
	// heartbeats) stays "live" long enough for the spawner's 250ms poll to
	// detect it. fastConfig uses 150ms AwayAfter, which is shorter than the
	// 250ms poll interval.
	cfg.RegistrationGrace = 5 * time.Second
	cfg.AwayAfter = 10 * time.Second
	cfg.EvictAfter = 20 * time.Second

	c := New(cfg, nil)
	if err := c.Start(); err != nil {
		t.Fatal("coordinator start:", err)
	}
	t.Cleanup(c.Stop)

	if c.experts == nil {
		t.Skip("auto-experts not armed (findMeshd unavailable in this environment)")
	}

	cli := dialBus(t, cfg)
	const role = "reviewer"

	// Channel the re-delivered review request must land in.
	received := make(chan envelope.Envelope, 1)

	// Fake launchFunc: simulates an expert that registers live quickly but
	// takes additional time to activate its review subscription (as proxy.Start
	// does in production). The 700ms subscription delay exceeds the old 500ms
	// fixed settle, proving that delay-based re-delivery was insufficient.
	c.experts.launchFunc = func(r, name string) (*os.Process, error) {
		go func() {
			// Phase 1: sidecar.Start() completes — agent appears as live.
			card := agentcard.Card{ID: name, Name: name, Role: r}
			regEnv, err := envelope.New(envelope.KindRegister, name,
				envelope.SubjectRegister, &envelope.RegisterPayload{Card: card})
			if err != nil {
				return
			}
			if err := cli.Publish(regEnv); err != nil {
				return
			}

			// Phase 2: proxy.Start() takes time — simulated by a delay that
			// exceeds the old fixed 500ms settle, proving delay-based delivery
			// was insufficient. ServeReviews subscribes and writes the ready key.
			time.Sleep(700 * time.Millisecond)

			sub, err := cli.Subscribe(envelope.SubjectReviewRequest(r),
				func(env envelope.Envelope) {
					select {
					case received <- env:
					default:
					}
				})
			if err != nil {
				return
			}
			defer sub.Unsubscribe()

			// Signal the coordinator that the subscription is active (#125).
			if _, err := cli.KVPut(envelope.BucketExpertReady, name, "ready",
				bus.PutOptions{}); err != nil {
				return
			}

			// Hold the subscription open until the re-delivery arrives.
			<-time.After(10 * time.Second)
		}()
		return nil, nil // no real OS process; spawner handles nil proc safely
	}

	// Publish a review request before any expert is live. The spawner buffers
	// it and cold-spawns via launchFunc.
	reqEnv, err := envelope.New(envelope.KindReviewRequest, "scheduler",
		envelope.SubjectReviewRequest(role),
		envelope.ReviewRequestPayload{Task: "task-redeliver-test", Role: role})
	if err != nil {
		t.Fatal("build review request:", err)
	}
	if err := cli.Publish(reqEnv); err != nil {
		t.Fatal("publish review request:", err)
	}

	select {
	case env := <-received:
		if env.Kind != envelope.KindReviewRequest {
			t.Fatalf("got kind %q, want %q", env.Kind, envelope.KindReviewRequest)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("review request was not re-delivered to the expert subscription within 8s; " +
			"the readiness-signal fix is likely not working")
	}
}

// TestAlreadyLiveExpertReceivesDirectly verifies that when a live expert
// already owns the role, the review request is delivered via normal pub/sub
// without buffering or re-delivery — unchanged behavior (#125 scope guard).
func TestAlreadyLiveExpertReceivesDirectly(t *testing.T) {
	cfg := fastConfig(t)
	cfg.AutoExperts = true

	c := New(cfg, nil)
	if err := c.Start(); err != nil {
		t.Fatal("coordinator start:", err)
	}
	t.Cleanup(c.Stop)

	if c.experts == nil {
		t.Skip("auto-experts not armed")
	}

	cli := dialBus(t, cfg)
	const (
		role      = "live-reviewer"
		agentName = "live-expert-1"
	)

	// Register the expert as live BEFORE the review request arrives.
	card := agentcard.Card{ID: agentName, Name: agentName, Role: role}
	regEnv, err := envelope.New(envelope.KindRegister, agentName,
		envelope.SubjectRegister, &envelope.RegisterPayload{Card: card})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(regEnv); err != nil {
		t.Fatal(err)
	}
	waitState(t, cli, agentName, agentcard.PresenceLive, 2*time.Second)

	// Subscribe to the review subject (simulates ServeReviews being active).
	received := make(chan envelope.Envelope, 1)
	sub, err := cli.Subscribe(envelope.SubjectReviewRequest(role),
		func(env envelope.Envelope) {
			select {
			case received <- env:
			default:
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Unsubscribe)

	// launchFunc must NOT be called when a live owner exists.
	c.experts.launchFunc = func(r, name string) (*os.Process, error) {
		t.Error("launchFunc called for already-live expert — spawner should have returned early")
		return nil, nil
	}

	// Publish the review request — spawner sees a live owner and does not buffer.
	reqEnv, err := envelope.New(envelope.KindReviewRequest, "scheduler",
		envelope.SubjectReviewRequest(role),
		envelope.ReviewRequestPayload{Task: "task-live-test", Role: role})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(reqEnv); err != nil {
		t.Fatal(err)
	}

	select {
	case env := <-received:
		if env.Kind != envelope.KindReviewRequest {
			t.Fatalf("got kind %q, want %q", env.Kind, envelope.KindReviewRequest)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("review request not received by already-live expert within 3s")
	}
}
