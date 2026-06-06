package dashboard

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/coordinator"
	"github.com/georgenijo/agent-mesh/internal/envelope"
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
	}
}

func startStack(t *testing.T) (config.Config, *bus.Client, *Dashboard) {
	t.Helper()
	cfg := fastConfig(t)
	coord := coordinator.New(cfg, nil)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord.Stop)

	d := New(cfg, "127.0.0.1:0", nil)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Stop)

	cli, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cli.Close)
	return cfg, cli, d
}

func registerAgent(t *testing.T, cli *bus.Client, id string) {
	t.Helper()
	card := agentcard.Card{ID: id, Name: id, Role: "builder"}
	env, err := envelope.New(envelope.KindRegister, id, envelope.SubjectRegister, &envelope.RegisterPayload{Card: card})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}
}

func TestRosterEndpointShowsRegisteredAgent(t *testing.T) {
	_, cli, d := startStack(t)
	registerAgent(t, cli, "vis")

	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := http.Get(fmt.Sprintf("http://%s/api/roster", d.Addr()))
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Agents []agentcard.RegistryRecord `json:"agents"`
		}
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err == nil && len(body.Agents) == 1 && body.Agents[0].Card.Name == "vis" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("roster never showed agent: %+v", body.Agents)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSSEStreamsLiveStatusEvent(t *testing.T) {
	_, cli, d := startStack(t)
	registerAgent(t, cli, "talker")

	resp, err := http.Get(fmt.Sprintf("http://%s/events", d.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}

	// Publish a status while the stream is open.
	env, err := envelope.New(envelope.KindStatus, "talker", envelope.SubjectStatus("talker"),
		&envelope.StatusPayload{ID: "talker", Text: "hello dashboard"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}

	type sseMsg struct {
		Type     string            `json:"type"`
		Envelope envelope.Envelope `json:"envelope"`
	}
	found := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var msg sseMsg
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &msg); err != nil {
				continue
			}
			if msg.Type == "event" && msg.Envelope.Kind == envelope.KindStatus {
				var p envelope.StatusPayload
				if envelope.DecodeInto(msg.Envelope, &p) == nil && p.Text == "hello dashboard" {
					found <- true
					return
				}
			}
		}
	}()

	select {
	case <-found:
	case <-time.After(3 * time.Second):
		t.Fatal("status event never appeared on the SSE stream")
	}
}

// TestDashboardIsReadOnly: killing the dashboard must not disturb agent
// state — it holds no claims and publishes nothing.
func TestDashboardDisconnectDoesNotAffectRegistry(t *testing.T) {
	_, cli, d := startStack(t)
	registerAgent(t, cli, "steady")

	// Keep the agent beating.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				env, err := envelope.New(envelope.KindHeartbeat, "steady",
					envelope.SubjectHeartbeat("steady"), &envelope.HeartbeatPayload{ID: "steady"})
				if err == nil {
					cli.Publish(env) //nolint:errcheck
				}
			}
		}
	}()

	d.Stop() // dashboard gone

	time.Sleep(300 * time.Millisecond)
	keys, err := cli.KVList(envelope.BucketRegistry)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("registry has %d entries after dashboard stop, want 1", len(keys))
	}
}
