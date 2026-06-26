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
	return startStackEvery(t, 0)
}

// startStackEvery is startStack with an injectable roster push interval
// (0 keeps the production default set in New).
func startStackEvery(t *testing.T, rosterEvery time.Duration) (config.Config, *bus.Client, *Dashboard) {
	t.Helper()
	cfg := fastConfig(t)
	coord := coordinator.New(cfg, nil)
	if err := coord.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coord.Stop)

	d := New(cfg, "127.0.0.1:0", nil)
	if rosterEvery > 0 {
		d.rosterEvery = rosterEvery
	}
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

// TestSSEPresenceLifecycleContract locks the P0 observer contract: the full
// two-tier lifecycle (live → away → evicted) asserted over the HTTP stream a
// browser actually consumes, not via KV reads. The stream uses data-only SSE
// frames — the discriminator is the JSON `type` field ("event" | "roster" |
// "claims" | "claimlog", the last two added by P1's claims snapshot and the
// claim-history ring), never an SSE `event:` name.
func TestSSEPresenceLifecycleContract(t *testing.T) {
	cfg, cli, d := startStackEvery(t, 25*time.Millisecond)
	const id = "lifer"

	resp, err := http.Get(fmt.Sprintf("http://%s/events", d.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	// Frame parser: the wire is strictly alternating `data: <json>` lines and
	// blank lines; anything else (an `event:` name, a bare line, JSON without
	// a known type) is a contract violation.
	type sseFrame struct {
		Type     string                     `json:"type"`
		Envelope envelope.Envelope          `json:"envelope"`
		Agents   []agentcard.RegistryRecord `json:"agents"`
	}
	frames := make(chan sseFrame, 256)
	scanErr := make(chan error, 1)
	go func() {
		defer close(frames)
		scanner := bufio.NewScanner(resp.Body)
		wantBlank := false
		for scanner.Scan() {
			line := scanner.Text()
			if wantBlank {
				if line != "" {
					scanErr <- fmt.Errorf("data line not followed by a blank line, got %q", line)
					return
				}
				wantBlank = false
				continue
			}
			data, ok := strings.CutPrefix(line, "data: ")
			if !ok {
				scanErr <- fmt.Errorf("non-data SSE line %q — the stream must be data-only frames", line)
				return
			}
			var f sseFrame
			if err := json.Unmarshal([]byte(data), &f); err != nil {
				scanErr <- fmt.Errorf("frame payload is not JSON: %v", err)
				return
			}
			// Accept the full set of frame types: P0/P1/P2 types plus the P3
			// lifecycle types added by issue #58. Any future type must be added
			// here too — the explicit allow-list is the SSE contract.
			switch f.Type {
			case "event", "roster", "claims", "claimlog",
				"jobs", "tasks", "workers", "triage", "fleet":
				// known and accepted
			default:
				scanErr <- fmt.Errorf("frame type %q is not in the SSE frame allow-list", f.Type)
				return
			}
			wantBlank = true
			frames <- f
		}
	}()

	registerAgent(t, cli, id)

	// Beat past RegistrationGrace, then go silent so the two-tier sweep takes
	// over: away after AwayAfter of silence, evicted after EvictAfter.
	beatUntil := time.Now().Add(cfg.RegistrationGrace + 2*cfg.HeartbeatInterval)
	go func() {
		ticker := time.NewTicker(cfg.HeartbeatInterval)
		defer ticker.Stop()
		for time.Now().Before(beatUntil) {
			<-ticker.C
			env, err := envelope.New(envelope.KindHeartbeat, id,
				envelope.SubjectHeartbeat(id), &envelope.HeartbeatPayload{ID: id})
			if err == nil {
				cli.Publish(env) //nolint:errcheck
			}
		}
	}()

	stateOf := func(f sseFrame) (agentcard.PresenceState, bool) {
		for _, rec := range f.Agents {
			if rec.Card.ID == id {
				return rec.State, true
			}
		}
		return "", false
	}

	// Walk the one stream in order through the four milestones. Under
	// fastConfig the whole lifecycle takes ~1s; bound the test at 5s.
	stages := []string{"roster shows live", "roster shows away", "evict leave event", "roster omits agent"}
	stage := 0
	deadline := time.After(5 * time.Second)
	for stage < len(stages) {
		select {
		case err := <-scanErr:
			t.Fatalf("SSE framing contract violated: %v", err)
		case <-deadline:
			t.Fatalf("timed out waiting for milestone %q", stages[stage])
		case f, ok := <-frames:
			if !ok {
				select {
				case err := <-scanErr:
					t.Fatalf("SSE framing contract violated: %v", err)
				default:
					t.Fatalf("SSE stream ended before milestone %q", stages[stage])
				}
			}
			switch stage {
			case 0: // roster shows the agent live
				if st, ok := stateOf(f); f.Type == "roster" && ok && st == agentcard.PresenceLive {
					stage++
				}
			case 1: // roster shows the agent away after AwayAfter silence
				if st, ok := stateOf(f); f.Type == "roster" && ok && st == agentcard.PresenceAway {
					stage++
				}
			case 2: // the coordinator's evict, published as a leave event
				if f.Type != "event" || f.Envelope.Kind != envelope.KindLeave {
					continue
				}
				if f.Envelope.From != "coordinator" {
					t.Fatalf("evict leave from %q, want coordinator", f.Envelope.From)
				}
				if f.Envelope.Subject != envelope.SubjectLeave {
					t.Fatalf("evict subject %q, want %q", f.Envelope.Subject, envelope.SubjectLeave)
				}
				var p envelope.LeavePayload
				if err := envelope.DecodeInto(f.Envelope, &p); err != nil {
					t.Fatalf("evict payload: %v", err)
				}
				if p.ID != id || p.Reason != "evicted" {
					t.Fatalf("evict payload = %+v, want id=%q reason=evicted", p, id)
				}
				stage++
			case 3: // final roster no longer carries the agent
				if _, ok := stateOf(f); f.Type == "roster" && !ok {
					stage++
				}
			}
		}
	}
	// A framing violation among frames buffered behind the milestones must
	// still fail the test.
	select {
	case err := <-scanErr:
		t.Fatalf("SSE framing contract violated: %v", err)
	default:
	}
}

// TestNotesEndpointAggregatesAcrossRepos locks the /api/notes observer
// contract (issue #88): with no ?repo filter the endpoint returns blackboard
// notes from every visible repo — discovered from agent cards and live
// claims, the same discovery tickNotes does — not only the default repo.
func TestNotesEndpointAggregatesAcrossRepos(t *testing.T) {
	_, cli, d := startStack(t)

	// An agent card carrying repo "alpha" is what makes that repo visible to
	// the observer (the bus deliberately has no list-streams op).
	card := agentcard.Card{ID: "worker", Name: "worker", Role: "builder", Repo: "alpha"}
	env, err := envelope.New(envelope.KindRegister, "worker", envelope.SubjectRegister,
		&envelope.RegisterPayload{Card: card})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Publish(env); err != nil {
		t.Fatal(err)
	}

	// Stream entries are full validated envelopes — same construction the
	// sidecar note verb uses.
	appendNote := func(from, repo, decision string) {
		t.Helper()
		env, err := envelope.New(envelope.KindNote, from, envelope.SubjectNote(repo),
			&envelope.NotePayload{ID: from, Decision: decision, Repo: repo, Kind: envelope.NoteKindDecision})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := envelope.Encode(env)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cli.StreamAppend(envelope.StreamNotes(repo), json.RawMessage(raw)); err != nil {
			t.Fatal(err)
		}
	}
	appendNote("worker", "alpha", "alpha shards by tenant_id")
	appendNote("worker", envelope.DefaultRepo, "default repo decision")

	// Registration reaches the registry asynchronously; poll the endpoint
	// until both repos' notes are visible (or fail at the deadline).
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := http.Get(fmt.Sprintf("http://%s/api/notes", d.Addr()))
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Notes []envelope.Envelope `json:"notes"`
		}
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err == nil {
			seen := map[string]bool{}
			for _, n := range body.Notes {
				var p envelope.NotePayload
				if envelope.DecodeInto(n, &p) == nil {
					seen[p.Decision] = true
				}
			}
			if seen["alpha shards by tenant_id"] && seen["default repo decision"] {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("aggregate /api/notes never carried both repos' notes; saw %v", seen)
			}
		} else if time.Now().After(deadline) {
			t.Fatalf("decode /api/notes: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
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

// TestSSEClaimLogRecordsTakesAndConflicts locks the claim-history contract: a
// claim take and a conflicting (lost) take are both recorded into the ring and
// pushed as a "claimlog" frame, carrying the real result so the UI can render
// conflicts. Derived observability — it never writes the claims KV.
func TestSSEClaimLogRecordsTakesAndConflicts(t *testing.T) {
	_, cli, d := startStack(t)

	resp, err := http.Get(fmt.Sprintf("http://%s/events", d.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	type frame struct {
		Type    string          `json:"type"`
		Entries []claimLogEntry `json:"entries"`
	}
	got := make(chan []claimLogEntry, 64)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			data, ok := strings.CutPrefix(scanner.Text(), "data: ")
			if !ok {
				continue
			}
			var f frame
			if err := json.Unmarshal([]byte(data), &f); err != nil || f.Type != "claimlog" {
				continue
			}
			got <- f.Entries
		}
	}()

	publishClaim := func(agent, path string, result envelope.ClaimResult) {
		env, err := envelope.New(envelope.KindClaim, agent, envelope.SubjectClaim(envelope.DefaultRepo),
			&envelope.ClaimPayload{ID: agent, Path: path, Repo: envelope.DefaultRepo, Result: result})
		if err != nil {
			t.Fatal(err)
		}
		if err := cli.Publish(env); err != nil {
			t.Fatal(err)
		}
	}

	publishClaim("alpha", "src/auth.ts", envelope.ClaimClaimed)
	publishClaim("beta", "src/auth.ts", envelope.ClaimLost)

	// Wait for a frame that holds both events with their real results.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("claimlog frame with both take and conflict never arrived")
		case entries := <-got:
			var claimed, lost bool
			for _, e := range entries {
				if e.From == "alpha" && e.Path == "src/auth.ts" && e.Result == "claimed" {
					claimed = true
				}
				if e.From == "beta" && e.Path == "src/auth.ts" && e.Result == "lost" {
					lost = true
				}
			}
			if claimed && lost {
				return
			}
		}
	}
}
