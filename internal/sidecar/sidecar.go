// Package sidecar is the per-agent daemon: the persistent bus connection and
// memory that the millisecond-lived `mesh` CLI lacks.
//
// It owns the agent's unix socket, registers the agent on the bus, renews the
// presence lease with heartbeats, translates CLI verbs into bus operations,
// and deregisters cleanly on leave. All envelopes are built and validated
// here, at the publish edge — the CLI never touches the bus.
package sidecar

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/claim"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
	"github.com/georgenijo/agent-mesh/internal/socket"
)

// Sidecar is one agent's mesh daemon.
type Sidecar struct {
	cfg     config.Config
	log     *slog.Logger
	sock    *socket.Server
	bus     *bus.Client
	pidFile string // written at boot, removed on Stop (the name never changes)

	mu         sync.Mutex
	card       agentcard.Card
	joined     bool
	lastStatus string
	// held tracks the claims this agent currently holds, keyed by
	// claim.Key(repo, normPath), so they can be re-established if the
	// coordinator restarts and its in-memory claims KV comes back empty
	// (presence recovers the same way via re-register). Value is the
	// (repo, path) needed to re-issue the claim.
	held      map[string]heldClaim
	startedAt time.Time

	childrenMu sync.Mutex
	children   []meshapi.ChildProc

	stop     chan struct{} // closes on Stop: ends background loops
	done     chan struct{} // closes when a leave verb requests daemon exit
	stopOnce sync.Once
	doneOnce sync.Once
	wg       sync.WaitGroup
}

// New creates a sidecar for the given agent identity.
func New(cfg config.Config, card agentcard.Card, log *slog.Logger) (*Sidecar, error) {
	if card.ID == "" {
		card.ID = card.Name
	}
	if err := card.Validate(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	return &Sidecar{
		cfg:       cfg,
		log:       log,
		card:      card,
		held:      make(map[string]heldClaim),
		pidFile:   cfg.AgentPIDFile(card.Name),
		startedAt: time.Now(),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}, nil
}

// Start connects to the bus, registers the agent, begins heartbeats, and
// opens the agent socket. The agent is joined from boot (issue #7).
func (s *Sidecar) Start() error {
	if err := s.cfg.EnsureDirs(); err != nil {
		return err
	}

	// Pidfile first, before the bus dial: a daemon that hangs mid-boot is
	// exactly what the ops plane must still be able to see (issue #35). The
	// error paths below remove it again — a failed boot exits and leaves no
	// process worth tracking.
	if err := os.WriteFile(s.pidFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		return fmt.Errorf("sidecar: write pid file: %w", err)
	}

	cli, err := bus.Dial(s.cfg.BusSocket(), bus.ClientOptions{
		// After a coordinator restart the registry AND the claims KV are
		// empty (both in-memory); re-registering and re-claiming on every
		// reconnect is how each repopulates. Order: register first so the
		// agent exists, then re-take held claims.
		OnReconnect: func() {
			s.register()          //nolint:errcheck // best-effort; next beat retries
			s.reestablishClaims() // re-take what we held before the drop
		},
	})
	if err != nil {
		os.Remove(s.pidFile) //nolint:errcheck
		return fmt.Errorf("sidecar: connect bus at %s: %w", s.cfg.BusSocket(), err)
	}
	s.bus = cli

	if err := s.register(); err != nil {
		cli.Close()
		os.Remove(s.pidFile) //nolint:errcheck
		return err
	}
	s.mu.Lock()
	s.joined = true
	s.mu.Unlock()

	s.sock = socket.NewServer(s.cfg.AgentSocket(s.card.Name), s.handle)
	if err := s.sock.Start(); err != nil {
		cli.Close()
		os.Remove(s.pidFile) //nolint:errcheck
		return err
	}

	s.wg.Add(1)
	go s.heartbeatLoop()

	s.log.Info("sidecar started", "agent", s.card.ID, "socket", s.sock.Path())
	return nil
}

// Done is closed when a `mesh leave` asks the daemon to exit.
func (s *Sidecar) Done() <-chan struct{} { return s.done }

// Stop shuts the sidecar down without publishing a leave (crash-equivalent:
// the lease expiry handles cleanup). Use Leave for graceful departure.
func (s *Sidecar) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
	if s.sock != nil {
		s.sock.Stop()
	}
	if s.bus != nil {
		s.bus.Close()
	}
	os.Remove(s.pidFile) //nolint:errcheck
}

// Leave publishes a graceful departure, then stops the sidecar.
func (s *Sidecar) Leave(reason string) {
	s.mu.Lock()
	wasJoined := s.joined
	s.joined = false
	s.mu.Unlock()

	if wasJoined {
		env, err := envelope.New(envelope.KindLeave, s.card.ID, envelope.SubjectLeave,
			&envelope.LeavePayload{ID: s.card.ID, Reason: reason})
		if err == nil {
			if err := s.bus.Publish(env); err != nil {
				s.log.Warn("publish leave failed", "err", err)
			}
		}
	}
	s.Stop()
}

// register publishes this agent's card. Idempotent: the coordinator treats a
// re-register as a presence refresh.
func (s *Sidecar) register() error {
	s.mu.Lock()
	card := s.card
	s.mu.Unlock()

	env, err := envelope.New(envelope.KindRegister, card.ID, envelope.SubjectRegister,
		&envelope.RegisterPayload{Card: card})
	if err != nil {
		return err
	}
	if err := s.bus.Publish(env); err != nil {
		return fmt.Errorf("sidecar: register: %w", err)
	}
	return nil
}

func (s *Sidecar) heartbeatLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			joined, id, status := s.joined, s.card.ID, s.lastStatus
			s.mu.Unlock()
			if !joined {
				continue
			}
			env, err := envelope.New(envelope.KindHeartbeat, id, envelope.SubjectHeartbeat(id),
				&envelope.HeartbeatPayload{ID: id, Status: status})
			if err != nil {
				continue
			}
			// Publish failures while disconnected are expected; the bus
			// client reconnects and OnReconnect re-registers.
			if err := s.bus.Publish(env); err != nil {
				s.log.Debug("heartbeat publish failed", "err", err)
			}
			// Claims are TTL leases renewed on the same beat as presence
			// (locked decision: reclaim-on-death). A renewal that loses its
			// CAS is a claim legitimately reclaimed — skipped, not retried.
			if _, err := claim.RenewOwned(s.bus, id, s.cfg.ClaimTTL); err != nil {
				s.log.Debug("claim renewal failed", "err", err)
			}
		}
	}
}

// --- CLI verb handling ----------------------------------------------------------

func (s *Sidecar) handle(req socket.Request) socket.Response {
	switch req.Verb {
	case meshapi.VerbPing:
		return s.handlePing()
	case meshapi.VerbJoin:
		return s.handleJoin(req)
	case meshapi.VerbLeave:
		return s.handleLeave(req)
	case meshapi.VerbStatus:
		return s.handleStatus(req)
	case meshapi.VerbWho:
		return s.handleWho()
	case meshapi.VerbClaim:
		return s.handleClaim(req)
	case meshapi.VerbRelease:
		return s.handleRelease(req)
	case meshapi.VerbAnnounce:
		return s.handleAnnounce(req)
	case meshapi.VerbNote:
		return s.handleNote(req)
	case meshapi.VerbContext:
		return s.handleContext(req)
	case meshapi.VerbRuntime:
		return s.handleRuntime()
	default:
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("unknown verb %q", req.Verb))
	}
}

func (s *Sidecar) handlePing() socket.Response {
	// Snapshot under the lock, then do the bus round-trip WITHOUT it — a
	// slow bus must never stall heartbeats and the other verb handlers.
	s.mu.Lock()
	id, joined := s.card.ID, s.joined
	s.mu.Unlock()
	busOK := s.bus != nil && s.bus.Ping() == nil
	return socket.OKData(meshapi.PingResult{ID: id, Joined: joined, Bus: busOK})
}

func (s *Sidecar) handleJoin(req socket.Request) socket.Response {
	var args meshapi.JoinArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("join args: %v", err))
	}
	if args.Card.ID == "" {
		args.Card.ID = args.Card.Name
	}
	if err := args.Card.Validate(); err != nil {
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}

	s.mu.Lock()
	if args.Card.Name != s.card.Name {
		name := s.card.Name
		s.mu.Unlock()
		return socket.Fail(socket.CodeBadRequest,
			fmt.Sprintf("this sidecar serves agent %q, not %q (one sidecar per agent)", name, args.Card.Name))
	}
	rejoined := s.joined
	if args.Card.PID == 0 {
		args.Card.PID = s.card.PID // keep the daemon's pid for diagnostics
	}
	s.card = args.Card
	s.joined = true
	s.mu.Unlock()

	if err := s.register(); err != nil {
		return socket.Fail(socket.CodeUnavailable, err.Error())
	}
	return socket.OKData(meshapi.JoinResult{Card: args.Card, Rejoined: rejoined})
}

func (s *Sidecar) handleLeave(req socket.Request) socket.Response {
	var args meshapi.LeaveArgs
	if len(req.Args) > 0 {
		if err := json.Unmarshal(req.Args, &args); err != nil {
			return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("leave args: %v", err))
		}
	}
	s.mu.Lock()
	joined := s.joined
	id := s.card.ID
	s.mu.Unlock()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}

	reason := args.Reason
	if reason == "" {
		reason = "left"
	}
	// Publish leave now, reply, then let the daemon exit: the CLI gets its
	// answer before the socket goes away.
	s.mu.Lock()
	s.joined = false
	s.mu.Unlock()
	env, err := envelope.New(envelope.KindLeave, id, envelope.SubjectLeave,
		&envelope.LeavePayload{ID: id, Reason: reason})
	if err == nil {
		if perr := s.bus.Publish(env); perr != nil {
			s.log.Warn("publish leave failed", "err", perr)
		}
	}
	s.doneOnce.Do(func() { close(s.done) })
	return socket.OKData(meshapi.LeaveResult{ID: id})
}

func (s *Sidecar) handleStatus(req socket.Request) socket.Response {
	var args meshapi.StatusArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("status args: %v", err))
	}
	if args.Text == "" {
		return socket.Fail(socket.CodeBadRequest, "empty status text")
	}
	if len(args.Text) > meshapi.MaxStatusLen {
		return socket.Fail(socket.CodeBadRequest,
			fmt.Sprintf("status text %d bytes exceeds limit %d", len(args.Text), meshapi.MaxStatusLen))
	}
	s.mu.Lock()
	if !s.joined {
		s.mu.Unlock()
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	id := s.card.ID
	s.lastStatus = args.Text
	s.mu.Unlock()

	env, err := envelope.New(envelope.KindStatus, id, envelope.SubjectStatus(id),
		&envelope.StatusPayload{ID: id, Text: args.Text})
	if err != nil {
		return socket.Fail(socket.CodeInternal, err.Error())
	}
	if err := s.bus.Publish(env); err != nil {
		return socket.Fail(socket.CodeUnavailable, fmt.Sprintf("bus publish: %v", err))
	}
	return socket.OKData(meshapi.StatusResult{ID: id, Text: args.Text})
}

func (s *Sidecar) handleWho() socket.Response {
	keys, err := s.bus.KVList(envelope.BucketRegistry)
	if err != nil {
		return socket.Fail(socket.CodeUnavailable, fmt.Sprintf("registry read: %v", err))
	}
	agents := make([]agentcard.RegistryRecord, 0, len(keys))
	for _, kv := range keys {
		var rec agentcard.RegistryRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue // skip unparseable record, never crash the roster
		}
		agents = append(agents, rec)
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].Card.Name < agents[j].Card.Name })
	return socket.OKData(meshapi.WhoResult{Agents: agents})
}

func (s *Sidecar) handleRuntime() socket.Response {
	s.mu.Lock()
	pid := s.card.PID
	if pid == 0 {
		pid = os.Getpid()
	}
	started := s.startedAt
	s.mu.Unlock()

	s.childrenMu.Lock()
	children := append([]meshapi.ChildProc(nil), s.children...)
	s.childrenMu.Unlock()

	return socket.OKData(meshapi.RuntimeResult{
		SidecarPID: pid,
		Uptime:     time.Since(started).Round(time.Millisecond).String(),
		Children:   children,
	})
}

// TrackChild records a child agent CLI process owned by this sidecar.
func (s *Sidecar) TrackChild(cmd string, pid int) {
	s.childrenMu.Lock()
	defer s.childrenMu.Unlock()
	s.children = append(s.children, meshapi.ChildProc{
		PID:       pid,
		Cmd:       cmd,
		StartedAt: time.Now().UTC(),
		State:     "running",
	})
}

// MarkChildExited marks a tracked child process as exited.
func (s *Sidecar) MarkChildExited(pid int) {
	s.childrenMu.Lock()
	defer s.childrenMu.Unlock()
	for i := range s.children {
		if s.children[i].PID == pid {
			s.children[i].State = "exited"
		}
	}
}
