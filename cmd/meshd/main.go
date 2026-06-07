// Command meshd is the Agent Mesh daemon. One binary, several modes selected
// by --mode: sidecar (one per agent), coordinator (one per mesh — embeds the
// bus/store), dashboard (read-only observer), or observe (runtime ops plane).
//
// No business logic lives here: flags are parsed and handed to internal/*.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/autostart"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/coordinator"
	"github.com/georgenijo/agent-mesh/internal/dashboard"
	"github.com/georgenijo/agent-mesh/internal/observe"
	agentruntime "github.com/georgenijo/agent-mesh/internal/runtime"
	"github.com/georgenijo/agent-mesh/internal/sidecar"
)

var version = "0.1.0-dev"

func main() {
	os.Exit(run())
}

func run() int {
	mode := flag.String("mode", "", "daemon mode: sidecar | coordinator | dashboard | observe | expert")
	showVersion := flag.Bool("version", false, "print version and exit")
	// --mesh-dir doubles as the ops-plane ownership marker: autostart always
	// passes it, so `mesh ops down` can verify a pid belongs to THIS mesh by
	// matching the daemon's argv — never by process name (issue #35).
	meshDir := flag.String("mesh-dir", "", "mesh directory override (default $MESH_DIR, else ~/.mesh)")

	// Sidecar flags.
	name := flag.String("name", "", "agent name (sidecar mode, required)")
	role := flag.String("role", "", "agent role (sidecar mode, required)")
	caps := flag.String("caps", "", "comma-separated capability tokens (sidecar mode)")
	repo := flag.String("repo", "", "repository the agent works on (sidecar mode)")
	model := flag.String("model", "", "model the agent runs (sidecar mode)")
	noAutoCoord := flag.Bool("no-autostart-coordinator", false,
		"fail instead of spawning a coordinator when the bus is down (sidecar mode)")

	// Dashboard / observe flags.
	addr := flag.String("addr", "", "listen address (dashboard: $MESH_DASHBOARD_ADDR; observe: $MESH_OBSERVE_ADDR)")

	flag.Parse()

	if *showVersion {
		fmt.Printf("meshd %s\n", version)
		return 0
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if *meshDir != "" {
		// Set the env (rather than patching cfg after Load) so daemons this
		// process spawns — e.g. a sidecar autostarting the coordinator —
		// inherit the same mesh dir.
		os.Setenv(config.EnvMeshDir, *meshDir) //nolint:errcheck
	}
	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		return 1
	}

	switch *mode {
	case "sidecar":
		return runSidecar(cfg, log, *name, *role, *caps, *repo, *model, *noAutoCoord)
	case "expert":
		return runExpert(cfg, log, *name, *role, *caps, *repo, *model, *noAutoCoord)
	case "coordinator":
		return runCoordinator(cfg, log)
	case "dashboard":
		return runDashboard(cfg, log, *addr)
	case "observe":
		return runObserve(cfg, log, *addr)
	case "":
		fmt.Fprintln(os.Stderr, "meshd: --mode is required (sidecar|coordinator|dashboard|observe|expert)")
		return 2
	default:
		fmt.Fprintf(os.Stderr, "meshd: unknown mode %q\n", *mode)
		return 2
	}
}

func runSidecar(cfg config.Config, log *slog.Logger, name, role, caps, repo, model string, noAutoCoord bool) int {
	if name == "" || role == "" {
		fmt.Fprintln(os.Stderr, "meshd --mode sidecar: --name and --role are required")
		return 2
	}
	card := agentcard.Card{
		ID: name, Name: name, Role: role,
		Caps: splitCaps(caps), Repo: repo, Model: model, PID: os.Getpid(),
	}
	if cwd, err := os.Getwd(); err == nil {
		card.CWD = cwd
	}

	if !noAutoCoord {
		if _, err := autostart.EnsureCoordinator(cfg); err != nil {
			log.Error("autostart coordinator", "err", err)
			return 1
		}
	}

	sc, err := sidecar.New(cfg, card, log)
	if err != nil {
		log.Error("sidecar", "err", err)
		return 1
	}
	if err := sc.Start(); err != nil {
		log.Error("sidecar start", "err", err)
		return 1
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sc.Done():
		// `mesh leave` already published the departure.
		sc.Stop()
	case s := <-sig:
		// Graceful daemon shutdown = graceful departure.
		log.Info("signal received, leaving mesh", "signal", s.String())
		sc.Leave("sidecar shutdown")
	}
	return 0
}

// expertAskTimeout caps one runtime turn. The runtime proxy's own AskTimeout
// defaults to unbounded, so the per-ask ctx carries the deadline; an LLM turn
// is 5–60s in practice, so this is generous.
const expertAskTimeout = 5 * time.Minute

// runExpert is the resident-expert daemon: a full role-owning sidecar plus a
// responder loop that answers its accepted asks through a long-lived runtime
// child (claude -p stream-json by default; MESH_EXPERT_CLI swaps the binary so
// CI can fake it). It is a `mesh expert serve` foreground process; the loop and
// the answer path live in internal/sidecar, the runtime wiring lives here.
func runExpert(cfg config.Config, log *slog.Logger, name, role, caps, repo, model string, noAutoCoord bool) int {
	if name == "" || role == "" {
		fmt.Fprintln(os.Stderr, "meshd --mode expert: --name and --role are required")
		return 2
	}
	card := agentcard.Card{
		ID: name, Name: name, Role: role,
		Caps: splitCaps(caps), Repo: repo, Model: model, PID: os.Getpid(),
	}
	if cwd, err := os.Getwd(); err == nil {
		card.CWD = cwd
	}

	if !noAutoCoord {
		if _, err := autostart.EnsureCoordinator(cfg); err != nil {
			log.Error("autostart coordinator", "err", err)
			return 1
		}
	}

	sc, err := sidecar.New(cfg, card, log)
	if err != nil {
		log.Error("sidecar", "err", err)
		return 1
	}
	if err := sc.Start(); err != nil {
		log.Error("sidecar start", "err", err)
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := agentruntime.New(agentruntime.Options{Binary: cfg.ExpertCLI, Stderr: os.Stderr})
	if err := proxy.Start(ctx); err != nil {
		log.Error("expert runtime start", "binary", cfg.ExpertCLI, "err", err)
		sc.Leave("expert runtime unavailable")
		return 1
	}
	childPID := proxy.Pid()
	sc.TrackChild(cfg.ExpertCLI, childPID)
	log.Info("expert serving", "agent", card.ID, "role", role, "runtime", cfg.ExpertCLI, "child", childPID)

	fn := func(askCtx context.Context, question, contextText string) (sidecar.ExpertResult, error) {
		content := question
		if contextText != "" {
			content = contextText + "\n\n" + question
		}
		turnCtx, turnCancel := context.WithTimeout(askCtx, expertAskTimeout)
		defer turnCancel()

		turn, err := proxy.Ask(turnCtx, content)
		if errors.Is(err, agentruntime.ErrProcessExited) && askCtx.Err() == nil {
			// Best-effort crash recovery: rehydrate via --resume and retry once.
			if rerr := proxy.Restart(askCtx); rerr != nil {
				return sidecar.ExpertResult{}, err
			}
			sc.MarkChildExited(childPID)
			childPID = proxy.Pid()
			sc.TrackChild(cfg.ExpertCLI, childPID)
			turn, err = proxy.Ask(turnCtx, content)
		}
		if err != nil {
			return sidecar.ExpertResult{}, err
		}
		return sidecar.ExpertResult{Answer: turn.Text, OK: turn.Status == agentruntime.TurnAnswered}, nil
	}

	go func() {
		if err := sc.ServeExpert(ctx, fn, cfg.HeartbeatInterval); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("expert loop ended", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sc.Done():
		// `mesh leave` already published the departure.
		cancel()
		proxy.Close() //nolint:errcheck // best-effort child teardown
		sc.MarkChildExited(childPID)
		sc.Stop()
	case s := <-sig:
		log.Info("signal received, leaving mesh", "signal", s.String())
		cancel()
		proxy.Close() //nolint:errcheck // close stdin first so the child exits cleanly
		sc.MarkChildExited(childPID)
		sc.Leave("expert shutdown")
	}
	return 0
}

func runCoordinator(cfg config.Config, log *slog.Logger) int {
	c := coordinator.New(cfg, log)
	if err := c.Start(); err != nil {
		log.Error("coordinator start", "err", err)
		return 1
	}
	waitSignal()
	c.Stop()
	return 0
}

func runDashboard(cfg config.Config, log *slog.Logger, addr string) int {
	d := dashboard.New(cfg, addr, log)
	if err := d.Start(); err != nil {
		log.Error("dashboard start", "err", err)
		return 1
	}
	fmt.Printf("dashboard: http://%s\n", d.Addr())
	waitSignal()
	d.Stop()
	return 0
}

func runObserve(cfg config.Config, log *slog.Logger, addr string) int {
	s := observe.New(cfg, addr, log)
	if err := s.Start(); err != nil {
		log.Error("observe start", "err", err)
		return 1
	}
	fmt.Printf("observe: http://%s\n", s.Addr())
	waitSignal()
	s.Stop()
	return 0
}

func waitSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

func splitCaps(csv string) []string {
	if strings.TrimSpace(csv) == "" {
		return nil
	}
	var out []string
	for _, c := range strings.Split(csv, ",") {
		if c = strings.TrimSpace(c); c != "" {
			out = append(out, c)
		}
	}
	return out
}
