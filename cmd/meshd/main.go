// Command meshd is the Agent Mesh daemon. One binary, several modes selected
// by --mode: sidecar (one per agent), coordinator (one per mesh — embeds the
// bus/store), or dashboard (read-only observer).
//
// No business logic lives here: flags are parsed and handed to internal/*.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/autostart"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/coordinator"
	"github.com/georgenijo/agent-mesh/internal/dashboard"
	"github.com/georgenijo/agent-mesh/internal/sidecar"
)

var version = "0.1.0-dev"

func main() {
	os.Exit(run())
}

func run() int {
	mode := flag.String("mode", "", "daemon mode: sidecar | coordinator | dashboard")
	showVersion := flag.Bool("version", false, "print version and exit")

	// Sidecar flags.
	name := flag.String("name", "", "agent name (sidecar mode, required)")
	role := flag.String("role", "", "agent role (sidecar mode, required)")
	caps := flag.String("caps", "", "comma-separated capability tokens (sidecar mode)")
	repo := flag.String("repo", "", "repository the agent works on (sidecar mode)")
	model := flag.String("model", "", "model the agent runs (sidecar mode)")
	noAutoCoord := flag.Bool("no-autostart-coordinator", false,
		"fail instead of spawning a coordinator when the bus is down (sidecar mode)")

	// Dashboard flags.
	addr := flag.String("addr", "", "dashboard listen address (default $MESH_DASHBOARD_ADDR or 127.0.0.1:8737)")

	flag.Parse()

	if *showVersion {
		fmt.Printf("meshd %s\n", version)
		return 0
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		return 1
	}

	switch *mode {
	case "sidecar":
		return runSidecar(cfg, log, *name, *role, *caps, *repo, *model, *noAutoCoord)
	case "coordinator":
		return runCoordinator(cfg, log)
	case "dashboard":
		return runDashboard(cfg, log, *addr)
	case "":
		fmt.Fprintln(os.Stderr, "meshd: --mode is required (sidecar|coordinator|dashboard)")
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
		if err := autostart.EnsureCoordinator(cfg); err != nil {
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
