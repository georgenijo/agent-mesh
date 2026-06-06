package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/georgenijo/agent-mesh/internal/autostart"
	"github.com/georgenijo/agent-mesh/internal/config"
)

// upCoordinator reports the control plane's bring-up outcome.
type upCoordinator struct {
	Status    autostart.EnsureStatus `json:"status"`
	BusSocket string                 `json:"busSocket"`
}

// upReport is the `mesh up --json` contract.
type upReport struct {
	MeshDir     string              `json:"meshDir"`
	Coordinator upCoordinator       `json:"coordinator"`
	Dashboard   autostart.ServiceUp `json:"dashboard"`
	Observe     autostart.ServiceUp `json:"observe"`
}

// runUp brings up the whole mesh infrastructure — coordinator, dashboard,
// observe — idempotently, and prints where to look. It is infrastructure
// only: agents join themselves (`mesh join` / hooks), and worker spawning is
// the coordinator's job. Order matters: the dashboard dials the bus on start,
// so the coordinator goes first. Partial progress is real progress — on
// failure, report what came up and exit 1; a rerun finishes the job.
func runUp(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("up", flag.ContinueOnError)
	fs.SetOutput(stderr)
	jsonOut := fs.Bool("json", false, "machine-readable output")
	dashAddr := fs.String("dashboard-addr", "", "dashboard listen address (default $MESH_DASHBOARD_ADDR or "+config.DefaultDashboardAddr+")")
	obsAddr := fs.String("observe-addr", "", "observe listen address (default $MESH_OBSERVE_ADDR or "+config.DefaultObserveAddr+")")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}

	rep := upReport{MeshDir: cfg.MeshDir}
	fail := func(what string, err error) int {
		if *jsonOut {
			obj := map[string]any{"ok": false, "failed": what, "message": err.Error(), "report": rep}
			b, _ := json.Marshal(obj) //nolint:errcheck
			fmt.Fprintln(stdout, string(b))
		} else {
			fmt.Fprintf(stderr, "mesh: up: %s: %v\n", what, err)
		}
		return ExitError
	}

	started, err := autostart.EnsureCoordinator(cfg)
	if err != nil {
		return fail("coordinator", err)
	}
	rep.Coordinator = upCoordinator{Status: autostart.StatusAlreadyRunning, BusSocket: cfg.BusSocket()}
	if started {
		rep.Coordinator.Status = autostart.StatusStarted
	}

	if rep.Dashboard, err = autostart.EnsureDashboard(cfg, *dashAddr); err != nil {
		return fail("dashboard", err)
	}
	if rep.Observe, err = autostart.EnsureObserve(cfg, *obsAddr); err != nil {
		return fail("observe", err)
	}

	if *jsonOut {
		b, _ := json.Marshal(rep) //nolint:errcheck
		fmt.Fprintln(stdout, string(b))
		return ExitOK
	}
	fmt.Fprintf(stdout, "mesh up (%s)\n", cfg.MeshDir)
	fmt.Fprintf(stdout, "coordinator: %s (bus %s)\n", rep.Coordinator.Status, cfg.BusSocket())
	for _, svc := range []autostart.ServiceUp{rep.Dashboard, rep.Observe} {
		fmt.Fprintf(stdout, "%-11s %s  (%s, pid %d)\n", svc.Name+":", svc.URL, svc.Status, svc.PID)
	}
	return ExitOK
}
