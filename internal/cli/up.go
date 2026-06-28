package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/georgenijo/agent-mesh/internal/autostart"
	"github.com/georgenijo/agent-mesh/internal/config"
)

// upCoordinator reports the control plane's bring-up outcome.
type upCoordinator struct {
	Status    autostart.EnsureStatus `json:"status"`
	BusSocket string                 `json:"busSocket"`
}

// upFleet captures which fleet capabilities were armed at bring-up time.
type upFleet struct {
	PlannerCLI   string  `json:"plannerCLI,omitempty"`
	PlannerModel string  `json:"plannerModel,omitempty"`
	WorkerCLI    string  `json:"workerCLI,omitempty"`
	WorkerModel  string  `json:"workerModel,omitempty"`
	ReposDir     string  `json:"reposDir,omitempty"`
	ReviewRole   string  `json:"reviewRole,omitempty"`
	BudgetUSD    float64 `json:"budgetUSD,omitempty"`
	AutoExperts  bool    `json:"autoExperts,omitempty"`
	JobsAddr     string  `json:"jobsAddr,omitempty"`
	GitHubRepo   string  `json:"githubRepo,omitempty"`
}

// upReport is the `mesh up --json` contract.
type upReport struct {
	MeshDir     string              `json:"meshDir"`
	Coordinator upCoordinator       `json:"coordinator"`
	Dashboard   autostart.ServiceUp `json:"dashboard"`
	Observe     autostart.ServiceUp `json:"observe"`
	Fleet       upFleet             `json:"fleet"`
}

// upFileConfig is the schema for $MESH_DIR/config.json (or --config path).
// All fields are optional; unset fields leave the corresponding env var untouched.
// Precedence: env vars > CLI flags > config file.
type upFileConfig struct {
	PlannerCLI    string   `json:"plannerCLI"`
	PlannerModel  string   `json:"plannerModel"`
	WorkerCLI     string   `json:"workerCLI"`
	WorkerModel   string   `json:"workerModel"`
	ReposDir      string   `json:"reposDir"`
	ReviewRole    string   `json:"reviewRole"`
	BudgetUSD     *float64 `json:"budgetUSD"`
	AutoExperts   *bool    `json:"autoExperts"`
	JobsAddr      string   `json:"jobsAddr"`
	GitHubRepo    string   `json:"githubRepo"`
	DashboardAddr string   `json:"dashboardAddr"`
	ObserveAddr   string   `json:"observeAddr"`
}

// applyUpConfigFile reads the JSON config file at path and applies any set
// fields to the process environment via setIfUnset, so they are inherited by
// coordinator/dashboard/observe processes spawned by this command. Missing
// file is silently ignored (the file is optional).
func applyUpConfigFile(path string) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var fc upFileConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	setIfUnset(config.EnvPlannerCLI, fc.PlannerCLI)
	setIfUnset(config.EnvPlannerModel, fc.PlannerModel)
	setIfUnset(config.EnvWorkerCLI, fc.WorkerCLI)
	setIfUnset(config.EnvWorkerModel, fc.WorkerModel)
	setIfUnset(config.EnvReposDir, fc.ReposDir)
	setIfUnset(config.EnvReviewRole, fc.ReviewRole)
	setIfUnset(config.EnvJobsAddr, fc.JobsAddr)
	setIfUnset(config.EnvGitHubRepo, fc.GitHubRepo)
	setIfUnset(config.EnvDashboardAddr, fc.DashboardAddr)
	setIfUnset(config.EnvObserveAddr, fc.ObserveAddr)
	if fc.BudgetUSD != nil && os.Getenv(config.EnvBudgetUSD) == "" {
		os.Setenv(config.EnvBudgetUSD, strconv.FormatFloat(*fc.BudgetUSD, 'f', -1, 64)) //nolint:errcheck
	}
	if fc.AutoExperts != nil && os.Getenv(config.EnvAutoExperts) == "" {
		val := "off"
		if *fc.AutoExperts {
			val = "on"
		}
		os.Setenv(config.EnvAutoExperts, val) //nolint:errcheck
	}
	return nil
}

// setIfUnset sets key=val in the process environment only when key is
// not already set and val is non-empty.
func setIfUnset(key, val string) {
	if val != "" && os.Getenv(key) == "" {
		os.Setenv(key, val) //nolint:errcheck
	}
}

// runUp brings up the whole mesh infrastructure — coordinator, dashboard,
// observe — idempotently, arms the fleet (planner/worker/expert/review) from
// a config file and/or flags, and prints the dashboard URL. It is infrastructure
// only: agents join themselves (`mesh join` / hooks), and worker spawning is
// the coordinator's job. Order matters: the dashboard dials the bus on start,
// so the coordinator goes first. Partial progress is real progress — on
// failure, report what came up and exit 1; a rerun finishes the job.
//
// Config precedence (lowest → highest): config file → env vars → CLI flags.
// The spawned coordinator inherits the current process's environment, so
// settings applied here take effect in the coordinator on first bring-up.
func runUp(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("up", flag.ContinueOnError)
	fs.SetOutput(stderr)
	jsonOut := fs.Bool("json", false, "machine-readable output")
	configFile := fs.String("config", "", "JSON config file (default $MESH_DIR/config.json; optional)")
	dashAddr := fs.String("dashboard-addr", "", "dashboard listen address (default $MESH_DASHBOARD_ADDR or "+config.DefaultDashboardAddr+")")
	obsAddr := fs.String("observe-addr", "", "observe listen address (default $MESH_OBSERVE_ADDR or "+config.DefaultObserveAddr+")")

	// Fleet-arming flags — these set the corresponding MESH_* env vars so the
	// spawned coordinator inherits them. Flags override the config file and any
	// already-set env vars.
	plannerCLI := fs.String("planner-cli", "", "CLI the coordinator's triage planner drives (e.g. claude); empty = triage disabled")
	plannerModel := fs.String("planner-model", "", "model flag for planner CLI (default: "+config.DefaultPlannerModel+")")
	workerCLI := fs.String("worker-cli", "", "CLI the coordinator's scheduler drives per task; empty = scheduler disabled")
	workerModel := fs.String("worker-model", "", "model flag for worker CLI (default: "+config.DefaultWorkerModel+")")
	reposDir := fs.String("repos-dir", "", "directory mapping job repo names to git checkouts (required for workers)")
	reviewRole := fs.String("review-role", "", "role whose expert reviews worker diffs; empty = review gating off")
	budget := fs.String("budget", "", "fleet budget cap in USD (e.g. 50.00); 0 or empty = unlimited")
	autoExperts := fs.String("auto-experts", "", "auto-spawn experts on demand: on|off (default off)")
	jobsAddr := fs.String("jobs-addr", "", "HTTP listen address for POST /jobs ingress (empty = disabled)")
	githubRepo := fs.String("github-repo", "", "GitHub owner/repo for 'mesh work' NL control (e.g. owner/repo)")

	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}

	// Resolve mesh dir independently (before config.Load) so we can find the
	// default config file.
	meshDirForFile := os.Getenv(config.EnvMeshDir)
	if meshDirForFile == "" {
		if home, err := os.UserHomeDir(); err == nil {
			meshDirForFile = filepath.Join(home, ".mesh")
		}
	}

	// Load config file: fills env vars for any key not already set in the env.
	cfgFilePath := *configFile
	if cfgFilePath == "" && meshDirForFile != "" {
		cfgFilePath = filepath.Join(meshDirForFile, "config.json")
	}
	if cfgFilePath != "" {
		if err := applyUpConfigFile(cfgFilePath); err != nil {
			fmt.Fprintf(stderr, "mesh: up: config file: %v\n", err)
			return ExitError
		}
	}

	// Apply CLI flags — they override both the config file and existing env vars.
	if *plannerCLI != "" {
		os.Setenv(config.EnvPlannerCLI, *plannerCLI) //nolint:errcheck
	}
	if *plannerModel != "" {
		os.Setenv(config.EnvPlannerModel, *plannerModel) //nolint:errcheck
	}
	if *workerCLI != "" {
		os.Setenv(config.EnvWorkerCLI, *workerCLI) //nolint:errcheck
	}
	if *workerModel != "" {
		os.Setenv(config.EnvWorkerModel, *workerModel) //nolint:errcheck
	}
	if *reposDir != "" {
		os.Setenv(config.EnvReposDir, *reposDir) //nolint:errcheck
	}
	if *reviewRole != "" {
		os.Setenv(config.EnvReviewRole, *reviewRole) //nolint:errcheck
	}
	if *budget != "" {
		os.Setenv(config.EnvBudgetUSD, *budget) //nolint:errcheck
	}
	if *autoExperts != "" {
		os.Setenv(config.EnvAutoExperts, *autoExperts) //nolint:errcheck
	}
	if *jobsAddr != "" {
		os.Setenv(config.EnvJobsAddr, *jobsAddr) //nolint:errcheck
	}
	if *githubRepo != "" {
		os.Setenv(config.EnvGitHubRepo, *githubRepo) //nolint:errcheck
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

	rep.Fleet = upFleet{
		PlannerCLI:   cfg.PlannerCLI,
		PlannerModel: cfg.PlannerModel,
		WorkerCLI:    cfg.WorkerCLI,
		WorkerModel:  cfg.WorkerModel,
		ReposDir:     cfg.ReposDir,
		ReviewRole:   cfg.ReviewRole,
		BudgetUSD:    cfg.BudgetUSD,
		AutoExperts:  cfg.AutoExperts,
		JobsAddr:     cfg.JobsAddr,
		GitHubRepo:   cfg.GitHubRepo,
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
	printFleet(stdout, rep.Fleet)
	return ExitOK
}

// printFleet summarizes which fleet capabilities are armed.
func printFleet(w io.Writer, f upFleet) {
	fmt.Fprintln(w, "fleet:")
	if f.PlannerCLI != "" {
		fmt.Fprintf(w, "  planner:   %s (model: %s)\n", f.PlannerCLI, f.PlannerModel)
	} else {
		fmt.Fprintln(w, "  planner:   (off — set --planner-cli or plannerCLI in config.json)")
	}
	if f.WorkerCLI != "" {
		fmt.Fprintf(w, "  worker:    %s (model: %s)\n", f.WorkerCLI, f.WorkerModel)
	} else {
		fmt.Fprintln(w, "  worker:    (off — set --worker-cli or workerCLI in config.json)")
	}
	if f.ReviewRole != "" {
		fmt.Fprintf(w, "  reviewer:  role %q\n", f.ReviewRole)
	} else {
		fmt.Fprintln(w, "  reviewer:  (off — set --review-role to enable)")
	}
	if f.AutoExperts {
		fmt.Fprintln(w, "  experts:   auto-spawn on")
	} else {
		fmt.Fprintln(w, "  experts:   manual (--auto-experts on to enable)")
	}
	if f.BudgetUSD > 0 {
		fmt.Fprintf(w, "  budget:    $%.2f\n", f.BudgetUSD)
	} else {
		fmt.Fprintln(w, "  budget:    unlimited")
	}
}
