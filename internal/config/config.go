// Package config resolves Agent Mesh paths and timing knobs.
//
// Everything is overridable via environment variables so tests and e2e runs
// can use temp dirs and fast clocks without touching production defaults.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Env variable names.
const (
	EnvMeshDir           = "MESH_DIR"
	EnvHeartbeatInterval = "MESH_HEARTBEAT_INTERVAL"
	EnvAwayAfter         = "MESH_AWAY_AFTER"
	EnvEvictAfter        = "MESH_EVICT_AFTER"
	EnvRegistrationGrace = "MESH_REGISTRATION_GRACE"
	EnvClaimTTL          = "MESH_CLAIM_TTL"
	EnvDashboardAddr     = "MESH_DASHBOARD_ADDR"
	EnvObserveAddr       = "MESH_OBSERVE_ADDR"
	EnvAgentSocket       = "MESH_SOCKET"              // CLI → sidecar socket override
	EnvMeshdBin          = "MESH_MESHD"               // path to meshd for autostart
	EnvExpertCLI         = "MESH_EXPERT_CLI"          // agent CLI an expert responder drives (default "claude")
	EnvPlannerCLI        = "MESH_PLANNER_CLI"         // CLI the coordinator's triage planner drives; empty = triage disabled
	EnvPlannerModel      = "MESH_PLANNER_MODEL"       // --model passed to the planner CLI (default "sonnet"; empty = CLI default)
	EnvTriageTimeout     = "MESH_TRIAGE_TIMEOUT"      // wall-clock bound on one planner invocation
	EnvTriageMaxAttempts = "MESH_TRIAGE_MAX_ATTEMPTS" // max planner attempts per job before open→failed (transient codes only); default 4
	EnvTriageBackoff     = "MESH_TRIAGE_BACKOFF"      // base delay for the exponential triage retry backoff; default 30s
	EnvWorkerCLI         = "MESH_WORKER_CLI"          // CLI the coordinator's scheduler drives per task; empty = scheduler disabled
	EnvWorkerModel       = "MESH_WORKER_MODEL"        // --model passed to the worker CLI (default "sonnet"; empty = CLI default)
	EnvWorkerTimeout     = "MESH_WORKER_TIMEOUT"      // wall-clock bound on one worker invocation
	EnvBudgetUSD         = "MESH_BUDGET_USD"          // fleet budget cap in USD; 0/unset = unlimited
	EnvMaxWorkers        = "MESH_MAX_WORKERS"         // max concurrent workers (default 4)
	EnvReposDir          = "MESH_REPOS_DIR"           // dir mapping job repo names to git checkouts; required by the #26 worker driver
	EnvKeepWorktrees     = "MESH_KEEP_WORKTREES"      // worker worktree retention: on-failure (default) | always | never
	EnvAuditFanout       = "MESH_AUDIT_FANOUT"        // coordinator fans bus-observed lifecycle events into the audit log: on (default) | off
	EnvReviewRole        = "MESH_REVIEW_ROLE"         // role whose expert reviews successful worker diffs (#80); empty = review gating off
	EnvReviewTimeout     = "MESH_REVIEW_TIMEOUT"      // wall-clock bound on one review round trip (request → verdict)

	EnvWorkerStream         = "MESH_WORKER_STREAM"          // on = workers run stream-json and write a live per-task transcript under runs/; default off
	EnvWorkerPermissionMode = "MESH_WORKER_PERMISSION_MODE" // claude --permission-mode for workers so they can edit headlessly; empty omits the flag
	EnvPlanCLI              = "MESH_PLAN_CLI"               // CLI the triage plan-step runs to produce an implementation plan written to the blackboard; empty = plan-step off
	EnvPlanModel            = "MESH_PLAN_MODEL"             // --model passed to the plan CLI (default "sonnet"; empty = CLI default)
	EnvPlanTimeout          = "MESH_PLAN_TIMEOUT"           // wall-clock bound on one plan-tool invocation
)

// Worker worktree retention policies (#26). The policy is deterministic:
// teardown consults only the run's typed success and this knob.
const (
	// KeepWorktreesOnFailure (the default) removes a worker's worktree after a
	// typed success — the work product survives as commits on the task branch —
	// and preserves it after anything else, for inspection.
	KeepWorktreesOnFailure = "on-failure"
	// KeepWorktreesAlways never removes worker worktrees.
	KeepWorktreesAlways = "always"
	// KeepWorktreesNever removes the worktree regardless of outcome. The task
	// branch (and any commits on it) is still never deleted.
	KeepWorktreesNever = "never"
)

// Defaults.
const (
	// DefaultExpertCLI is the agent CLI an expert responder drives when
	// MESH_EXPERT_CLI is unset. A literal (not internal/runtime.DefaultBinary)
	// so config carries no dependency on the runtime package.
	DefaultExpertCLI = "claude"

	// DefaultPlannerModel pins the triage planner's model (M0 spike: the CLI
	// default model is the expensive one; planning does not need it).
	DefaultPlannerModel = "sonnet"

	// DefaultTriageTimeout bounds one planner invocation. A planning turn is
	// one LLM call (5–60s observed); minutes means a wedged child.
	DefaultTriageTimeout = 2 * time.Minute

	// DefaultTriageMaxAttempts caps how many planner invocations a single job
	// gets before a transient failure becomes terminal (open→failed). Each
	// attempt is one planner LLM turn = money, so the cap is the budget guard
	// (locked hard-cap billing posture): a down planner must never be retried
	// forever. 4 = the first try plus three backed-off retries. PERMANENT codes
	// (bad_plan, invalid_dag) ignore this and fail on the first attempt.
	DefaultTriageMaxAttempts = 4

	// DefaultTriageBackoff is the base delay of the exponential triage retry
	// schedule: attempt N waits base*2^(N-1), capped at maxTriageBackoff. 30s
	// base gives 30s/60s/120s for attempts 1→2, 2→3, 3→4 under the default cap.
	DefaultTriageBackoff = 30 * time.Second

	// DefaultWorkerModel pins the worker's model (locked fleet decision:
	// always pin --model; an un-pinned `claude -p` defaults to the most
	// expensive tier).
	DefaultWorkerModel = "sonnet"

	// DefaultWorkerTimeout bounds one worker invocation. A worker turn does
	// real implementation work (multi-minute), unlike a planning call.
	DefaultWorkerTimeout = 10 * time.Minute

	// DefaultMaxWorkers caps concurrent workers (fleet spike: safe parallelism
	// is host-bound at 4–8).
	DefaultMaxWorkers = 4

	// DefaultReviewTimeout bounds one review round trip (#80): publish the
	// review request, wait for the expert's typed verdict event. A review is
	// one resident-expert LLM turn (5–60s observed), so this is generous;
	// past it the gate treats the review as lost — never as an approval.
	DefaultReviewTimeout = 5 * time.Minute

	// DefaultWorkerPermissionMode lets a worker's headless `claude -p` actually
	// edit files and run tooling without a prompt — autonomous workers cannot
	// answer an interactive permission request. The M0 spike only ever verified
	// the planner (text-only); the worker path needs this to do real work.
	// Overridable (e.g. acceptEdits) or empty to omit the flag.
	DefaultWorkerPermissionMode = "bypassPermissions"

	// DefaultPlanModel pins the plan-step's model, same rationale as the planner.
	DefaultPlanModel = "sonnet"

	// DefaultPlanTimeout bounds one plan-tool invocation. A real implementation
	// plan is a full multi-minute claude run, unlike the decomposition planner.
	DefaultPlanTimeout = 5 * time.Minute

	DefaultHeartbeatInterval = 5 * time.Second
	DefaultAwayAfter         = 15 * time.Second // 3 missed beats
	DefaultEvictAfter        = 60 * time.Second
	DefaultRegistrationGrace = 10 * time.Second
	DefaultDashboardAddr     = "127.0.0.1:8737"
	DefaultObserveAddr       = "127.0.0.1:8739"
)

// Config carries resolved paths and timings for all meshd modes and the CLI.
type Config struct {
	MeshDir           string
	HeartbeatInterval time.Duration
	AwayAfter         time.Duration // last beat older than this → away
	EvictAfter        time.Duration // last beat older than this → evicted
	RegistrationGrace time.Duration // no away/evict this soon after register
	ClaimTTL          time.Duration // claim lease backstop; renewed each heartbeat
	DashboardAddr     string
	ObserveAddr       string
	ExpertCLI         string // agent CLI an expert responder drives (meshd --mode expert)

	// PlannerCLI is the agent CLI the coordinator's triage loop drives for
	// one-shot planning (#24). Deliberately NO default: an autostarted
	// coordinator must never spawn LLM processes unless the operator opted in
	// (MESH_PLANNER_CLI=claude in production, a fake binary in tests). Empty
	// disables triage entirely.
	PlannerCLI    string
	PlannerModel  string        // --model for the planner CLI; empty = CLI default
	TriageTimeout time.Duration // wall-clock bound on one planner invocation

	// TriageMaxAttempts and TriageBackoff configure the #64 retry/backoff
	// policy for TRANSIENT planner failures (planner_unavailable / planner_failed
	// / internal). A transient failure leaves the job open with persisted attempt
	// metadata and schedules a backed-off retry; PERMANENT failures (bad_plan /
	// invalid_dag — the planner produced garbage, a retry burns money for nothing)
	// fail the job immediately regardless of these. The cap enforces the hard-cap
	// billing posture: each attempt is a planner LLM turn, so transient failures
	// are never retried infinitely.
	TriageMaxAttempts int           // max planner attempts per job (transient codes); default 4
	TriageBackoff     time.Duration // base delay of the exponential retry schedule; default 30s

	// WorkerCLI is the agent CLI the coordinator's scheduler (#25) drives to
	// execute one task. Deliberately NO default, exactly like PlannerCLI: an
	// autostarted coordinator must never spawn worker LLM processes unless the
	// operator opted in. Empty disables the scheduler entirely.
	WorkerCLI     string
	WorkerModel   string        // --model for the worker CLI; empty = CLI default
	WorkerTimeout time.Duration // wall-clock bound on one worker invocation

	// WorkerPermissionMode is the claude --permission-mode a worker child runs
	// under so it can edit files / run tooling headlessly. Empty omits the flag.
	WorkerPermissionMode string

	// WorkerStream, when true, runs the worker CLI in stream-json mode and writes
	// a live newline-delimited transcript to RunsDir()/<task>.jsonl as the agent
	// works, so the dashboard can tail what an agent is doing in real time. Off by
	// default: the proven one-shot `--output-format json` path is unchanged.
	WorkerStream bool

	// PlanCLI is the optional implementation-plan tool the coordinator's triage
	// plan-step runs BEFORE decomposition: its stdout is written to the repo
	// blackboard as a context note, so decomposition and every worker read the
	// plan (mesh context + primer). Deliberately NO default — empty disables the
	// plan-step. Only consulted when PlannerCLI is set (triage enabled).
	PlanCLI     string
	PlanModel   string        // --model for the plan CLI; empty = CLI default
	PlanTimeout time.Duration // wall-clock bound on one plan-tool invocation
	BudgetUSD   float64       // fleet budget cap (locked decision: hard cap, pause-not-fail); 0 = unlimited
	MaxWorkers  int           // max concurrent workers

	// ReviewRole gates the #80 review integration: when set (and the scheduler
	// is enabled), every successful worker diff is routed to the expert serving
	// this role and the task's terminal state is gated on the typed verdict.
	// Deliberately NO default, exactly like PlannerCLI/WorkerCLI: unset means
	// review gating is off and a worker success transitions the task to done
	// exactly as before.
	ReviewRole string
	// ReviewTimeout bounds one review round trip (request → verdict event).
	ReviewTimeout time.Duration

	// ReposDir maps a job's repo NAME to a git checkout at <ReposDir>/<name>.
	// Deliberately NO default: the #26 worker driver refuses to start without
	// it (a worker must never guess which directory tree it may rewrite).
	// Only consulted when WorkerCLI is set.
	ReposDir string
	// KeepWorktrees is the worker worktree retention policy
	// (KeepWorktreesOnFailure | KeepWorktreesAlways | KeepWorktreesNever).
	KeepWorktrees string

	// AuditFanout enables the #29 unified audit log: the coordinator taps
	// mesh.> and fans the major lifecycle events of every domain (ask/answer/
	// ticket/job/task/triage/worker/fleet, on top of the always-on presence/
	// claim audits) into envelope.StreamAudit, so one ordered read reconstructs
	// how any ticket/job/claim reached its state. On by default; MESH_AUDIT_FANOUT=off
	// disables only the bus-observed fan-out (presence/claim audits, which
	// existing reducer/sweep paths depend on, are always emitted) — for local
	// tuning and test determinism (issue #29: "knobs for ... test determinism").
	AuditFanout bool
}

// Load resolves config from the environment with defaults.
func Load() (Config, error) {
	cfg := Config{
		HeartbeatInterval:    DefaultHeartbeatInterval,
		AwayAfter:            DefaultAwayAfter,
		EvictAfter:           DefaultEvictAfter,
		RegistrationGrace:    DefaultRegistrationGrace,
		DashboardAddr:        DefaultDashboardAddr,
		ObserveAddr:          DefaultObserveAddr,
		ExpertCLI:            DefaultExpertCLI,
		PlannerModel:         DefaultPlannerModel,
		TriageTimeout:        DefaultTriageTimeout,
		TriageMaxAttempts:    DefaultTriageMaxAttempts,
		TriageBackoff:        DefaultTriageBackoff,
		WorkerModel:          DefaultWorkerModel,
		WorkerTimeout:        DefaultWorkerTimeout,
		WorkerPermissionMode: DefaultWorkerPermissionMode,
		PlanModel:            DefaultPlanModel,
		PlanTimeout:          DefaultPlanTimeout,
		MaxWorkers:           DefaultMaxWorkers,
		ReviewTimeout:        DefaultReviewTimeout,
		KeepWorktrees:        KeepWorktreesOnFailure,
		AuditFanout:          true,
	}

	if dir := os.Getenv(EnvMeshDir); dir != "" {
		cfg.MeshDir = dir
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return Config{}, fmt.Errorf("config: resolve home dir: %w", err)
		}
		cfg.MeshDir = filepath.Join(home, ".mesh")
	}

	for _, d := range []struct {
		env string
		dst *time.Duration
	}{
		{EnvHeartbeatInterval, &cfg.HeartbeatInterval},
		{EnvAwayAfter, &cfg.AwayAfter},
		{EnvEvictAfter, &cfg.EvictAfter},
		{EnvRegistrationGrace, &cfg.RegistrationGrace},
		{EnvClaimTTL, &cfg.ClaimTTL},
		{EnvTriageTimeout, &cfg.TriageTimeout},
		{EnvTriageBackoff, &cfg.TriageBackoff},
		{EnvWorkerTimeout, &cfg.WorkerTimeout},
		{EnvReviewTimeout, &cfg.ReviewTimeout},
		{EnvPlanTimeout, &cfg.PlanTimeout},
	} {
		raw := os.Getenv(d.env)
		if raw == "" {
			continue
		}
		dur, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("config: %s=%q: %w", d.env, raw, err)
		}
		if dur <= 0 {
			return Config{}, fmt.Errorf("config: %s must be positive, got %q", d.env, raw)
		}
		*d.dst = dur
	}

	if addr := os.Getenv(EnvDashboardAddr); addr != "" {
		cfg.DashboardAddr = addr
	}
	if addr := os.Getenv(EnvObserveAddr); addr != "" {
		cfg.ObserveAddr = addr
	}
	if cli := os.Getenv(EnvExpertCLI); cli != "" {
		cfg.ExpertCLI = cli
	}
	cfg.PlannerCLI = os.Getenv(EnvPlannerCLI) // empty = triage disabled
	if model, ok := os.LookupEnv(EnvPlannerModel); ok {
		cfg.PlannerModel = model // explicit empty = use the CLI's default model
	}
	cfg.WorkerCLI = os.Getenv(EnvWorkerCLI) // empty = scheduler disabled
	if model, ok := os.LookupEnv(EnvWorkerModel); ok {
		cfg.WorkerModel = model // explicit empty = use the CLI's default model
	}
	if mode, ok := os.LookupEnv(EnvWorkerPermissionMode); ok {
		cfg.WorkerPermissionMode = mode // explicit empty = omit the --permission-mode flag
	}
	if raw := os.Getenv(EnvWorkerStream); raw != "" {
		switch raw {
		case "on", "true", "1":
			cfg.WorkerStream = true
		case "off", "false", "0":
			cfg.WorkerStream = false
		default:
			return Config{}, fmt.Errorf("config: %s=%q: want on|off", EnvWorkerStream, raw)
		}
	}
	cfg.PlanCLI = os.Getenv(EnvPlanCLI) // empty = plan-step disabled
	if model, ok := os.LookupEnv(EnvPlanModel); ok {
		cfg.PlanModel = model // explicit empty = use the CLI's default model
	}
	if raw := os.Getenv(EnvBudgetUSD); raw != "" {
		budget, err := strconv.ParseFloat(raw, 64)
		if err != nil || budget < 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a non-negative USD amount", EnvBudgetUSD, raw)
		}
		cfg.BudgetUSD = budget
	}
	if raw := os.Getenv(EnvMaxWorkers); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a positive integer", EnvMaxWorkers, raw)
		}
		cfg.MaxWorkers = n
	}
	if raw := os.Getenv(EnvTriageMaxAttempts); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a positive integer", EnvTriageMaxAttempts, raw)
		}
		cfg.TriageMaxAttempts = n
	}
	if raw := os.Getenv(EnvAuditFanout); raw != "" {
		switch raw {
		case "on", "true", "1":
			cfg.AuditFanout = true
		case "off", "false", "0":
			cfg.AuditFanout = false
		default:
			return Config{}, fmt.Errorf("config: %s=%q: want on|off", EnvAuditFanout, raw)
		}
	}
	cfg.ReviewRole = os.Getenv(EnvReviewRole) // empty = review gating off
	cfg.ReposDir = os.Getenv(EnvReposDir)     // empty = worker driver refuses to construct
	if raw := os.Getenv(EnvKeepWorktrees); raw != "" {
		switch raw {
		case KeepWorktreesOnFailure, KeepWorktreesAlways, KeepWorktreesNever:
			cfg.KeepWorktrees = raw
		default:
			return Config{}, fmt.Errorf("config: %s=%q: want %s|%s|%s", EnvKeepWorktrees, raw,
				KeepWorktreesOnFailure, KeepWorktreesAlways, KeepWorktreesNever)
		}
	}

	if cfg.AwayAfter < cfg.HeartbeatInterval {
		return Config{}, fmt.Errorf("config: away-after (%s) must be >= heartbeat interval (%s)",
			cfg.AwayAfter, cfg.HeartbeatInterval)
	}
	if cfg.EvictAfter <= cfg.AwayAfter {
		return Config{}, fmt.Errorf("config: evict-after (%s) must be > away-after (%s)",
			cfg.EvictAfter, cfg.AwayAfter)
	}
	// Claim lease backstop: like the registry record TTL, it must outlast
	// every legitimate silent window (the eviction sweep is the primary
	// release path; the TTL self-heals if the coordinator is down). Derived
	// from EvictAfter unless explicitly set.
	if cfg.ClaimTTL == 0 {
		cfg.ClaimTTL = 2 * (cfg.EvictAfter + cfg.RegistrationGrace)
	}
	if cfg.ClaimTTL <= cfg.HeartbeatInterval {
		return Config{}, fmt.Errorf("config: claim-ttl (%s) must be > heartbeat interval (%s)",
			cfg.ClaimTTL, cfg.HeartbeatInterval)
	}
	return cfg, nil
}

// BusSocket is the coordinator-owned bus socket path.
func (c Config) BusSocket() string { return filepath.Join(c.MeshDir, "bus.sock") }

// AgentsDir holds per-agent sidecar sockets.
func (c Config) AgentsDir() string { return filepath.Join(c.MeshDir, "agents") }

// AgentSocket is the sidecar socket path for the named agent.
func (c Config) AgentSocket(name string) string {
	return filepath.Join(c.AgentsDir(), name+".sock")
}

// AgentPIDFile is the pidfile the sidecar writes beside its socket. It is the
// fact source of last resort for the ops plane: an agent evicted from the
// registry whose socket is hung is otherwise invisible.
func (c Config) AgentPIDFile(name string) string {
	return filepath.Join(c.AgentsDir(), name+".pid")
}

// CoordinatorLock is the flock file used to elect a single coordinator
// autostarter when several sidecars race to boot one.
func (c Config) CoordinatorLock() string { return filepath.Join(c.MeshDir, "coordinator.lock") }

// StreamsDir holds the bus server's durable stream files (one JSONL per
// stream). Owned by the coordinator-embedded bus server only.
func (c Config) StreamsDir() string { return filepath.Join(c.MeshDir, "streams") }

// BucketsDir holds the bus server's durable KV op logs (one bucket-<name>.jsonl
// per persisted bucket: jobs, tasks — #65). Owned by the coordinator-embedded
// bus server only. The lease buckets (registry, claims) are NOT persisted here:
// they self-heal by re-registration / re-establishment.
func (c Config) BucketsDir() string { return filepath.Join(c.MeshDir, "buckets") }

// WorkersDir holds the per-task worker worktrees the #26 driver creates
// (one isolated git worktree per dispatched task). Owned by the
// coordinator-embedded worker driver only.
func (c Config) WorkersDir() string { return filepath.Join(c.MeshDir, "workers") }

// RunsDir holds per-task live transcripts (one <task>.jsonl per worker run)
// when WorkerStream is on. The dashboard tails these to show what an agent is
// doing in real time.
func (c Config) RunsDir() string { return filepath.Join(c.MeshDir, "runs") }

// CoordinatorPID is written by the running coordinator for ops inspection.
func (c Config) CoordinatorPID() string { return filepath.Join(c.MeshDir, "coordinator.pid") }

// DashboardPID is written by the running dashboard for ops inspection.
func (c Config) DashboardPID() string { return filepath.Join(c.MeshDir, "dashboard.pid") }

// DashboardAddrFile holds the dashboard's real bound address (it may differ
// from DashboardAddr after a port-conflict fallback or :0). The one authority
// for "where is the UI" — the daemon's stdout goes to a logfile when spawned
// detached.
func (c Config) DashboardAddrFile() string { return filepath.Join(c.MeshDir, "dashboard.addr") }

// DashboardLock is the flock file electing a single dashboard autostarter.
func (c Config) DashboardLock() string { return filepath.Join(c.MeshDir, "dashboard.lock") }

// DashboardTokenFile holds the write-API bearer token the dashboard generated
// on start. The UI fetches it from GET /api/write-token (never directly from
// disk); CLI users can read the file. Observer endpoints stay unauthenticated.
func (c Config) DashboardTokenFile() string {
	return filepath.Join(c.MeshDir, "dashboard.token")
}

// ObservePID is written by the running observe server for ops inspection.
func (c Config) ObservePID() string { return filepath.Join(c.MeshDir, "observe.pid") }

// ObserveAddrFile holds the observe server's real bound address (see
// DashboardAddrFile).
func (c Config) ObserveAddrFile() string { return filepath.Join(c.MeshDir, "observe.addr") }

// ObserveLock is the flock file electing a single observe autostarter.
func (c Config) ObserveLock() string { return filepath.Join(c.MeshDir, "observe.lock") }

// EnsureDirs creates the mesh directories with owner-only permissions.
func (c Config) EnsureDirs() error {
	for _, dir := range []string{c.MeshDir, c.AgentsDir()} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("config: create %s: %w", dir, err)
		}
	}
	return nil
}
