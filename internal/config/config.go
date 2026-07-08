// Package config resolves Agent Mesh paths and timing knobs.
//
// Everything is overridable via environment variables so tests and e2e runs
// can use temp dirs and fast clocks without touching production defaults.
package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	// EnvDashboardAllowedHosts is a comma-separated allow-list of extra Host
	// header values the dashboard accepts in addition to loopback. Empty (the
	// default) keeps the secure loopback-only posture; set it to authorize a
	// trusted remote name such as a tailnet MagicDNS host or IP.
	EnvDashboardAllowedHosts = "MESH_DASHBOARD_ALLOWED_HOSTS"
	EnvObserveAddr           = "MESH_OBSERVE_ADDR"
	EnvAgentSocket           = "MESH_SOCKET"              // CLI → sidecar socket override
	EnvMeshdBin              = "MESH_MESHD"               // path to meshd for autostart
	EnvExpertCLI             = "MESH_EXPERT_CLI"          // agent CLI an expert responder drives (default "claude")
	EnvPlannerCLI            = "MESH_PLANNER_CLI"         // CLI the coordinator's triage planner drives; empty = triage disabled
	EnvPlannerModel          = "MESH_PLANNER_MODEL"       // --model passed to the planner CLI (default "sonnet"; empty = CLI default)
	EnvTriageTimeout         = "MESH_TRIAGE_TIMEOUT"      // wall-clock bound on one planner invocation
	EnvTriageMaxAttempts     = "MESH_TRIAGE_MAX_ATTEMPTS" // max planner attempts per job before open→failed (transient codes only); default 4
	EnvTriageBackoff         = "MESH_TRIAGE_BACKOFF"      // base delay for the exponential triage retry backoff; default 30s
	EnvWorkerCLI             = "MESH_WORKER_CLI"          // CLI the coordinator's scheduler drives per task; empty = scheduler disabled
	EnvWorkerModel           = "MESH_WORKER_MODEL"        // --model passed to the worker CLI (default "sonnet"; empty = CLI default)
	EnvWorkerTimeout         = "MESH_WORKER_TIMEOUT"      // wall-clock bound on one worker invocation
	EnvBudgetUSD             = "MESH_BUDGET_USD"          // fleet budget cap in USD; 0/unset = unlimited
	EnvMaxWorkers            = "MESH_MAX_WORKERS"         // max concurrent workers (default 4)
	EnvReposDir              = "MESH_REPOS_DIR"           // dir mapping job repo names to git checkouts; required by the #26 worker driver
	EnvKeepWorktrees         = "MESH_KEEP_WORKTREES"      // worker worktree retention: on-failure (default) | always | never
	EnvAuditFanout           = "MESH_AUDIT_FANOUT"        // coordinator fans bus-observed lifecycle events into the audit log: on (default) | off
	EnvReviewRole            = "MESH_REVIEW_ROLE"         // role whose expert reviews successful worker diffs (#80); empty = review gating off
	EnvReviewTimeout         = "MESH_REVIEW_TIMEOUT"      // wall-clock bound on one review round trip (request → verdict)
	EnvReviewPoolSize        = "MESH_REVIEW_POOL_SIZE"    // number of resident reviewer experts the fleet maintains for MESH_REVIEW_ROLE (#123); default 1
	EnvReviewRetries         = "MESH_REVIEW_RETRIES"      // max re-dispatch attempts when a reviewer returns request_changes (#85); default 2; 0 = fail immediately
	EnvAutoExperts           = "MESH_AUTO_EXPERTS"        // coordinator auto-spawns a resident expert when a role-ask/review-req has no live owner (#117): on | off (default off)
	EnvExpertIdleTTL         = "MESH_EXPERT_IDLE_TTL"     // expert self-terminates after this period with no ask/review activity (#105); 0 = never
	EnvReDispatchBackoff     = "MESH_REDISPATCH_BACKOFF"  // scheduler rate-limit re-dispatch delay; default 30s
	EnvJobsAddr              = "MESH_JOBS_ADDR"           // HTTP ingress for POST /jobs (#119); empty (default) = disabled
	EnvGitHubRepo            = "MESH_GITHUB_REPO"         // GitHub repo (owner/repo) for NL job control (`mesh work`); empty = mesh work disabled
	// EnvEscalationFile is the path the worker child can write an escalation
	// question to (via `mesh escalate "<question>"`). The worker runtime sets
	// this in the child's env; `mesh escalate` reads it and writes the question.
	// Empty outside of a worker-spawned child context.
	EnvEscalationFile = "MESH_ESCALATION_FILE"

	// Feature 5 — struggle → ask-the-expert nudge (worker stream observer).
	EnvStruggleNudge      = "MESH_STRUGGLE_NUDGE"       // on|off (default off): auto-ask an expert when a worker loops
	EnvStruggleRole       = "MESH_STRUGGLE_ROLE"        // expert role for auto-asks; empty → ReviewRole → "architect"
	EnvStruggleTestRepeat = "MESH_STRUGGLE_TEST_REPEAT" // same test-failure fingerprint count before nudge; default 3
	EnvStruggleEditRepeat = "MESH_STRUGGLE_EDIT_REPEAT" // same-path edit count before nudge; default 4
	EnvStruggleCooldown   = "MESH_STRUGGLE_COOLDOWN"    // min gap between auto-asks per worker run; default 5m
	EnvStruggleMaxAsks    = "MESH_STRUGGLE_MAX_ASKS"    // cap auto-asks per worker run; default 2

	// Feature 6 — exact-match answer cache (#29 slice).
	EnvAnswerCache           = "MESH_ANSWER_CACHE"             // on|off (default off): reuse last good answer for same role+q+ctx
	EnvAnswerCacheTTL        = "MESH_ANSWER_CACHE_TTL"         // entry TTL; default 15m
	EnvAnswerCacheIncludeCtx = "MESH_ANSWER_CACHE_INCLUDE_CTX" // on|off (default on): include ask ctx in cache key
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

	// DefaultReviewPoolSize is the number of resident reviewer experts the
	// fleet maintains for MESH_REVIEW_ROLE (#123). Pool size 1 preserves the
	// current single-reviewer behaviour; a larger pool allows N concurrent
	// review turns instead of serialising on one expert.
	DefaultReviewPoolSize = 1

	// DefaultReviewRetries is the number of re-dispatch attempts the scheduler
	// makes when a reviewer returns request_changes (#85). Each retry continues
	// on the task's existing branch and injects the reviewer's notes into the
	// worker prompt. 0 = fail immediately on request_changes.
	DefaultReviewRetries = 2

	// DefaultExpertIdleTTL is how long an auto-spawned expert may sit idle
	// (no ask or review handled) before it self-terminates (#105). Five
	// minutes matches a typical LLM turn round-trip; set MESH_EXPERT_IDLE_TTL=0
	// to disable the reaper.
	DefaultExpertIdleTTL = 5 * time.Minute

	// DefaultReDispatchBackoff is the scheduler's rate-limit re-dispatch delay:
	// after a worker result maps to WorkerRateLimited the task waits this long
	// before being re-dispatched. Wired into scheduler.Options.Backoff (a field
	// that previously had no config source — the phantom knob).
	DefaultReDispatchBackoff = 30 * time.Second

	// DefaultStruggleTestRepeat / EditRepeat / Cooldown / MaxAsks gate Feature 5
	// auto-asks so a noisy worker cannot burn the expert budget.
	DefaultStruggleTestRepeat = 3
	DefaultStruggleEditRepeat = 4
	DefaultStruggleCooldown   = 5 * time.Minute
	DefaultStruggleMaxAsks    = 2

	// DefaultAnswerCacheTTL is how long a successful role-ask answer stays
	// reusable for an identical (role, q, ctx) key before expiry.
	DefaultAnswerCacheTTL = 15 * time.Minute

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
	// DashboardAllowedHosts are extra Host header values the dashboard accepts
	// beyond loopback (lower-cased, port stripped). Empty = loopback-only.
	DashboardAllowedHosts []string
	ObserveAddr           string
	ExpertCLI             string // agent CLI an expert responder drives (meshd --mode expert)

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
	BudgetUSD     float64       // fleet budget cap (locked decision: hard cap, pause-not-fail); 0 = unlimited
	MaxWorkers    int           // max concurrent workers
	// Backoff is the scheduler's rate-limit re-dispatch delay (scheduler.Options.Backoff).
	Backoff time.Duration

	// ReviewRole gates the #80 review integration: when set (and the scheduler
	// is enabled), every successful worker diff is routed to the expert serving
	// this role and the task's terminal state is gated on the typed verdict.
	// Deliberately NO default, exactly like PlannerCLI/WorkerCLI: unset means
	// review gating is off and a worker success transitions the task to done
	// exactly as before.
	ReviewRole string
	// ReviewTimeout bounds one review round trip (request → verdict event).
	ReviewTimeout time.Duration
	// ReviewPoolSize is the number of resident reviewer experts the fleet
	// maintains for ReviewRole (#123). Default 1 (single reviewer, pre-#123
	// behaviour). A larger pool lets N worker diffs be reviewed concurrently
	// instead of serialising on one expert turn.
	ReviewPoolSize int
	// ReviewRetries is the number of re-dispatch attempts the scheduler makes
	// when a reviewer returns request_changes (#85). Each retry continues on
	// the task's existing branch carrying prior commits, and injects the
	// reviewer's notes into the worker prompt. 0 = fail immediately.
	ReviewRetries int

	// AutoExperts arms autonomous on-demand expert spawning (#117): when on,
	// the coordinator watches role-addressed asks (and #80 review requests) and
	// launches a resident `meshd --mode expert --role R` the moment one targets
	// a role no live agent fills, then re-delivers the triggering message once
	// the fresh expert is listening. The asker's sidecar reads the same flag to
	// stop short-circuiting a role-ask that currently has no owner. Off by
	// default — and on the same opt-in posture as PlannerCLI/WorkerCLI: an
	// autostarted coordinator never spawns LLM processes unless armed.
	AutoExperts bool

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

	// ExpertIdleTTL is the idle reaper window for auto-spawned expert agents
	// (#105): an expert that handles no ask or review for this long exits
	// cleanly and deregisters, so the coordinator forgets it and re-spawns on
	// demand. 0 disables the reaper (the expert runs until signalled).
	ExpertIdleTTL time.Duration

	// JobsAddr is the listen address for the HTTP POST /jobs dispatch ingress
	// (#119). Empty (the default) disables the listener entirely — the
	// ingress only starts when explicitly configured, on the same opt-in
	// posture as PlannerCLI/WorkerCLI: an autostarted coordinator must never
	// open extra network ports unless the operator set this.
	JobsAddr string
	// GitHubRepo is the owner/repo string used by `mesh work` to resolve
	// natural-language issue references against GitHub. Empty means `mesh work`
	// is not configured and will return a clear error.
	GitHubRepo string

	// StruggleNudge arms Feature 5: the worker stream observer auto-asks an
	// expert when it sees a repeated test-failure fingerprint or repeated
	// edits to the same path. Off by default (same opt-in posture as AutoExperts).
	StruggleNudge      bool
	StruggleRole       string        // empty → ReviewRole → "architect" at use site
	StruggleTestRepeat int           // same failure fingerprint count; default 3
	StruggleEditRepeat int           // same-path edit count; default 4
	StruggleCooldown   time.Duration // min gap between auto-asks; default 5m
	StruggleMaxAsks    int           // cap per worker run; default 2

	// AnswerCache arms Feature 6 (#29 exact-match answer cache): expert
	// drainInbox reuses a prior successful answer for the same role+q(+ctx)
	// without invoking the LLM. Off by default.
	AnswerCache           bool
	AnswerCacheTTL        time.Duration // entry TTL; default 15m
	AnswerCacheIncludeCtx bool          // include ask ctx in key; default true
}

// Load resolves config from the environment with defaults.
func Load() (Config, error) {
	cfg := Config{
		HeartbeatInterval:     DefaultHeartbeatInterval,
		AwayAfter:             DefaultAwayAfter,
		EvictAfter:            DefaultEvictAfter,
		RegistrationGrace:     DefaultRegistrationGrace,
		DashboardAddr:         DefaultDashboardAddr,
		ObserveAddr:           DefaultObserveAddr,
		ExpertCLI:             DefaultExpertCLI,
		PlannerModel:          DefaultPlannerModel,
		TriageTimeout:         DefaultTriageTimeout,
		TriageMaxAttempts:     DefaultTriageMaxAttempts,
		TriageBackoff:         DefaultTriageBackoff,
		WorkerModel:           DefaultWorkerModel,
		WorkerTimeout:         DefaultWorkerTimeout,
		MaxWorkers:            DefaultMaxWorkers,
		ReviewTimeout:         DefaultReviewTimeout,
		ReviewPoolSize:        DefaultReviewPoolSize,
		ReviewRetries:         DefaultReviewRetries,
		KeepWorktrees:         KeepWorktreesOnFailure,
		AuditFanout:           true,
		ExpertIdleTTL:         DefaultExpertIdleTTL,
		Backoff:               DefaultReDispatchBackoff,
		StruggleTestRepeat:    DefaultStruggleTestRepeat,
		StruggleEditRepeat:    DefaultStruggleEditRepeat,
		StruggleCooldown:      DefaultStruggleCooldown,
		StruggleMaxAsks:       DefaultStruggleMaxAsks,
		AnswerCacheTTL:        DefaultAnswerCacheTTL,
		AnswerCacheIncludeCtx: true,
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
	absDir, err := filepath.Abs(cfg.MeshDir)
	if err != nil {
		return Config{}, fmt.Errorf("config: absolutize %s: %w", EnvMeshDir, err)
	}
	cfg.MeshDir = absDir

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
		{EnvReDispatchBackoff, &cfg.Backoff},
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
	if hosts := os.Getenv(EnvDashboardAllowedHosts); hosts != "" {
		for _, h := range strings.Split(hosts, ",") {
			if h = strings.TrimSpace(h); h != "" {
				cfg.DashboardAllowedHosts = append(cfg.DashboardAllowedHosts, strings.ToLower(h))
			}
		}
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
	if raw := os.Getenv(EnvBudgetUSD); raw != "" {
		budget, err := strconv.ParseFloat(raw, 64)
		if err != nil || budget < 0 || math.IsNaN(budget) || math.IsInf(budget, 0) {
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
	if raw := os.Getenv(EnvAutoExperts); raw != "" {
		switch raw {
		case "on", "true", "1":
			cfg.AutoExperts = true
		case "off", "false", "0":
			cfg.AutoExperts = false
		default:
			return Config{}, fmt.Errorf("config: %s=%q: want on|off", EnvAutoExperts, raw)
		}
	}
	// ExpertIdleTTL: 0 is a valid value (disables the reaper), so this knob
	// uses its own parse path instead of the positive-only duration list above.
	if raw := os.Getenv(EnvExpertIdleTTL); raw != "" {
		dur, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("config: %s=%q: %w", EnvExpertIdleTTL, raw, err)
		}
		if dur < 0 {
			return Config{}, fmt.Errorf("config: %s must be non-negative, got %q", EnvExpertIdleTTL, raw)
		}
		cfg.ExpertIdleTTL = dur // 0 = disabled (reaper never fires)
	}
	cfg.ReviewRole = os.Getenv(EnvReviewRole) // empty = review gating off
	if raw := os.Getenv(EnvReviewPoolSize); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a positive integer", EnvReviewPoolSize, raw)
		}
		cfg.ReviewPoolSize = n
	}
	if raw := os.Getenv(EnvReviewRetries); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a non-negative integer", EnvReviewRetries, raw)
		}
		cfg.ReviewRetries = n
	}
	cfg.JobsAddr = os.Getenv(EnvJobsAddr)     // empty = ingress disabled
	cfg.GitHubRepo = os.Getenv(EnvGitHubRepo) // empty = mesh work disabled
	cfg.ReposDir = os.Getenv(EnvReposDir)     // empty = worker driver refuses to construct
	if cfg.ReposDir != "" {
		absRepos, err := filepath.Abs(cfg.ReposDir)
		if err != nil {
			return Config{}, fmt.Errorf("config: absolutize %s: %w", EnvReposDir, err)
		}
		cfg.ReposDir = absRepos
	}
	if raw := os.Getenv(EnvKeepWorktrees); raw != "" {
		switch raw {
		case KeepWorktreesOnFailure, KeepWorktreesAlways, KeepWorktreesNever:
			cfg.KeepWorktrees = raw
		default:
			return Config{}, fmt.Errorf("config: %s=%q: want %s|%s|%s", EnvKeepWorktrees, raw,
				KeepWorktreesOnFailure, KeepWorktreesAlways, KeepWorktreesNever)
		}
	}

	// Feature 5 — struggle nudge (opt-in).
	if raw := os.Getenv(EnvStruggleNudge); raw != "" {
		switch raw {
		case "on", "true", "1":
			cfg.StruggleNudge = true
		case "off", "false", "0":
			cfg.StruggleNudge = false
		default:
			return Config{}, fmt.Errorf("config: %s=%q: want on|off", EnvStruggleNudge, raw)
		}
	}
	cfg.StruggleRole = os.Getenv(EnvStruggleRole)
	if raw := os.Getenv(EnvStruggleTestRepeat); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a positive integer", EnvStruggleTestRepeat, raw)
		}
		cfg.StruggleTestRepeat = n
	}
	if raw := os.Getenv(EnvStruggleEditRepeat); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a positive integer", EnvStruggleEditRepeat, raw)
		}
		cfg.StruggleEditRepeat = n
	}
	if raw := os.Getenv(EnvStruggleCooldown); raw != "" {
		dur, err := time.ParseDuration(raw)
		if err != nil || dur <= 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a positive duration", EnvStruggleCooldown, raw)
		}
		cfg.StruggleCooldown = dur
	}
	if raw := os.Getenv(EnvStruggleMaxAsks); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a positive integer", EnvStruggleMaxAsks, raw)
		}
		cfg.StruggleMaxAsks = n
	}

	// Feature 6 — exact-match answer cache (opt-in).
	if raw := os.Getenv(EnvAnswerCache); raw != "" {
		switch raw {
		case "on", "true", "1":
			cfg.AnswerCache = true
		case "off", "false", "0":
			cfg.AnswerCache = false
		default:
			return Config{}, fmt.Errorf("config: %s=%q: want on|off", EnvAnswerCache, raw)
		}
	}
	if raw := os.Getenv(EnvAnswerCacheTTL); raw != "" {
		dur, err := time.ParseDuration(raw)
		if err != nil || dur <= 0 {
			return Config{}, fmt.Errorf("config: %s=%q: want a positive duration", EnvAnswerCacheTTL, raw)
		}
		cfg.AnswerCacheTTL = dur
	}
	if raw := os.Getenv(EnvAnswerCacheIncludeCtx); raw != "" {
		switch raw {
		case "on", "true", "1":
			cfg.AnswerCacheIncludeCtx = true
		case "off", "false", "0":
			cfg.AnswerCacheIncludeCtx = false
		default:
			return Config{}, fmt.Errorf("config: %s=%q: want on|off", EnvAnswerCacheIncludeCtx, raw)
		}
	}

	// Claim lease backstop: like the registry record TTL, it must outlast
	// every legitimate silent window (the eviction sweep is the primary
	// release path; the TTL self-heals if the coordinator is down). Derived
	// from EvictAfter unless explicitly set.
	if cfg.ClaimTTL == 0 {
		cfg.ClaimTTL = 2 * (cfg.EvictAfter + cfg.RegistrationGrace)
	}
	if err := Validate(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate enforces the cross-field and range invariants on a fully-resolved
// Config (all durations set, ClaimTTL already derived). Load calls it as its
// last step; the settings overlay (internal/settings) calls it too, so a staged
// value that would push the running system into a state Load itself would have
// rejected at boot is caught before it is applied. Pure — no I/O, no env reads.
func Validate(cfg Config) error {
	for name, d := range map[string]time.Duration{
		EnvHeartbeatInterval: cfg.HeartbeatInterval,
		EnvAwayAfter:         cfg.AwayAfter,
		EnvEvictAfter:        cfg.EvictAfter,
		EnvRegistrationGrace: cfg.RegistrationGrace,
		EnvClaimTTL:          cfg.ClaimTTL,
		EnvTriageTimeout:     cfg.TriageTimeout,
		EnvTriageBackoff:     cfg.TriageBackoff,
		EnvWorkerTimeout:     cfg.WorkerTimeout,
		EnvReviewTimeout:     cfg.ReviewTimeout,
		EnvReDispatchBackoff: cfg.Backoff,
	} {
		if d <= 0 {
			return fmt.Errorf("config: %s must be positive, got %s", name, d)
		}
	}
	if cfg.ExpertIdleTTL < 0 {
		return fmt.Errorf("config: %s must be non-negative, got %s", EnvExpertIdleTTL, cfg.ExpertIdleTTL)
	}
	if cfg.AwayAfter < cfg.HeartbeatInterval {
		return fmt.Errorf("config: away-after (%s) must be >= heartbeat interval (%s)",
			cfg.AwayAfter, cfg.HeartbeatInterval)
	}
	if cfg.EvictAfter <= cfg.AwayAfter {
		return fmt.Errorf("config: evict-after (%s) must be > away-after (%s)",
			cfg.EvictAfter, cfg.AwayAfter)
	}
	if cfg.ClaimTTL <= cfg.HeartbeatInterval {
		return fmt.Errorf("config: claim-ttl (%s) must be > heartbeat interval (%s)",
			cfg.ClaimTTL, cfg.HeartbeatInterval)
	}
	if cfg.BudgetUSD < 0 || math.IsNaN(cfg.BudgetUSD) || math.IsInf(cfg.BudgetUSD, 0) {
		return fmt.Errorf("config: %s must be a non-negative finite USD amount, got %v", EnvBudgetUSD, cfg.BudgetUSD)
	}
	if cfg.MaxWorkers <= 0 {
		return fmt.Errorf("config: %s must be a positive integer, got %d", EnvMaxWorkers, cfg.MaxWorkers)
	}
	if cfg.TriageMaxAttempts <= 0 {
		return fmt.Errorf("config: %s must be a positive integer, got %d", EnvTriageMaxAttempts, cfg.TriageMaxAttempts)
	}
	if cfg.ReviewPoolSize < 1 {
		return fmt.Errorf("config: %s must be >= 1, got %d", EnvReviewPoolSize, cfg.ReviewPoolSize)
	}
	if cfg.ReviewRetries < 0 {
		return fmt.Errorf("config: %s must be non-negative, got %d", EnvReviewRetries, cfg.ReviewRetries)
	}
	if cfg.StruggleTestRepeat <= 0 {
		return fmt.Errorf("config: %s must be a positive integer, got %d", EnvStruggleTestRepeat, cfg.StruggleTestRepeat)
	}
	if cfg.StruggleEditRepeat <= 0 {
		return fmt.Errorf("config: %s must be a positive integer, got %d", EnvStruggleEditRepeat, cfg.StruggleEditRepeat)
	}
	if cfg.StruggleCooldown <= 0 {
		return fmt.Errorf("config: %s must be positive, got %s", EnvStruggleCooldown, cfg.StruggleCooldown)
	}
	if cfg.StruggleMaxAsks <= 0 {
		return fmt.Errorf("config: %s must be a positive integer, got %d", EnvStruggleMaxAsks, cfg.StruggleMaxAsks)
	}
	if cfg.AnswerCacheTTL <= 0 {
		return fmt.Errorf("config: %s must be positive, got %s", EnvAnswerCacheTTL, cfg.AnswerCacheTTL)
	}
	switch cfg.KeepWorktrees {
	case KeepWorktreesOnFailure, KeepWorktreesAlways, KeepWorktreesNever:
	default:
		return fmt.Errorf("config: %s=%q: want %s|%s|%s", EnvKeepWorktrees, cfg.KeepWorktrees,
			KeepWorktreesOnFailure, KeepWorktreesAlways, KeepWorktreesNever)
	}
	return nil
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
