// Package worker is the task-scoped worker runtime (#26): the production
// scheduler.Driver. Each dispatched task gets a disposable worker that is
// PHYSICALLY isolated in its own git worktree (locked decision 2026-06-08:
// worktree-per-worker; P1 CAS file-claims stay the cross-worker advisory
// signal — both, not either) and runs one headless `<cli> -p --output-format
// json` child, the same M0-verified contract the triage planner and the
// provisional scheduler.CLIDriver speak.
//
// Lifecycle, behind the frozen #25 Driver seam (the scheduler is untouched —
// swapping this in is coordinator wiring):
//
//   - Spawn: resolve the task's job repo NAME to a checkout at
//     <MESH_REPOS_DIR>/<repo>, create one fresh worktree under
//     $MESH_DIR/workers/<task-id> on its own branch (mesh/worker/<task-id>),
//     and join an embedded per-worker sidecar to the mesh (name w-<id>, the
//     task's role, CWD = the worktree). Nothing here blocks on the work.
//   - Run: drive the worker CLI in the worktree cwd with the task
//     instructions, repo context, a compacted blackboard primer, and the
//     worker role prompt injected. The child inherits MESH_DIR and
//     MESH_SOCKET (its own sidecar), so `mesh claim`, `mesh context`,
//     `mesh note`, and `mesh ask --wait` all work from inside the run — a
//     worker blocked on an expert ask resumes when the answer lands (or its
//     wall-clock timeout maps it to a typed failure). The stdout result is
//     parsed with internal/runtime's never-fake-success discriminators; on a
//     typed success the worker's diff is committed onto the task branch and
//     the changed files + base/head SHAs are reported in Result.Summary.
//   - Teardown (called exactly once, by the scheduler): leave the mesh, then
//     apply the deterministic worktree retention policy (MESH_KEEP_WORKTREES):
//     on-failure (default) removes the worktree only after a typed success —
//     the work product survives as commits on the task branch — and preserves
//     it after anything else for inspection; always / never override. The
//     task BRANCH is never deleted: on success it is the work product, and
//     deleting refs is not a janitor's call.
package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	meshruntime "github.com/georgenijo/agent-mesh/internal/runtime"
	"github.com/georgenijo/agent-mesh/internal/scheduler"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// Session is one worker's live mesh membership: the per-worker sidecar that
// owns the agent socket the child's `mesh` calls land on. It is a seam (the
// same pattern as the expert loop's ExpertFunc) so this package never imports
// internal/sidecar — the sidecar package's own tests import the coordinator,
// which imports this package, and Go forbids that cycle. The production
// implementation (a real internal/sidecar.Sidecar) is wired in cmd/meshd.
type Session interface {
	// BuildPrimer renders the repo's compacted blackboard memory primer
	// (internal/sidecar.BuildMemoryPrimer); empty when there are no notes.
	BuildPrimer(repo string, budget int) (string, error)
	// TrackChild/MarkChildExited surface the worker CLI child to the ops plane.
	TrackChild(cmd string, pid int)
	MarkChildExited(pid int)
	// Leave publishes a graceful departure and stops the sidecar.
	Leave(reason string)
}

// JoinFunc joins one worker agent to the mesh and returns its live session.
type JoinFunc func(card agentcard.Card) (Session, error)

// gitOpTimeout bounds each plumbing git invocation (worktree add/remove,
// commit, rev-parse). Local plumbing is sub-second; minutes means a wedged
// child. Teardown uses it too, so a cancelled scheduler ctx cannot leak
// worktrees.
const gitOpTimeout = 30 * time.Second

// maxWorktreeProbes bounds the search for a free worktree dir name when
// earlier attempts of the same task left preserved worktrees behind.
const maxWorktreeProbes = 32

// Driver implements scheduler.Driver with worktree-per-worker isolation.
type Driver struct {
	cfg  config.Config
	log  *slog.Logger
	jobs job.Store
	join JoinFunc

	// gitMu serializes mutations of a repo's worktree metadata
	// (.git/worktrees, refs): concurrent `git worktree add` calls on one
	// repository race on its lock files.
	gitMu sync.Mutex
}

// NewDriver validates the worker configuration and builds the driver.
// MESH_REPOS_DIR is required: a worker must never guess which directory tree
// it may rewrite, so an unset mapping is a startup error, not a per-task one.
func NewDriver(cli *bus.Client, cfg config.Config, join JoinFunc, log *slog.Logger) (*Driver, error) {
	if cfg.WorkerCLI == "" {
		return nil, errors.New("worker: WorkerCLI is required")
	}
	if cfg.ReposDir == "" {
		return nil, fmt.Errorf("worker: %s is required when %s is set (it maps a job's repo name to a git checkout)",
			config.EnvReposDir, config.EnvWorkerCLI)
	}
	if cli == nil {
		return nil, errors.New("worker: bus client is required")
	}
	if join == nil {
		return nil, errors.New("worker: a JoinFunc is required (wired in cmd/meshd)")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Driver{cfg: cfg, log: log, jobs: job.NewStore(cli), join: join}, nil
}

// Spawn allocates the task's isolated execution context: repo lookup,
// worktree + branch, and the worker's mesh identity. A failure here is the
// scheduler's typed spawn_failed. It never blocks on the work itself.
func (d *Driver) Spawn(ctx context.Context, rec task.Record) (scheduler.Worker, error) {
	jrec, found, err := d.jobs.Get(rec.Job)
	if err != nil {
		return nil, fmt.Errorf("worker: read job %s: %w", rec.Job, err)
	}
	if !found {
		return nil, fmt.Errorf("worker: task %s references unknown job %s", rec.ID, rec.Job)
	}
	repoPath, err := d.resolveRepo(ctx, jrec.Repo)
	if err != nil {
		return nil, err
	}
	dir, branch, err := d.addWorktree(ctx, repoPath, rec.ID)
	if err != nil {
		return nil, err
	}
	baseSHA, err := gitOut(ctx, dir, "rev-parse", "HEAD")
	if err != nil {
		d.removeWorktree(repoPath, dir)
		return nil, fmt.Errorf("worker: read worktree base: %w", err)
	}

	// Join the worker to the mesh through its own embedded sidecar: that
	// sidecar's socket is what makes `mesh` work inside the child (claims are
	// canonicalized against CWD = the worktree, the repo defaults to the
	// job's). It heartbeats for the worker, so a coordinator crash still
	// evicts and reclaims through the normal lease machinery.
	name := workerName(rec.ID)
	card := agentcard.Card{
		ID: name, Name: name, Role: rec.Role,
		Repo: jrec.Repo, CWD: dir, Model: d.cfg.WorkerModel, PID: os.Getpid(),
	}
	sc, err := d.join(card)
	if err != nil {
		d.removeWorktree(repoPath, dir)
		return nil, fmt.Errorf("worker: join worker sidecar: %w", err)
	}

	d.log.Info("worker spawned", "task", rec.ID, "agent", name, "worktree", dir, "branch", branch)
	return &worker{
		d: d, rec: rec, jrec: jrec,
		repoPath: repoPath, dir: dir, branch: branch, baseSHA: baseSHA,
		sc: sc, sockPath: d.cfg.AgentSocket(name),
	}, nil
}

// resolveRepo maps the job's repo name to <ReposDir>/<name> and verifies it
// is a git work tree. Names are plain directory names — anything that could
// escape ReposDir is refused.
func (d *Driver) resolveRepo(ctx context.Context, repo string) (string, error) {
	if repo != filepath.Base(repo) || repo == "." || repo == ".." || repo == "" {
		return "", fmt.Errorf("worker: repo %q is not a plain directory name", repo)
	}
	path := filepath.Join(d.cfg.ReposDir, repo)
	if _, err := gitOut(ctx, path, "rev-parse", "--git-dir"); err != nil {
		return "", fmt.Errorf("worker: repo %q is not a git checkout under %s: %w", repo, d.cfg.ReposDir, err)
	}
	return path, nil
}

// addWorktree creates one fresh worktree + branch for the task. When earlier
// attempts of the same task preserved their worktrees (failure policy,
// coordinator restart), a numbered suffix keeps every attempt distinct
// instead of clobbering evidence.
func (d *Driver) addWorktree(ctx context.Context, repoPath, taskID string) (string, string, error) {
	base := d.cfg.WorkersDir()
	if err := os.MkdirAll(base, 0o700); err != nil {
		return "", "", fmt.Errorf("worker: create workers dir: %w", err)
	}
	d.gitMu.Lock()
	defer d.gitMu.Unlock()
	for n := 1; n <= maxWorktreeProbes; n++ {
		name := taskID
		if n > 1 {
			name = fmt.Sprintf("%s-%d", taskID, n)
		}
		dir := filepath.Join(base, name)
		if _, err := os.Stat(dir); err == nil {
			continue // a previous attempt's preserved worktree
		}
		branch := "mesh/worker/" + name
		if _, err := gitOut(ctx, repoPath, "worktree", "add", "-b", branch, dir); err != nil {
			return "", "", fmt.Errorf("worker: git worktree add: %w", err)
		}
		return dir, branch, nil
	}
	return "", "", fmt.Errorf("worker: no free worktree slot for task %s after %d probes", taskID, maxWorktreeProbes)
}

// removeWorktree unregisters and deletes one worktree (never its branch).
// Best-effort with a forced-removal fallback; called under policy at teardown
// and on spawn-failure cleanup.
func (d *Driver) removeWorktree(repoPath, dir string) {
	d.gitMu.Lock()
	defer d.gitMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), gitOpTimeout)
	defer cancel()
	if _, err := gitOut(ctx, repoPath, "worktree", "remove", "--force", dir); err != nil {
		d.log.Warn("worker: git worktree remove failed; forcing", "dir", dir, "err", err)
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			d.log.Warn("worker: remove worktree dir failed", "dir", dir, "err", rmErr)
		}
		if _, pruneErr := gitOut(ctx, repoPath, "worktree", "prune"); pruneErr != nil {
			d.log.Warn("worker: git worktree prune failed", "repo", repoPath, "err", pruneErr)
		}
	}
}

// worker executes exactly one task in its isolated worktree.
type worker struct {
	d        *Driver
	rec      task.Record
	jrec     job.Record
	repoPath string
	dir      string // the isolated worktree; the child's cwd
	branch   string
	baseSHA  string
	sc       Session
	sockPath string

	// succeeded is written by Run and read by Teardown. The scheduler runs
	// both sequentially on the worker's goroutine, so no lock is needed.
	succeeded bool
}

// Run drives the one-shot worker child and maps its stdout to a typed Result.
// Result mapping is identical to the provisional CLIDriver's documented
// contract (the locked fleet posture hangs off it):
//
//   - success discriminators pass → ok, with the run's total_cost_usd
//   - api_error_status non-null   → rate_limited (back off and retry)
//   - anything else               → worker_failed
//
// billing_error is still deliberately NOT synthesized: the real CLI's
// credit-exhaustion shape is unverified; the enum and the fleet-pause path
// are exercised through the scheduler's fake-driver tests.
func (w *worker) Run(ctx context.Context) (scheduler.Result, error) {
	timeout := w.d.cfg.WorkerTimeout
	if timeout <= 0 {
		timeout = config.DefaultWorkerTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	args := []string{"-p", "--output-format", "json"}
	if w.d.cfg.WorkerModel != "" {
		args = append(args, "--model", w.d.cfg.WorkerModel)
	}
	args = append(args, w.buildPrompt())

	cmd := exec.CommandContext(ctx, w.d.cfg.WorkerCLI, args...)
	cmd.Dir = w.dir
	// The child's `mesh` calls must land on THIS worker's sidecar. os/exec
	// uses the last duplicate, so appending overrides any ambient values.
	cmd.Env = append(os.Environ(),
		config.EnvMeshDir+"="+w.d.cfg.MeshDir,
		config.EnvAgentSocket+"="+w.sockPath,
	)
	var stdout, stderrBuf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderrBuf
	// On kill, a grandchild holding the stdout pipe would keep Wait blocked
	// indefinitely. WaitDelay bounds that wait (same hardening as the triage
	// planner and CLIDriver execs).
	cmd.WaitDelay = 3 * time.Second

	if err := cmd.Start(); err != nil {
		return scheduler.Result{}, fmt.Errorf("worker failed to start: %w", err)
	}
	w.sc.TrackChild(w.d.cfg.WorkerCLI, cmd.Process.Pid)
	err := cmd.Wait()
	w.sc.MarkChildExited(cmd.Process.Pid)
	if err != nil {
		if ctx.Err() != nil {
			return scheduler.Result{}, fmt.Errorf("worker timed out or cancelled: %w", ctx.Err())
		}
		return scheduler.Result{}, fmt.Errorf("worker exited: %w: %s", err, truncate(stderrBuf.String(), 2048))
	}
	out := stdout.Bytes()
	if len(out) > scheduler.MaxWorkerResultBytes {
		return scheduler.Result{}, fmt.Errorf("worker stdout %d bytes exceeds %d", len(out), scheduler.MaxWorkerResultBytes)
	}
	ev, err := meshruntime.ParseEvent(out)
	if err != nil {
		return scheduler.Result{}, err
	}
	if ev.Result == nil {
		return scheduler.Result{}, fmt.Errorf("worker stdout is %q, not a result envelope", ev.Type)
	}

	res := scheduler.Result{CostUSD: ev.Result.TotalCostUSD, SessionID: ev.Result.SessionID}
	switch {
	case ev.Result.Succeeded():
		meta, err := w.commitAndDescribe(ctx)
		if err != nil {
			// The run succeeded but its work product could not be committed or
			// described — reporting ok without the diff would be fake-success
			// on the metadata, and removal-on-success would destroy the only
			// copy. Typed failure; default policy preserves the worktree.
			res.Code = envelope.WorkerFailed
			res.Summary = fmt.Sprintf("worker succeeded but capturing its diff failed: %v", err)
			return res, nil
		}
		w.succeeded = true
		res.Summary = ev.Result.Result + "\n\n" + meta
	case ev.Result.HasAPIError():
		res.Code = envelope.WorkerRateLimited
		res.Summary = fmt.Sprintf("api_error_status %s", ev.Result.APIErrorStatus)
	default:
		res.Code = envelope.WorkerFailed
		res.Summary = fmt.Sprintf("result not a success (subtype=%q is_error=%v)",
			ev.Result.Subtype, ev.Result.IsError)
	}
	return res, nil
}

// commitAndDescribe commits any uncommitted work onto the task branch (the
// backstop for a worker that edited but never committed) and renders the
// run's diff/commit metadata: branch, base/head SHAs, changed files.
func (w *worker) commitAndDescribe(ctx context.Context) (string, error) {
	status, err := gitOut(ctx, w.dir, "status", "--porcelain")
	if err != nil {
		return "", err
	}
	if status != "" {
		if _, err := gitOut(ctx, w.dir, "add", "-A"); err != nil {
			return "", err
		}
		msg := fmt.Sprintf("mesh worker: %s (task %s)", w.rec.Title, w.rec.ID)
		if _, err := gitOut(ctx, w.dir,
			"-c", "user.name=mesh-worker", "-c", "user.email=mesh-worker@localhost",
			"commit", "--no-gpg-sign", "-m", msg); err != nil {
			return "", err
		}
	}
	head, err := gitOut(ctx, w.dir, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "[mesh worker] task=%s branch=%s\nbase=%s head=%s\n", w.rec.ID, w.branch, w.baseSHA, head)
	if head == w.baseSHA {
		b.WriteString("no file changes")
		return b.String(), nil
	}
	changed, err := gitOut(ctx, w.dir, "diff", "--name-only", w.baseSHA+".."+head)
	if err != nil {
		return "", err
	}
	files := strings.Fields(changed)
	fmt.Fprintf(&b, "changed files (%d):", len(files))
	for _, f := range files {
		b.WriteString("\n  " + f)
	}
	return b.String(), nil
}

// buildPrompt renders the worker's full instruction set: role prompt, task
// instructions, repo/worktree context, mesh CLI access, and a compacted
// blackboard primer (the durable per-repo decision history).
func (w *worker) buildPrompt() string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are an autonomous %s worker agent executing one task of a larger job.\n\n", w.rec.Role)
	fmt.Fprintf(&b, "Job: %s\n", w.jrec.Title)
	fmt.Fprintf(&b, "Task: %s\n", w.rec.Title)
	if w.rec.Description != "" {
		fmt.Fprintf(&b, "Description: %s\n", w.rec.Description)
	}
	if len(w.rec.Files) > 0 {
		fmt.Fprintf(&b, "Files in scope: %s\n", strings.Join(w.rec.Files, ", "))
	}
	if len(w.rec.Acceptance) > 0 {
		b.WriteString("Acceptance criteria:\n")
		for _, a := range w.rec.Acceptance {
			fmt.Fprintf(&b, "- %s\n", a)
		}
	}
	fmt.Fprintf(&b, "\nYou are working in an ISOLATED git worktree of repo %q on branch %s.\n", w.jrec.Repo, w.branch)
	b.WriteString("Only edit files inside this worktree. Commit your changes when done; any\n")
	b.WriteString("uncommitted changes are committed onto your branch for you afterwards.\n")
	b.WriteString("\nThe `mesh` CLI is available and you are already joined to the mesh:\n")
	b.WriteString("- `mesh claim <path>` before editing files other workers may share (exit 6 = someone else holds it)\n")
	b.WriteString("- `mesh context` replays this repo's durable decision blackboard\n")
	b.WriteString("- `mesh note \"<decision>\"` records a durable decision for other agents\n")
	b.WriteString("- `mesh ask --role <role> \"<question>\" --wait` asks an expert and blocks until the answer\n")

	if primer, err := w.sc.BuildPrimer(w.jrec.Repo, 0); err != nil {
		w.d.log.Warn("worker: blackboard primer failed; continuing without", "task", w.rec.ID, "err", err)
	} else if primer != "" {
		b.WriteString("\n" + primer + "\n")
	}

	b.WriteString("\nDo the work, then reply with a concise summary of what you did.")
	return b.String()
}

// Teardown leaves the mesh and applies the deterministic worktree retention
// policy. Called exactly once, by the scheduler.
func (w *worker) Teardown() error {
	// Graceful departure: publishes leave, so the coordinator releases the
	// worker's remaining claims immediately instead of waiting out the lease.
	w.sc.Leave("worker teardown")

	keep := false
	switch w.d.cfg.KeepWorktrees {
	case config.KeepWorktreesAlways:
		keep = true
	case config.KeepWorktreesNever:
		keep = false
	default: // config.KeepWorktreesOnFailure (and unset)
		keep = !w.succeeded
	}
	if keep {
		w.d.log.Info("worker: preserving worktree", "task", w.rec.ID, "worktree", w.dir,
			"succeeded", w.succeeded, "policy", w.d.cfg.KeepWorktrees)
		return nil
	}
	w.d.removeWorktree(w.repoPath, w.dir)
	return nil
}

// workerName derives the worker's mesh identity from its task id: short
// (socket paths have a ~104-byte OS limit) but collision-free within a mesh —
// the scheduler never has two workers in flight for one task.
func workerName(taskID string) string {
	compact := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		}
		return -1
	}, taskID)
	if len(compact) > 12 {
		compact = compact[len(compact)-12:]
	}
	if compact == "" {
		compact = "task"
	}
	return "w-" + compact
}

// gitOut runs one git command against dir and returns its trimmed stdout;
// failures carry stderr.
func gitOut(ctx context.Context, dir string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, gitOpTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, truncate(stderr.String(), 512))
	}
	return strings.TrimSpace(stdout.String()), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
