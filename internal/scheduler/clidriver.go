package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	meshruntime "github.com/georgenijo/agent-mesh/internal/runtime"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// CLIDriver is the PROVISIONAL exec-based worker driver: one one-shot
// `<cli> -p --output-format json` child per task — the same M0-verified
// contract the triage planner uses — parsed with internal/runtime's
// never-fake-success result discriminators. It existed so the scheduler spine
// was real end-to-end before #26 landed. The coordinator now wires
// internal/worker.Driver (worktree-per-worker isolation, embedded per-worker
// sidecar, diff collection) behind the same Driver seam; CLIDriver remains as
// the minimal reference implementation of the one-shot exec contract and the
// scheduler package's own contract-test subject.
//
// Result mapping (the locked fleet posture hangs off these):
//   - success discriminators pass        → ok, with the run's total_cost_usd
//   - api_error_status non-null          → rate_limited (the spike's
//     transport/rate-limit signal; the scheduler backs off and retries)
//   - anything else                      → worker_failed
//
// billing_error is deliberately NOT synthesized here: the real CLI's
// credit-exhaustion shape is unverified until #26 exercises it. The enum,
// the fleet-pause path, and its tests run against the fake driver.
type CLIDriver struct {
	CLI     string        // required: worker binary (claude | fake)
	Model   string        // optional --model (locked decision: pin it in production)
	Timeout time.Duration // wall-clock bound per run (default 10m)
	WorkDir string        // working dir for the child
}

// MaxWorkerResultBytes bounds the worker stdout we are willing to parse.
const MaxWorkerResultBytes = 1 << 20 // 1 MiB

// Spawn validates the driver and hands back the one-shot worker. The child
// process starts inside Run (one-shot exec has nothing to allocate earlier).
func (d CLIDriver) Spawn(_ context.Context, rec task.Record) (Worker, error) {
	if d.CLI == "" {
		return nil, errors.New("scheduler: CLIDriver.CLI is required")
	}
	return &cliWorker{d: d, rec: rec}, nil
}

type cliWorker struct {
	d   CLIDriver
	rec task.Record
}

// Run executes the one-shot worker child and maps its stdout to a typed
// Result. ctx cancellation (scheduler stop) kills the child.
func (w *cliWorker) Run(ctx context.Context) (Result, error) {
	timeout := w.d.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	args := []string{"-p", "--output-format", "json"}
	if w.d.Model != "" {
		args = append(args, "--model", w.d.Model)
	}
	args = append(args, workerPrompt(w.rec))
	cmd := exec.CommandContext(ctx, w.d.CLI, args...)
	if w.d.WorkDir != "" {
		cmd.Dir = w.d.WorkDir
	}
	// On kill, a grandchild holding the stdout pipe would keep Output()
	// blocked indefinitely. WaitDelay bounds that wait (same hardening as the
	// triage planner exec).
	cmd.WaitDelay = 3 * time.Second
	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return Result{}, fmt.Errorf("worker timed out or cancelled: %w", ctx.Err())
		}
		if ee, ok := err.(*exec.ExitError); ok && len(ee.Stderr) > 0 {
			return Result{}, fmt.Errorf("worker exited: %w: %s", err, ee.Stderr)
		}
		return Result{}, fmt.Errorf("worker failed to run: %w", err)
	}
	if len(out) > MaxWorkerResultBytes {
		return Result{}, fmt.Errorf("worker stdout %d bytes exceeds %d", len(out), MaxWorkerResultBytes)
	}
	ev, err := meshruntime.ParseEvent(out)
	if err != nil {
		return Result{}, err
	}
	if ev.Result == nil {
		return Result{}, fmt.Errorf("worker stdout is %q, not a result envelope", ev.Type)
	}
	res := Result{CostUSD: ev.Result.TotalCostUSD, SessionID: ev.Result.SessionID, Model: w.d.Model}
	switch {
	case ev.Result.Succeeded():
		res.Summary = ev.Result.Result
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

// Teardown is a no-op for the one-shot driver: the child is reaped by Run
// (or killed by ctx). Worktree/session teardown arrives with #26's driver.
func (w *cliWorker) Teardown() error { return nil }

// workerPrompt renders one task into the worker's prompt. Deliberately
// minimal — #26 owns the real prompt and execution context.
func workerPrompt(rec task.Record) string {
	var b strings.Builder
	b.WriteString("You are an autonomous worker agent executing one task of a larger job.\n\n")
	fmt.Fprintf(&b, "Task: %s\n", rec.Title)
	if rec.Description != "" {
		fmt.Fprintf(&b, "Description: %s\n", rec.Description)
	}
	if len(rec.Files) > 0 {
		fmt.Fprintf(&b, "Files in scope: %s\n", strings.Join(rec.Files, ", "))
	}
	if len(rec.Acceptance) > 0 {
		b.WriteString("Acceptance criteria:\n")
		for _, a := range rec.Acceptance {
			fmt.Fprintf(&b, "- %s\n", a)
		}
	}
	b.WriteString("\nDo the work, then reply with a concise summary of what you did.")
	return b.String()
}
