// Command fakeworker is a test-only stand-in for the `claude` CLI in
// scheduler/worker e2e runs. It speaks the one-shot headless contract the
// worker driver drives — `<cli> -p --output-format json <prompt>` emitting
// exactly one result object on stdout (the same M0-verified contract
// test/e2e/fakeplanner speaks) — so the coordinator's scheduler and the #26
// worktree worker driver can be exercised across real process boundaries
// without a real LLM or API key.
//
// It ignores all argv (the driver passes claude's -p/--output-format/--model
// flags plus the task prompt). FAKEWORKER_MODE selects the behavior:
//
//	""     emit a success result with a small total_cost_usd
//	"fail" emit a typed error result (is_error) — never fake-success
//	"mesh" act like a worker using the mesh from inside its run: call
//	       `mesh context`, edit a file in its cwd (the isolated worktree),
//	       `mesh claim` it, then emit a success result whose text is the cwd.
//	       Requires FAKEWORKER_MESH_BIN; the worker driver supplies MESH_DIR
//	       and MESH_SOCKET in the child env. Any failing step emits a typed
//	       error result — never fake-success.
//	"ask"  block on `mesh ask --role expert --wait` until an expert answers,
//	       then emit a success result carrying the answer. A worker blocked
//	       on an ask either resumes with the answer or (wait failure) emits a
//	       typed error result.
//	"context" replay `mesh context` from inside the run and persist what the
//	       worker SAW to worker-saw-context-<pid>.txt in the worktree cwd, so a
//	       test can assert a blackboard plan note reached the worker that
//	       implements the ticket. Any failing step emits a typed error result.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
)

func main() {
	res := map[string]any{
		"type": "result", "subtype": "success", "is_error": false,
		"result":     "did the work",
		"session_id": "sess-fake-worker", "num_turns": 1, "duration_ms": 1,
		"total_cost_usd": 0.001,
	}
	switch os.Getenv("FAKEWORKER_MODE") {
	case "fail":
		res["subtype"] = "error_during_execution"
		res["is_error"] = true
		res["result"] = ""
	case "mesh":
		if err := meshMode(res); err != nil {
			emitError(res, err)
		}
	case "ask":
		if err := askMode(res); err != nil {
			emitError(res, err)
		}
	case "context":
		if err := contextMode(res); err != nil {
			emitError(res, err)
		}
	}
	emit(res)
}

// contextMode proves the worker RECEIVES the repo blackboard from inside its
// run: replay `mesh context`, persist what it saw to worker-saw-context-<pid>.txt
// in the worktree cwd (so the e2e test can assert a plan note landed there
// reached the worker), then succeed. Any failing step emits a typed error
// result — never fake-success.
func contextMode(res map[string]any) error {
	meshBin, err := requireMeshBin()
	if err != nil {
		return err
	}
	out, err := exec.Command(meshBin, "context", "--json").CombinedOutput()
	if err != nil {
		return fmt.Errorf("mesh context: %v: %s", err, out)
	}
	marker := fmt.Sprintf("worker-saw-context-%d.txt", os.Getpid())
	if err := os.WriteFile(marker, out, 0o644); err != nil {
		return fmt.Errorf("write context marker: %w", err)
	}
	res["result"] = string(out)
	return nil
}

// meshMode proves in-run mesh access and worktree containment: replay the
// blackboard, edit a file in the cwd, take the CAS claim on it.
func meshMode(res map[string]any) error {
	meshBin, err := requireMeshBin()
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getwd: %w", err)
	}
	if out, err := exec.Command(meshBin, "context", "--json").CombinedOutput(); err != nil {
		return fmt.Errorf("mesh context: %v: %s", err, out)
	}
	marker := fmt.Sprintf("worker-edit-%d.txt", os.Getpid())
	if err := os.WriteFile(marker, []byte(cwd+"\n"), 0o644); err != nil {
		return fmt.Errorf("write marker: %w", err)
	}
	if out, err := exec.Command(meshBin, "claim", marker, "--json").CombinedOutput(); err != nil {
		return fmt.Errorf("mesh claim %s: %v: %s", marker, err, out)
	}
	res["result"] = cwd
	return nil
}

// askMode blocks on an expert ask — explicit task-local blocking via the
// existing `mesh ask --wait` — and resumes with the answer.
func askMode(res map[string]any) error {
	meshBin, err := requireMeshBin()
	if err != nil {
		return err
	}
	out, err := exec.Command(meshBin, "ask", "--role", "expert",
		"what is the magic word", "--wait", "--json").CombinedOutput()
	if err != nil {
		return fmt.Errorf("mesh ask --wait: %v: %s", err, out)
	}
	res["result"] = string(out)
	return nil
}

func requireMeshBin() (string, error) {
	bin := os.Getenv("FAKEWORKER_MESH_BIN")
	if bin == "" {
		return "", fmt.Errorf("FAKEWORKER_MESH_BIN unset")
	}
	return bin, nil
}

// emitError rewrites res into a typed error result — never fake-success.
func emitError(res map[string]any, err error) {
	res["subtype"] = "error_during_execution"
	res["is_error"] = true
	res["result"] = err.Error()
}

func emit(res map[string]any) {
	out, err := json.Marshal(res)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakeworker: marshal: %v\n", err)
		os.Exit(2)
	}
	os.Stdout.Write(append(out, '\n')) //nolint:errcheck
}
