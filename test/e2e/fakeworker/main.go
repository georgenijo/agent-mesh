// Command fakeworker is a test-only stand-in for the `claude` CLI in
// scheduler e2e runs. It speaks the one-shot headless contract the
// provisional scheduler.CLIDriver drives — `<cli> -p --output-format json
// <prompt>` emitting exactly one result object on stdout (the same
// M0-verified contract test/e2e/fakeplanner speaks) — so the coordinator's
// scheduler can be exercised across real process boundaries without a real
// LLM or API key.
//
// It ignores all argv (the driver passes claude's -p/--output-format/--model
// flags plus the task prompt). FAKEWORKER_MODE selects the behavior:
//
//	""     emit a success result with a small total_cost_usd
//	"fail" emit a typed error result (is_error) — never fake-success
package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	res := map[string]any{
		"type": "result", "subtype": "success", "is_error": false,
		"result":     "did the work",
		"session_id": "sess-fake-worker", "num_turns": 1, "duration_ms": 1,
		"total_cost_usd": 0.001,
	}
	if os.Getenv("FAKEWORKER_MODE") == "fail" {
		res["subtype"] = "error_during_execution"
		res["is_error"] = true
		res["result"] = ""
	}
	out, err := json.Marshal(res)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakeworker: marshal: %v\n", err)
		os.Exit(2)
	}
	os.Stdout.Write(append(out, '\n')) //nolint:errcheck
}
