// Command fakeplanner is a test-only stand-in for the `claude` CLI in triage
// e2e runs. It speaks the one-shot headless contract internal/triage drives —
// `<cli> -p --output-format json <prompt>` emitting exactly one result object
// on stdout (docs/spikes/M0-feasibility.md) — so the coordinator's triage
// loop can be exercised across real process boundaries without a real LLM or
// API key.
//
// It ignores all argv (the triager passes claude's -p/--output-format/--model
// flags plus the prompt). FAKEPLANNER_MODE selects the behavior:
//
//	""        emit a success result whose text is a small valid plan DAG
//	"garbage" emit prose that is not JSON at all (malformed-planner path)
package main

import (
	"encoding/json"
	"fmt"
	"os"
)

const planJSON = `{"version":1,"nodes":[` +
	`{"id":"impl","title":"implement the change","role":"builder",` +
	`"files":["src/x.go"],"acceptance":["unit tests pass"]},` +
	`{"id":"review","title":"review the change","role":"reviewer","dependsOn":["impl"]}]}`

func main() {
	if os.Getenv("FAKEPLANNER_MODE") == "garbage" {
		fmt.Println("Sure! Here is a plan I made up in prose, hope that helps.")
		return
	}
	out, err := json.Marshal(map[string]any{
		"type": "result", "subtype": "success", "is_error": false,
		"result":     planJSON,
		"session_id": "sess-fake-planner", "num_turns": 1, "duration_ms": 1,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakeplanner: marshal: %v\n", err)
		os.Exit(2)
	}
	os.Stdout.Write(append(out, '\n')) //nolint:errcheck
}
