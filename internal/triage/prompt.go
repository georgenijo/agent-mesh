package triage

// The planner prompt — centralized here (with the model/CLI knobs in
// Options) so the planner stays replaceable in one place. The prompt pins
// the exact plan document internal/task validates: same version, same field
// names, same role vocabulary. Free text from the job travels as opaque
// content inside the prompt; nothing is ever parsed back out of prose.

import (
	"fmt"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/task"
)

// maxPromptBody bounds how much of the job body is injected into the prompt.
const maxPromptBody = 16 << 10 // 16 KiB

// buildPrompt renders the one-shot planning prompt for a job.
func buildPrompt(rec job.Record, roles []string, notes []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `You are the triage planner for an autonomous coding system. Decompose the job below into a directed acyclic graph (DAG) of concrete subtasks.

Output requirements — follow these exactly:
- Respond with ONLY one JSON object. No prose, no markdown fences, no explanation.
- The object must have this shape:
  {"version":%d,"nodes":[{"id":"<slug>","title":"<short imperative>","description":"<what and why>","role":"<role>","dependsOn":["<id>",...],"files":["<path or area>",...],"acceptance":["<verifiable criterion>",...]}]}
- "version" must be %d.
- 1 to %d nodes. Prefer the smallest decomposition that lets independent work run in parallel.
- "id": a short unique slug per node (letters, digits, - or _; max 64 chars).
- "role": exactly one of: %s.
- "dependsOn": ids of nodes that must complete first. No cycles. Omit or use [] when independent.
- "files": repo-relative paths or areas the node will likely touch (best effort).
- "acceptance": objectively checkable completion criteria for the node.

Job to decompose:
- repo: %s
- title: %s
`, task.PlanVersion, task.PlanVersion, task.MaxPlanNodes, strings.Join(roles, ", "), rec.Repo, rec.Title)

	if body := strings.TrimSpace(rec.Body); body != "" {
		if len(body) > maxPromptBody {
			body = body[:maxPromptBody]
		}
		fmt.Fprintf(&b, "- body:\n%s\n", body)
	}
	if len(notes) > 0 {
		b.WriteString("\nRecent decisions from the repo blackboard (honor them):\n")
		for _, n := range notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
	}
	return b.String()
}
