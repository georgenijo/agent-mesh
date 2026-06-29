package cli

// escalate verb: `mesh escalate "<question>"` signals that the current worker
// task is too ambiguous to complete correctly without human input. It is only
// meaningful inside a worker-spawned child process that has MESH_ESCALATION_FILE
// set in its environment (the worker runtime sets this before launching the
// child CLI). The verb writes the question to that file and exits 0; the worker
// runtime detects the file after the child exits and reports WorkerEscalated
// to the scheduler, which transitions the task to TaskEscalated (never failed,
// never retried) and records the question for human review.
//
// Usage: mesh escalate "<specific question>"

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/config"
)

func runEscalate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("escalate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	jsonOut := fs.Bool("json", false, "JSON output")
	positional, err := parseFlagsAnywhere(fs, args)
	if err != nil {
		return ExitUsage
	}
	if len(positional) == 0 || strings.TrimSpace(positional[0]) == "" {
		fmt.Fprintln(stderr, `usage: mesh escalate "<specific question>"`)
		fmt.Fprintln(stderr, "  escalate signals that this task is too ambiguous to complete without human input")
		return ExitUsage
	}

	question := strings.TrimSpace(strings.Join(positional, " "))

	escalationFile := os.Getenv(config.EnvEscalationFile)
	if escalationFile == "" {
		err := fmt.Errorf("`mesh escalate` is only valid inside a worker-spawned child (%s is not set)", config.EnvEscalationFile)
		if *jsonOut {
			fmt.Fprintf(stdout, `{"ok":false,"code":"","message":%q}`+"\n", err.Error())
			return ExitError
		}
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}

	if werr := os.WriteFile(escalationFile, []byte(question), 0o600); werr != nil {
		if *jsonOut {
			fmt.Fprintf(stdout, `{"ok":false,"code":"","message":%q}`+"\n", werr.Error())
			return ExitError
		}
		fmt.Fprintln(stderr, "mesh: escalate: write escalation file:", werr)
		return ExitError
	}

	if *jsonOut {
		fmt.Fprintf(stdout, `{"ok":true,"question":%q}`+"\n", question)
		return ExitOK
	}
	fmt.Fprintf(stdout, "escalated: %s\n", question)
	return ExitOK
}
