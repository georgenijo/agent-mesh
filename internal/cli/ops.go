package cli

// The `mesh ops` actuator subcommands (issue #35): doctor / down / clean.
// Like the bare ops snapshot, they run in-process against MESH_DIR — no
// sidecar required, so they work on a mesh whose daemons are wedged.

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/observe"
	"github.com/georgenijo/agent-mesh/internal/ops"
)

func runOpsDoctor(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ops doctor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	jsonOut := fs.Bool("json", false, "JSON output")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}
	snap, err := observe.Collect(cfg)
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}
	rep := ops.Diagnose(snap)
	if *jsonOut {
		b, err := json.Marshal(rep)
		if err != nil {
			fmt.Fprintln(stderr, "mesh:", err)
			return ExitError
		}
		fmt.Fprintln(stdout, string(b))
	} else {
		fmt.Fprintf(stdout, "mesh doctor (%s): %s\n", rep.Meta.MeshDir, rep.Verdict)
		if len(rep.Findings) > 0 {
			tw := tabwriter.NewWriter(stdout, 2, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "ENTITY\tSTATE\tPID\tDETAIL")
			for _, f := range rep.Findings {
				detail := f.Detail
				if detail == "" {
					detail = "-"
				}
				fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", f.Entity, f.State, f.PID, detail)
			}
			tw.Flush() //nolint:errcheck
		}
		if len(rep.Anomalies) > 0 {
			fmt.Fprintln(stdout, "anomalies:")
			for _, a := range rep.Anomalies {
				fmt.Fprintf(stdout, "  - %s\n", a)
			}
		}
	}
	if rep.Verdict != ops.VerdictClean {
		return ExitDirty
	}
	return ExitOK
}
