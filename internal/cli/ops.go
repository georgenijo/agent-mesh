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
	"time"

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

func runOpsDown(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ops down", flag.ContinueOnError)
	fs.SetOutput(stderr)
	jsonOut := fs.Bool("json", false, "JSON output")
	meshDir := fs.String("mesh", "", "mesh directory to tear down (default $MESH_DIR)")
	timeout := fs.Duration("timeout", 5*time.Second, "SIGTERM grace before SIGKILL escalation")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}
	if *meshDir != "" {
		cfg.MeshDir = *meshDir
	}
	rep, err := ops.Down(cfg, ops.DownOptions{TermTimeout: *timeout})
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}
	if *jsonOut {
		b, err := json.Marshal(rep)
		if err != nil {
			fmt.Fprintln(stderr, "mesh:", err)
			return ExitError
		}
		fmt.Fprintln(stdout, string(b))
	} else {
		verdict := "clean"
		if !rep.Clean {
			verdict = "dirty"
		}
		fmt.Fprintf(stdout, "mesh down (%s): %s\n", rep.Meta.MeshDir, verdict)
		if len(rep.Targets) > 0 {
			tw := tabwriter.NewWriter(stdout, 2, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "PID\tKIND\tNAME\tOUTCOME\tDETAIL")
			for _, t := range rep.Targets {
				detail := t.Detail
				if detail == "" {
					detail = "-"
				}
				fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", t.PID, t.Kind, t.Name, t.Outcome, detail)
			}
			tw.Flush() //nolint:errcheck
		}
	}
	if !rep.Clean {
		return ExitDirty
	}
	return ExitOK
}

func runOpsClean(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ops clean", flag.ContinueOnError)
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
	rep, err := ops.Clean(cfg)
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}
	failed := false
	for _, e := range rep.Entries {
		if e.Action == ops.CleanFailed {
			failed = true
		}
	}
	if *jsonOut {
		b, err := json.Marshal(rep)
		if err != nil {
			fmt.Fprintln(stderr, "mesh:", err)
			return ExitError
		}
		fmt.Fprintln(stdout, string(b))
	} else {
		fmt.Fprintf(stdout, "mesh clean (%s): %d artifacts inspected\n", rep.Meta.MeshDir, len(rep.Entries))
		if len(rep.Entries) > 0 {
			tw := tabwriter.NewWriter(stdout, 2, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "PATH\tKIND\tACTION\tREASON")
			for _, e := range rep.Entries {
				reason := e.Reason
				if reason == "" {
					reason = "-"
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", e.Path, e.Kind, e.Action, reason)
			}
			tw.Flush() //nolint:errcheck
		}
	}
	// Kept entries are correct outcomes, not failures; only unlink errors
	// are operational errors.
	if failed {
		return ExitError
	}
	return ExitOK
}
