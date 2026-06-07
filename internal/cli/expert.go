package cli

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/autostart"
	"github.com/georgenijo/agent-mesh/internal/config"
)

// runExpert dispatches the `mesh expert <subcommand>` group. Only `serve`
// exists today.
func runExpert(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "serve" {
		fmt.Fprintln(stderr, `usage: mesh expert serve --name <id> --role <role> [--caps a,b] [--repo R] [--model M]`)
		return ExitUsage
	}
	return runExpertServe(args[1:], stdout, stderr)
}

// runExpertServe runs a resident expert in the FOREGROUND: it execs
// `meshd --mode expert`, which brings up this agent's sidecar (joining the role)
// and a long-lived runtime child that answers role-routed asks automatically.
// The process blocks until Ctrl-C (meshd leaves the mesh on SIGINT/SIGTERM).
//
// Unlike `mesh join`, this does not detach — an expert is a server you watch.
// The --mesh-dir marker is passed so `mesh ops down` can argv-verify and tear
// it down like any other mesh-owned daemon.
func runExpertServe(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("expert serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	name := fs.String("name", "", "expert name (required)")
	role := fs.String("role", "", "role this expert owns (required)")
	caps := fs.String("caps", "", "comma-separated capability tokens")
	repo := fs.String("repo", "", "repository this expert works on")
	model := fs.String("model", "", "model the expert runs")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *name == "" || *role == "" {
		fmt.Fprintln(stderr, "mesh expert serve: --name and --role are required")
		return ExitUsage
	}
	if !agentcard.ValidName(*name) {
		fmt.Fprintf(stderr, "mesh expert serve: invalid name %q (want [A-Za-z0-9_-]{1,64})\n", *name)
		return ExitUsage
	}
	if !agentcard.ValidName(*role) {
		fmt.Fprintf(stderr, "mesh expert serve: invalid role %q (want [A-Za-z0-9_-]{1,64})\n", *role)
		return ExitUsage
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}
	meshd, err := autostart.FindMeshd()
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}

	margs := []string{
		"--mode", "expert",
		"--name", *name,
		"--role", *role,
		"--mesh-dir", cfg.MeshDir, // ops-plane ownership marker (mesh ops down)
	}
	if *caps != "" {
		margs = append(margs, "--caps", *caps)
	}
	if *repo != "" {
		margs = append(margs, "--repo", *repo)
	}
	if *model != "" {
		margs = append(margs, "--model", *model)
	}

	cmd := exec.Command(meshd, margs...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, stdout, stderr
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return ee.ExitCode()
		}
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}
	return ExitOK
}
