package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/autostart"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
	"github.com/georgenijo/agent-mesh/internal/observe"
	"github.com/georgenijo/agent-mesh/internal/socket"
)

func runJoin(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	fs.SetOutput(stderr)
	name := fs.String("name", "", "agent name (required)")
	role := fs.String("role", "", "agent role (required)")
	caps := fs.String("caps", "", "comma-separated capability tokens")
	repo := fs.String("repo", "", "repository this agent works on")
	model := fs.String("model", "", "model the agent runs")
	sock := fs.String("socket", "", "sidecar socket path override")
	noAutostart := fs.Bool("no-autostart", false, "fail instead of spawning a sidecar")
	jsonOut := fs.Bool("json", false, "JSON output")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *name == "" || *role == "" {
		fmt.Fprintln(stderr, "mesh join: --name and --role are required")
		return ExitUsage
	}
	if !agentcard.ValidName(*name) {
		fmt.Fprintf(stderr, "mesh join: invalid name %q (want [A-Za-z0-9_-]{1,64})\n", *name)
		return ExitUsage
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return ExitError
	}
	cwd, _ := os.Getwd() //nolint:errcheck
	card := agentcard.Card{
		ID: *name, Name: *name, Role: *role,
		Caps: splitCaps(*caps), Repo: *repo, CWD: cwd, Model: *model,
	}

	socketPath := *sock
	if socketPath == "" {
		socketPath = cfg.AgentSocket(*name)
	}

	// Autostart the sidecar when nothing is listening yet.
	spawned := false
	if _, err := socket.Do(socketPath, socket.Request{Verb: meshapi.VerbPing}, time.Second); err != nil {
		if *noAutostart {
			fmt.Fprintf(stderr, "mesh: no sidecar at %s and --no-autostart given\n", socketPath)
			return ExitNotJoined
		}
		if err := autostart.SpawnSidecar(cfg, card); err != nil {
			fmt.Fprintln(stderr, "mesh:", err)
			return ExitError
		}
		spawned = true
	}

	resp, code, err := doVerb(socketPath, meshapi.VerbJoin, meshapi.JoinArgs{Card: card})
	// When we spawned the sidecar ourselves, the sidecar's boot-time
	// self-registration means handleJoin always returns rejoined:true.
	// Correct it to rejoined:false — this is genuinely the first join.
	if err == nil && spawned {
		var res meshapi.JoinResult
		if json.Unmarshal(resp.Data, &res) == nil && res.Rejoined {
			res.Rejoined = false
			if b, merr := json.Marshal(res); merr == nil {
				resp.Data = b
			}
		}
	}
	return emit(stdout, stderr, *jsonOut, resp, code, err, func(w io.Writer) {
		var res meshapi.JoinResult
		// "Rejoined" from a sidecar this very command spawned is just the
		// boot registration — report it as a fresh join.
		if !spawned && json.Unmarshal(resp.Data, &res) == nil && res.Rejoined {
			fmt.Fprintf(w, "rejoined mesh as %s (role %s)\n", card.Name, card.Role)
			return
		}
		fmt.Fprintf(w, "joined mesh as %s (role %s)\n", card.Name, card.Role)
		fmt.Fprintf(w, "sidecar socket: %s\n", socketPath)
	})
}

func runLeave(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("leave", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := fs.String("socket", "", "sidecar socket path override")
	reason := fs.String("reason", "", "leave reason")
	jsonOut := fs.Bool("json", false, "JSON output")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	cfg, err := config.Load()
	if err != nil {
		return emitSetupErr(stdout, stderr, *jsonOut, ExitError, err)
	}
	socketPath, code, err := resolveSocket(cfg, *sock)
	if err != nil {
		return emitSetupErr(stdout, stderr, *jsonOut, code, err)
	}
	resp, code, err := doVerb(socketPath, meshapi.VerbLeave, meshapi.LeaveArgs{Reason: *reason})
	return emit(stdout, stderr, *jsonOut, resp, code, err, func(w io.Writer) {
		fmt.Fprintln(w, "left mesh")
	})
}

func runStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := fs.String("socket", "", "sidecar socket path override")
	jsonOut := fs.Bool("json", false, "JSON output")
	positional, err := parseFlagsAnywhere(fs, args)
	if err != nil {
		return ExitUsage
	}
	if len(positional) != 1 || positional[0] == "" {
		fmt.Fprintln(stderr, `usage: mesh status "<text>"`)
		return ExitUsage
	}
	cfg, err := config.Load()
	if err != nil {
		return emitSetupErr(stdout, stderr, *jsonOut, ExitError, err)
	}
	socketPath, code, err := resolveSocket(cfg, *sock)
	if err != nil {
		return emitSetupErr(stdout, stderr, *jsonOut, code, err)
	}
	resp, code, err := doVerb(socketPath, meshapi.VerbStatus, meshapi.StatusArgs{Text: positional[0]})
	return emit(stdout, stderr, *jsonOut, resp, code, err, func(w io.Writer) {
		fmt.Fprintln(w, "ok")
	})
}

func runWho(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("who", flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := fs.String("socket", "", "sidecar socket path override")
	jsonOut := fs.Bool("json", false, "JSON output")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	cfg, err := config.Load()
	if err != nil {
		return emitSetupErr(stdout, stderr, *jsonOut, ExitError, err)
	}
	socketPath, code, err := resolveSocket(cfg, *sock)
	if err != nil {
		return emitSetupErr(stdout, stderr, *jsonOut, code, err)
	}
	resp, code, err := doVerb(socketPath, meshapi.VerbWho, nil)
	return emit(stdout, stderr, *jsonOut, resp, code, err, func(w io.Writer) {
		var res meshapi.WhoResult
		if err := json.Unmarshal(resp.Data, &res); err != nil {
			fmt.Fprintln(stderr, "mesh: bad who response:", err)
			return
		}
		if len(res.Agents) == 0 {
			fmt.Fprintln(w, "no agents on the mesh")
			return
		}
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tROLE\tSTATE\tSTATUS\tCAPS")
		for _, a := range res.Agents {
			status := a.LastStatus
			if status == "" {
				status = "-"
			}
			caps := "-"
			if len(a.Card.Caps) > 0 {
				caps = joinComma(a.Card.Caps)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", a.Card.Name, a.Card.Role, a.State, status, caps)
		}
		tw.Flush() //nolint:errcheck
	})
}

func runOps(args []string, stdout, stderr io.Writer) int {
	// Actuator subcommands (issue #35); bare `mesh ops` stays the snapshot.
	if len(args) > 0 {
		switch args[0] {
		case "doctor":
			return runOpsDoctor(args[1:], stdout, stderr)
		case "down":
			return runOpsDown(args[1:], stdout, stderr)
		case "clean":
			return runOpsClean(args[1:], stdout, stderr)
		}
	}
	fs := flag.NewFlagSet("ops", flag.ContinueOnError)
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
	if *jsonOut {
		b, err := json.Marshal(snap)
		if err != nil {
			fmt.Fprintln(stderr, "mesh:", err)
			return ExitError
		}
		fmt.Fprintln(stdout, string(b))
		return ExitOK
	}
	fmt.Fprintf(stdout, "mesh runtime (%s)\n", snap.Meta.MeshDir)
	c := snap.Coordinator
	fmt.Fprintf(stdout, "coordinator: pid=%d alive=%t bus=%t lock=%t\n",
		c.PID, c.PIDAlive, c.BusDialable, c.LockPresent)
	for _, svc := range snap.Services {
		drift := ""
		if len(svc.Drift) > 0 {
			drift = " drift=" + joinComma(svc.Drift)
		}
		fmt.Fprintf(stdout, "%s: pid=%d alive=%t addr=%s dialable=%t%s\n",
			svc.Name, svc.PID, svc.PIDAlive, svc.Addr, svc.Dialable, drift)
	}
	if len(snap.Sidecars) == 0 {
		fmt.Fprintln(stdout, "sidecars: none")
	} else {
		tw := tabwriter.NewWriter(stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "SIDECAR\tPID\tALIVE\tSOCKET\tSTATE\tDRIFT")
		for _, sc := range snap.Sidecars {
			state := "-"
			if sc.Registry != nil {
				state = string(sc.Registry.State)
			}
			drift := "-"
			if len(sc.Drift) > 0 {
				drift = joinComma(sc.Drift)
			}
			fmt.Fprintf(tw, "%s\t%d\t%t\t%t\t%s\t%s\n",
				sc.Name, sc.PID, sc.PIDAlive, sc.SocketDialable, state, drift)
		}
		tw.Flush() //nolint:errcheck
	}
	if len(snap.Children) > 0 {
		tw := tabwriter.NewWriter(stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "CHILD\tSIDECAR\tPID\tALIVE\tSTATE\tCMD")
		for _, ch := range snap.Children {
			fmt.Fprintf(tw, "%d\t%s\t%d\t%t\t%s\t%s\n",
				ch.PID, ch.Sidecar, ch.PID, ch.Alive, ch.State, ch.Cmd)
		}
		tw.Flush() //nolint:errcheck
	}
	if len(snap.Anomalies) > 0 {
		fmt.Fprintln(stdout, "anomalies:")
		for _, a := range snap.Anomalies {
			fmt.Fprintf(stdout, "  - %s\n", a)
		}
	}
	return ExitOK
}

func joinComma[T ~string](items []T) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ","
		}
		out += string(s)
	}
	return out
}
