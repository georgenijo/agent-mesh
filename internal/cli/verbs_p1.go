package cli

// P1 verbs: claim/release (CAS file-claims), announce (advisory broadcast),
// note/context (durable blackboard). Same thin-client discipline as P0: one
// socket request, one printed reply, a meaningful exit code. The only new
// exit semantics: a lost claim is 6 — a legitimate race outcome scripts and
// hooks branch on, never conflated with error (1).

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
)

// verbSetup parses common flags + positionals and resolves the sidecar
// socket: the shared preamble of every P1 verb.
type verbSetup struct {
	socketPath string
	jsonOut    bool
	positional []string
}

func setupVerb(name string, args []string, stderr io.Writer, extra func(fs *flag.FlagSet)) (verbSetup, int, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	sock := fs.String("socket", "", "sidecar socket path override")
	jsonOut := fs.Bool("json", false, "JSON output")
	if extra != nil {
		extra(fs)
	}
	positional, err := parseFlagsAnywhere(fs, args)
	if err != nil {
		return verbSetup{jsonOut: *jsonOut}, ExitUsage, err
	}
	cfg, err := config.Load()
	if err != nil {
		return verbSetup{jsonOut: *jsonOut}, ExitError, err
	}
	socketPath, code, err := resolveSocket(cfg, *sock)
	if err != nil {
		return verbSetup{jsonOut: *jsonOut}, code, err
	}
	return verbSetup{socketPath: socketPath, jsonOut: *jsonOut, positional: positional}, ExitOK, nil
}

func runClaim(args []string, stdout, stderr io.Writer) int {
	var repo string
	vs, code, err := setupVerb("claim", args, stderr, func(fs *flag.FlagSet) {
		fs.StringVar(&repo, "repo", "", "repo id (default: agent card repo)")
	})
	if err != nil {
		return emitSetupErr(stdout, stderr, vs.jsonOut, code, err)
	}
	if len(vs.positional) != 1 || vs.positional[0] == "" {
		fmt.Fprintln(stderr, `usage: mesh claim <path> [--repo R]`)
		return ExitUsage
	}

	resp, code, err := doVerb(vs.socketPath, meshapi.VerbClaim, meshapi.ClaimArgs{Path: vs.positional[0], Repo: repo})
	if err != nil {
		return emit(stdout, stderr, vs.jsonOut, resp, code, err, nil)
	}
	var res meshapi.ClaimVerbResult
	if jerr := json.Unmarshal(resp.Data, &res); jerr != nil {
		fmt.Fprintln(stderr, "mesh: bad claim response:", jerr)
		return ExitError
	}
	// claimed → 0; lost → 6. Both are successful protocol exchanges — the
	// exit code carries the race outcome.
	exit := ExitOK
	if res.Result == envelope.ClaimLost {
		exit = ExitClaimLost
	}
	if vs.jsonOut {
		fmt.Fprintln(stdout, string(resp.Data))
		return exit
	}
	switch res.Result {
	case envelope.ClaimClaimed:
		fmt.Fprintf(stdout, "claimed %s (repo %s)\n", res.Path, res.Repo)
	case envelope.ClaimLost:
		fmt.Fprintf(stderr, "mesh: %s is claimed by %s since %s\n", res.Path, res.Owner, res.Since.Format("15:04:05"))
	}
	return exit
}

func runRelease(args []string, stdout, stderr io.Writer) int {
	var repo string
	vs, code, err := setupVerb("release", args, stderr, func(fs *flag.FlagSet) {
		fs.StringVar(&repo, "repo", "", "repo id (default: agent card repo)")
	})
	if err != nil {
		return emitSetupErr(stdout, stderr, vs.jsonOut, code, err)
	}
	if len(vs.positional) != 1 || vs.positional[0] == "" {
		fmt.Fprintln(stderr, `usage: mesh release <path> [--repo R]`)
		return ExitUsage
	}

	resp, code, err := doVerb(vs.socketPath, meshapi.VerbRelease, meshapi.ReleaseArgs{Path: vs.positional[0], Repo: repo})
	if err != nil {
		return emit(stdout, stderr, vs.jsonOut, resp, code, err, nil)
	}
	var res meshapi.ReleaseVerbResult
	if jerr := json.Unmarshal(resp.Data, &res); jerr != nil {
		fmt.Fprintln(stderr, "mesh: bad release response:", jerr)
		return ExitError
	}
	exit := ExitOK
	if res.Result == meshapi.ReleaseNotOwner {
		exit = ExitError
	}
	if vs.jsonOut {
		fmt.Fprintln(stdout, string(resp.Data))
		return exit
	}
	switch res.Result {
	case meshapi.ReleaseReleased:
		fmt.Fprintf(stdout, "released %s (repo %s)\n", res.Path, res.Repo)
	case meshapi.ReleaseNotOwner:
		fmt.Fprintf(stderr, "mesh: cannot release %s — held by %s\n", res.Path, res.Owner)
	}
	return exit
}

func runAnnounce(args []string, stdout, stderr io.Writer) int {
	var repo, paths string
	vs, code, err := setupVerb("announce", args, stderr, func(fs *flag.FlagSet) {
		fs.StringVar(&repo, "repo", "", "repo id (default: agent card repo)")
		fs.StringVar(&paths, "paths", "", "comma-separated paths this intent touches")
	})
	if err != nil {
		return emitSetupErr(stdout, stderr, vs.jsonOut, code, err)
	}
	if len(vs.positional) != 1 || vs.positional[0] == "" {
		fmt.Fprintln(stderr, `usage: mesh announce "<intent>" [--paths a,b] [--repo R]`)
		return ExitUsage
	}

	resp, code, err := doVerb(vs.socketPath, meshapi.VerbAnnounce,
		meshapi.AnnounceArgs{Intent: vs.positional[0], Paths: splitCaps(paths), Repo: repo})
	return emit(stdout, stderr, vs.jsonOut, resp, code, err, func(w io.Writer) {
		var res meshapi.AnnounceResult
		if json.Unmarshal(resp.Data, &res) == nil {
			fmt.Fprintf(w, "announced to %s\n", res.Repo)
			return
		}
		fmt.Fprintln(w, "announced")
	})
}

func runNote(args []string, stdout, stderr io.Writer) int {
	var repo, kind, ticket string
	vs, code, err := setupVerb("note", args, stderr, func(fs *flag.FlagSet) {
		fs.StringVar(&repo, "repo", "", "repo id (default: agent card repo)")
		fs.StringVar(&kind, "kind", "", "note kind: decision|context|summary|other (default decision)")
		fs.StringVar(&ticket, "ticket", "", "related ticket id")
	})
	if err != nil {
		return emitSetupErr(stdout, stderr, vs.jsonOut, code, err)
	}
	if len(vs.positional) != 1 || vs.positional[0] == "" {
		fmt.Fprintln(stderr, `usage: mesh note "<text>" [--repo R] [--kind K] [--ticket T]`)
		return ExitUsage
	}

	resp, code, err := doVerb(vs.socketPath, meshapi.VerbNote,
		meshapi.NoteArgs{Text: vs.positional[0], Repo: repo, Kind: kind, Ticket: ticket})
	return emit(stdout, stderr, vs.jsonOut, resp, code, err, func(w io.Writer) {
		var res meshapi.NoteResult
		if json.Unmarshal(resp.Data, &res) == nil {
			fmt.Fprintf(w, "noted (seq %d, repo %s)\n", res.Seq, res.Repo)
			return
		}
		fmt.Fprintln(w, "noted")
	})
}

func runContext(args []string, stdout, stderr io.Writer) int {
	var repo string
	vs, code, err := setupVerb("context", args, stderr, func(fs *flag.FlagSet) {
		fs.StringVar(&repo, "repo", "", "repo id (default: agent card repo)")
	})
	if err != nil {
		return emitSetupErr(stdout, stderr, vs.jsonOut, code, err)
	}
	if len(vs.positional) != 0 {
		fmt.Fprintln(stderr, `usage: mesh context [--repo R]`)
		return ExitUsage
	}

	resp, code, err := doVerb(vs.socketPath, meshapi.VerbContext, meshapi.ContextArgs{Repo: repo})
	return emit(stdout, stderr, vs.jsonOut, resp, code, err, func(w io.Writer) {
		var res meshapi.ContextResult
		if err := json.Unmarshal(resp.Data, &res); err != nil {
			fmt.Fprintln(stderr, "mesh: bad context response:", err)
			return
		}
		if len(res.Notes) == 0 {
			fmt.Fprintf(w, "no notes for repo %s\n", res.Repo)
			return
		}
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "SEQ\tTIME\tAUTHOR\tKIND\tNOTE")
		for _, n := range res.Notes {
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n",
				n.Seq, n.TS.Local().Format("01-02 15:04"), n.Author, n.Kind, n.Text)
		}
		tw.Flush() //nolint:errcheck
	})
}
