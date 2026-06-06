// Package cli implements the `mesh` verbs. The CLI stays deliberately thin:
// resolve the sidecar socket, send one typed request, print one reply, exit
// with a meaningful code. No state lives here (ARCHITECTURE §4).
package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/socket"
)

// Exit codes (ARCHITECTURE §4). 3 and 4 are reserved for the P2 ask/poll
// verbs; P0 uses 0, 1, 2, 5; P1 adds 6 (claim lost — a legitimate race
// outcome, distinct from error so scripts and hooks can branch on it).
const (
	ExitOK        = 0
	ExitError     = 1
	ExitUsage     = 2
	ExitNoAnswer  = 3 // reserved: poll, no answer yet
	ExitNoTicket  = 4 // reserved: no such ticket
	ExitNotJoined = 5
	ExitClaimLost = 6 // claim: another agent holds the path
)

const requestTimeout = 10 * time.Second

// Run executes one mesh verb and returns the process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return ExitUsage
	}
	verb, rest := args[0], args[1:]
	switch verb {
	case "join":
		return runJoin(rest, stdout, stderr)
	case "leave":
		return runLeave(rest, stdout, stderr)
	case "who":
		return runWho(rest, stdout, stderr)
	case "status":
		return runStatus(rest, stdout, stderr)
	case "claim":
		return runClaim(rest, stdout, stderr)
	case "release":
		return runRelease(rest, stdout, stderr)
	case "announce":
		return runAnnounce(rest, stdout, stderr)
	case "note":
		return runNote(rest, stdout, stderr)
	case "context":
		return runContext(rest, stdout, stderr)
	case "version":
		fmt.Fprintln(stdout, "mesh", Version)
		return ExitOK
	case "help", "-h", "--help":
		usage(stdout)
		return ExitOK
	default:
		fmt.Fprintf(stderr, "mesh: unknown command %q\n", verb)
		usage(stderr)
		return ExitUsage
	}
}

// Version is stamped by the build (Makefile -ldflags); dev fallback otherwise.
var Version = "0.1.0-dev"

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: mesh <command> [flags]

commands:
  join    --name <id> --role <role> [--caps a,b,c] [--repo R] [--model M]
          register this agent on the mesh (autostarts its sidecar)
  leave   deregister and stop this agent's sidecar
  who     show the roster (presence + latest status)
  status  "<text>"   post what this agent is doing now

  claim    <path> [--repo R]   take the CAS lock on a path (exit 6 if lost)
  release  <path> [--repo R]   release a claim this agent holds
  announce "<intent>" [--paths a,b] [--repo R]   broadcast advisory intent
  note     "<text>" [--repo R] [--kind K] [--ticket T]   append to blackboard
  context  [--repo R]          replay the repo's blackboard history

common flags:
  --json            machine-readable output
  --socket <path>   sidecar socket (default: $MESH_SOCKET, else the single
                    socket under $MESH_DIR/agents)

exit codes: 0 ok · 1 error · 2 usage · 3 no-answer-yet · 4 no-such-ticket ·
            5 not-joined · 6 claim-lost
`)
}

// resolveSocket picks the sidecar socket: explicit flag → $MESH_SOCKET → the
// single socket in $MESH_DIR/agents. Zero sockets is a not-joined condition;
// several without an explicit choice is ambiguous (usage error).
func resolveSocket(cfg config.Config, explicit string) (string, int, error) {
	if explicit != "" {
		return explicit, ExitOK, nil
	}
	if env := os.Getenv(config.EnvAgentSocket); env != "" {
		return env, ExitOK, nil
	}
	matches, err := filepath.Glob(filepath.Join(cfg.AgentsDir(), "*.sock"))
	if err != nil {
		return "", ExitError, err
	}
	switch len(matches) {
	case 0:
		return "", ExitNotJoined, fmt.Errorf("not joined: no sidecar socket under %s (run `mesh join` first)", cfg.AgentsDir())
	case 1:
		return matches[0], ExitOK, nil
	default:
		return "", ExitUsage, fmt.Errorf("%d agents on this mesh: pick one with --socket or %s=%s",
			len(matches), config.EnvAgentSocket, cfg.AgentSocket("<name>"))
	}
}

// doVerb performs one request against the sidecar and maps every failure to
// a typed exit code.
func doVerb(socketPath, verb string, args any) (socket.Response, int, error) {
	var raw json.RawMessage
	if args != nil {
		b, err := json.Marshal(args)
		if err != nil {
			return socket.Response{}, ExitError, err
		}
		raw = b
	}
	resp, err := socket.Do(socketPath, socket.Request{Verb: verb, Args: raw}, requestTimeout)
	if err != nil {
		if errors.Is(err, socket.ErrNoSocket) {
			return socket.Response{}, ExitNotJoined, fmt.Errorf("not joined: no sidecar at %s", socketPath)
		}
		return socket.Response{}, ExitError, err
	}
	if !resp.OK {
		return resp, exitForCode(resp.Code), fmt.Errorf("%s", failMessage(resp))
	}
	return resp, ExitOK, nil
}

func exitForCode(code string) int {
	switch code {
	case socket.CodeNotJoined:
		return ExitNotJoined
	case socket.CodeBadRequest:
		return ExitUsage
	default:
		// Includes CodeUnavailable (sidecar up, bus/coordinator down):
		// deliberately generic exit 1 — the documented contract reserves
		// only 0/2/3/4/5. Scripts needing the distinction use --json,
		// which carries the typed code.
		return ExitError
	}
}

func failMessage(resp socket.Response) string {
	if resp.Message != "" {
		return resp.Message
	}
	return "request failed: " + resp.Code
}

// emit prints either the machine contract (raw JSON data / typed error
// object) or the human line(s).
func emit(stdout, stderr io.Writer, jsonOut bool, resp socket.Response, code int, err error, human func(io.Writer)) int {
	if jsonOut {
		if err != nil {
			obj := map[string]any{"ok": false, "code": resp.Code, "message": err.Error()}
			b, _ := json.Marshal(obj) //nolint:errcheck
			fmt.Fprintln(stdout, string(b))
			return code
		}
		fmt.Fprintln(stdout, string(resp.Data))
		return code
	}
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return code
	}
	human(stdout)
	return code
}

// parseFlagsAnywhere parses fs while allowing positional arguments to appear
// before, between, or after flags (Go's flag package stops at the first
// positional). Returns the positional arguments in order.
func parseFlagsAnywhere(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
}

func splitCaps(csv string) []string {
	if strings.TrimSpace(csv) == "" {
		return nil
	}
	var out []string
	for _, c := range strings.Split(csv, ",") {
		if c = strings.TrimSpace(c); c != "" {
			out = append(out, c)
		}
	}
	return out
}
