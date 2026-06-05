// Command mesh is the Agent Mesh CLI — the whole agent-facing API. A thin
// client: open the sidecar's unix socket, send one request, print, exit. It
// holds no state. See ARCHITECTURE.md §4 for the verb surface.
package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "0.0.0-dev"

// Exit codes (ARCHITECTURE.md §4): 0 ok, 3 no-answer-yet, 4 no-such-ticket, 5 not-joined.

func main() {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	switch args[0] {
	case "version":
		fmt.Printf("mesh %s\n", version)
	case "join", "leave", "who", "status", "announce", "claim", "release",
		"ask", "poll", "inbox", "answer", "note", "context":
		fmt.Fprintf(os.Stderr, "mesh: %q not implemented yet\n", args[0])
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "mesh: unknown command %q\n", args[0])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mesh <command> [args]")
	fmt.Fprintln(os.Stderr, "commands: join leave who status announce claim release ask poll inbox answer note context version")
}
