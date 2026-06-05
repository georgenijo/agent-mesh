// Command meshd is the Agent Mesh daemon. One binary, several modes selected by
// --mode: sidecar (one per agent), coordinator (one per mesh), or dashboard.
// See docs/repo-layout.md and docs/components.md.
package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "0.0.0-dev"

func main() {
	mode := flag.String("mode", "", "daemon mode: sidecar | coordinator | dashboard")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("meshd %s\n", version)
		return
	}

	switch *mode {
	case "sidecar":
		fmt.Println("meshd: sidecar mode not implemented yet")
	case "coordinator":
		fmt.Println("meshd: coordinator mode not implemented yet")
	case "dashboard":
		fmt.Println("meshd: dashboard mode not implemented yet")
	case "":
		fmt.Fprintln(os.Stderr, "meshd: --mode is required (sidecar|coordinator|dashboard)")
		os.Exit(2)
	default:
		fmt.Fprintf(os.Stderr, "meshd: unknown mode %q\n", *mode)
		os.Exit(2)
	}
}
