// Command mesh is the Agent Mesh CLI — the whole agent-facing API. A thin
// client: open the sidecar's unix socket, send one request, print, exit. It
// holds no state. See ARCHITECTURE.md §4 for the verb surface and exit codes.
package main

import (
	"os"

	"github.com/georgenijo/agent-mesh/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
