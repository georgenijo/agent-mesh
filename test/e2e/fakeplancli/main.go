// Command fakeplancli is a test-only MESH_PLAN_CLI: it ignores argv and prints
// an implementation plan to stdout (the contract the triage plan-step captures).
package main

import (
	"fmt"
	"os"
)

func main() {
	token := os.Getenv("FAKEPLANCLI_TOKEN")
	if token == "" {
		token = "PLANTOKEN"
	}
	fmt.Printf("# Implementation Plan\n\nMARKER %s\n- add Order#market_ids= setter\n- update order_history_view.rb consumer\n", token)
}
