// Command fakeclaude is a test-only stand-in for the `claude` CLI in expert
// e2e runs. It speaks the same resident stream-json contract internal/runtime
// drives (see internal/runtime/proxy_test.go's helper), so the expert loop can
// be exercised across real process boundaries without a real LLM or API key.
//
// It ignores all argv (the proxy passes claude's -p/stream-json flags), emits a
// system/init event carrying a session id, then echoes one success `result`
// event per stdin user message. It exits when stdin closes — the backstop that
// reaps it when its parent meshd goes away.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	session := os.Getenv("FAKECLAUDE_SESSION")
	if session == "" {
		session = "sess-fake-expert"
	}
	for i, a := range os.Args {
		if a == "--resume" && i+1 < len(os.Args) {
			session = os.Args[i+1] // mimic claude: a resumed child reports its id
		}
	}

	out := bufio.NewWriter(os.Stdout)
	emit := func(v any) {
		b, err := json.Marshal(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fakeclaude: marshal: %v\n", err)
			os.Exit(2)
		}
		out.Write(append(b, '\n')) //nolint:errcheck
		out.Flush()                //nolint:errcheck
	}

	emit(map[string]any{
		"type": "system", "subtype": "init", "session_id": session,
		"model": "fake-model", "pid": os.Getpid(),
	})

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 64*1024), 8*1024*1024)
	turn := 0
	for in.Scan() {
		var msg struct {
			Type    string `json:"type"`
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(in.Bytes(), &msg); err != nil ||
			msg.Type != "user" || msg.Message.Role != "user" {
			fmt.Fprintln(os.Stderr, "fakeclaude: bad stdin line")
			os.Exit(2)
		}
		turn++
		emit(map[string]any{
			"type": "result", "subtype": "success", "is_error": false,
			"result":     "expert answer: " + msg.Message.Content,
			"session_id": session, "num_turns": turn, "duration_ms": 1,
		})
	}
}
