// Command fakeclaude is a test-only stand-in for the `claude` CLI in expert
// e2e runs. It speaks the same resident stream-json contract internal/runtime
// drives (see internal/runtime/proxy_test.go's helper), so the expert loop can
// be exercised across real process boundaries without a real LLM or API key.
//
// It ignores all argv (the proxy passes claude's -p/stream-json flags), emits a
// system/init event carrying a session id, then echoes one success `result`
// event per stdin user message. It exits when stdin closes — the backstop that
// reaps it when its parent meshd goes away.
//
// Expert-memory hooks (#28), both opt-in via env and inert when unset:
//   - FAKECLAUDE_MSGLOG: append every received user message (JSON object with
//     "turn" and "content") to this file. The e2e test reads it to prove the
//     blackboard memory primer was actually delivered to the child process.
//   - When a received message looks like a memory primer (the runtime injects
//     it as a context-setting turn carrying the blackboard header), the fake
//     "remembers" it and echoes its content back in later answers — modelling a
//     warm expert that rehydrated. It never fabricates memory it was not given.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// primerMarker is the stable header the sidecar's memory primer always carries
// (see internal/sidecar/memory.go renderPrimer). The fake uses it to recognize a
// rehydration turn — it does not parse the primer, only detects and remembers it.
const primerMarker = "Mesh expert memory"

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

	var msgLog *os.File
	if path := os.Getenv("FAKECLAUDE_MSGLOG"); path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fakeclaude: open msglog: %v\n", err)
			os.Exit(2)
		}
		msgLog = f
		defer msgLog.Close()
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
	var memory string // the last primer this child rehydrated from, if any
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
		content := msg.Message.Content
		if msgLog != nil {
			line, _ := json.Marshal(map[string]any{"turn": turn, "content": content})
			msgLog.Write(append(line, '\n')) //nolint:errcheck
		}
		// A primer turn rehydrates the child; remember it and answer succinctly.
		if strings.Contains(content, primerMarker) {
			memory = content
			emit(map[string]any{
				"type": "result", "subtype": "success", "is_error": false,
				"result":     "memory loaded",
				"session_id": session, "num_turns": turn, "duration_ms": 1,
			})
			continue
		}
		// A normal question: a warm expert answers from its rehydrated memory.
		answer := "expert answer: " + content
		if memory != "" {
			answer = "expert answer (from memory): " + content + " :: " + memory
		}
		emit(map[string]any{
			"type": "result", "subtype": "success", "is_error": false,
			"result":     answer,
			"session_id": session, "num_turns": turn, "duration_ms": 1,
		})
	}
}
