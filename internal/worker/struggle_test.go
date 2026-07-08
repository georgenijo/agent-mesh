package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/task"
)

func TestResolveStruggleRole(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		struggle string
		review   string
		want     string
	}{
		{"struggle wins", "debugger", "reviewer", "debugger"},
		{"review fallback", "", "reviewer", "reviewer"},
		{"default architect", "", "", "architect"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveStruggleRole(config.Config{
				StruggleRole: tc.struggle,
				ReviewRole:   tc.review,
			})
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func assistantEdit(path string) string {
	b, err := json.Marshal(map[string]any{
		"type": "assistant",
		"message": map[string]any{
			"content": []any{
				map[string]any{
					"type":  "tool_use",
					"id":    "edit1",
					"name":  "Edit",
					"input": map[string]any{"file_path": path},
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}
	return string(b) + "\n"
}

func assistantBash(id, cmd string) string {
	b, err := json.Marshal(map[string]any{
		"type": "assistant",
		"message": map[string]any{
			"content": []any{
				map[string]any{
					"type":  "tool_use",
					"id":    id,
					"name":  "Bash",
					"input": map[string]any{"command": cmd},
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}
	return string(b) + "\n"
}

func userToolResult(id, content string) string {
	b, err := json.Marshal(map[string]any{
		"type": "user",
		"message": map[string]any{
			"content": []any{
				map[string]any{
					"type":        "tool_result",
					"tool_use_id": id,
					"content":     content,
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}
	return string(b) + "\n"
}

type askCall struct {
	role, question string
}

func feed(t *testing.T, o *Observer, lines ...string) {
	t.Helper()
	for _, line := range lines {
		if _, err := o.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
}

func waitCalls(t *testing.T, calls *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("want %d calls, got %d", want, calls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitHint(t *testing.T, dir string) ExpertHint {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if h, ok := readExpertHint(dir); ok {
			return h
		}
		if time.Now().After(deadline) {
			t.Fatal("hint file not written")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestObserverEditLoop(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var calls atomic.Int32
	var mu sync.Mutex
	var seen []askCall
	done := make(chan struct{})

	now := time.Unix(1_700_000_000, 0)
	o := NewObserver(context.Background(), ObserverConfig{
		Enabled:    true,
		Role:       "architect",
		EditRepeat: 4,
		TestRepeat: 3,
		Cooldown:   time.Minute,
		MaxAsks:    2,
		WorkDir:    dir,
		Now:        func() time.Time { return now },
		Asker: func(_ context.Context, role, question string) (string, string, string, error) {
			mu.Lock()
			seen = append(seen, askCall{role, question})
			mu.Unlock()
			calls.Add(1)
			close(done)
			return "t-edit", "try a different approach", "expert-1", nil
		},
	})
	defer o.Close()

	path := "internal/foo.go"
	for i := 0; i < 3; i++ {
		feed(t, o, assistantEdit(path))
	}
	if calls.Load() != 0 {
		t.Fatal("asked before edit threshold")
	}
	feed(t, o, assistantEdit(path)) // 4th → fire

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ask")
	}
	hint := waitHint(t, dir)
	if hint.Signal != signalEditLoop {
		t.Fatalf("signal=%q", hint.Signal)
	}
	if hint.Answer != "try a different approach" || hint.AnsweredBy != "expert-1" {
		t.Fatalf("hint=%+v", hint)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0].role != "architect" {
		t.Fatalf("asks=%+v", seen)
	}
}

func TestObserverEditLoopBelowThreshold(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var calls atomic.Int32
	o := NewObserver(context.Background(), ObserverConfig{
		Enabled:    true,
		Role:       "architect",
		EditRepeat: 4,
		TestRepeat: 3,
		Cooldown:   time.Minute,
		MaxAsks:    2,
		WorkDir:    dir,
		Now:        func() time.Time { return time.Unix(1, 0) },
		Asker: func(context.Context, string, string) (string, string, string, error) {
			calls.Add(1)
			return "", "", "", nil
		},
	})
	defer o.Close()
	for i := 0; i < 3; i++ {
		feed(t, o, assistantEdit("a.go"))
	}
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("unexpected asks: %d", calls.Load())
	}
}

func TestObserverTestLoop(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	done := make(chan struct{})
	var calls atomic.Int32

	now := time.Unix(1_700_000_000, 0)
	o := NewObserver(context.Background(), ObserverConfig{
		Enabled:    true,
		Role:       "debugger",
		EditRepeat: 4,
		TestRepeat: 3,
		Cooldown:   time.Minute,
		MaxAsks:    2,
		WorkDir:    dir,
		Now:        func() time.Time { return now },
		Asker: func(_ context.Context, role, _ string) (string, string, string, error) {
			if role != "debugger" {
				t.Errorf("role=%q", role)
			}
			calls.Add(1)
			close(done)
			return "t-test", "fix the assertion", "expert-2", nil
		},
	})
	defer o.Close()

	failOut := "--- FAIL: TestFoo (0.01s)\nFAIL: TestFoo\nexit status 1\n"
	for i := 0; i < 2; i++ {
		id := "bash-" + string(rune('a'+i))
		feed(t, o, assistantBash(id, "go test ./..."), userToolResult(id, failOut))
	}
	if calls.Load() != 0 {
		t.Fatal("asked before test threshold")
	}
	feed(t, o, assistantBash("bash-c", "go test ./internal/foo"), userToolResult("bash-c", failOut))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ask")
	}
	hint := waitHint(t, dir)
	if hint.Signal != signalTestLoop {
		t.Fatalf("signal=%q", hint.Signal)
	}
}

func TestObserverTestSuccessDoesNotCount(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var calls atomic.Int32
	o := NewObserver(context.Background(), ObserverConfig{
		Enabled:    true,
		Role:       "architect",
		EditRepeat: 4,
		TestRepeat: 2,
		Cooldown:   time.Minute,
		MaxAsks:    2,
		WorkDir:    dir,
		Now:        func() time.Time { return time.Unix(1, 0) },
		Asker: func(context.Context, string, string) (string, string, string, error) {
			calls.Add(1)
			return "", "", "", nil
		},
	})
	defer o.Close()
	for i := 0; i < 3; i++ {
		id := "ok-" + string(rune('a'+i))
		feed(t, o, assistantBash(id, "go test ./..."), userToolResult(id, "ok\nPASS\n"))
	}
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("unexpected asks on success: %d", calls.Load())
	}
}

func TestObserverCooldownAndMaxAsks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var calls atomic.Int32
	var mu sync.Mutex
	clock := time.Unix(1_700_000_000, 0)

	o := NewObserver(context.Background(), ObserverConfig{
		Enabled:    true,
		Role:       "architect",
		EditRepeat: 2,
		TestRepeat: 99,
		Cooldown:   5 * time.Minute,
		MaxAsks:    2,
		WorkDir:    dir,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return clock
		},
		Asker: func(context.Context, string, string) (string, string, string, error) {
			calls.Add(1)
			return "t", "ans", "e", nil
		},
	})
	defer o.Close()

	feed(t, o, assistantEdit("a.go"), assistantEdit("a.go"))
	waitCalls(t, &calls, 1)

	feed(t, o, assistantEdit("a.go"), assistantEdit("a.go"))
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("cooldown broken: calls=%d", calls.Load())
	}

	mu.Lock()
	clock = clock.Add(6 * time.Minute)
	mu.Unlock()
	feed(t, o, assistantEdit("a.go"), assistantEdit("a.go"))
	waitCalls(t, &calls, 2)

	mu.Lock()
	clock = clock.Add(6 * time.Minute)
	mu.Unlock()
	feed(t, o, assistantEdit("a.go"), assistantEdit("a.go"))
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 2 {
		t.Fatalf("max asks broken: calls=%d", calls.Load())
	}
}

func TestObserverDisabled(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var calls atomic.Int32
	o := NewObserver(context.Background(), ObserverConfig{
		Enabled:    false,
		Role:       "architect",
		EditRepeat: 1,
		TestRepeat: 1,
		Cooldown:   time.Second,
		MaxAsks:    5,
		WorkDir:    dir,
		Asker: func(context.Context, string, string) (string, string, string, error) {
			calls.Add(1)
			return "", "", "", nil
		},
	})
	defer o.Close()
	feed(t, o, assistantEdit("x.go"), assistantEdit("x.go"))
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("disabled observer fired")
	}
}

func TestFailureFingerprintStable(t *testing.T) {
	t.Parallel()
	a := failureFingerprint("--- FAIL: TestX\nFAIL: TestX\n")
	c := failureFingerprint("--- FAIL: TestX\nFAIL: TestX\n")
	if a == "" || a != c {
		t.Fatalf("unstable: %q vs %q", a, c)
	}
	if failureFingerprint("ok\nPASS\n") != "" {
		t.Fatal("success should not fingerprint")
	}
}

func TestReadExpertHint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if _, ok := readExpertHint(dir); ok {
		t.Fatal("expected missing")
	}
	path := filepath.Join(dir, expertHintFileName)
	hint := ExpertHint{Ticket: "t1", Signal: signalEditLoop, Answer: "do X", AnsweredBy: "e"}
	data, err := json.Marshal(hint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	got, ok := readExpertHint(dir)
	if !ok || got.Answer != "do X" {
		t.Fatalf("got=%+v ok=%v", got, ok)
	}
}

func TestBuildPromptIncludesStruggleClause(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	hint := ExpertHint{Ticket: "t", Signal: signalTestLoop, Answer: "check imports", AnsweredBy: "arch"}
	data, err := json.Marshal(hint)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, expertHintFileName), data, 0o644); err != nil {
		t.Fatal(err)
	}
	w := &worker{
		d:      &Driver{cfg: config.Config{StruggleNudge: true}, log: nil},
		dir:    dir,
		rec:    task.Record{Role: "builder", Title: "t"},
		jrec:   job.Record{Title: "j", Repo: "r"},
		branch: "mesh/worker/t",
		sc:     stubSession{},
	}
	prompt := w.buildPrompt()
	for _, want := range []string{".mesh_expert_hint", "check imports", "Expert guidance"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

type stubSession struct{}

func (stubSession) BuildPrimer(string, int) (string, error) { return "", nil }
func (stubSession) TrackChild(string, int)                  {}
func (stubSession) MarkChildExited(int)                     {}
func (stubSession) Leave(string)                            {}
