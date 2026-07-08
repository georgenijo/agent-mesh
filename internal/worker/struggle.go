package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
)

// Feature 5 — struggle → ask-the-expert nudge.
//
// Observer is an io.Writer that consumes Claude stream-json NDJSON lines
// (assistant tool_use + user tool_result) and, when StruggleNudge is armed,
// auto-asks an expert on edit/test loops. Detection never scrapes result
// prose: only structured tool_use inputs and tool_result content fingerprints.
// Asks run in goroutines so the worker Run path never blocks on an expert.

const (
	expertHintFileName = ".mesh_expert_hint"
	envExpertHintFile  = "MESH_EXPERT_HINT_FILE"

	signalEditLoop = "edit_loop"
	signalTestLoop = "test_loop"

	defaultStruggleRole = "architect"
	struggleAskPollGap  = 500 * time.Millisecond
	struggleAskTimeout  = 10 * time.Minute
)

// testCmdRe matches common test invocations inside a Bash tool_use command.
var testCmdRe = regexp.MustCompile(`(?i)\b(?:go test|make test|npm test|pytest|cargo test)\b`)

// failureLineRe extracts stable failure lines from tool_result content for
// fingerprinting (never the whole transcript).
var failureLineRe = regexp.MustCompile(`(?i)(?:FAIL:|Error:|\bfailed\b).+`)

// ExpertHint is the JSON written to <worktree>/.mesh_expert_hint when an
// auto-ask is answered. Hooks (and the worker prompt) read it mid-run.
type ExpertHint struct {
	Ticket     string `json:"ticket"`
	Signal     string `json:"signal"`
	Answer     string `json:"answer"`
	AnsweredBy string `json:"answeredBy"`
}

// StruggleAsker runs one async ask+poll. Tests inject a fake; production uses
// the mesh CLI over the worker's sidecar socket.
type StruggleAsker func(ctx context.Context, role, question string) (ticket, answer, answeredBy string, err error)

// ObserverConfig configures one per-Run struggle observer.
type ObserverConfig struct {
	Enabled    bool
	Role       string // already resolved (StruggleRole → ReviewRole → architect)
	TestRepeat int
	EditRepeat int
	Cooldown   time.Duration
	MaxAsks    int

	WorkDir string // worktree; hint file lands here
	MeshDir string
	Socket  string
	Repo    string // best-effort mesh note --repo
	MeshBin string // default "mesh"
	Log     *slog.Logger
	Now     func() time.Time
	Asker   StruggleAsker // nil → exec mesh ask/poll
}

// Observer tees stream-json lines and fires capped, async expert asks on
// struggle signals. Write is safe for concurrent use with Close.
type Observer struct {
	cfg ObserverConfig

	mu      sync.Mutex
	buf     []byte
	edits   map[string]int  // file_path → edit tool_use count
	tests   map[string]int  // failure fingerprint → count
	pending map[string]bool // tool_use_id → awaiting test tool_result
	asks    int
	lastAsk time.Time
	closed  bool
	cancel  context.CancelFunc
	ctx     context.Context
	wg      sync.WaitGroup
}

// NewObserver builds a stream observer. When cfg.Enabled is false, Write is a
// no-op sink (still safe to tee). The returned context is cancelled by Close.
func NewObserver(parent context.Context, cfg ObserverConfig) *Observer {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MeshBin == "" {
		cfg.MeshBin = "mesh"
	}
	if cfg.TestRepeat <= 0 {
		cfg.TestRepeat = config.DefaultStruggleTestRepeat
	}
	if cfg.EditRepeat <= 0 {
		cfg.EditRepeat = config.DefaultStruggleEditRepeat
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = config.DefaultStruggleCooldown
	}
	if cfg.MaxAsks <= 0 {
		cfg.MaxAsks = config.DefaultStruggleMaxAsks
	}
	if cfg.Role == "" {
		cfg.Role = defaultStruggleRole
	}
	if cfg.Asker == nil {
		cfg.Asker = meshAskPoll(cfg)
	}
	ctx, cancel := context.WithCancel(parent)
	return &Observer{
		cfg:     cfg,
		edits:   make(map[string]int),
		tests:   make(map[string]int),
		pending: make(map[string]bool),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// resolveStruggleRole picks StruggleRole, else ReviewRole, else "architect".
func resolveStruggleRole(cfg config.Config) string {
	if r := strings.TrimSpace(cfg.StruggleRole); r != "" {
		return r
	}
	if r := strings.TrimSpace(cfg.ReviewRole); r != "" {
		return r
	}
	return defaultStruggleRole
}

// Write implements io.Writer: split on newlines and feed complete NDJSON lines
// to the detector. Partial trailing bytes are buffered across calls.
func (o *Observer) Write(p []byte) (int, error) {
	if o == nil {
		return len(p), nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || !o.cfg.Enabled {
		return len(p), nil
	}
	o.buf = append(o.buf, p...)
	for {
		i := bytes.IndexByte(o.buf, '\n')
		if i < 0 {
			break
		}
		line := o.buf[:i]
		o.buf = o.buf[i+1:]
		o.handleLineLocked(bytes.TrimSpace(line))
	}
	return len(p), nil
}

// Close stops accepting new lines, cancels in-flight asks' poll loops, and
// waits for ask goroutines to exit. Best-effort; never required for
// correctness of Run (asks must not block the worker).
func (o *Observer) Close() {
	if o == nil {
		return
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.closed = true
	o.cancel()
	o.mu.Unlock()
	o.wg.Wait()
}

func (o *Observer) handleLineLocked(line []byte) {
	if len(line) == 0 || line[0] != '{' {
		return
	}
	var head struct {
		Type    string `json:"type"`
		Message struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(line, &head); err != nil {
		return
	}
	switch head.Type {
	case "assistant":
		o.handleAssistantContentLocked(head.Message.Content)
	case "user":
		o.handleUserContentLocked(head.Message.Content)
	}
}

type contentBlock struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
}

func (o *Observer) handleAssistantContentLocked(raw json.RawMessage) {
	blocks := decodeContentBlocks(raw)
	for _, b := range blocks {
		if b.Type != "tool_use" {
			continue
		}
		switch b.Name {
		case "Edit", "Write", "MultiEdit":
			path := toolFilePath(b.Input)
			if path == "" {
				continue
			}
			o.edits[path]++
			if o.edits[path] >= o.cfg.EditRepeat {
				o.tryFireLocked(signalEditLoop, fmt.Sprintf(
					"Worker is stuck in an edit loop on %s (edited %d times without resolving the issue). Advise a concrete next step — what to change, what to stop trying, or what to verify.",
					path, o.edits[path]))
			}
		case "Bash":
			cmd := toolCommand(b.Input)
			if cmd != "" && testCmdRe.MatchString(cmd) && b.ID != "" {
				o.pending[b.ID] = true
			}
		}
	}
}

func (o *Observer) handleUserContentLocked(raw json.RawMessage) {
	blocks := decodeContentBlocks(raw)
	for _, b := range blocks {
		if b.Type != "tool_result" || b.ToolUseID == "" {
			continue
		}
		if !o.pending[b.ToolUseID] {
			continue
		}
		delete(o.pending, b.ToolUseID)
		text := toolResultText(b.Content)
		fp := failureFingerprint(text)
		if fp == "" {
			continue
		}
		o.tests[fp]++
		if o.tests[fp] >= o.cfg.TestRepeat {
			o.tryFireLocked(signalTestLoop, fmt.Sprintf(
				"Worker is stuck in a test-failure loop (same failure fingerprint %s seen %d times). Advise how to fix the failing tests or what to try next.",
				fp, o.tests[fp]))
		}
	}
}

func (o *Observer) tryFireLocked(signal, question string) {
	now := o.cfg.Now()
	if o.asks >= o.cfg.MaxAsks {
		return
	}
	if !o.lastAsk.IsZero() && now.Sub(o.lastAsk) < o.cfg.Cooldown {
		return
	}
	o.asks++
	askN := o.asks
	o.lastAsk = now
	role := o.cfg.Role
	ctx := o.ctx
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		o.runAsk(ctx, role, signal, question, askN)
	}()
}

func (o *Observer) runAsk(ctx context.Context, role, signal, question string, askN int) {
	o.cfg.Log.Info("struggle nudge: asking expert",
		"signal", signal, "role", role, "ask", askN)
	ticket, answer, by, err := o.cfg.Asker(ctx, role, question)
	if err != nil {
		o.cfg.Log.Warn("struggle nudge: ask failed", "signal", signal, "err", err)
		return
	}
	if answer == "" {
		o.cfg.Log.Warn("struggle nudge: empty answer", "ticket", ticket, "signal", signal)
		return
	}
	hint := ExpertHint{Ticket: ticket, Signal: signal, Answer: answer, AnsweredBy: by}
	path := filepath.Join(o.cfg.WorkDir, expertHintFileName)
	data, err := json.Marshal(hint)
	if err != nil {
		o.cfg.Log.Warn("struggle nudge: marshal hint", "err", err)
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil { //nolint:gosec
		o.cfg.Log.Warn("struggle nudge: write hint file", "path", path, "err", err)
		return
	}
	o.cfg.Log.Info("struggle nudge: wrote expert hint",
		"path", path, "ticket", ticket, "signal", signal, "answeredBy", by)
	o.bestEffortNote(ctx, signal, ticket, answer)
}

func (o *Observer) bestEffortNote(ctx context.Context, signal, ticket, answer string) {
	summary := fmt.Sprintf("struggle %s (ticket %s): %s", signal, ticket, truncate(answer, 240))
	args := []string{"note", "--json"}
	if o.cfg.Repo != "" {
		args = append(args, "--repo", o.cfg.Repo)
	}
	if ticket != "" {
		args = append(args, "--ticket", ticket)
	}
	args = append(args, summary)
	cmd := exec.CommandContext(ctx, o.cfg.MeshBin, args...)
	cmd.Env = meshChildEnv(o.cfg)
	if out, err := cmd.CombinedOutput(); err != nil {
		o.cfg.Log.Debug("struggle nudge: mesh note failed", "err", err, "out", truncate(string(out), 200))
	}
}

// meshAskPoll returns the production Asker: mesh ask --json (no --wait), then
// poll the ticket until answered / ctx done / timeout.
func meshAskPoll(cfg ObserverConfig) StruggleAsker {
	return func(ctx context.Context, role, question string) (string, string, string, error) {
		ctx, cancel := context.WithTimeout(ctx, struggleAskTimeout)
		defer cancel()

		askCmd := exec.CommandContext(ctx, cfg.MeshBin, "ask", "--role", role, "--json", question)
		askCmd.Env = meshChildEnv(cfg)
		out, err := askCmd.Output()
		if err != nil {
			return "", "", "", fmt.Errorf("mesh ask: %w", err)
		}
		var askRes meshapi.AskVerbResult
		if err := json.Unmarshal(bytes.TrimSpace(out), &askRes); err != nil {
			return "", "", "", fmt.Errorf("mesh ask decode: %w", err)
		}
		if askRes.Ticket == "" {
			return "", "", "", fmt.Errorf("mesh ask: empty ticket")
		}

		for {
			if err := ctx.Err(); err != nil {
				return askRes.Ticket, "", "", err
			}
			pollCmd := exec.CommandContext(ctx, cfg.MeshBin, "poll", "--json", askRes.Ticket)
			pollCmd.Env = meshChildEnv(cfg)
			pout, perr := pollCmd.Output()
			// exit 3 (no answer yet) still prints JSON on stdout with --json.
			if len(bytes.TrimSpace(pout)) == 0 && perr != nil {
				return askRes.Ticket, "", "", fmt.Errorf("mesh poll: %w", perr)
			}
			var pollRes meshapi.PollResult
			if err := json.Unmarshal(bytes.TrimSpace(pout), &pollRes); err != nil {
				if perr != nil {
					return askRes.Ticket, "", "", fmt.Errorf("mesh poll: %w", perr)
				}
				return askRes.Ticket, "", "", fmt.Errorf("mesh poll decode: %w", err)
			}
			switch pollRes.Result {
			case envelope.AskAnswered:
				return pollRes.Ticket, pollRes.Answer, pollRes.AnsweredBy, nil
			case envelope.AskNoSuchTicket:
				return askRes.Ticket, "", "", fmt.Errorf("mesh poll: no such ticket")
			case envelope.AskPending:
				select {
				case <-ctx.Done():
					return askRes.Ticket, "", "", ctx.Err()
				case <-time.After(struggleAskPollGap):
				}
			default:
				if pollRes.Answer != "" {
					return pollRes.Ticket, pollRes.Answer, pollRes.AnsweredBy, nil
				}
				select {
				case <-ctx.Done():
					return askRes.Ticket, "", "", ctx.Err()
				case <-time.After(struggleAskPollGap):
				}
			}
		}
	}
}

func meshChildEnv(cfg ObserverConfig) []string {
	env := os.Environ()
	if cfg.MeshDir != "" {
		env = append(env, config.EnvMeshDir+"="+cfg.MeshDir)
	}
	if cfg.Socket != "" {
		env = append(env, config.EnvAgentSocket+"="+cfg.Socket)
	}
	return env
}

func decodeContentBlocks(raw json.RawMessage) []contentBlock {
	if len(raw) == 0 {
		return nil
	}
	// content is usually an array of blocks; sometimes a plain string.
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return nil
		}
		return nil // plain string user message — not a tool_result
	}
	var blocks []contentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil
	}
	return blocks
}

func toolFilePath(input json.RawMessage) string {
	if len(input) == 0 {
		return ""
	}
	var in struct {
		FilePath string `json:"file_path"`
	}
	if json.Unmarshal(input, &in) != nil {
		return ""
	}
	return strings.TrimSpace(in.FilePath)
}

func toolCommand(input json.RawMessage) string {
	if len(input) == 0 {
		return ""
	}
	var in struct {
		Command string `json:"command"`
	}
	if json.Unmarshal(input, &in) != nil {
		return ""
	}
	return strings.TrimSpace(in.Command)
}

func toolResultText(content json.RawMessage) string {
	if len(content) == 0 {
		return ""
	}
	content = bytes.TrimSpace(content)
	if len(content) == 0 {
		return ""
	}
	switch content[0] {
	case '"':
		var s string
		if json.Unmarshal(content, &s) == nil {
			return s
		}
	case '[':
		// content can be an array of {type:text,text:...} blocks
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(content, &parts) == nil {
			var b strings.Builder
			for _, p := range parts {
				if p.Text != "" {
					b.WriteString(p.Text)
					b.WriteByte('\n')
				}
			}
			return b.String()
		}
	}
	return string(content)
}

// failureFingerprint hashes FAIL:/Error:/failed lines, or trimmed content when
// it looks like a failure. Empty string means "not a failure" (success or
// uninformative output) — those must not count toward test_loop.
func failureFingerprint(content string) string {
	content = strings.TrimSpace(content)
	if content == "" {
		return ""
	}
	var failLines []string
	sc := bufio.NewScanner(strings.NewReader(content))
	sc.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if failureLineRe.MatchString(line) {
			failLines = append(failLines, strings.TrimSpace(line))
		}
	}
	if len(failLines) > 0 {
		sum := sha256.Sum256([]byte(strings.Join(failLines, "\n")))
		return hex.EncodeToString(sum[:16])
	}
	if looksLikeFailure(content) {
		sum := sha256.Sum256([]byte(content))
		return hex.EncodeToString(sum[:16])
	}
	return ""
}

func looksLikeFailure(s string) bool {
	lower := strings.ToLower(s)
	switch {
	case strings.Contains(lower, "exit status"),
		strings.Contains(lower, "exit code"),
		strings.Contains(lower, "non-zero"),
		strings.Contains(lower, "assertionerror"),
		strings.Contains(lower, "panic:"):
		return true
	}
	// Avoid treating ordinary success chatter as failure: require a clear
	// failure token and no dominant "PASS"/"ok" alone.
	if strings.Contains(lower, "error") || strings.Contains(lower, "fail") {
		if strings.Contains(lower, "\npass\n") || strings.HasPrefix(lower, "pass\n") {
			return false
		}
		return true
	}
	return false
}

// readExpertHint loads <dir>/.mesh_expert_hint when present and non-empty.
func readExpertHint(dir string) (ExpertHint, bool) {
	data, err := os.ReadFile(filepath.Join(dir, expertHintFileName))
	if err != nil || len(bytes.TrimSpace(data)) == 0 {
		return ExpertHint{}, false
	}
	var h ExpertHint
	if json.Unmarshal(data, &h) != nil || strings.TrimSpace(h.Answer) == "" {
		return ExpertHint{}, false
	}
	return h, true
}

// Ensure Observer is an io.Writer.
var _ io.Writer = (*Observer)(nil)
