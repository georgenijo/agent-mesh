package worker

// Worker-driver contract tests against REAL child processes and REAL git
// worktrees — no LLM, no API key. The worker CLI is a shell script speaking
// the documented one-shot result contract; the repo is a throwaway local git
// checkout. Under test: worktree isolation (distinct trees, contained edits),
// the typed result mapping, mesh env plumbing into the child, commit/diff
// capture, and the deterministic teardown retention policy.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/agentcard"
	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/scheduler"
	"github.com/georgenijo/agent-mesh/internal/sidecar"
	"github.com/georgenijo/agent-mesh/internal/task"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

// sidecarSession adapts a real internal/sidecar.Sidecar to the Session seam —
// the same adapter cmd/meshd wires in production. Importing sidecar HERE is
// legal (sidecar does not import worker); only the coordinator package is
// barred from it.
type sidecarSession struct{ *sidecar.Sidecar }

func (s sidecarSession) BuildPrimer(repo string, budget int) (string, error) {
	p, err := s.Sidecar.BuildMemoryPrimer(repo, budget)
	if err != nil {
		return "", err
	}
	return p.Text, nil
}

// sidecarJoin is the test JoinFunc: a real per-worker sidecar over the
// fixture's bus.
func sidecarJoin(cfg config.Config) JoinFunc {
	return func(card agentcard.Card) (Session, error) {
		sc, err := sidecar.New(cfg, card, nil)
		if err != nil {
			return nil, err
		}
		if err := sc.Start(); err != nil {
			return nil, err
		}
		return sidecarSession{sc}, nil
	}
}

const successResult = `{"type":"result","subtype":"success","is_error":false,` +
	`"result":"did the work","session_id":"s1","num_turns":1,"duration_ms":1,"total_cost_usd":0.001}`

// workerScript writes a fake worker shell script. body runs before the result
// line is emitted (e.g. file edits, env assertions); a non-zero exit from body
// aborts without emitting a result.
func workerScript(t *testing.T, body, payload string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell script worker stub")
	}
	path := filepath.Join(t.TempDir(), "fakeworker.sh")
	script := "#!/bin/sh\n" + body + "\ncat <<'WORKER_EOF'\n" + payload + "\nWORKER_EOF\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// initRepo creates a git repo with one commit at dir.
func initRepo(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-q")
	run("config", "user.name", "test")
	run("config", "user.email", "test@localhost")
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "-A")
	run("commit", "-q", "--no-gpg-sign", "-m", "init")
}

type fixture struct {
	cfg      config.Config
	cli      *bus.Client
	repoPath string
	jobRec   job.Record
}

// newFixture stands up a bus server at the mesh dir, a git repo under
// ReposDir, and one persisted job pointing at it.
func newFixture(t *testing.T, workerCLI string) *fixture {
	t.Helper()
	dir := testsock.Dir(t)
	reposDir := filepath.Join(dir, "repos")
	cfg := config.Config{
		MeshDir:           dir,
		HeartbeatInterval: 50 * time.Millisecond,
		AwayAfter:         150 * time.Millisecond,
		EvictAfter:        400 * time.Millisecond,
		RegistrationGrace: 100 * time.Millisecond,
		ClaimTTL:          2 * time.Second,
		WorkerCLI:         workerCLI,
		WorkerTimeout:     10 * time.Second,
		ReposDir:          reposDir,
		KeepWorktrees:     config.KeepWorktreesOnFailure,
	}
	srv := bus.NewServer(cfg.BusSocket(), bus.Options{})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	cli, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cli.Close()
		srv.Stop()
	})

	repoPath := filepath.Join(reposDir, "demo")
	initRepo(t, repoPath)
	jrec, err := job.NewStore(cli).Create(job.Record{Repo: "demo", Source: job.SourceManual, Title: "do X"})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{cfg: cfg, cli: cli, repoPath: repoPath, jobRec: jrec}
}

func (f *fixture) driver(t *testing.T) *Driver {
	t.Helper()
	d, err := NewDriver(f.cli, f.cfg, sidecarJoin(f.cfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func (f *fixture) task(role string) task.Record {
	return task.Record{
		ID: envelope.NewID(), Job: f.jobRec.ID, Node: "impl",
		Title: "implement the change", Role: role, State: envelope.TaskPending,
	}
}

// gitInRepo runs git against the fixture repo and returns trimmed stdout.
func (f *fixture) gitInRepo(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", f.repoPath}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestNewDriverValidation(t *testing.T) {
	f := newFixture(t, workerScript(t, "", successResult))

	join := sidecarJoin(f.cfg)
	noRepos := f.cfg
	noRepos.ReposDir = ""
	if _, err := NewDriver(f.cli, noRepos, join, nil); err == nil {
		t.Fatal("NewDriver accepted an empty ReposDir")
	}
	noCLI := f.cfg
	noCLI.WorkerCLI = ""
	if _, err := NewDriver(f.cli, noCLI, join, nil); err == nil {
		t.Fatal("NewDriver accepted an empty WorkerCLI")
	}
	if _, err := NewDriver(nil, f.cfg, join, nil); err == nil {
		t.Fatal("NewDriver accepted a nil bus client")
	}
	if _, err := NewDriver(f.cli, f.cfg, nil, nil); err == nil {
		t.Fatal("NewDriver accepted a nil JoinFunc")
	}
}

func TestSpawnRejectsUnknownJobAndBadRepos(t *testing.T) {
	f := newFixture(t, workerScript(t, "", successResult))
	d := f.driver(t)
	ctx := context.Background()

	orphan := f.task("builder")
	orphan.Job = envelope.NewID()
	if _, err := d.Spawn(ctx, orphan); err == nil {
		t.Fatal("Spawn accepted a task whose job does not exist")
	}

	for _, repo := range []string{"missing", "..", "a/b"} {
		jrec, err := job.NewStore(f.cli).Create(job.Record{Repo: repo, Source: job.SourceManual, Title: "x"})
		if err != nil {
			t.Fatal(err)
		}
		rec := f.task("builder")
		rec.Job = jrec.ID
		if _, err := d.Spawn(ctx, rec); err == nil {
			t.Fatalf("Spawn accepted unresolvable repo %q", repo)
		}
	}

	// Failed spawns must not leak worktrees.
	if got := f.gitInRepo(t, "worktree", "list"); strings.Contains(got, "workers") {
		t.Fatalf("failed spawns leaked worktrees:\n%s", got)
	}
}

func TestParallelWorkersAreIsolatedAndTyped(t *testing.T) {
	// Each worker requires a live mesh socket, edits a file named after its
	// own pid in its cwd, and reports a typed success.
	script := workerScript(t,
		`[ -S "$MESH_SOCKET" ] || { echo "MESH_SOCKET is not a live socket" >&2; exit 1; }
[ -n "$MESH_DIR" ] || { echo "MESH_DIR unset" >&2; exit 1; }
echo work > "marker-$$.txt"`,
		successResult)
	f := newFixture(t, script)
	d := f.driver(t)
	ctx := context.Background()

	recs := []task.Record{f.task("builder"), f.task("builder")}
	workers := make([]scheduler.Worker, len(recs))
	for i, rec := range recs {
		w, err := d.Spawn(ctx, rec)
		if err != nil {
			t.Fatalf("spawn %d: %v", i, err)
		}
		workers[i] = w
	}
	dirs := make([]string, len(workers))
	for i, w := range workers {
		dirs[i] = w.(*worker).dir
	}
	if dirs[0] == dirs[1] {
		t.Fatalf("both workers share one worktree: %s", dirs[0])
	}

	var wg sync.WaitGroup
	results := make([]scheduler.Result, len(workers))
	errs := make([]error, len(workers))
	for i, w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = w.Run(ctx)
		}()
	}
	wg.Wait()

	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("run %d: %v", i, errs[i])
		}
		if !results[i].Succeeded() {
			t.Fatalf("run %d: code=%q summary=%q", i, results[i].Code, results[i].Summary)
		}
		if results[i].CostUSD != 0.001 || results[i].SessionID != "s1" {
			t.Fatalf("run %d: cost/session = %v/%q", i, results[i].CostUSD, results[i].SessionID)
		}
	}

	// Edits are contained: exactly one marker per worktree, none in the repo.
	for i, dir := range dirs {
		markers, err := filepath.Glob(filepath.Join(dir, "marker-*.txt"))
		if err != nil || len(markers) != 1 {
			t.Fatalf("worktree %d markers = %v (err %v), want exactly 1", i, markers, err)
		}
	}
	if leaked, _ := filepath.Glob(filepath.Join(f.repoPath, "marker-*.txt")); len(leaked) != 0 {
		t.Fatalf("worker edits leaked into the main repo tree: %v", leaked)
	}

	// The diff was committed onto each task branch and described.
	for i, w := range workers {
		wk := w.(*worker)
		head := f.gitInRepo(t, "rev-parse", wk.branch)
		if head == wk.baseSHA {
			t.Fatalf("worker %d: no commit landed on %s", i, wk.branch)
		}
		if !strings.Contains(results[i].Summary, "changed files (1):") {
			t.Fatalf("worker %d summary lacks diff metadata:\n%s", i, results[i].Summary)
		}
	}

	for i, w := range workers {
		if err := w.Teardown(); err != nil {
			t.Fatalf("teardown %d: %v", i, err)
		}
		// Default policy removes a successful worker's worktree; the branch
		// (the work product) survives.
		if _, err := os.Stat(dirs[i]); !os.IsNotExist(err) {
			t.Fatalf("worktree %d not removed after success: %v", i, err)
		}
		f.gitInRepo(t, "rev-parse", "--verify", w.(*worker).branch)
	}
}

func TestRunMapsTypedFailures(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    envelope.WorkerErrorCode
	}{
		{"error result", `{"type":"result","subtype":"error_during_execution","is_error":true,` +
			`"result":"","session_id":"s1","total_cost_usd":0.002}`, envelope.WorkerFailed},
		{"api error", `{"type":"result","subtype":"success","is_error":false,` +
			`"result":"x","session_id":"s1","api_error_status":429}`, envelope.WorkerRateLimited},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, workerScript(t, "", tc.payload))
			d := f.driver(t)
			w, err := d.Spawn(context.Background(), f.task("builder"))
			if err != nil {
				t.Fatal(err)
			}
			res, err := w.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if res.Code != tc.want {
				t.Fatalf("code = %q, want %q", res.Code, tc.want)
			}
			dir := w.(*worker).dir
			if err := w.Teardown(); err != nil {
				t.Fatal(err)
			}
			// Default policy preserves a non-success worktree for inspection.
			if _, err := os.Stat(dir); err != nil {
				t.Fatalf("failed worker's worktree not preserved: %v", err)
			}
		})
	}
}

func TestRunRejectsNonResultStdout(t *testing.T) {
	f := newFixture(t, workerScript(t, "", `{"type":"assistant","subtype":"message"}`))
	d := f.driver(t)
	w, err := d.Spawn(context.Background(), f.task("builder"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Teardown() //nolint:errcheck
	if _, err := w.Run(context.Background()); err == nil {
		t.Fatal("Run accepted stdout without a result envelope")
	}
}

func TestRunChildExitFailureIsAnError(t *testing.T) {
	f := newFixture(t, workerScript(t, "exit 3", successResult))
	d := f.driver(t)
	w, err := d.Spawn(context.Background(), f.task("builder"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Teardown() //nolint:errcheck
	if _, err := w.Run(context.Background()); err == nil {
		t.Fatal("Run accepted a crashed child")
	}
}

func TestTeardownPolicyOverrides(t *testing.T) {
	t.Run("always keeps a successful worktree", func(t *testing.T) {
		f := newFixture(t, workerScript(t, `echo x > kept.txt`, successResult))
		f.cfg.KeepWorktrees = config.KeepWorktreesAlways
		d := f.driver(t)
		w, err := d.Spawn(context.Background(), f.task("builder"))
		if err != nil {
			t.Fatal(err)
		}
		if res, err := w.Run(context.Background()); err != nil || !res.Succeeded() {
			t.Fatalf("run: %v %+v", err, res)
		}
		if err := w.Teardown(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(w.(*worker).dir); err != nil {
			t.Fatalf("always-policy worktree removed: %v", err)
		}
	})
	t.Run("never removes even a failed worktree", func(t *testing.T) {
		payload := `{"type":"result","subtype":"error_during_execution","is_error":true,"result":"","session_id":"s1"}`
		f := newFixture(t, workerScript(t, "", payload))
		f.cfg.KeepWorktrees = config.KeepWorktreesNever
		d := f.driver(t)
		w, err := d.Spawn(context.Background(), f.task("builder"))
		if err != nil {
			t.Fatal(err)
		}
		if res, err := w.Run(context.Background()); err != nil || res.Code != envelope.WorkerFailed {
			t.Fatalf("run: %v %+v", err, res)
		}
		if err := w.Teardown(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(w.(*worker).dir); !os.IsNotExist(err) {
			t.Fatalf("never-policy worktree kept: %v", err)
		}
	})
}

func TestReDispatchAfterPreservedFailureGetsAFreshWorktree(t *testing.T) {
	payload := `{"type":"result","subtype":"error_during_execution","is_error":true,"result":"","session_id":"s1"}`
	f := newFixture(t, workerScript(t, "", payload))
	d := f.driver(t)
	rec := f.task("builder")

	w1, err := d.Spawn(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w1.Teardown(); err != nil {
		t.Fatal(err)
	}

	// Same task id again (rate-limit retry / next coordinator lifetime): the
	// preserved worktree must not be clobbered.
	w2, err := d.Spawn(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Teardown() //nolint:errcheck
	if w1.(*worker).dir == w2.(*worker).dir {
		t.Fatalf("re-dispatch reused the preserved worktree %s", w2.(*worker).dir)
	}
	if _, err := os.Stat(w1.(*worker).dir); err != nil {
		t.Fatalf("first attempt's preserved worktree disappeared: %v", err)
	}
}

func TestWorkerPromptCarriesTaskAndMeshInstructions(t *testing.T) {
	// The child dumps its last argv (the prompt) to a file; the test asserts
	// the injected sections — task, isolation context, mesh CLI usage.
	f := newFixture(t, "")
	promptFile := filepath.Join(f.cfg.MeshDir, "prompt.txt")
	script := workerScript(t,
		fmt.Sprintf(`for last; do :; done
printf '%%s' "$last" > %q`, promptFile),
		successResult)
	f.cfg.WorkerCLI = script
	d := f.driver(t)

	rec := f.task("builder")
	rec.Description = "swap the frobnicator"
	rec.Files = []string{"src/x.go"}
	rec.Acceptance = []string{"unit tests pass"}
	w, err := d.Spawn(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Teardown() //nolint:errcheck
	if res, err := w.Run(context.Background()); err != nil || !res.Succeeded() {
		t.Fatalf("run: %v %+v", err, res)
	}
	prompt, err := os.ReadFile(promptFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"builder worker agent",
		"Task: implement the change",
		"Description: swap the frobnicator",
		"Files in scope: src/x.go",
		"unit tests pass",
		"ISOLATED git worktree",
		"mesh claim",
		"mesh context",
		"mesh ask --role",
	} {
		if !strings.Contains(string(prompt), want) {
			t.Errorf("prompt lacks %q:\n%s", want, prompt)
		}
	}
}

func TestWorkerNameIsShortAndValid(t *testing.T) {
	id := envelope.NewID()
	name := workerName(id)
	if len(name) > 20 {
		t.Fatalf("worker name %q too long", name)
	}
	if !strings.HasPrefix(name, "w-") {
		t.Fatalf("worker name %q lacks prefix", name)
	}
}
