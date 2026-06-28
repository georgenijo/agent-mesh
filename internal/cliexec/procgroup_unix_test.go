//go:build !windows

package cliexec_test

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/cliexec"
)

// TestClaudeAdapterInvokeKillsProcessGroup is the regression gate for #122: a
// cancelled/timed-out invocation must reap the child's WHOLE process tree, not
// just the direct child. The fake CLI spawns a long-lived grandchild (the
// stand-in for claude's node/MCP subprocesses) and records its pid; after we
// cancel, that grandchild must be gone. Pre-fix it survived as an orphan and,
// across a long run, those orphans exhausted account session capacity.
func TestClaudeAdapterInvokeKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	pidfile := filepath.Join(dir, "grandchild.pid")
	script := filepath.Join(dir, "fake-cli")
	// Spawn a grandchild that sleeps, record its pid, then block so the direct
	// child stays alive too (forcing the cancel path, not a natural exit).
	body := "#!/bin/sh\nsleep 999 &\necho $! > " + pidfile + "\nwait\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write fake CLI: %v", err)
	}

	a := cliexec.ClaudeAdapter{Binary: script}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _ = a.Invoke(ctx, "go", cliexec.InvokeOptions{WaitDelay: 500 * time.Millisecond})
		close(done)
	}()

	gpid := waitForPid(t, pidfile)
	if !processAlive(gpid) {
		t.Fatalf("grandchild %d never came up", gpid)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Invoke did not return after cancel")
	}

	// The group kill is delivered to the grandchild asynchronously; poll briefly.
	deadline := time.Now().Add(3 * time.Second)
	for processAlive(gpid) {
		if time.Now().After(deadline) {
			t.Fatalf("grandchild %d survived cancellation — process group not reaped (#122)", gpid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForPid(t *testing.T, pidfile string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(pidfile)
		if err == nil {
			var pid int
			if _, perr := fscanPid(b, &pid); perr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("grandchild pid file never written")
	return 0
}

// processAlive reports whether pid exists (signal 0 probes without delivering).
func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// fscanPid parses a trailing-newline decimal pid without pulling in fmt.Sscan's
// whitespace quirks across shells.
func fscanPid(b []byte, out *int) (int, error) {
	n := 0
	v := 0
	for _, c := range b {
		if c >= '0' && c <= '9' {
			v = v*10 + int(c-'0')
			n++
		} else if n > 0 {
			break
		}
	}
	*out = v
	if n == 0 {
		return 0, errNoPid
	}
	return n, nil
}

var errNoPid = &pidErr{}

type pidErr struct{}

func (*pidErr) Error() string { return "no pid digits" }
